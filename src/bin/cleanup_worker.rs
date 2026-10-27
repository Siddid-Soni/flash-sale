//! Cleanup worker: releases holds whose time is up, and restores a
//! product's counters if Redis lost them. Any number can run at once: the claim
//! script hands each expired hold to exactly one worker.
//!
//! Usage: `cargo run --bin cleanup_worker`

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use chrono::Utc;
use uuid::Uuid;

use flash_sale::{
    actions,
    cache::{self, RebuildInput},
    config::Config,
    db::{self, DbPool},
};

// Holds released per claim; a full batch is followed at once by another
const BATCH: usize = 500;
const IDLE_WAIT: Duration = Duration::from_millis(500);
const ERROR_RETRY: Duration = Duration::from_secs(1);
// How often to look for products whose counters are missing
const REBUILD_EVERY: Duration = Duration::from_secs(10);
// How often to expire orders a crashed worker left PAYMENT_PENDING, and how
// long after its hold ended an order must be to count as left behind
const SWEEP_EVERY: Duration = Duration::from_secs(60);
const SWEEP_AFTER: chrono::Duration = chrono::Duration::minutes(10);

type BoxError = Box<dyn std::error::Error + Send + Sync>;

fn main() {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));
    let config = Config::from_env();
    let pool = db::init_pool(&config);
    let client = redis::Client::open(config.redis_url.as_str()).expect("REDIS_URL should be valid");
    let mut redis: Option<redis::Connection> = None;
    // Orders to mark EXPIRED, kept until Postgres accepts them
    let mut unmarked: Vec<Uuid> = Vec::new();
    let (mut last_rebuild, mut last_sweep) = (None::<Instant>, None::<Instant>);
    log::info!("Releasing expired holds");

    loop {
        let con = match redis.as_mut() {
            Some(con) => con,
            None => match client.get_connection() {
                Ok(con) => redis.insert(con),
                Err(e) => {
                    log::error!("Connecting to Redis failed: {e}");
                    std::thread::sleep(ERROR_RETRY);
                    continue;
                }
            },
        };

        if last_rebuild.is_none_or(|at| at.elapsed() >= REBUILD_EVERY) {
            last_rebuild = Some(Instant::now());
            if let Err(e) = rebuild_missing(&pool, con) {
                log::error!("Checking for lost counters failed: {e}");
            }
        }
        if last_sweep.is_none_or(|at| at.elapsed() >= SWEEP_EVERY) {
            last_sweep = Some(Instant::now());
            match pool.get().map_err(BoxError::from).and_then(|mut conn| {
                Ok(actions::orders::expire_stale(&mut conn, Utc::now() - SWEEP_AFTER)?)
            }) {
                Ok(0) => {}
                Ok(n) => log::warn!("Expired {n} orders left PAYMENT_PENDING after their hold ended"),
                Err(e) => log::error!("Sweeping stale orders failed: {e}"),
            }
        }

        let full = match cache::claim_expired(con, BATCH) {
            Ok((scanned, released)) => {
                if !released.is_empty() {
                    log::info!("Released {} expired holds", released.len());
                }
                unmarked.extend(
                    released.iter().filter(|hold| hold.state == "paying").filter_map(|hold| hold.reservation_id.parse::<Uuid>().ok()),
                );
                scanned == BATCH
            }
            Err(e) => {
                log::error!("Claiming expired holds failed: {e}");
                redis = None;
                std::thread::sleep(ERROR_RETRY);
                continue;
            }
        };

        // The units are back in stock in Redis already; the orders follow
        if !unmarked.is_empty() {
            match pool.get().map_err(BoxError::from).and_then(|mut conn| {
                Ok(actions::orders::expire_payment_pending(&mut conn, &unmarked)?)
            }) {
                Ok(n) => {
                    if n > 0 {
                        log::info!("Marked {n} orders EXPIRED");
                    }
                    unmarked.clear();
                }
                Err(e) => {
                    log::error!("Marking {} orders EXPIRED failed, will retry: {e}", unmarked.len());
                    std::thread::sleep(ERROR_RETRY);
                    continue;
                }
            }
        }

        if !full {
            std::thread::sleep(IDLE_WAIT);
        }
    }
}

/// Rebuilds every live product whose counters are missing from Redis (lost
/// data, or Redis restarted without its AOF). Cheap when nothing is missing:
/// one query and one pipelined `EXISTS` per product.
fn rebuild_missing(pool: &DbPool, redis: &mut redis::Connection) -> Result<(), BoxError> {
    let mut conn = pool.get()?;
    let products = actions::products::live_products(&mut conn)?;
    if products.is_empty() {
        return Ok(());
    }
    let mut pipe = redis::pipe();
    for product in &products {
        pipe.exists(cache::stock_total_key(product.id));
    }
    let present: Vec<bool> = pipe.query(redis)?;

    for (product, _) in products.iter().zip(present).filter(|(_, present)| !present) {
        // One worker rebuilds; the script would refuse a second run anyway
        let locked: Option<String> =
            redis::cmd("SET").arg(format!("rebuild_lock:{}", product.id)).arg(1).arg("NX").arg("PX").arg(30_000).query(redis)?;
        if locked.is_none() {
            continue;
        }
        // Holds are scanned first, then Postgres is read in one snapshot: the
        // sold units, the paid units per user, and which scanned holds have
        // already committed (those are in the sold count; counting them as
        // holds too would take their units twice, and nothing would give
        // them back). A hold that commits after the snapshot is not yet in
        // the sold count, and still counts as a hold.
        let scanned = cache::live_holds(redis, product.id)?;
        let ids: Vec<Uuid> = scanned.iter().filter_map(|id| id.parse().ok()).collect();
        let (stock_total, sold, max_per_user, user_units, committed) =
            conn.build_transaction().repeatable_read().run::<_, BoxError, _>(|conn| {
                let (stock_total, sold, max_per_user) = actions::products::inventory_state(conn, product.id)?;
                let user_units: HashMap<String, i64> = actions::orders::paid_units_by_user(conn, product.id)?
                    .into_iter()
                    .map(|(user, count)| (user.to_string(), count))
                    .collect();
                let committed: HashSet<String> =
                    actions::orders::confirmed_among(conn, &ids)?.into_iter().map(|id| id.to_string()).collect();
                Ok((stock_total, sold, max_per_user, user_units, committed))
            })?;
        let input = RebuildInput {
            stock_total,
            sold,
            max_per_user,
            sale_opens_at: product.sale_opens_at,
            closed: product.status == "CLOSED",
            on_sale: product.status != "SCHEDULED",
            holds: scanned.into_iter().filter(|id| !committed.contains(id)).collect(),
            user_units,
        };
        if cache::rebuild_product(redis, product.id, &input)? {
            log::warn!(
                "Rebuilt the counters of product {} from Postgres: {} units, {} sold, {} live holds",
                product.id,
                stock_total,
                sold,
                input.holds.len()
            );
        }
    }
    Ok(())
}
