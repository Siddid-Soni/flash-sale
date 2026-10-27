//! Payment worker: processes provider events from `payment_inbox`. Several
//! workers can run at once; each locks one row at a time with `SKIP LOCKED`.
//!
//! A successful payment is confirmed in three phases so that a crash at any
//! point is retried safely:
//! 1. claim the hold in Redis (`confirm.lua` records this payment as its
//!    winner, so a retry of the same payment claims it again). Normally this
//!    already happened when the event was received (`payment::record_event`),
//!    so however far behind this worker is, a payment that arrived before its
//!    hold ended is confirmed; here it finds its own claim,
//! 2. in one Postgres transaction, sell the units, confirm the order and
//!    mark the event processed,
//! 3. after the commit, delete the hold and count the units sold
//!    (`finalize.lua`).
//!
//! A payment for a hold that expired before the payment arrived is refunded.
//!
//! Usage: `cargo run --bin payment_worker`

use std::time::Duration;

use diesel::prelude::*;
use uuid::Uuid;

use flash_sale::{
    actions,
    cache::{self, ClaimOutcome},
    config::Config,
    db::{self, DbPool},
    models::{OrderStatus, PaymentInboxEntry},
    payment::{self, parse_event, SharedProvider, PAYMENT_SUCCEEDED},
};

// Wait when the inbox is empty, and back-off after an error
const IDLE_WAIT: Duration = Duration::from_millis(500);
const ERROR_RETRY: Duration = Duration::from_secs(1);

#[derive(Debug, thiserror::Error)]
enum WorkerError {
    #[error("database: {0}")]
    Db(#[from] diesel::result::Error),
    #[error("database pool: {0}")]
    Pool(#[from] diesel::r2d2::PoolError),
    #[error("redis: {0}")]
    Redis(#[from] redis::RedisError),
    #[error("{0}")]
    Payment(#[from] payment::PaymentError),
}

/// What to do in Redis once the transaction has committed
enum Finalize {
    Nothing,
    Sold(Uuid),
    Refunded(Uuid),
}

fn main() {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));
    let config = Config::from_env();
    let pool = db::init_pool(&config);
    let provider = payment::from_config(&config, pool.clone());
    let client = redis::Client::open(config.redis_url.as_str()).expect("REDIS_URL should be valid");
    let mut redis: Option<redis::Connection> = None;
    log::info!("Processing payment events");

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
        match process_next(&pool, con, &provider) {
            Ok(true) => {}
            Ok(false) => std::thread::sleep(IDLE_WAIT),
            Err(e) => {
                log::error!("Processing a payment event failed: {e}");
                if matches!(e, WorkerError::Redis(_)) {
                    redis = None;
                }
                std::thread::sleep(ERROR_RETRY);
            }
        }
    }
}

/// Processes the oldest unlocked event. Returns false when there was none.
fn process_next(pool: &DbPool, redis: &mut redis::Connection, provider: &SharedProvider) -> Result<bool, WorkerError> {
    let mut conn = pool.get()?;
    let mut current: Option<i64> = None;
    let outcome = conn.transaction(|conn| {
        let Some(entry) = actions::payments::claim_next(conn)? else {
            return Ok(None);
        };
        current = Some(entry.id);
        let finalize = handle(conn, redis, provider, &entry)?;
        actions::payments::mark_processed(conn, entry.id)?;
        Ok::<_, WorkerError>(Some(finalize))
    });

    match outcome {
        Ok(None) => Ok(false),
        Ok(Some(finalize)) => {
            // Phase 3. If this fails the hold expires on its own TTL, and the
            // startup rebuild recomputes `sold` from Postgres
            let finalized = match finalize {
                Finalize::Nothing => Ok(true),
                Finalize::Sold(id) => cache::finalize_hold(redis, &id.to_string(), true),
                Finalize::Refunded(id) => cache::finalize_hold(redis, &id.to_string(), false),
            };
            if let Err(e) = finalized {
                log::warn!("Finalizing in Redis failed (the hold will expire on its own): {e}");
            }
            Ok(true)
        }
        Err(e) => {
            // Rolled back; the event stays unprocessed and is retried
            if let Some(id) = current {
                let recorded = actions::payments::record_failure(&mut conn, id, &e.to_string());
                if let Err(record_error) = recorded {
                    log::error!("Recording the failure of event {id} failed: {record_error}");
                }
            }
            Err(e)
        }
    }
}

/// Phases 1 and 2 for one event, inside the transaction that marks it processed.
fn handle(
    conn: &mut PgConnection,
    redis: &mut redis::Connection,
    provider: &SharedProvider,
    entry: &PaymentInboxEntry,
) -> Result<Finalize, WorkerError> {
    if entry.event_type != PAYMENT_SUCCEEDED {
        // A failed payment changes nothing: the hold runs out and expires
        log::info!("Event {} ({}) needs no action", entry.provider_event_id, entry.event_type);
        return Ok(Finalize::Nothing);
    }
    let Some(event) = parse_event(&entry.payload) else {
        log::warn!("Event {} has no intent or reservation id; ignored", entry.provider_event_id);
        return Ok(Finalize::Nothing);
    };
    let resv = event.reservation_id;

    let order = actions::orders::find_order(conn, resv).optional()?;
    let Some(order) = order else {
        // Intents are created after their order, so this is not ours to keep
        log::warn!("Payment {} has no order {resv}; refunding", event.intent_id);
        provider.refund(&event.intent_id)?;
        return Ok(Finalize::Nothing);
    };
    if matches!(order.status, OrderStatus::Confirmed | OrderStatus::Refunded) {
        log::info!("Order {resv} is already {:?}; duplicate event {}", order.status, entry.provider_event_id);
        return Ok(Finalize::Nothing);
    }

    match cache::claim_hold(redis, &resv.to_string(), &event.intent_id)? {
        ClaimOutcome::Won(hold) => {
            if actions::orders::confirm_order(conn, resv, hold.product, hold.quantity)? {
                log::info!("Order {resv} is CONFIRMED ({} units)", hold.quantity);
                Ok(Finalize::Sold(resv))
            } else {
                // Postgres has fewer units in stock: Redis and Postgres disagreed
                log::error!("Order {resv}: Postgres has fewer than {} units of product {} in stock; refunding", hold.quantity, hold.product);
                provider.refund(&event.intent_id)?;
                actions::orders::set_status(conn, resv, OrderStatus::Refunded)?;
                Ok(Finalize::Refunded(resv))
            }
        }
        ClaimOutcome::Gone => {
            log::info!("Order {resv}: the hold expired before payment {} arrived; refunding", event.intent_id);
            provider.refund(&event.intent_id)?;
            actions::orders::set_status(conn, resv, OrderStatus::Refunded)?;
            Ok(Finalize::Nothing)
        }
    }
}
