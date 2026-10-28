//! Expiration worker: settles the holds whose payment deadline has passed
//! (`holds:expiry`): PostgreSQL says which were paid and which were not, the
//! unpaid ones are released and the paid ones closed (sales are counted only in PostgreSQL). Also
//! releases leaked holds (`holds:pending` behind processor progress), the only
//! other place stock is returned besides immediate compensation, and sweeps up
//! orders a crashed worker left behind. The logic is in `flash_sale::expiry`;
//! this is the loop. Any number can run at once: the scripts hand each hold to
//! one worker.
//!
//! The sales to watch are read from PostgreSQL every few seconds. After an
//! error in Redis, Kafka or PostgreSQL the worker backs off (0.5 s doubling
//! to 10 s) and tries again; the clients reconnect on their own. Each outage
//! is logged once when it starts and once when it ends.
//!
//! Stops on SIGINT or SIGTERM, after the pass in hand.
//!
//! Usage: `cargo run --bin expiration_worker`

use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use tokio::{signal::unix, sync::watch};

use flash_sale::{
    cache,
    config::Config,
    db,
    expiry::{Expirer, KafkaLag, Outages, Pass, Settings},
    payment,
};

/// How often the list of sales to watch is read from PostgreSQL
const SALES_REFRESH: Duration = Duration::from_secs(10);
const BACKOFF_MIN: Duration = Duration::from_millis(500);
const BACKOFF_MAX: Duration = Duration::from_secs(10);

/// Whether `last` is unset or older than `every`
fn due(last: Option<Instant>, every: Duration) -> bool {
    last.is_none_or(|at| at.elapsed() >= every)
}

#[tokio::main]
async fn main() {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));
    let config = Config::from_env();
    let pool = db::init_pool(&config);
    let inventory = cache::connect(&config).await;
    let read_cache = cache::connect_read_cache(&config).await;
    let provider = payment::from_config(&config);
    let lag = KafkaLag::new(&config).expect("the Kafka consumer should start");
    let expirer = Expirer::new(pool, inventory, read_cache, provider, Arc::new(lag), Settings::from_config(&config));

    let (stop, mut shutdown) = watch::channel(false);
    tokio::spawn(async move {
        let mut sigterm = unix::signal(unix::SignalKind::terminate()).expect("a SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
        log::info!("Stop requested");
        let _ = stop.send(true);
    });

    let idle = Duration::from_millis(config.expiry_interval_ms);
    let leaked_every = Duration::from_millis(config.expiry_leaked_every_ms);
    let sweep_every = Duration::from_secs(config.expiry_sweep_secs);
    let mut outages = Outages::default();
    let mut sales: Vec<i32> = Vec::new();
    let (mut last_sales, mut last_leaked, mut last_sweep) = (None::<Instant>, None::<Instant>, None::<Instant>);
    let mut backoff = BACKOFF_MIN;
    log::info!("Releasing expired and leaked holds");

    while !*shutdown.borrow() {
        let mut healthy = true;
        let mut more = false;

        if due(last_sales, SALES_REFRESH) {
            match expirer.load_sales().await {
                Ok(loaded) => {
                    outages.worked("Reading the sales");
                    if loaded != sales {
                        log::info!("Watching {} sales", loaded.len());
                    }
                    sales = loaded;
                    last_sales = Some(Instant::now());
                }
                Err(e) => {
                    outages.failed("Reading the sales", &e);
                    healthy = false;
                }
            }
        }

        match expirer.unpaid_pass(&sales).await {
            Ok(Pass { released, confirmed, dropped, more: full }) => {
                outages.worked("The unpaid-hold loop");
                more |= full;
                if released > 0 {
                    log::info!("Released {released} unpaid holds");
                }
                if confirmed > 0 {
                    log::info!("Closed {confirmed} paid holds");
                }
                if dropped > 0 {
                    log::warn!("Dropped {dropped} holds whose payment PostgreSQL refused");
                }
            }
            Err(e) => {
                outages.failed("The unpaid-hold loop", &e);
                healthy = false;
            }
        }

        if due(last_leaked, leaked_every) {
            match expirer.leaked_pass(&sales).await {
                Ok(Pass { released, more: full, .. }) => {
                    last_leaked = Some(Instant::now());
                    outages.worked("The leaked-hold loop");
                    more |= full;
                    if released > 0 {
                        log::warn!("Released {released} leaked holds");
                    }
                }
                Err(e) => {
                    outages.failed("The leaked-hold loop", &e);
                    healthy = false;
                }
            }
        }

        if due(last_sweep, sweep_every) {
            match expirer.sweep(Utc::now()).await {
                Ok(swept) => {
                    last_sweep = Some(Instant::now());
                    outages.worked("The stale-order sweep");
                    if swept.expired + swept.failed + swept.repaired > 0 {
                        log::warn!(
                            "Swept {} orders left AWAITING_PAYMENT after their hold expired and {} after it was released as leaked; repaired {} EXPIRED orders",
                            swept.expired,
                            swept.failed,
                            swept.repaired
                        );
                    }
                }
                Err(e) => {
                    outages.failed("The stale-order sweep", &e);
                    healthy = false;
                }
            }
        }

        let wait = if !healthy {
            let wait = backoff;
            backoff = (backoff * 2).min(BACKOFF_MAX);
            wait
        } else {
            backoff = BACKOFF_MIN;
            if more {
                continue;
            }
            idle
        };
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            _ = shutdown.changed() => {}
        }
    }
    log::info!("Expiration worker stopped");
}
