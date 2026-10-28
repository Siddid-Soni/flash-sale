//! Expiration worker: expires the unpaid orders whose payment deadline has
//! passed. Driven by PostgreSQL (`orders.expires_at`): each tick one statement
//! expires a batch, and for each order it expired the worker returns the unit to
//! stock once (the only place stock is returned, besides the Order Service's
//! compensation for a failed publish), writes `EXPIRED` to the read cache and
//! expires the payment session. A sweep finishes the status and session of
//! orders a crashed worker left behind. The logic is in `flash_sale::expiry`;
//! this is the loop. Any number can run at once: `SKIP LOCKED` hands each order
//! to one worker.
//!
//! After an error in Redis, the payment provider or PostgreSQL the worker backs
//! off (0.5 s doubling to 10 s) and tries again; the clients reconnect on their
//! own. Each outage is logged once when it starts and once when it ends.
//!
//! Stops on SIGINT or SIGTERM, after the pass in hand.
//!
//! Usage: `cargo run --bin expiration_worker`

use std::time::{Duration, Instant};

use chrono::Utc;
use tokio::{signal::unix, sync::watch};

use flash_sale::{
    cache,
    config::Config,
    db,
    expiry::{Expirer, Outages, Pass, Settings},
    payment,
};

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
    let expirer = Expirer::new(pool, inventory, read_cache, provider, Settings::from_config(&config));

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
    let sweep_every = Duration::from_secs(config.expiry_sweep_secs);
    let mut outages = Outages::default();
    let mut last_sweep = None::<Instant>;
    let mut backoff = BACKOFF_MIN;
    log::info!("Expiring unpaid orders");

    while !*shutdown.borrow() {
        let mut healthy = true;
        let mut more = false;

        match expirer.expire_pass().await {
            Ok(Pass { expired, lost, more: full }) => {
                outages.worked("The expiration pass");
                more |= full;
                if expired > 0 {
                    log::info!("Expired {expired} unpaid orders and returned {} units", expired - lost);
                }
            }
            Err(e) => {
                outages.failed("The expiration pass", &e);
                healthy = false;
            }
        }

        if due(last_sweep, sweep_every) {
            match expirer.sweep(Utc::now()).await {
                Ok(swept) => {
                    last_sweep = Some(Instant::now());
                    outages.worked("The expired-order sweep");
                    if swept.repaired > 0 {
                        log::warn!("Repaired the status or session of {} EXPIRED orders", swept.repaired);
                    }
                }
                Err(e) => {
                    outages.failed("The expired-order sweep", &e);
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
