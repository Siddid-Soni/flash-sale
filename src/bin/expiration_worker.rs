//! Expiration worker: releases unpaid holds (`holds:expiry`) and leaked holds
//! (`holds:pending` behind processor progress), the only place stock is
//! returned besides immediate compensation. Skeleton: Stage 6 adds both loops.
//! Any number can run at once: the release script hands each hold to one worker.
//!
//! Usage: `cargo run --bin expiration_worker`

use std::time::Duration;

use flash_sale::{cache, config::Config, db};

const IDLE_WAIT: Duration = Duration::from_millis(500);

#[tokio::main]
async fn main() {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));
    let config = Config::from_env();
    let _pool = db::init_pool(&config);
    let _inventory = cache::connect(&config).await;
    let _read_cache = cache::connect_read_cache(&config).await;
    log::info!("Expiration worker started (not implemented yet)");

    loop {
        // TODO(stage 6): unpaid-hold loop and leaked-hold loop
        tokio::time::sleep(IDLE_WAIT).await;
    }
}
