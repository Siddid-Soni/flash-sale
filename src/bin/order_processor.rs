//! Order processor: consumes `orders.checkout`, creates each order and its
//! payment session, publishes the payment link and reports progress (see
//! `flash_sale::processor`). Stage 5 adds the payment inbox loop. Any number
//! can run; Kafka splits the partitions among them.
//!
//! Stops on SIGINT or SIGTERM: it stops fetching, lets the messages in flight
//! finish (for a bounded time) and commits.
//!
//! Usage: `cargo run --bin order_processor`

use std::sync::Arc;

use tokio::{signal::unix, sync::watch};

use flash_sale::{
    cache, config::Config, db, payment,
    processor::{self, Processor},
    queue,
};

#[tokio::main]
async fn main() {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));
    let config = Config::from_env();
    let pool = db::init_pool(&config);
    let inventory = cache::connect(&config).await;
    let read_cache = cache::connect_read_cache(&config).await;
    let provider = payment::from_config(&config, inventory.clone());
    let processor =
        Arc::new(Processor::new(pool, inventory.clone(), read_cache.clone(), provider, config.payment_window_secs));

    // The Order Service creates the topic too; whichever starts first does it
    if let Err(e) = queue::ensure_topic(&config).await {
        log::warn!("Could not check the checkout topic: {e}");
    }

    let (stop, shutdown) = watch::channel(false);
    tokio::spawn(async move {
        let mut sigterm = unix::signal(unix::SignalKind::terminate()).expect("a SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
        log::info!("Stop requested");
        let _ = stop.send(true);
    });

    if let Err(e) = processor::run(&config, processor, inventory, read_cache, shutdown).await {
        log::error!("Order processor stopped: {e}");
        std::process::exit(1);
    }
    log::info!("Order processor stopped");
}
