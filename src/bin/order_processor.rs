//! Order processor: consumes `orders.checkout`, creates each order and its
//! payment session, publishes the payment link and reports progress (see
//! `flash_sale::processor`), and, alongside, confirms or refunds paid orders
//! from the payment inbox (`flash_sale::inbox`). Any number can run; Kafka
//! splits the partitions among them and the inbox rows are locked with `SKIP LOCKED`.
//!
//! Stops on SIGINT or SIGTERM: it stops fetching, lets the messages in flight
//! finish (for a bounded time) and commits.
//!
//! Usage: `cargo run --bin order_processor`

use std::sync::Arc;

use tokio::{signal::unix, sync::watch};

use flash_sale::{
    cache,
    config::Config,
    db,
    inbox::InboxWorker,
    payment,
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
    let inbox = Arc::new(InboxWorker::new(pool.clone(), inventory.clone(), read_cache.clone(), provider.clone()));
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

    let inbox_loop = tokio::spawn(inbox.run(shutdown.clone()));

    if let Err(e) = processor::run(&config, processor, inventory, read_cache, shutdown).await {
        log::error!("Order processor stopped: {e}");
        std::process::exit(1);
    }
    // The checkout loop only returns once stop was requested; let the inbox
    // loop finish the event in hand
    if tokio::time::timeout(std::time::Duration::from_secs(20), inbox_loop).await.is_err() {
        log::warn!("The payment inbox loop did not stop in time");
    }
    log::info!("Order processor stopped");
}
