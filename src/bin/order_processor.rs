//! Order processor: consumes `orders.checkout`, creates each order and its
//! payment session, publishes the payment link, and processes the payment
//! inbox (confirm or refund). Skeleton: Stage 4 adds the checkout consumer,
//! Stage 5 the inbox loop. Any number can run; Kafka splits the partitions.
//!
//! Usage: `cargo run --bin order_processor`

use std::time::Duration;

use flash_sale::{cache, config::Config, db};

const IDLE_WAIT: Duration = Duration::from_secs(1);

#[tokio::main]
async fn main() {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));
    let config = Config::from_env();
    let _pool = db::init_pool(&config);
    let _inventory = cache::connect(&config).await;
    let _read_cache = cache::connect_read_cache(&config).await;
    log::info!("Order processor started: consuming topic {} (not implemented yet)", config.checkout_topic);

    loop {
        // TODO(stage 4): consume orders.checkout; TODO(stage 5): process payment_inbox
        tokio::time::sleep(IDLE_WAIT).await;
    }
}
