//! Order processor: consumes `orders.checkout`, creates each order and its
//! payment session and publishes the payment link (see
//! `flash_sale::processor`); serves the payment provider's webhook
//! (`POST /v1/payments/webhook` on `PROCESSOR_HTTP_ADDR`), which confirms or
//! refunds a paid order at once; and, alongside, retries the payment events
//! not yet processed from the inbox (`flash_sale::inbox`). Any number can run;
//! Kafka splits the partitions among them, the inbox rows are locked with
//! `SKIP LOCKED`, and the gateway balances the webhook over them.
//!
//! Stops on SIGINT or SIGTERM: it stops fetching, lets the messages in flight
//! finish (for a bounded time) and commits.
//!
//! Usage: `cargo run --bin order_processor`

use std::sync::Arc;

use actix_web::{web, App, HttpServer};
use tokio::{signal::unix, sync::watch};

use flash_sale::{
    api,
    cache,
    config::Config,
    db,
    inbox::InboxWorker,
    payment::{self, SharedProvider},
    processor::{self, Processor},
    queue,
};

#[tokio::main]
async fn main() {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));
    let config = Config::from_env();
    let pool = db::init_pool(&config);
    let read_cache = cache::connect_read_cache(&config).await;
    let provider = payment::from_config(&config);
    let inbox = Arc::new(InboxWorker::new(pool.clone(), read_cache.clone(), provider.clone()));
    let processor = Arc::new(Processor::new(pool, read_cache.clone(), provider.clone(), config.payment_window_secs));

    // The Order Service creates the topic too; whichever starts first does it
    if let Err(e) = queue::ensure_topic(&config).await {
        log::warn!("Could not check the checkout topic: {e}");
    }

    // The provider's webhook. It is reached through the gateway, which admits
    // only the provider's addresses; the signature is checked here as well
    let webhook_provider: web::Data<SharedProvider> = web::Data::new(provider);
    let webhook_inbox = web::Data::from(inbox.clone());
    let server = HttpServer::new(move || {
        App::new()
            .app_data(webhook_provider.clone())
            .app_data(webhook_inbox.clone())
            .wrap(actix_web::middleware::Logger::default())
            .configure(api::payments::configure)
    })
    // Longer than the gateway's upstream keepalive_timeout, so NGINX is the side that closes
    .workers(4)
    .keep_alive(std::time::Duration::from_secs(config.http_keep_alive_secs))
    .bind(&config.processor_http_addr)
    .unwrap_or_else(|e| panic!("PROCESSOR_HTTP_ADDR {} should be free: {e}", config.processor_http_addr))
    // Stopped below, with the rest of the processor
    .disable_signals()
    .run();
    let server_handle = server.handle();
    log::info!("Serving the payment webhook at http://{}/v1/payments/webhook", config.processor_http_addr);
    let server_task = tokio::spawn(server);

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

    if let Err(e) = processor::run(&config, processor, shutdown).await {
        log::error!("Order processor stopped: {e}");
        std::process::exit(1);
    }
    // The checkout loop only returns once stop was requested; let the web
    // server answer what it is handling and the inbox loop finish the event in hand
    server_handle.stop(true).await;
    let _ = server_task.await;
    if tokio::time::timeout(std::time::Duration::from_secs(20), inbox_loop).await.is_err() {
        log::warn!("The payment inbox loop did not stop in time");
    }
    log::info!("Order processor stopped");
}
