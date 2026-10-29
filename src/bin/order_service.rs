//! The Order Service: the stateless HTTP API of the case study, checkout and
//! order status, nothing else. Any number of replicas can run. It owns the
//! database migrations and the checkout topic, so it starts before the other
//! binaries. Sessions, sales and admin are the `site` service's; the payment
//! webhook is served by the order processor and the payment page by the fake
//! provider, each in its own container.
//!
//! Usage: `cargo run --bin order_service`

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use actix_web::{middleware, web, App, HttpServer};

use flash_sale::{
    api::{self, checkout::SoldOutFlags},
    cache,
    config::Config,
    db,
    queue,
    queue::{KafkaQueue, SharedQueue},
};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    let config = Config::from_env();

    // Created outside `HttpServer::new` so all workers share one pool
    let pool = db::init_pool(&config);
    db::run_migrations(&pool);

    queue::ensure_topic(&config).await.expect("Kafka should be reachable at KAFKA_BROKERS");
    let checkout_queue: SharedQueue = Arc::new(KafkaQueue::new(&config).expect("the Kafka producer should start"));
    let checkout_queue = web::Data::new(checkout_queue);

    // Worker threads take turns picking a connection, so each connection
    // carries the requests of a few threads (see REDIS_CONNECTIONS)
    let connections = config.redis_connections.max(1);
    let mut redis = Vec::new();
    let mut read_cache = Vec::new();
    for _ in 0..connections {
        redis.push(cache::connect(&config).await);
        read_cache.push(cache::connect_read_cache(&config).await);
    }
    log::info!("Connected to Redis ({connections} connections each to inventory and read cache)");
    let next_connection = Arc::new(AtomicUsize::new(0));

    // One flag set and one refresher task per replica, shared by all workers
    let sold_out = web::Data::new(SoldOutFlags::default());
    actix_web::rt::spawn(api::checkout::run_refresher(sold_out.clone(), redis[0].clone()));

    let config = web::Data::new(config);

    let bind_addr = config.bind_addr.clone();
    let replica_id = config.replica_id.clone();
    let (max_connections, backlog) = (config.http_max_connections, config.http_backlog);
    let keep_alive = Duration::from_secs(config.http_keep_alive_secs);
    log::info!("starting HTTP server at http://{bind_addr} (replica {})", replica_id.as_deref().unwrap_or("-"));

    HttpServer::new(move || {
        let i = next_connection.fetch_add(1, Ordering::Relaxed) % connections;
        App::new()
            .app_data(web::Data::new(pool.clone()))
            .app_data(web::Data::new(redis[i].clone()))
            .app_data(web::Data::new(read_cache[i].clone()))
            .app_data(checkout_queue.clone())
            .app_data(sold_out.clone())
            .app_data(config.clone())
            .app_data(api::json_config())
            .app_data(api::query_config())
            .wrap(middleware::Logger::default())
            // `X-Replica: <id>` on every response, only when REPLICA_ID is set
            .wrap(middleware::Condition::new(
                replica_id.is_some(),
                middleware::DefaultHeaders::new().add(("X-Replica", replica_id.clone().unwrap_or_default())),
            ))
            .configure(api::configure_order_service)
    })
    // Sized so a burst queues in the kernel instead of being refused (see gateway/nginx.conf)
    .max_connections(max_connections)
    .backlog(backlog)
    .keep_alive(keep_alive)
    .bind(bind_addr)?
    .run()
    .await
}
