use actix_web::{middleware, web, App, HttpServer};

use std::sync::Arc;

use flash_sale::{
    api, cache,
    config::Config,
    db,
    payment,
    waiting::{KafkaQueue, SharedQueue},
};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    let config = Config::from_env();

    // Created outside `HttpServer::new` so all workers share one pool
    let pool = db::init_pool(&config);
    db::run_migrations(&pool);
    // Worker threads take turns picking a connection, so each connection
    // carries the requests of a few threads (see REDIS_CONNECTIONS)
    let mut redis = Vec::new();
    for _ in 0..config.redis_connections.max(1) {
        redis.push(cache::connect(&config).await);
    }
    let next_redis = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let queue: SharedQueue = Arc::new(KafkaQueue::connect(&config).await.expect("Kafka should be reachable at KAFKA_BROKERS"));
    let queue = web::Data::new(queue);
    let provider = web::Data::new(payment::from_config(&config, pool.clone()));
    let config = web::Data::new(config);

    let bind_addr = config.bind_addr.clone();
    log::info!("starting HTTP server at http://{bind_addr}");

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(pool.clone()))
            .app_data(web::Data::new(
                redis[next_redis.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % redis.len()].clone(),
            ))
            .app_data(queue.clone())
            .app_data(provider.clone())
            .app_data(config.clone())
            .app_data(api::json_config())
            .app_data(api::query_config())
            .wrap(middleware::Logger::default())
            .configure(api::configure)
    })
    .bind(bind_addr)?
    .run()
    .await
}
