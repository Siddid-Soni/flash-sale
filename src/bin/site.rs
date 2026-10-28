//! The site service: everything the storefront needs besides checkout and
//! order status, so none of it can take capacity, connections or database time
//! from the Order Service. It serves guest sessions, sale details, the sale
//! list, search and the admin route that creates a sale. One replica, with its
//! own small PostgreSQL pool (`DB_POOL_SIZE`); it reads Redis only for the
//! sold-out flags, and writes it only when a sale is created.
//!
//! Usage: `cargo run --bin site`

use std::time::Duration;

use actix_web::{middleware, web, App, HttpServer};

use flash_sale::{
    api::{self, sales::SaleCache},
    cache, challenge,
    config::Config,
    db,
};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    let config = Config::from_env();
    let pool = db::init_pool(&config);
    // One multiplexed connection is plenty for a `GET`/`MGET` per cache period
    let redis = cache::connect(&config).await;
    let sale_cache = web::Data::new(SaleCache::default());
    let challenge = web::Data::new(challenge::from_config(&config));

    let bind_addr = config.site_bind_addr.clone();
    let keep_alive = Duration::from_secs(config.http_keep_alive_secs);
    let config = web::Data::new(config);
    log::info!("starting the site service at http://{bind_addr}");

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(pool.clone()))
            .app_data(web::Data::new(redis.clone()))
            .app_data(sale_cache.clone())
            .app_data(config.clone())
            .app_data(challenge.clone())
            .app_data(api::json_config())
            .app_data(api::query_config())
            .wrap(middleware::Logger::default())
            .configure(api::configure_site)
    })
    .keep_alive(keep_alive)
    .bind(bind_addr)?
    .run()
    .await
}
