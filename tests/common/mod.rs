//! Shared setup for the integration tests: the real Postgres at `DATABASE_URL`
//! and the Redis at `TEST_REDIS_URL` (default: database 15 of the local
//! Redis, so a running dev server's data in database 0 is left alone).
//! Start both with `docker compose up -d postgres redis`.
#![allow(dead_code)]

use flash_sale::{
    cache::{self, RedisConn},
    config::Config,
    db::{self, DbPool},
};

pub const ADMIN_TOKEN: &str = "test-admin-token";

pub struct Env {
    pub config: Config,
    pub pool: DbPool,
    pub redis: RedisConn,
}

pub async fn env() -> Env {
    let mut config = Config::from_env();
    config.redis_url =
        std::env::var("TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/15".to_string());
    config.read_cache_url = config.redis_url.clone();
    config.admin_token = ADMIN_TOKEN.to_string();
    let pool = db::init_pool(&config);
    db::run_migrations(&pool);
    let redis = cache::connect(&config).await;
    Env { config, pool, redis }
}

/// The app as `order_service` builds it, minus Kafka and the logger. A macro
/// because the service type returned by `init_service` cannot be named.
#[macro_export]
macro_rules! test_app {
    ($env:expr) => {
        actix_web::test::init_service(
            actix_web::App::new()
                .app_data(actix_web::web::Data::new($env.pool.clone()))
                .app_data(actix_web::web::Data::new($env.redis.clone()))
                .app_data(actix_web::web::Data::new(flash_sale::api::sales::SaleCache::default()))
                .app_data(actix_web::web::Data::new($env.config.clone()))
                .app_data(flash_sale::api::json_config())
                .app_data(flash_sale::api::query_config())
                .configure(flash_sale::api::configure),
        )
        .await
    };
}
