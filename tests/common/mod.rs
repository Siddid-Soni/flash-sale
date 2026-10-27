//! Shared setup for the integration tests: the real Postgres at `DATABASE_URL`
//! and the Redis at `TEST_REDIS_URL` (default: database 15 of the local
//! Redis, so a running dev server's data in database 0 is left alone).
//! Start both with `docker compose up -d postgres redis`.
#![allow(dead_code)]

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

use async_trait::async_trait;
use flash_sale::{
    cache::{self, ReadCache, RedisConn},
    config::Config,
    db::{self, DbPool},
    payment::{self, FakeProvider, SharedProvider},
    queue::{CheckoutEvent, CheckoutQueue, Published, QueueError, QueueResult, SharedQueue},
};

pub mod sale;

pub const ADMIN_TOKEN: &str = "test-admin-token";
pub const WEBHOOK_SECRET: &str = "test-webhook-secret";

pub struct Env {
    pub config: Config,
    pub pool: DbPool,
    pub redis: RedisConn,
    pub read_cache: ReadCache,
    /// The fake provider on the test Redis, as the Order Service builds it
    pub provider: Arc<FakeProvider>,
}

impl Env {
    /// The provider as the webhook endpoint takes it
    pub fn shared_provider(&self) -> SharedProvider {
        self.provider.clone()
    }
}

pub async fn env() -> Env {
    let mut config = Config::from_env();
    config.redis_url =
        std::env::var("TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/15".to_string());
    config.read_cache_url = config.redis_url.clone();
    config.admin_token = ADMIN_TOKEN.to_string();
    config.payment_webhook_secret = WEBHOOK_SECRET.to_string();
    let pool = db::init_pool(&config);
    db::run_migrations(&pool);
    let redis = cache::connect(&config).await;
    let read_cache = cache::connect_read_cache(&config).await;
    let provider = payment::fake_from_config(&config, redis.clone());
    Env { config, pool, redis, read_cache, provider }
}

/// A `CheckoutQueue` without Kafka: records what is published, hands out
/// offsets 0, 1, 2... on partition 3, and fails while `fail` is set.
#[derive(Default)]
pub struct FakeQueue {
    pub fail: AtomicBool,
    pub published: Mutex<Vec<CheckoutEvent>>,
}

impl FakeQueue {
    pub fn set_failing(&self, failing: bool) {
        self.fail.store(failing, Ordering::SeqCst);
    }

    pub fn events(&self) -> Vec<CheckoutEvent> {
        self.published.lock().unwrap().clone()
    }
}

#[async_trait]
impl CheckoutQueue for FakeQueue {
    async fn publish(&self, event: &CheckoutEvent) -> QueueResult<Published> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(QueueError("broker down".into()));
        }
        let mut published = self.published.lock().unwrap();
        published.push(event.clone());
        Ok(Published { partition: 3, offset: published.len() as i64 - 1 })
    }
}

pub fn fake_queue() -> (Arc<FakeQueue>, SharedQueue) {
    let fake = Arc::new(FakeQueue::default());
    let shared: SharedQueue = fake.clone();
    (fake, shared)
}

/// The app as `order_service` builds it, minus the logger and the sold-out
/// refresher task (tests call `SoldOutFlags::refresh` themselves). Kafka is a
/// `FakeQueue` unless a `SharedQueue` is passed as the second argument; a third, a
/// `web::Data<SoldOutFlags>`, lets the test keep a handle to the flags. A
/// macro because the service type returned by `init_service` cannot be named.
#[macro_export]
macro_rules! test_app {
    ($env:expr) => {
        $crate::test_app!($env, $crate::common::fake_queue().1)
    };
    ($env:expr, $queue:expr) => {
        $crate::test_app!($env, $queue, actix_web::web::Data::new(flash_sale::api::checkout::SoldOutFlags::default()))
    };
    ($env:expr, $queue:expr, $flags:expr) => {
        actix_web::test::init_service(
            actix_web::App::new()
                .app_data(actix_web::web::Data::new($env.pool.clone()))
                .app_data(actix_web::web::Data::new($env.redis.clone()))
                .app_data(actix_web::web::Data::new($env.read_cache.clone()))
                .app_data(actix_web::web::Data::new($queue))
                .app_data(actix_web::web::Data::new($env.shared_provider()))
                .app_data(actix_web::web::Data::from($env.provider.clone()))
                .app_data(actix_web::web::Data::new(flash_sale::api::sales::SaleCache::default()))
                .app_data(actix_web::web::Data::new(flash_sale::api::orders::ProgressCache::default()))
                .app_data($flags.clone())
                .app_data(actix_web::web::Data::new($env.config.clone()))
                .app_data(flash_sale::api::json_config())
                .app_data(flash_sale::api::query_config())
                .configure(flash_sale::api::configure),
        )
        .await
    };
}
