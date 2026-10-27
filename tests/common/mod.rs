//! Shared setup for the integration tests: the real Postgres at `DATABASE_URL`
//! and the Redis at `TEST_REDIS_URL` (default: database 15 of the local
//! Redis, so a running dev server's data in database 0 is left alone).
//! Start both with `docker compose up -d postgres redis`.
//!
//! The payment provider is real too: each test binary starts the fake
//! provider's HTTP API and payment page on a free port in a thread of its own
//! (`provider_url`), and the backend reaches it through `HttpProvider`, as the
//! order processor does. `Env::provider` is a second handle on the same
//! provider state (Redis), for tests that play the user at the payment page.
#![allow(dead_code)]

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, OnceLock,
};

use async_trait::async_trait;
use flash_sale::{
    cache::{self, ReadCache, RedisConn},
    config::Config,
    db::{self, DbPool},
    payment::{client::HttpProvider, FakeProvider, SharedProvider},
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
    /// The fake provider's state on the test Redis, for playing the user at
    /// its payment page and looking at sessions
    pub provider: Arc<FakeProvider>,
    /// The backend's view of the provider: `HttpProvider` against the test
    /// provider server
    pub http: SharedProvider,
}

impl Env {
    /// The provider as the order processor and the expiration worker take it
    pub fn shared_provider(&self) -> SharedProvider {
        self.http.clone()
    }
}

/// A base URL the test provider builds its payment page links from
pub const PAGE_BASE_URL: &str = "http://pay.test";

/// A test config: the test Redis, the test secrets
pub fn test_config() -> Config {
    let mut config = Config::from_env();
    config.redis_url = std::env::var("TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/15".to_string());
    config.read_cache_url = config.redis_url.clone();
    config.admin_token = ADMIN_TOKEN.to_string();
    config.payment_webhook_secret = WEBHOOK_SECRET.to_string();
    config.payment_page_base_url = PAGE_BASE_URL.to_string();
    config
}

/// The URL of the fake provider's HTTP API and payment page, served from a
/// thread of this test binary (started on first use, kept until the process ends)
pub fn provider_url() -> String {
    static URL: OnceLock<String> = OnceLock::new();
    URL.get_or_init(|| {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            actix_web::rt::System::new().block_on(async move {
                let config = test_config();
                let redis = cache::connect(&config).await;
                let provider = actix_web::web::Data::new(FakeProvider::new(
                    redis,
                    config.payment_page_base_url.clone(),
                    config.payment_webhook_secret.clone(),
                ));
                let config = actix_web::web::Data::new(config);
                let server = actix_web::HttpServer::new(move || {
                    actix_web::App::new()
                        .app_data(provider.clone())
                        .app_data(config.clone())
                        .app_data(actix_web::web::JsonConfig::default().error_handler(flash_sale::payment::provider_api::json_error))
                        .configure(flash_sale::payment::pay_page::configure)
                        .configure(flash_sale::payment::provider_api::configure)
                })
                .workers(2)
                .bind(("127.0.0.1", 0))
                .unwrap();
                tx.send(format!("http://{}", server.addrs()[0])).unwrap();
                server.run().await.unwrap();
            })
        });
        rx.recv().unwrap()
    })
    .clone()
}

pub async fn env() -> Env {
    let config = test_config();
    let pool = db::init_pool(&config);
    db::run_migrations(&pool);
    let redis = cache::connect(&config).await;
    let read_cache = cache::connect_read_cache(&config).await;
    let provider = Arc::new(FakeProvider::new(
        redis.clone(),
        config.payment_page_base_url.clone(),
        config.payment_webhook_secret.clone(),
    ));
    let http: SharedProvider = Arc::new(HttpProvider::new(provider_url(), config.payment_webhook_secret.clone()));
    Env { config, pool, redis, read_cache, provider, http }
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
                .app_data(actix_web::web::Data::new(flash_sale::api::sales::SaleCache::default()))
                .app_data(actix_web::web::Data::new(flash_sale::api::orders::ProgressCache::default()))
                .app_data($flags.clone())
                .app_data(actix_web::web::Data::new($env.config.clone()))
                .app_data(actix_web::web::Data::new(flash_sale::challenge::from_config(&$env.config)))
                .app_data(flash_sale::api::json_config())
                .app_data(flash_sale::api::query_config())
                .configure(flash_sale::api::configure),
        )
        .await
    };
}

/// The fake provider's payment page and HTTP API as `fake_payment_provider`
/// builds them, on the env's provider state and config (so a test can change
/// `config.frontend_url` first), without the network.
#[macro_export]
macro_rules! provider_app {
    ($env:expr) => {
        actix_web::test::init_service(
            actix_web::App::new()
                .app_data(actix_web::web::Data::from($env.provider.clone()))
                .app_data(actix_web::web::Data::new($env.config.clone()))
                .app_data(actix_web::web::JsonConfig::default().error_handler(flash_sale::payment::provider_api::json_error))
                .configure(flash_sale::payment::pay_page::configure)
                .configure(flash_sale::payment::provider_api::configure),
        )
        .await
    };
}

/// The order processor's webhook endpoint on an `InboxWorker`, as `order_processor` builds it
#[macro_export]
macro_rules! processor_app {
    ($env:expr, $inbox:expr) => {
        actix_web::test::init_service(
            actix_web::App::new()
                .app_data(actix_web::web::Data::new($env.shared_provider()))
                .app_data(actix_web::web::Data::from($inbox.clone()))
                .configure(flash_sale::api::payments::configure),
        )
        .await
    };
}
