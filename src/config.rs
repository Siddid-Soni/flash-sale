use std::env;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    /// Where the API listens; `0.0.0.0:8080` in a container
    pub bind_addr: String,
    /// Inventory Redis: stock, holds, queues, processor progress. All inventory
    /// scripts run here and touch only inventory keys
    pub redis_url: String,
    /// Read cache Redis: only `status:<ticket>`, what clients poll. Defaults to
    /// `REDIS_URL` (one instance locally, separate clusters in production)
    pub read_cache_url: String,
    pub kafka_brokers: String,
    /// Topic the Order Service publishes checkout events to
    pub checkout_topic: String,
    /// Partitions the topic is created with (processors scale up to this many)
    pub checkout_partitions: i32,
    /// How long the producer retries a publish before reporting failure. Also
    /// the margin the expiration worker keeps before it releases a leaked hold
    pub kafka_delivery_timeout_ms: u32,
    pub db_pool_size: u32,
    /// Redis connections the API spreads its worker threads over. Each one
    /// multiplexes many requests, but a single one becomes the bottleneck
    pub redis_connections: usize,
    pub jwt_secret: String,
    pub jwt_ttl_secs: u64,
    /// Secret for `X-Admin-Token` on `/v1/admin/*`
    pub admin_token: String,
    /// How long a user has to pay once the payment link is ready
    pub payment_window_secs: u64,
    /// `fake` (default) or `stripe`
    pub payment_provider: String,
    /// How long the fake provider takes to report a payment
    pub fake_payment_delay_ms: u64,
    /// Where clients reach the API; the fake provider builds its payment page
    /// URL (`{PUBLIC_BASE_URL}/fake-pay/{session}`) from it
    pub public_base_url: String,
    /// Checkout messages one order processor works on at once, per partition
    pub processor_concurrency: usize,
    /// Cap on checkout messages one order processor starts per second
    /// (0 = unlimited); the Stage 9 backlog test throttles with it
    pub processor_max_per_sec: u32,
}

impl Config {
    pub fn from_env() -> Self {
        dotenvy::dotenv().ok();

        let redis_url = or_default("REDIS_URL", "redis://127.0.0.1:6379".to_string());
        Self {
            database_url: required("DATABASE_URL"),
            bind_addr: or_default("BIND_ADDR", "127.0.0.1:8080".to_string()),
            redis_url: redis_url.clone(),
            read_cache_url: or_default("READ_CACHE_URL", redis_url),
            kafka_brokers: or_default("KAFKA_BROKERS", "localhost:9092".to_string()),
            checkout_topic: or_default("CHECKOUT_TOPIC", "orders.checkout".to_string()),
            checkout_partitions: or_default("CHECKOUT_PARTITIONS", 6),
            kafka_delivery_timeout_ms: or_default("KAFKA_DELIVERY_TIMEOUT_MS", 30_000),
            db_pool_size: or_default("DB_POOL_SIZE", 10),
            redis_connections: or_default("REDIS_CONNECTIONS", 8),
            jwt_secret: required("JWT_SECRET"),
            jwt_ttl_secs: or_default("JWT_TTL_SECS", 3600),
            admin_token: required("ADMIN_TOKEN"),
            payment_window_secs: or_default("PAYMENT_WINDOW_SECS", 300),
            payment_provider: or_default("PAYMENT_PROVIDER", "fake".to_string()),
            fake_payment_delay_ms: or_default("FAKE_PAYMENT_DELAY_MS", 2000),
            public_base_url: or_default("PUBLIC_BASE_URL", "http://localhost:8080".to_string()),
            processor_concurrency: or_default("PROCESSOR_CONCURRENCY", 32),
            processor_max_per_sec: or_default("PROCESSOR_MAX_PER_SEC", 0),
        }
    }
}

fn required(key: &str) -> String {
    env::var(key).unwrap_or_else(|_| panic!("{key} must be set"))
}

fn or_default<T: FromStr>(key: &str, default: T) -> T {
    match env::var(key) {
        Ok(value) => value
            .parse()
            .unwrap_or_else(|_| panic!("{key} has an invalid value: {value}")),
        Err(_) => default,
    }
}
