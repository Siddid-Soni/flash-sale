use std::env;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    /// Where the API listens; `0.0.0.0:8080` in a container
    pub bind_addr: String,
    pub redis_url: String,
    pub kafka_brokers: String,
    pub db_pool_size: u32,
    /// Redis connections the API spreads its worker threads over. Each one
    /// multiplexes many requests, but a single one becomes the bottleneck
    pub redis_connections: usize,
    /// Maximum tokens ACTIVE at once per product (people shopping at the same time)
    pub max_active_tokens: u32,
    pub token_ttl_secs: u64,
    pub hold_ttl_secs: u64,
    pub payment_extension_secs: u64,
    /// Units one user may hold or buy of a product created without `max_per_user`
    pub default_max_per_user: u32,
    pub jwt_secret: String,
    pub jwt_ttl_secs: u64,
    /// `fake` (default) or `stripe`
    pub payment_provider: String,
    /// How long the fake provider takes to report a payment
    pub fake_payment_delay_ms: u64,
}

impl Config {
    pub fn from_env() -> Self {
        dotenvy::dotenv().ok();

        Self {
            database_url: required("DATABASE_URL"),
            bind_addr: or_default("BIND_ADDR", "127.0.0.1:8080".to_string()),
            redis_url: or_default("REDIS_URL", "redis://127.0.0.1:6379".to_string()),
            kafka_brokers: or_default("KAFKA_BROKERS", "localhost:9092".to_string()),
            db_pool_size: or_default("DB_POOL_SIZE", 10),
            redis_connections: or_default("REDIS_CONNECTIONS", 8),
            max_active_tokens: or_default("MAX_ACTIVE_TOKENS", 2000),
            token_ttl_secs: or_default("TOKEN_TTL_SECS", 600),
            hold_ttl_secs: or_default("HOLD_TTL_SECS", 300),
            payment_extension_secs: or_default("PAYMENT_EXTENSION_SECS", 120),
            default_max_per_user: or_default("DEFAULT_MAX_PER_USER", 1),
            jwt_secret: required("JWT_SECRET"),
            jwt_ttl_secs: or_default("JWT_TTL_SECS", 3600),
            payment_provider: or_default("PAYMENT_PROVIDER", "fake".to_string()),
            fake_payment_delay_ms: or_default("FAKE_PAYMENT_DELAY_MS", 2000),
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
