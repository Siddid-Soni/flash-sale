//! Redis connections and the keys stored in Redis (see ROADMAP.md, Redis Keys).
//!
//! There are two roles. The inventory Redis (`REDIS_URL`) holds stock, holds,
//! queues and processor progress; every inventory Lua script runs there and
//! touches only inventory keys. The read cache (`READ_CACHE_URL`, the same
//! instance locally) holds only `status:<ticket>`, what clients poll, so
//! polling can never slow down checkout.
//!
//! Keys for one sale carry the literal hash tag `{sale}`, e.g. `stock:{42}`,
//! so a script's keys stay in one slot if Redis is clustered.

use chrono::{DateTime, Utc};

use crate::config::Config;

/// One multiplexed connection to the inventory Redis; clone it per request
/// (cheap). It reconnects on its own after Redis restarts.
pub type RedisConn = redis::aio::ConnectionManager;

/// A connection to the read cache. A newtype so handlers ask for the role they
/// mean (`web::Data<RedisConn>` is inventory, `web::Data<ReadCache>` the cache).
#[derive(Clone)]
pub struct ReadCache(pub RedisConn);

async fn connect_to(url: &str, name: &str) -> RedisConn {
    let client = redis::Client::open(url).unwrap_or_else(|e| panic!("{name} should be valid: {e}"));
    redis::aio::ConnectionManager::new(client)
        .await
        .unwrap_or_else(|e| panic!("Redis should be reachable at {name}: {e}"))
}

/// Connects to the inventory Redis (`REDIS_URL`).
pub async fn connect(config: &Config) -> RedisConn {
    connect_to(&config.redis_url, "REDIS_URL").await
}

/// Connects to the read cache (`READ_CACHE_URL`).
pub async fn connect_read_cache(config: &Config) -> ReadCache {
    ReadCache(connect_to(&config.read_cache_url, "READ_CACHE_URL").await)
}

/// Units free to reserve
pub fn stock_key(sale: i32) -> String {
    format!("stock:{{{sale}}}")
}

/// Units paid
pub fn sold_key(sale: i32) -> String {
    format!("sold:{{{sale}}}")
}

/// Opening time in epoch milliseconds, compared with Redis `TIME` by `checkout.lua`
pub fn sale_start_key(sale: i32) -> String {
    format!("sale_start:{{{sale}}}")
}

/// The user's live ticket (one per user)
pub fn user_live_key(sale: i32, user: i32) -> String {
    format!("user_live:{{{sale}}}:{user}")
}

/// Ticket for an idempotency key
pub fn idem_key(sale: i32, user: i32, key: &str) -> String {
    format!("idem:{{{sale}}}:{user}:{key}")
}

/// Hash with `state`, `user`, `checkout_ms`, `payment_session`
pub fn hold_key(sale: i32, ticket: &str) -> String {
    format!("hold:{{{sale}}}:{ticket}")
}

/// Sorted set of `PENDING` holds, score = checkout time (ms)
pub fn holds_pending_key(sale: i32) -> String {
    format!("holds:pending:{{{sale}}}")
}

/// Sorted set of `AWAITING_PAYMENT` holds, score = link ready + payment window (ms)
pub fn holds_expiry_key(sale: i32) -> String {
    format!("holds:expiry:{{{sale}}}")
}

/// Order processor progress for one partition: checkout time (ms)
pub fn processed_upto_key(sale: i32, partition: i32) -> String {
    format!("processed_upto:{{{sale}}}:{partition}")
}

/// Read-cache hash with `status`, `payment_url`, `expires_at`, `offset`. Not
/// tagged: it lives in the read cache, which no script shares keys with.
pub fn status_key(ticket: &str) -> String {
    format!("status:{ticket}")
}

/// Loads a new sale's counters in one atomic pipeline: the stock to sell, no
/// units sold, and the opening time (epoch ms).
pub async fn init_sale(
    con: &mut RedisConn,
    sale: i32,
    total_inventory: i32,
    sale_start: DateTime<Utc>,
) -> redis::RedisResult<()> {
    redis::pipe()
        .atomic()
        .set(stock_key(sale), total_inventory)
        .set(sold_key(sale), 0)
        .set(sale_start_key(sale), sale_start.timestamp_millis())
        .query_async(con)
        .await
}

/// Whether no unit is left to reserve. A missing key counts as sold out, as
/// `checkout.lua` also refuses at zero.
pub async fn sold_out(con: &mut RedisConn, sale: i32) -> redis::RedisResult<bool> {
    let stock: Option<i64> = redis::cmd("GET").arg(stock_key(sale)).query_async(con).await?;
    Ok(stock.unwrap_or(0) <= 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_carry_the_literal_hash_tag() {
        assert_eq!(stock_key(42), "stock:{42}");
        assert_eq!(sold_key(42), "sold:{42}");
        assert_eq!(sale_start_key(42), "sale_start:{42}");
        assert_eq!(user_live_key(42, 7), "user_live:{42}:7");
        assert_eq!(idem_key(42, 7, "k1"), "idem:{42}:7:k1");
        assert_eq!(hold_key(42, "42.abc"), "hold:{42}:42.abc");
        assert_eq!(holds_pending_key(42), "holds:pending:{42}");
        assert_eq!(holds_expiry_key(42), "holds:expiry:{42}");
        assert_eq!(processed_upto_key(42, 3), "processed_upto:{42}:3");
        assert_eq!(status_key("42.abc"), "status:42.abc");
    }
}
