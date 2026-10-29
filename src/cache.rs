//! Redis connections and the keys stored in Redis (see the README, Architecture).
//!
//! There are two roles. The inventory Redis (`REDIS_URL`) gates admission only:
//! stock, the one-per-user marker, idempotency keys and the sale's opening
//! time; every inventory Lua script runs there and
//! touches only inventory keys. Kafka is the queue and PostgreSQL decides every
//! outcome, so Redis keeps no per-ticket state. The read cache (`READ_CACHE_URL`, the same
//! instance locally) holds only `status:<ticket>`, what clients poll, so polling
//! can never slow down checkout.
//!
//! Keys for one sale carry the literal hash tag `{sale}`, e.g. `stock:{42}`,
//! so a script's keys stay in one slot if Redis is clustered.

use std::collections::HashMap;
use std::sync::LazyLock;

use chrono::{DateTime, Utc};

use crate::{config::Config, models::OrderStatus};

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

/// Opening time in epoch milliseconds, compared with Redis `TIME` by `checkout.lua`
pub fn sale_start_key(sale: i32) -> String {
    format!("sale_start:{{{sale}}}")
}

/// The user's live ticket (one per user): set by `checkout.lua` with a TTL
/// that outlives the sale, deleted by whoever returns the unit
pub fn user_live_key(sale: i32, user: i32) -> String {
    format!("user_live:{{{sale}}}:{user}")
}

/// Ticket for an idempotency key
pub fn idem_key(sale: i32, user: i32, key: &str) -> String {
    format!("idem:{{{sale}}}:{user}:{key}")
}

/// Read-cache hash with `status`, `user`, `payment_url`, `expires_at` (RFC 3339,
/// UTC), `sale`. Not tagged: it lives in the read cache, which no script shares keys with.
pub fn status_key(ticket: &str) -> String {
    format!("status:{ticket}")
}

/// Loads a new sale's keys in one atomic pipeline: the stock to sell and the
/// opening time (epoch ms). Sales are counted only in PostgreSQL.
pub async fn init_sale(
    con: &mut RedisConn,
    sale: i32,
    total_inventory: i32,
    sale_start: DateTime<Utc>,
) -> redis::RedisResult<()> {
    redis::pipe()
        .atomic()
        .set(stock_key(sale), total_inventory)
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

/// How long an idempotency key and a status entry live (24 h), and the least
/// TTL of `user_live`
pub const DAY_SECS: u64 = 24 * 60 * 60;

/// A new queue ticket for a sale: `<sale>.<uuid v4>`. The sale id in front
/// tells the status endpoint, from the ticket alone, which sale it belongs to.
pub fn new_ticket(sale: i32) -> String {
    format!("{sale}.{}", uuid::Uuid::new_v4())
}

/// The sale id of a ticket, or `None` if it is not of the form
/// `<sale>.<uuid>`. Strict, because tickets from the URL are used in key names.
pub fn ticket_sale(ticket: &str) -> Option<i32> {
    let (sale, id) = ticket.split_once('.')?;
    if sale.is_empty() || !sale.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let canonical = uuid::Uuid::parse_str(id).ok()?.hyphenated().to_string();
    (canonical == id).then(|| sale.parse().ok()).flatten()
}

fn unexpected_reply(what: &'static str) -> redis::RedisError {
    (redis::ErrorKind::UnexpectedReturnType, what).into()
}

static CHECKOUT_SCRIPT: LazyLock<redis::Script> =
    LazyLock::new(|| redis::Script::new(include_str!("../scripts/checkout.lua")));

/// What `checkout.lua` decided
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckoutOutcome {
    /// A unit is reserved for the ticket passed in; `checkout_ms` is Redis
    /// `TIME` when it was
    Reserved { checkout_ms: i64 },
    /// The idempotency key was seen before: the ticket it made (a retry)
    Existing { ticket: String },
    /// Before the opening time (epoch ms)
    NotStarted { sale_start_ms: i64 },
    /// No `sale_start:{sale}`: the sale does not exist in Redis
    NotFound,
    /// The user already holds or bought a unit
    AlreadyLive,
    SoldOut,
}

/// Runs `checkout.lua`: reserves one unit for `user` under `ticket` (made with
/// `new_ticket`), or says why not. Inventory Redis only.
pub async fn checkout(
    con: &mut RedisConn,
    sale: i32,
    user: i32,
    idem: &str,
    ticket: &str,
) -> redis::RedisResult<CheckoutOutcome> {
    let r: Vec<String> = CHECKOUT_SCRIPT
        .key(sale_start_key(sale))
        .key(idem_key(sale, user, idem))
        .key(user_live_key(sale, user))
        .key(stock_key(sale))
        .arg(ticket)
        .arg(DAY_SECS)
        .invoke_async(con)
        .await?;
    let bad = || unexpected_reply("unexpected checkout.lua reply");
    Ok(match (r.first().map(String::as_str), r.len()) {
        (Some("ok"), 2) => CheckoutOutcome::Reserved { checkout_ms: r[1].parse().map_err(|_| bad())? },
        (Some("existing"), 2) => CheckoutOutcome::Existing { ticket: r[1].clone() },
        (Some("not_started"), 2) => CheckoutOutcome::NotStarted { sale_start_ms: r[1].parse().map_err(|_| bad())? },
        (Some("not_found"), _) => CheckoutOutcome::NotFound,
        (Some("already"), _) => CheckoutOutcome::AlreadyLive,
        (Some("sold_out"), _) => CheckoutOutcome::SoldOut,
        _ => return Err(bad()),
    })
}

static RETURN_UNIT_SCRIPT: LazyLock<redis::Script> =
    LazyLock::new(|| redis::Script::new(include_str!("../scripts/return_unit.lua")));

/// Runs `return_unit.lua`: `INCR stock:{sale}`, delete the user's `user_live`
/// if it still names `ticket` and, when `idem` is given, delete the idempotency
/// key if it still names `ticket` (compensation after a failed publish, so the
/// client's retry with the same key makes a fresh checkout).
///
/// Not idempotent: each caller has just won a PostgreSQL decision that happens
/// once per ticket and calls this once, without retry. Inventory Redis only.
pub async fn return_unit(
    con: &mut RedisConn,
    sale: i32,
    user: i32,
    ticket: &str,
    idem: Option<&str>,
) -> redis::RedisResult<()> {
    let mut call = RETURN_UNIT_SCRIPT.key(stock_key(sale));
    call.key(user_live_key(sale, user));
    if let Some(idem) = idem {
        call.key(idem_key(sale, user, idem));
    }
    let _: i64 = call.arg(ticket).invoke_async(con).await?;
    Ok(())
}

static SET_STATUS_SCRIPT: LazyLock<redis::Script> =
    LazyLock::new(|| redis::Script::new(include_str!("../scripts/set_status.lua")));

/// What to write to `status:<ticket>`. Only `status` and `user` are required.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusUpdate {
    pub status: OrderStatus,
    /// Whose ticket it is; the status endpoint answers `403` to anyone else
    pub user: i32,
    pub payment_url: Option<String>,
    /// RFC 3339, UTC
    pub expires_at: Option<String>,
    pub sale: Option<i32>,
}

impl StatusUpdate {
    pub fn new(status: OrderStatus, user: i32) -> Self {
        Self { status, user, payment_url: None, expires_at: None, sale: None }
    }
}

/// Runs `set_status.lua` on the read cache. Returns whether the entry was
/// written; `false` means it already holds a later (or the first final) state.
pub async fn set_status(cache: &mut ReadCache, ticket: &str, update: &StatusUpdate) -> redis::RedisResult<bool> {
    let mut call = SET_STATUS_SCRIPT.key(status_key(ticket));
    let mut invocation = call.arg(update.status.as_str()).arg(update.user).arg(DAY_SECS);
    let optional = [
        ("payment_url", update.payment_url.clone()),
        ("expires_at", update.expires_at.clone()),
        ("sale", update.sale.map(|v| v.to_string())),
    ];
    for (field, value) in optional {
        if let Some(value) = value {
            invocation = invocation.arg(field).arg(value);
        }
    }
    let reply: String = invocation.invoke_async(&mut cache.0).await?;
    match reply.as_str() {
        "written" => Ok(true),
        "ignored" => Ok(false),
        _ => Err(unexpected_reply("unexpected set_status.lua reply")),
    }
}

/// What the read cache holds for a ticket
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedStatus {
    pub status: OrderStatus,
    pub user: i32,
    pub payment_url: Option<String>,
    pub expires_at: Option<String>,
}

/// Reads `status:<ticket>` from the read cache; `None` if missing (never
/// written, expired, or the write was lost: callers fall back to PostgreSQL)
pub async fn read_status(cache: &mut ReadCache, ticket: &str) -> redis::RedisResult<Option<CachedStatus>> {
    let fields: HashMap<String, String> = redis::cmd("HGETALL").arg(status_key(ticket)).query_async(&mut cache.0).await?;
    let status = fields.get("status").and_then(|s| OrderStatus::parse(s));
    let user = fields.get("user").and_then(|u| u.parse().ok());
    let (Some(status), Some(user)) = (status, user) else { return Ok(None) };
    Ok(Some(CachedStatus {
        status,
        user,
        payment_url: fields.get("payment_url").cloned(),
        expires_at: fields.get("expires_at").cloned(),
    }))
}

/// The stock of several sales in one round trip (`None` for a missing key),
/// in the order asked. What the sold-out flag refresher reads.
pub async fn stock_levels(con: &mut RedisConn, sales: &[i32]) -> redis::RedisResult<Vec<Option<i64>>> {
    if sales.is_empty() {
        return Ok(Vec::new());
    }
    redis::cmd("MGET").arg(sales.iter().map(|&sale| stock_key(sale)).collect::<Vec<_>>()).query_async(con).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_carry_the_literal_hash_tag() {
        assert_eq!(stock_key(42), "stock:{42}");
        assert_eq!(sale_start_key(42), "sale_start:{42}");
        assert_eq!(user_live_key(42, 7), "user_live:{42}:7");
        assert_eq!(idem_key(42, 7, "k1"), "idem:{42}:7:k1");
        assert_eq!(status_key("42.abc"), "status:42.abc");
    }

    #[test]
    fn tickets_name_their_sale() {
        let ticket = new_ticket(42);
        assert!(ticket.starts_with("42."));
        assert_eq!(ticket_sale(&ticket), Some(42));
        assert_ne!(new_ticket(42), ticket);

        let id = "6f1c2d3e-0a1b-4c5d-8e9f-0123456789ab";
        assert_eq!(ticket_sale(&format!("7.{id}")), Some(7));
        for bad in [
            "".to_string(),
            "42".to_string(),
            format!(".{id}"),
            format!("-1.{id}"),
            format!("+1.{id}"),
            format!("4x.{id}"),
            format!("99999999999.{id}"),
            "42.not-a-uuid".to_string(),
            format!("42.{}", id.to_uppercase()),
            format!("42.{id}.x"),
            format!("42.{}", id.replace('-', "")),
        ] {
            assert_eq!(ticket_sale(&bad), None, "{bad}");
        }
    }
}
