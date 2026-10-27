//! Redis connections and the keys stored in Redis (see ROADMAP.md, Redis Keys).
//!
//! There are two roles. The inventory Redis (`REDIS_URL`) holds stock, holds,
//! queues and processor progress; every inventory Lua script runs there and
//! touches only inventory keys. The read cache (`READ_CACHE_URL`, the same
//! instance locally) holds only `status:<ticket>`, what clients poll, and
//! `progress:<topic>:<partition>`, what turns an offset into a queue position,
//! so polling can never slow down checkout.
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

/// Hash with `state`, `user`, `checkout_ms`, `idem` (the idempotency key the
/// order is stored with), later `payment_session`
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

/// Processor progress for one partition in the *read cache*: the highest
/// offset below which every message has been processed (a plain string, no
/// hash tag). With `status:<ticket>` it is the only other key the read cache
/// holds: the status endpoint turns a ticket's offset and this into a queue
/// position, so polling never reads the inventory Redis.
pub fn progress_key(topic: &str, partition: i32) -> String {
    format!("progress:{topic}:{partition}")
}

/// Read-cache hash with `status`, `user`, `payment_url`, `expires_at` (RFC 3339,
/// UTC), `partition`, `offset`, `sale`. Not tagged: it lives in the read cache, which no script shares keys with.
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

/// How long an idempotency key, a released hold and a status entry live (24 h)
pub const DAY_SECS: u64 = 24 * 60 * 60;

/// A new queue ticket for a sale: `<sale>.<uuid v4>`. The sale id in front lets
/// the status fallback find `hold:{sale}:<ticket>` from the ticket alone.
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
    /// A `PENDING` hold now exists for the ticket passed in
    Held { checkout_ms: i64 },
    /// The idempotency key was seen before: the ticket it made (a retry)
    Existing { ticket: String },
    /// Before the opening time (epoch ms)
    NotStarted { sale_start_ms: i64 },
    /// No `sale_start:{sale}`: the sale does not exist in Redis
    NotFound,
    /// The user already holds or bought a unit
    AlreadyHolds,
    SoldOut,
}

/// Runs `checkout.lua`: holds one unit for `user` under `ticket` (made with
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
        .key(hold_key(sale, ticket))
        .key(holds_pending_key(sale))
        .arg(ticket)
        .arg(user)
        .arg(idem)
        .arg(DAY_SECS)
        .invoke_async(con)
        .await?;
    let bad = || unexpected_reply("unexpected checkout.lua reply");
    Ok(match (r.first().map(String::as_str), r.len()) {
        (Some("ok"), 2) => CheckoutOutcome::Held { checkout_ms: r[1].parse().map_err(|_| bad())? },
        (Some("existing"), 2) => CheckoutOutcome::Existing { ticket: r[1].clone() },
        (Some("not_started"), 2) => CheckoutOutcome::NotStarted { sale_start_ms: r[1].parse().map_err(|_| bad())? },
        (Some("not_found"), _) => CheckoutOutcome::NotFound,
        (Some("already"), _) => CheckoutOutcome::AlreadyHolds,
        (Some("sold_out"), _) => CheckoutOutcome::SoldOut,
        _ => return Err(bad()),
    })
}

static RELEASE_SCRIPT: LazyLock<redis::Script> =
    LazyLock::new(|| redis::Script::new(include_str!("../scripts/release.lua")));

/// The state a hold must be in to be released; decides the sorted set it is in
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HoldState {
    Pending,
    AwaitingPayment,
}

impl HoldState {
    pub fn as_str(self) -> &'static str {
        match self {
            HoldState::Pending => "PENDING",
            HoldState::AwaitingPayment => "AWAITING_PAYMENT",
        }
    }

    /// The sorted set holding the sale's holds in this state
    pub fn queue_key(self, sale: i32) -> String {
        match self {
            HoldState::Pending => holds_pending_key(sale),
            HoldState::AwaitingPayment => holds_expiry_key(sale),
        }
    }
}

/// The state a released hold ends in
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleasedAs {
    /// A `PENDING` hold that never got an order (publish failed, or leaked)
    Failed,
    /// An `AWAITING_PAYMENT` hold nobody paid for
    Expired,
}

impl ReleasedAs {
    pub fn as_str(self) -> &'static str {
        match self {
            ReleasedAs::Failed => "FAILED",
            ReleasedAs::Expired => "EXPIRED",
        }
    }
}

/// Release only holds whose score in their sorted set is at or below this
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaxScore {
    /// Whatever the score (compensation)
    Any,
    /// Redis `TIME` at the moment of the release (unpaid-hold loop)
    Now,
    /// An epoch-ms bound (leaked-hold loop: behind processor progress)
    UpTo(i64),
}

impl MaxScore {
    fn arg(self) -> String {
        match self {
            MaxScore::Any => "+inf".into(),
            MaxScore::Now => "now".into(),
            MaxScore::UpTo(ms) => ms.to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleaseOutcome {
    /// This call returned the unit to stock
    Released,
    /// Not this call's to release: another caller won, the hold changed state,
    /// does not exist, or its score is above the bound
    NotReleased,
}

/// Runs `release.lua`. Exactly one of several concurrent callers gets
/// `Released` for a hold. `clear_idem` also deletes the hold's idempotency key
/// so the client's retry with the same key makes a fresh checkout; only
/// compensation after a failed publish sets it.
pub async fn release_hold(
    con: &mut RedisConn,
    sale: i32,
    ticket: &str,
    expected: HoldState,
    max_score: MaxScore,
    released_as: ReleasedAs,
    clear_idem: bool,
) -> redis::RedisResult<ReleaseOutcome> {
    let reply: String = RELEASE_SCRIPT
        .key(hold_key(sale, ticket))
        .key(expected.queue_key(sale))
        .key(stock_key(sale))
        .arg(ticket)
        .arg(sale)
        .arg(expected.as_str())
        .arg(max_score.arg())
        .arg(released_as.as_str())
        .arg(if clear_idem { "1" } else { "0" })
        .arg(DAY_SECS)
        .invoke_async(con)
        .await?;
    match reply.as_str() {
        "released" => Ok(ReleaseOutcome::Released),
        "not_released" => Ok(ReleaseOutcome::NotReleased),
        _ => Err(unexpected_reply("unexpected release.lua reply")),
    }
}

/// A hold as stored in `hold:{sale}:{ticket}`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hold {
    /// `PENDING`, `AWAITING_PAYMENT`, `PAID`, `FAILED` or `EXPIRED`
    pub state: String,
    pub user: i32,
    pub checkout_ms: i64,
    /// The idempotency key the order is stored with
    pub idem: String,
}

/// Reads a hold from the inventory Redis; `None` if it does not exist (never
/// did, or deleted/expired after it ended)
pub async fn read_hold(con: &mut RedisConn, sale: i32, ticket: &str) -> redis::RedisResult<Option<Hold>> {
    let fields: HashMap<String, String> = redis::cmd("HGETALL").arg(hold_key(sale, ticket)).query_async(con).await?;
    let (Some(state), Some(user)) = (fields.get("state"), fields.get("user").and_then(|u| u.parse().ok())) else {
        return Ok(None);
    };
    Ok(Some(Hold {
        state: state.clone(),
        user,
        checkout_ms: fields.get("checkout_ms").and_then(|v| v.parse().ok()).unwrap_or(0),
        idem: fields.get("idem").cloned().unwrap_or_default(),
    }))
}

static LINK_READY_SCRIPT: LazyLock<redis::Script> =
    LazyLock::new(|| redis::Script::new(include_str!("../scripts/link_ready.lua")));

/// What `link_ready.lua` decided
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkOutcome {
    /// The hold moved from `PENDING` to `AWAITING_PAYMENT`
    Ready,
    /// It already was, with this session (a redelivered message): success
    Already,
    /// Released, expired or linked to another session
    Gone,
}

/// Runs `link_ready.lua`: moves the hold from `holds:pending` to
/// `holds:expiry` with score `expires_at_ms`, only if it is still `PENDING`.
pub async fn link_ready(
    con: &mut RedisConn,
    sale: i32,
    ticket: &str,
    payment_session: &str,
    expires_at_ms: i64,
) -> redis::RedisResult<LinkOutcome> {
    let reply: String = LINK_READY_SCRIPT
        .key(hold_key(sale, ticket))
        .key(holds_pending_key(sale))
        .key(holds_expiry_key(sale))
        .arg(ticket)
        .arg(payment_session)
        .arg(expires_at_ms)
        .invoke_async(con)
        .await?;
    match reply.as_str() {
        "ready" => Ok(LinkOutcome::Ready),
        "already" => Ok(LinkOutcome::Already),
        "gone" => Ok(LinkOutcome::Gone),
        _ => Err(unexpected_reply("unexpected link_ready.lua reply")),
    }
}

static SET_MAX_SCRIPT: LazyLock<redis::Script> =
    LazyLock::new(|| redis::Script::new(include_str!("../scripts/set_max.lua")));

/// Raises the integer at `key` to `value` and never lowers it (`set_max.lua`).
/// Returns whether it changed. For `processed_upto` and `progress` keys, which
/// several replicas write and which must only move forward.
pub async fn raise_to(con: &mut RedisConn, key: &str, value: i64) -> redis::RedisResult<bool> {
    let changed: i64 = SET_MAX_SCRIPT.key(key).arg(value).invoke_async(con).await?;
    Ok(changed == 1)
}

/// Reads `processed_upto:{sale}:{partition}`: checkout time (ms) up to which
/// the partition's processor has finished; `None` if never written
pub async fn read_processed_upto(con: &mut RedisConn, sale: i32, partition: i32) -> redis::RedisResult<Option<i64>> {
    redis::cmd("GET").arg(processed_upto_key(sale, partition)).query_async(con).await
}

/// Reports that the contiguous prefix of `partition` is processed up to and
/// including `offset`. The one place both progress keys are written, so they
/// cannot drift apart: `processed_upto:{sale}:{partition}` (inventory; for each
/// `(sale, checkout_ms)` in `upto`, the checkout time of the last message of
/// that sale in the prefix) and `progress:<topic>:<partition>` (read cache; the
/// offset, for queue positions). Both only ever rise.
pub async fn record_progress(
    inventory: &mut RedisConn,
    read_cache: &mut ReadCache,
    topic: &str,
    partition: i32,
    offset: i64,
    upto: &[(i32, i64)],
) -> redis::RedisResult<()> {
    for &(sale, checkout_ms) in upto {
        raise_to(inventory, &processed_upto_key(sale, partition), checkout_ms).await?;
    }
    raise_to(&mut read_cache.0, &progress_key(topic, partition), offset).await?;
    Ok(())
}

/// The highest offset of `partition` up to which everything is processed;
/// `None` if no processor has recorded progress yet
pub async fn read_progress(cache: &mut ReadCache, topic: &str, partition: i32) -> redis::RedisResult<Option<i64>> {
    redis::cmd("GET").arg(progress_key(topic, partition)).query_async(&mut cache.0).await
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
    pub partition: Option<i32>,
    pub offset: Option<i64>,
    pub sale: Option<i32>,
}

impl StatusUpdate {
    pub fn new(status: OrderStatus, user: i32) -> Self {
        Self { status, user, payment_url: None, expires_at: None, partition: None, offset: None, sale: None }
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
        ("partition", update.partition.map(|v| v.to_string())),
        ("offset", update.offset.map(|v| v.to_string())),
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
    pub partition: Option<i32>,
    pub offset: Option<i64>,
}

/// Reads `status:<ticket>` from the read cache; `None` if missing (never
/// written, expired, or the write was lost: callers fall back to the hold)
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
        partition: fields.get("partition").and_then(|v| v.parse().ok()),
        offset: fields.get("offset").and_then(|v| v.parse().ok()),
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
        assert_eq!(sold_key(42), "sold:{42}");
        assert_eq!(sale_start_key(42), "sale_start:{42}");
        assert_eq!(user_live_key(42, 7), "user_live:{42}:7");
        assert_eq!(idem_key(42, 7, "k1"), "idem:{42}:7:k1");
        assert_eq!(hold_key(42, "42.abc"), "hold:{42}:42.abc");
        assert_eq!(holds_pending_key(42), "holds:pending:{42}");
        assert_eq!(holds_expiry_key(42), "holds:expiry:{42}");
        assert_eq!(processed_upto_key(42, 3), "processed_upto:{42}:3");
        assert_eq!(status_key("42.abc"), "status:42.abc");
        assert_eq!(progress_key("orders.checkout", 3), "progress:orders.checkout:3");
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
