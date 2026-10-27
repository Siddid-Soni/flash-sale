//! Redis connection and the per-product keys stored in Redis.

use std::sync::LazyLock;

use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::config::Config;

// One multiplexed connection shared by all handlers; clone it per request (cheap).
// It reconnects on its own after Redis restarts.
pub type RedisConn = redis::aio::ConnectionManager;

pub async fn connect(config: &Config) -> RedisConn {
    let client = redis::Client::open(config.redis_url.as_str()).expect("REDIS_URL should be valid");
    let conn = redis::aio::ConnectionManager::new(client)
        .await
        .expect("Redis should be reachable at REDIS_URL");
    log::info!("Connected to Redis");
    conn
}

// Stock for a product: `stock:{product}` counts the units neither held nor
// sold, `stock_total:{product}` the total (its existence means Redis knows the
// product), and `sold:{product}` the paid ones. `max_per_user:{product}` is the
// per-user limit, read by `reserve.lua`.

pub fn stock_key(product_id: i32) -> String {
    format!("stock:{product_id}")
}

pub fn stock_total_key(product_id: i32) -> String {
    format!("stock_total:{product_id}")
}

/// Units paid for; equal to `stock_total` means sold out (until a cancellation)
pub fn sold_key(product_id: i32) -> String {
    format!("sold:{product_id}")
}

pub fn max_per_user_key(product_id: i32) -> String {
    format!("max_per_user:{product_id}")
}

/// Sets up a new product in Redis, replacing anything left from before: the
/// stock, total and sold counters, the per-user limit, and the sale opening
/// time (read by the waiting room status without touching Postgres). The
/// waiting room keys are cleared too: a recreated database reuses product ids,
/// and a stale `closed` or `lobby_flushed` flag would end or skip the new sale.
/// (Per-user keys such as `user_token` cannot be cleared without a scan; after
/// recreating the database, flush Redis as well.)
pub async fn init_product(
    con: &mut RedisConn,
    product_id: i32,
    stock_total: u32,
    max_per_user: u32,
    sale_opens_at: DateTime<Utc>,
) -> redis::RedisResult<()> {
    let p = product_id;
    redis::pipe()
        .atomic()
        .del(&[
            closed_key(p),
            format!("lobby:{p}"),
            format!("lobby_order:{p}"),
            format!("lobby_published:{p}"),
            format!("lobby_flushed:{p}"),
            format!("admitted_upto:{p}"),
            format!("active_tokens:{p}"),
            admit_samples_key(p),
        ])
        .set(stock_key(product_id), stock_total)
        .set(stock_total_key(product_id), stock_total)
        .set(sold_key(product_id), 0)
        .set(max_per_user_key(product_id), max_per_user)
        .set(format!("sale_opens_at:{product_id}"), sale_opens_at.to_rfc3339())
        .query_async(con)
        .await
}

/// What Redis knows about a product's units, read in one round trip
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StockSnapshot {
    /// Units neither held nor sold
    pub available: i64,
    pub sold: i64,
    pub total: i64,
    pub closed: bool,
}

/// Reads the counters of a product, without touching Postgres. `None` when
/// Redis does not know the product (`stock_total` is missing).
pub async fn stock_snapshot(con: &mut RedisConn, product_id: i32) -> redis::RedisResult<Option<StockSnapshot>> {
    let (counters, closed): (Vec<Option<i64>>, bool) = redis::pipe()
        .atomic()
        .cmd("MGET").arg(stock_key(product_id)).arg(sold_key(product_id)).arg(stock_total_key(product_id))
        .cmd("EXISTS").arg(closed_key(product_id))
        .query_async(con)
        .await?;
    let [available, sold, total] = counters[..] else {
        return Err((redis::ErrorKind::UnexpectedReturnType, "unexpected MGET reply").into());
    };
    Ok(total.map(|total| StockSnapshot { available: available.unwrap_or(0), sold: sold.unwrap_or(0), total, closed }))
}

// Waiting room tokens: `token:{id}` is a hash with user, product, status
// (LOBBY, QUEUED, ACTIVE, USED), seq, and published. `user_token:{product}:{user}`
// points to the user's current token for the product.

pub fn token_key(token_id: &str) -> String {
    format!("token:{token_id}")
}

static JOIN_SCRIPT: LazyLock<redis::Script> = LazyLock::new(|| redis::Script::new(include_str!("../scripts/join.lua")));

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinedToken {
    pub token_id: String,
    pub status: String,
    pub seq: Option<u64>,
    /// Whether the token is in the product's Kafka topic, or being published
    /// there by the admission worker (lobby tokens): either way, not the
    /// caller's to publish
    pub published: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinOutcome {
    Closed,
    ProductNotFound,
    Existing(JoinedToken),
    Created(JoinedToken),
}

/// Runs `join.lua`: returns the user's live token for the product, or creates one
/// in the lobby (before the sale opens) or at the end of the queue.
pub async fn join_waiting_room(
    con: &mut RedisConn,
    product_id: i32,
    user_id: i32,
    new_token_id: &str,
) -> redis::RedisResult<JoinOutcome> {
    let reply: Vec<String> = JOIN_SCRIPT
        .key(closed_key(product_id))
        .key(stock_total_key(product_id))
        .key(format!("user_token:{product_id}:{user_id}"))
        .key(format!("lobby_flushed:{product_id}"))
        .key(format!("lobby:{product_id}"))
        .arg(new_token_id)
        .arg(user_id)
        .arg(product_id)
        .invoke_async(con)
        .await?;

    let token = |r: &[String]| JoinedToken {
        token_id: r[1].clone(),
        status: r[2].clone(),
        seq: r[3].parse().ok(),
        // "1" once in Kafka, "lobby" while the admission worker publishes it
        published: !r[4].is_empty(),
    };
    Ok(match reply.first().map(String::as_str) {
        Some("closed") => JoinOutcome::Closed,
        Some("not_found") => JoinOutcome::ProductNotFound,
        Some("existing") => JoinOutcome::Existing(token(&reply)),
        Some("created") => JoinOutcome::Created(token(&reply)),
        _ => return Err((redis::ErrorKind::UnexpectedReturnType, "unexpected join.lua reply").into()),
    })
}

/// Records that the token is in the product's Kafka topic at `offset` and returns
/// its place in line (`seq = offset + 1`). If the token was published twice
/// (a retried join), the first recorded place is kept.
pub async fn mark_published(con: &mut RedisConn, token_id: &str, offset: i64) -> redis::RedisResult<u64> {
    let key = token_key(token_id);
    let (seq,): (u64,) = redis::pipe()
        .atomic()
        .cmd("HSETNX").arg(&key).arg("seq").arg(offset + 1).ignore()
        .cmd("HSET").arg(&key).arg("published").arg(1).ignore()
        .cmd("HGET").arg(&key).arg("seq")
        .query_async(con)
        .await?;
    Ok(seq)
}

// Opening the sale: `lobby:{product}` is the set of tokens that joined early,
// `lobby_order:{product}` the shuffled list they are moved into,
// `lobby_published:{product}` how much of that list is in Kafka, and
// `lobby_flushed:{product}` is set once all of it is (joins then queue directly).

static FLUSH_LOBBY_SCRIPT: LazyLock<redis::Script> =
    LazyLock::new(|| redis::Script::new(include_str!("../scripts/flush_lobby.lua")));

/// Runs `flush_lobby.lua`: moves up to `batch` random lobby tokens, shuffled, to
/// the end of `lobby_order` as QUEUED. Returns true once the lobby is empty and everything
/// in `lobby_order` is published, having set `lobby_flushed`.
pub async fn flush_lobby(con: &mut RedisConn, product_id: i32, batch: usize) -> redis::RedisResult<bool> {
    let (kind, _moved): (String, u64) = FLUSH_LOBBY_SCRIPT
        .key(format!("lobby:{product_id}"))
        .key(format!("lobby_order:{product_id}"))
        .key(format!("lobby_published:{product_id}"))
        .key(format!("lobby_flushed:{product_id}"))
        .arg(batch)
        // For the shuffle; Lua's own generator is not reliably seeded
        .arg(uuid::Uuid::new_v4().as_u128() % (1 << 31))
        .invoke_async(con)
        .await?;
    Ok(kind == "done")
}

/// The next (up to) `max` tokens of `lobby_order` not yet published, and the
/// index of the first one.
pub async fn lobby_unpublished(con: &mut RedisConn, product_id: i32, max: usize) -> redis::RedisResult<(u64, Vec<String>)> {
    let published: Option<u64> = redis::cmd("GET").arg(format!("lobby_published:{product_id}")).query_async(con).await?;
    let start = published.unwrap_or(0);
    let ids: Vec<String> = redis::cmd("LRANGE")
        .arg(format!("lobby_order:{product_id}"))
        .arg(start)
        .arg(start + max as u64 - 1)
        .query_async(con)
        .await?;
    Ok((start, ids))
}

/// Records the Kafka offsets of published lobby tokens as `mark_published`
/// does, and, if given, advances `lobby_published` in the same transaction.
pub async fn record_lobby_published(
    con: &mut RedisConn,
    product_id: i32,
    offsets: &[(String, i64)],
    published_upto: Option<u64>,
) -> redis::RedisResult<()> {
    let mut pipe = redis::pipe();
    pipe.atomic();
    for (token_id, offset) in offsets {
        let key = token_key(token_id);
        pipe.cmd("HSETNX").arg(&key).arg("seq").arg(offset + 1).ignore();
        pipe.cmd("HSET").arg(&key).arg("published").arg(1).ignore();
    }
    if let Some(upto) = published_upto {
        pipe.set(format!("lobby_published:{product_id}"), upto).ignore();
    }
    pipe.query_async(con).await
}

static ADMIT_SCRIPT: LazyLock<redis::Script> = LazyLock::new(|| redis::Script::new(include_str!("../scripts/admit.lua")));

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmitOutcome {
    /// The token is now ACTIVE
    Admitted,
    /// The token expired or was already admitted; move on to the next one
    Skipped,
    /// As many tokens are ACTIVE as the limit allows; retry the same token later
    Full,
    /// The sale is closed; stop admitting
    Closed,
}

/// Runs `admit.lua`: makes a QUEUED token ACTIVE for `ttl_ms` if fewer than
/// `min(max_active, units in stock)` tokens are ACTIVE for the product. `seq` is the
/// token's place in line (its Kafka offset + 1).
pub async fn admit_token(
    con: &mut RedisConn,
    product_id: i32,
    token_id: &str,
    seq: i64,
    max_active: u32,
    ttl_ms: u64,
) -> redis::RedisResult<AdmitOutcome> {
    let reply: String = ADMIT_SCRIPT
        .key(token_key(token_id))
        .key(format!("active_tokens:{product_id}"))
        .key(stock_key(product_id))
        .key(format!("admitted_upto:{product_id}"))
        .key(closed_key(product_id))
        .arg(token_id)
        .arg(seq)
        .arg(max_active)
        .arg(ttl_ms)
        .invoke_async(con)
        .await?;

    Ok(match reply.as_str() {
        "admitted" => AdmitOutcome::Admitted,
        "skipped" => AdmitOutcome::Skipped,
        "full" => AdmitOutcome::Full,
        "closed" => AdmitOutcome::Closed,
        _ => return Err((redis::ErrorKind::UnexpectedReturnType, "unexpected admit.lua reply").into()),
    })
}

static STATUS_SCRIPT: LazyLock<redis::Script> =
    LazyLock::new(|| redis::Script::new(include_str!("../scripts/status.lua")));

/// What `status.lua` saw for a user's token and the product's counters.
#[derive(Debug, Clone)]
pub struct TokenSnapshot {
    pub token_id: String,
    pub status: String,
    pub seq: Option<u64>,
    /// As in `JoinedToken`
    pub published: bool,
    /// Remaining lifetime; `None` when the token has no expiry
    pub expires_in_ms: Option<u64>,
    pub admitted_upto: u64,
    pub available: i64,
    pub sold: i64,
    pub stock_total: i64,
    pub closed: bool,
    pub sale_opens_at: Option<DateTime<Utc>>,
    /// Admissions per second over the last minute of samples; `None` with
    /// fewer than two samples
    pub admit_rate: Option<f64>,
    /// A USED token's reservation still lasts (units held or being paid for)
    pub hold_live: bool,
}

#[derive(Debug, Clone)]
pub enum StatusOutcome {
    NotJoined,
    Expired { token_id: String, closed: bool },
    Token(TokenSnapshot),
}

/// Runs `status.lua`: the caller's token for the product plus everything needed to
/// describe their place in line, in one round trip.
pub async fn waiting_status(con: &mut RedisConn, product_id: i32, user_id: i32) -> redis::RedisResult<StatusOutcome> {
    let r: Vec<String> = STATUS_SCRIPT
        .key(format!("user_token:{product_id}:{user_id}"))
        .key(format!("admitted_upto:{product_id}"))
        .key(stock_key(product_id))
        .key(sold_key(product_id))
        .key(stock_total_key(product_id))
        .key(closed_key(product_id))
        .key(format!("sale_opens_at:{product_id}"))
        .key(admit_samples_key(product_id))
        .invoke_async(con)
        .await?;

    Ok(match r.first().map(String::as_str) {
        Some("none") => StatusOutcome::NotJoined,
        Some("expired") if r.len() == 3 => StatusOutcome::Expired { token_id: r[1].clone(), closed: r[2] == "1" },
        Some("token") if r.len() == 15 => StatusOutcome::Token(TokenSnapshot {
            token_id: r[1].clone(),
            status: r[2].clone(),
            seq: r[3].parse().ok(),
            // "1" once in Kafka, "lobby" while the admission worker publishes it
        published: !r[4].is_empty(),
            // PTTL is -1 for no expiry
            expires_in_ms: r[5].parse().ok(),
            admitted_upto: r[6].parse().unwrap_or(0),
            available: r[7].parse().unwrap_or(0),
            sold: r[8].parse().unwrap_or(0),
            stock_total: r[9].parse().unwrap_or(0),
            closed: r[10] == "1",
            sale_opens_at: DateTime::parse_from_rfc3339(&r[11]).ok().map(|t| t.with_timezone(&Utc)),
            admit_rate: admit_rate(&r[12], &r[13]),
            hold_live: r[14] == "1",
        }),
        _ => return Err((redis::ErrorKind::UnexpectedReturnType, "unexpected status.lua reply").into()),
    })
}

// Unit holds: `resv:{id}` is JSON {user, product, token, quantity}, and
// `holds:expiry` scores each reservation id by when its hold ends (the cleanup
// worker releases holds past it). `user_units:{product}:{user}` counts the units
// a user holds or bought, for the per-user limit.

pub const HOLDS_EXPIRY_KEY: &str = "holds:expiry";

// How long `resv:{id}` outlives its hold, so the cleanup worker still finds it
const HOLD_GRACE_MS: u64 = 10 * 60 * 1000;

static RESERVE_SCRIPT: LazyLock<redis::Script> =
    LazyLock::new(|| redis::Script::new(include_str!("../scripts/reserve.lua")));

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReserveOutcome {
    Held { reservation_id: String, expires_at: DateTime<Utc> },
    /// The user's token already made this hold (a retried request)
    Existing { reservation_id: String, expires_at: DateTime<Utc>, quantity: i32 },
    Closed,
    /// The caller's token for the product is not ACTIVE
    NotAdmitted,
    /// The caller already made a reservation with their turn, and it is over
    TurnUsed,
    /// The quantity is not a whole number from 1 to the per-user limit
    InvalidQuantity { max: u32 },
    TooMany { held: u32 },
    /// Fewer units are left than asked for
    OutOfStock { available: i64 },
}

/// Runs `reserve.lua`: holds `quantity` units for `hold_ttl_ms`, or none, and
/// uses up the caller's ACTIVE token. The per-user limit is read inside the
/// script from `max_per_user:{product}`. Redis only: the order row is created
/// when payment starts.
pub async fn reserve_units(
    con: &mut RedisConn,
    product_id: i32,
    user_id: i32,
    reservation_id: &str,
    quantity: i32,
    hold_ttl_ms: u64,
) -> redis::RedisResult<ReserveOutcome> {
    let r: Vec<String> = RESERVE_SCRIPT
        .key(format!("user_token:{product_id}:{user_id}"))
        .key(stock_key(product_id))
        .key(format!("user_units:{product_id}:{user_id}"))
        .key(HOLDS_EXPIRY_KEY)
        .key(format!("active_tokens:{product_id}"))
        .key(closed_key(product_id))
        .key(stock_total_key(product_id))
        .key(max_per_user_key(product_id))
        .arg(reservation_id)
        .arg(user_id)
        .arg(product_id)
        .arg(hold_ttl_ms)
        .arg(HOLD_GRACE_MS)
        .arg(quantity)
        .invoke_async(con)
        .await?;

    let unexpected = || redis::RedisError::from((redis::ErrorKind::UnexpectedReturnType, "unexpected reserve.lua reply"));
    Ok(match (r.first().map(String::as_str), r.len()) {
        (Some("ok"), 3) => ReserveOutcome::Held {
            reservation_id: r[1].clone(),
            expires_at: r[2].parse().ok().and_then(DateTime::from_timestamp_millis).ok_or_else(unexpected)?,
        },
        (Some("existing"), 4) => ReserveOutcome::Existing {
            reservation_id: r[1].clone(),
            expires_at: r[2].parse().ok().and_then(DateTime::from_timestamp_millis).ok_or_else(unexpected)?,
            quantity: r[3].parse().map_err(|_| unexpected())?,
        },
        (Some("closed"), _) => ReserveOutcome::Closed,
        (Some("not_admitted"), _) => ReserveOutcome::NotAdmitted,
        (Some("turn_used"), _) => ReserveOutcome::TurnUsed,
        (Some("invalid_quantity"), 2) => ReserveOutcome::InvalidQuantity { max: r[1].parse().unwrap_or(0) },
        (Some("too_many"), 2) => ReserveOutcome::TooMany { held: r[1].parse().unwrap_or(0) },
        (Some("out_of_stock"), 2) => ReserveOutcome::OutOfStock { available: r[1].parse().unwrap_or(0) },
        _ => return Err(unexpected()),
    })
}

static RELEASE_SCRIPT: LazyLock<redis::Script> =
    LazyLock::new(|| redis::Script::new(include_str!("../scripts/release.lua")));

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleaseOutcome {
    Released,
    /// No live hold: never existed, already released, expired or paid
    NotFound,
    /// The hold belongs to another user
    Forbidden,
    /// Payment has started; only expiry or the payment outcome ends the hold
    PaymentInProgress,
}

/// Runs `release.lua`: returns the hold's units to stock. The user's token stays USED.
/// `owner` is checked in the same call; `None` is the system.
pub async fn release_hold(
    con: &mut RedisConn,
    reservation_id: &str,
    owner: Option<i32>,
) -> redis::RedisResult<ReleaseOutcome> {
    let reply: String = RELEASE_SCRIPT
        .key(format!("resv:{reservation_id}"))
        .key(HOLDS_EXPIRY_KEY)
        .arg(reservation_id)
        .arg(owner.map(|id| id.to_string()).unwrap_or_default())
        .invoke_async(con)
        .await?;
    Ok(match reply.as_str() {
        "released" => ReleaseOutcome::Released,
        "not_found" => ReleaseOutcome::NotFound,
        "forbidden" => ReleaseOutcome::Forbidden,
        "paying" => ReleaseOutcome::PaymentInProgress,
        _ => return Err((redis::ErrorKind::UnexpectedReturnType, "unexpected release.lua reply").into()),
    })
}

/// A hold on units as stored in `resv:{id}`
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Hold {
    pub user: i32,
    pub product: i32,
    pub token: String,
    pub quantity: i32,
    /// `held` (when absent), `paying` or `paid`
    #[serde(default)]
    pub state: Option<String>,
    /// The payment intent that claimed it
    #[serde(default)]
    pub paid_by: Option<String>,
}

fn parse_hold(json: &str) -> redis::RedisResult<Hold> {
    serde_json::from_str(json)
        .map_err(|e| (redis::ErrorKind::UnexpectedReturnType, "resv:{id} is not a hold", e.to_string()).into())
}

static START_PAYMENT_SCRIPT: LazyLock<redis::Script> =
    LazyLock::new(|| redis::Script::new(include_str!("../scripts/start_payment.lua")));

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartPaymentOutcome {
    /// Extended once and now `paying` (unchanged if payment had already started)
    Paying { hold: Hold, expires_at: DateTime<Utc> },
    /// Expired or released
    Gone,
    Forbidden,
    Paid,
}

/// Runs `start_payment.lua`: extends the caller's hold once by `extension_ms`
/// and marks it `paying`, so it can no longer be released.
pub async fn start_payment(
    con: &mut RedisConn,
    reservation_id: &str,
    user_id: i32,
    extension_ms: u64,
) -> redis::RedisResult<StartPaymentOutcome> {
    let r: Vec<String> = START_PAYMENT_SCRIPT
        .key(format!("resv:{reservation_id}"))
        .key(HOLDS_EXPIRY_KEY)
        .arg(reservation_id)
        .arg(user_id)
        .arg(extension_ms)
        .arg(HOLD_GRACE_MS)
        .invoke_async(con)
        .await?;
    let unexpected = || redis::RedisError::from((redis::ErrorKind::UnexpectedReturnType, "unexpected start_payment.lua reply"));
    Ok(match (r.first().map(String::as_str), r.len()) {
        (Some("paying"), 3) => StartPaymentOutcome::Paying {
            hold: parse_hold(&r[1])?,
            expires_at: r[2].parse().ok().and_then(DateTime::from_timestamp_millis).ok_or_else(unexpected)?,
        },
        (Some("gone"), _) => StartPaymentOutcome::Gone,
        (Some("forbidden"), _) => StartPaymentOutcome::Forbidden,
        (Some("paid"), _) => StartPaymentOutcome::Paid,
        _ => return Err(unexpected()),
    })
}

// The payment worker is synchronous (it holds an inbox row lock across these
// calls), so the confirmation scripts take a blocking connection.

static CONFIRM_SCRIPT: LazyLock<redis::Script> =
    LazyLock::new(|| redis::Script::new(include_str!("../scripts/confirm.lua")));
static FINALIZE_SCRIPT: LazyLock<redis::Script> =
    LazyLock::new(|| redis::Script::new(include_str!("../scripts/finalize.lua")));

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimOutcome {
    /// This payment owns the hold (also on a retry of the same payment)
    Won(Hold),
    /// Expired, or claimed by another payment: refund
    Gone,
}

/// How long a claimed hold is kept while its confirmation waits for the
/// payment worker. Far longer than any backlog; the commit deletes it.
pub const CLAIMED_HOLD_TTL_MS: u64 = 24 * 60 * 60 * 1000;

/// Runs `confirm.lua`: phase 1 of a confirmation (claim, commit, finalize).
/// Called when the success event arrives and again by the payment worker.
pub fn claim_hold(con: &mut dyn redis::ConnectionLike, reservation_id: &str, intent_id: &str) -> redis::RedisResult<ClaimOutcome> {
    let r: Vec<String> = CONFIRM_SCRIPT
        .key(format!("resv:{reservation_id}"))
        .key(HOLDS_EXPIRY_KEY)
        .arg(reservation_id)
        .arg(intent_id)
        .arg(CLAIMED_HOLD_TTL_MS)
        .invoke(con)?;
    Ok(match (r.first().map(String::as_str), r.len()) {
        (Some("won"), 2) => ClaimOutcome::Won(parse_hold(&r[1])?),
        (Some("gone"), _) => ClaimOutcome::Gone,
        _ => return Err((redis::ErrorKind::UnexpectedReturnType, "unexpected confirm.lua reply").into()),
    })
}

/// Runs `finalize.lua` after the Postgres commit: deletes the hold and counts
/// its units as sold, or, for a refund, stops counting them for the user.
/// Returns false if it was already finalized.
pub fn finalize_hold(con: &mut dyn redis::ConnectionLike, reservation_id: &str, sold: bool) -> redis::RedisResult<bool> {
    FINALIZE_SCRIPT
        .key(format!("resv:{reservation_id}"))
        .arg(if sold { "sold" } else { "refunded" })
        .invoke(con)
}

// Expiry and recovery, for the cleanup worker (synchronous, like the payment worker)

static CLAIM_EXPIRED_SCRIPT: LazyLock<redis::Script> =
    LazyLock::new(|| redis::Script::new(include_str!("../scripts/claim_expired.lua")));

/// A hold released by `claim_expired`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpiredHold {
    pub reservation_id: String,
    /// `paying` holds have an order to mark EXPIRED; `held` ones have none
    pub state: String,
}

/// Runs `claim_expired.lua`: releases up to `batch` past-due holds. Returns
/// how many were due (equal to `batch` means more may be) and those released.
pub fn claim_expired(con: &mut dyn redis::ConnectionLike, batch: usize) -> redis::RedisResult<(usize, Vec<ExpiredHold>)> {
    let r: Vec<String> = CLAIM_EXPIRED_SCRIPT.key(HOLDS_EXPIRY_KEY).arg(batch).invoke(con)?;
    let scanned = r.first().and_then(|n| n.parse().ok()).unwrap_or(0);
    let released = r[1.min(r.len())..]
        .chunks_exact(2)
        .map(|pair| ExpiredHold { reservation_id: pair[0].clone(), state: pair[1].clone() })
        .collect();
    Ok((scanned, released))
}

/// Ids of the live holds for a product, by scanning `resv:*`. Only used by
/// the rebuild after Redis lost data, never on a request path.
pub fn live_holds(con: &mut redis::Connection, product_id: i32) -> redis::RedisResult<Vec<String>> {
    let keys: Vec<String> = redis::cmd("SCAN")
        .cursor_arg(0)
        .arg("MATCH")
        .arg("resv:*")
        .arg("COUNT")
        .arg(1000)
        .clone()
        .iter(con)?
        .collect::<redis::RedisResult<_>>()?;
    let mut ids = Vec::new();
    for chunk in keys.chunks(500) {
        let values: Vec<Option<String>> = redis::cmd("MGET").arg(chunk).query(con)?;
        for (key, value) in chunk.iter().zip(values) {
            let Some(hold) = value.and_then(|json| parse_hold(&json).ok()) else { continue };
            if hold.product == product_id {
                ids.push(key.trim_start_matches("resv:").to_owned());
            }
        }
    }
    Ok(ids)
}

static REBUILD_SCRIPT: LazyLock<redis::Script> = LazyLock::new(|| redis::Script::new(include_str!("../scripts/rebuild.lua")));

/// What Postgres and the hold scan say about a product whose counters are gone
#[derive(Debug, Clone, serde::Serialize)]
pub struct RebuildInput {
    #[serde(skip)]
    pub stock_total: i32,
    #[serde(skip)]
    pub sold: i64,
    #[serde(skip)]
    pub max_per_user: i32,
    #[serde(skip)]
    pub sale_opens_at: DateTime<Utc>,
    #[serde(skip)]
    pub closed: bool,
    #[serde(skip)]
    pub on_sale: bool,
    pub holds: Vec<String>,
    /// Paid units per user
    pub user_units: std::collections::HashMap<String, i64>,
}

/// Runs `rebuild.lua`. Returns false if the product was already there.
pub fn rebuild_product(con: &mut dyn redis::ConnectionLike, product_id: i32, input: &RebuildInput) -> redis::RedisResult<bool> {
    let json = serde_json::to_string(input)
        .map_err(|e| redis::RedisError::from((redis::ErrorKind::Client, "encode rebuild input", e.to_string())))?;
    REBUILD_SCRIPT
        .key(stock_key(product_id))
        .key(stock_total_key(product_id))
        .key(sold_key(product_id))
        .key(max_per_user_key(product_id))
        .key(format!("sale_opens_at:{product_id}"))
        .key(closed_key(product_id))
        .key(format!("lobby_flushed:{product_id}"))
        .arg(product_id)
        .arg(input.stock_total)
        .arg(input.sold)
        .arg(input.sale_opens_at.to_rfc3339())
        .arg(if input.closed { 1 } else { 0 })
        .arg(if input.on_sale { 1 } else { 0 })
        .arg(input.max_per_user)
        .arg(json)
        .invoke(con)
}

/// Set when the sale closes, for good: joins get `410` and waiting tokens
/// report CLOSED
pub fn closed_key(product_id: i32) -> String {
    format!("closed:{product_id}")
}

pub async fn close_product(con: &mut RedisConn, product_id: i32) -> redis::RedisResult<()> {
    redis::cmd("SET").arg(closed_key(product_id)).arg(1).query_async(con).await
}

static CLOSE_SCRIPT: LazyLock<redis::Script> = LazyLock::new(|| redis::Script::new(include_str!("../scripts/close.lua")));

/// Runs `close.lua`: sets the closed flag if the product exists in Redis (has
/// a `stock_total`). Returns false for an unknown product, which gets no flag.
pub async fn close_existing_product(con: &mut RedisConn, product_id: i32) -> redis::RedisResult<bool> {
    CLOSE_SCRIPT.key(stock_total_key(product_id)).key(closed_key(product_id)).invoke_async(con).await
}

pub async fn is_closed(con: &mut RedisConn, product_id: i32) -> redis::RedisResult<bool> {
    redis::cmd("EXISTS").arg(closed_key(product_id)).query_async(con).await
}

// Admission rate: the admission worker pushes "{now_ms}:{admitted_upto}" to
// `admit_samples:{product}` every few seconds, keeping the last minute.

pub fn admit_samples_key(product_id: i32) -> String {
    format!("admit_samples:{product_id}")
}

/// Samples kept: one every 5 seconds for a minute
pub const ADMIT_SAMPLES_KEPT: isize = 12;

/// Records how far admission has got, for the estimated wait.
pub async fn record_admit_sample(con: &mut RedisConn, product_id: i32, now_ms: i64) -> redis::RedisResult<()> {
    let upto: Option<u64> = redis::cmd("GET").arg(format!("admitted_upto:{product_id}")).query_async(con).await?;
    let key = admit_samples_key(product_id);
    redis::pipe()
        .atomic()
        .lpush(&key, format!("{now_ms}:{}", upto.unwrap_or(0)))
        .ltrim(&key, 0, ADMIT_SAMPLES_KEPT - 1)
        .query_async(con)
        .await
}

/// Admissions per second between the oldest and newest sample; `None` if
/// there are not two samples yet (the newest and oldest are then the same).
fn admit_rate(newest: &str, oldest: &str) -> Option<f64> {
    let parse = |s: &str| -> Option<(f64, f64)> {
        let (ms, upto) = s.split_once(':')?;
        Some((ms.parse().ok()?, upto.parse().ok()?))
    };
    let ((t1, n1), (t0, n0)) = (parse(newest)?, parse(oldest)?);
    (t1 > t0).then(|| (n1 - n0).max(0.0) * 1000.0 / (t1 - t0))
}

#[cfg(test)]
mod tests {
    use super::admit_rate;

    #[test]
    fn admit_rate_from_samples() {
        // 50 admitted in 10 seconds
        assert_eq!(admit_rate("20000:80", "10000:30"), Some(5.0));
        assert_eq!(admit_rate("20000:30", "10000:30"), Some(0.0));
        // One sample, or none
        assert_eq!(admit_rate("10000:30", "10000:30"), None);
        assert_eq!(admit_rate("", ""), None);
    }
}
