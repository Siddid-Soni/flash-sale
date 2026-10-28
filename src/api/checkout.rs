//! `POST /v1/sales/{sale_id}/checkout`: the hot path of the flash sale.
//!
//! One Lua call decides (`checkout.lua`); winners then go through Kafka and
//! get `202` with a ticket to poll. PostgreSQL is touched only when a publish
//! fails (below), and once a replica knows a sale is sold out it does not touch
//! Redis either.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::Duration;

use actix_web::{http::header, post, web, HttpRequest, HttpResponse};
use serde::Serialize;

use crate::{
    auth::AuthUser,
    actions::orders::FailedInsert,
    cache::{self, CheckoutOutcome, ReadCache, RedisConn, StatusUpdate},
    db::DbPool,
    error::{ApiError, ErrorBody},
    models::OrderStatus,
    queue::{CheckoutEvent, SharedQueue},
};

const KEY_LEN: std::ops::RangeInclusive<usize> = 1..=255;
/// How often the refresher reads the stock of the sales a replica has seen
pub const SOLD_OUT_REFRESH: Duration = Duration::from_millis(100);
/// Sales one replica tracks; checkouts for more are simply not short-circuited
const MAX_TRACKED_SALES: usize = 1_000;

/// Which sales this replica believes are sold out. After the last unit is
/// gone, the thousands of requests that follow are answered `410` from
/// memory. A sale is only tracked after Redis has answered for it once, and
/// the flag is refreshed from `stock:{sale}` every 100 ms by `run_refresher`,
/// so it clears again when an expired order returns a unit. Unknown sales go
/// to Redis.
#[derive(Default)]
pub struct SoldOutFlags {
    sales: RwLock<HashMap<i32, bool>>,
}

impl SoldOutFlags {
    /// Whether the sale is tracked and had no stock at the last refresh
    pub fn is_sold_out(&self, sale: i32) -> bool {
        self.sales.read().unwrap_or_else(|poisoned| poisoned.into_inner()).get(&sale).copied().unwrap_or(false)
    }

    /// Starts tracking a sale (not sold out until the next refresh says so)
    pub fn watch(&self, sale: i32) {
        if self.sales.read().unwrap_or_else(|poisoned| poisoned.into_inner()).contains_key(&sale) {
            return;
        }
        let mut sales = self.sales.write().unwrap_or_else(|poisoned| poisoned.into_inner());
        if sales.len() < MAX_TRACKED_SALES {
            sales.entry(sale).or_insert(false);
        }
    }

    /// The tracked sales
    pub fn watched(&self) -> Vec<i32> {
        self.sales.read().unwrap_or_else(|poisoned| poisoned.into_inner()).keys().copied().collect()
    }

    /// Sets the flags from the stock levels read for `sales`; a missing key
    /// counts as no stock
    pub fn apply(&self, sales: &[i32], levels: &[Option<i64>]) {
        let mut tracked = self.sales.write().unwrap_or_else(|poisoned| poisoned.into_inner());
        for (sale, level) in sales.iter().zip(levels) {
            if let Some(flag) = tracked.get_mut(sale) {
                *flag = level.unwrap_or(0) <= 0;
            }
        }
    }

    /// One refresh: reads the stock of every tracked sale and sets the flags
    pub async fn refresh(&self, con: &mut RedisConn) -> redis::RedisResult<()> {
        let sales = self.watched();
        let levels = cache::stock_levels(con, &sales).await?;
        self.apply(&sales, &levels);
        Ok(())
    }
}

/// Refreshes the flags every `SOLD_OUT_REFRESH`, forever: one task per
/// replica. A failed refresh keeps the previous flags and is logged once per
/// outage.
pub async fn run_refresher(flags: web::Data<SoldOutFlags>, mut con: RedisConn) {
    let mut ticker = tokio::time::interval(SOLD_OUT_REFRESH);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut failing = false;
    loop {
        ticker.tick().await;
        match flags.refresh(&mut con).await {
            Ok(()) => failing = false,
            Err(e) if !failing => {
                failing = true;
                log::warn!("sold-out flag refresh failed: {e}");
            }
            Err(_) => {}
        }
    }
}

#[derive(Debug, Serialize)]
struct CheckoutResponse {
    queue_ticket_id: String,
    status_url: String,
}

fn ticket_response(mut builder: actix_web::HttpResponseBuilder, ticket: String) -> HttpResponse {
    let status_url = format!("/v1/orders/status/{ticket}");
    builder.insert_header((header::LOCATION, status_url.clone())).json(CheckoutResponse { queue_ticket_id: ticket, status_url })
}

fn idempotency_key(req: &HttpRequest) -> Result<String, ApiError> {
    let key = req
        .headers()
        .get("Idempotency-Key")
        .and_then(|value| value.to_str().ok())
        .filter(|key| KEY_LEN.contains(&key.chars().count()))
        .ok_or_else(|| ApiError::BadRequest("an Idempotency-Key header of 1-255 characters is required".into()))?;
    Ok(key.to_string())
}

/// Reserves one unit for the caller.
///
/// - `202` with the new ticket; `200` with the same body for a retry of the
///   same `Idempotency-Key`
/// - `403` before the sale opens, `409` if the caller already holds or bought
///   one, `410` if sold out, `404` for a sale Redis does not know
/// - `503` (`Retry-After: 1`) if the publish failed: the order is recorded
///   `FAILED` in PostgreSQL and the unit returned at once, and the key
///   forgotten, so a retry with the same key starts over
///
/// A winner retrying after the last unit was taken gets `410` from a replica
/// whose flag is set, because that answer is given without asking Redis.
#[post("/sales/{sale_id}/checkout")]
// One parameter per extractor
#[allow(clippy::too_many_arguments)]
async fn checkout(
    req: HttpRequest,
    user: AuthUser,
    path: web::Path<i32>,
    pool: web::Data<DbPool>,
    redis: web::Data<RedisConn>,
    read_cache: web::Data<ReadCache>,
    queue: web::Data<SharedQueue>,
    flags: web::Data<SoldOutFlags>,
) -> Result<HttpResponse, ApiError> {
    let sale = path.into_inner();
    let key = idempotency_key(&req)?;

    if flags.is_sold_out(sale) {
        return Err(ApiError::Gone("sold out".into()));
    }

    let mut con = redis.get_ref().clone();
    let ticket = cache::new_ticket(sale);
    let outcome = cache::checkout(&mut con, sale, user.id, &key, &ticket).await?;
    if outcome != CheckoutOutcome::NotFound {
        flags.watch(sale);
    }

    let checkout_ms = match outcome {
        CheckoutOutcome::Reserved { checkout_ms } => checkout_ms,
        CheckoutOutcome::Existing { ticket } => return Ok(ticket_response(HttpResponse::Ok(), ticket)),
        CheckoutOutcome::NotStarted { .. } => return Err(ApiError::Forbidden("the sale has not started".into())),
        CheckoutOutcome::NotFound => return Err(ApiError::NotFound(format!("sale {sale} not found"))),
        CheckoutOutcome::AlreadyLive => return Err(ApiError::Conflict("you already hold or bought one".into())),
        CheckoutOutcome::SoldOut => return Err(ApiError::Gone("sold out".into())),
    };

    // The event carries everything the processor needs: Redis keeps no record
    // of the ticket
    let event = CheckoutEvent { ticket: ticket.clone(), sale, user: user.id, checkout_ms, idempotency_key: key.clone() };
    let published = match queue.publish(&event).await {
        Ok(published) => published,
        Err(e) => {
            log::error!("publish of {ticket} failed, compensating: {e}");
            return compensate(&pool, &mut con, &read_cache, sale, user.id, ticket, &key).await;
        }
    };

    // The reservation stands whether or not this write lands: the processor
    // creates the order from the message, and until then the user just sees no
    // place in the queue
    let mut status = StatusUpdate::new(OrderStatus::Pending, user.id);
    status.partition = Some(published.partition);
    status.offset = Some(published.offset);
    status.sale = Some(sale);
    let mut cache_con = read_cache.get_ref().clone();
    if let Err(e) = cache::set_status(&mut cache_con, &ticket, &status).await {
        log::warn!("status write for {ticket} failed: {e}");
    }

    Ok(ticket_response(HttpResponse::Accepted(), ticket))
}

/// The publish failed. PostgreSQL decides what happens to the unit: the order
/// is inserted as `FAILED` (`ON CONFLICT DO NOTHING` on the ticket), and only a
/// call that created the row returns the unit to stock, clears `user_live` (if
/// it still names the ticket) and forgets the idempotency key, then answers
/// `503`. If the row already existed the message reached Kafka after all and a
/// processor made the order: the checkout succeeded. If PostgreSQL cannot be
/// reached the unit stays reserved (lost for the rest of the sale: accepted,
/// `docs/FAILURE_CASES.md` case 1) and the answer is still `503`.
async fn compensate(
    pool: &web::Data<DbPool>,
    con: &mut RedisConn,
    read_cache: &web::Data<ReadCache>,
    sale: i32,
    user: i32,
    ticket: String,
    key: &str,
) -> Result<HttpResponse, ApiError> {
    let (pool, lookup) = (pool.get_ref().clone(), ticket.clone());
    let recorded = web::block(move || -> Result<FailedInsert, ApiError> {
        let mut conn = pool.get()?;
        Ok(crate::actions::orders::insert_failed(&mut conn, user, sale, &lookup)?)
    })
    .await;
    match recorded {
        Ok(Ok(FailedInsert::Created)) => {
            match cache::return_unit(con, sale, user, &ticket, Some(key)).await {
                Ok(()) => log::info!("compensation for {ticket}: unit returned"),
                Err(e) => log::error!("compensation for {ticket}: the unit could not be returned: {e}"),
            }
            // The status endpoint reads only the read cache
            write_status(read_cache, &ticket, OrderStatus::Failed, user).await;
        }
        Ok(Ok(FailedInsert::Existed)) => {
            log::info!("{ticket}: its order exists, the message reached the queue after all");
            // Forward-only: never overrides the processor's link, which follows
            write_status(read_cache, &ticket, OrderStatus::Pending, user).await;
            return Ok(ticket_response(HttpResponse::Accepted(), ticket));
        }
        Ok(Err(e)) => log::error!("compensation for {ticket}: recording the failure failed, the unit stays reserved: {e}"),
        Err(e) => log::error!("compensation for {ticket}: recording the failure failed, the unit stays reserved: {e}"),
    }
    Ok(HttpResponse::ServiceUnavailable()
        .insert_header((header::RETRY_AFTER, "1"))
        .json(ErrorBody { message: "the checkout queue is unavailable, retry with the same key".into() }))
}

/// Best effort, like the write after a successful publish
async fn write_status(read_cache: &web::Data<ReadCache>, ticket: &str, status: OrderStatus, user: i32) {
    let mut status_con = read_cache.get_ref().clone();
    if let Err(e) = cache::set_status(&mut status_con, ticket, &StatusUpdate::new(status, user)).await {
        log::warn!("status write for {ticket} failed: {e}");
    }
}

// POST /v1/sales/{sale_id}/checkout
pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(checkout);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flags_follow_stock_and_clear_when_it_returns() {
        let flags = SoldOutFlags::default();
        assert!(!flags.is_sold_out(1), "unknown sales go to Redis");
        flags.apply(&[1], &[Some(0)]);
        assert!(!flags.is_sold_out(1), "untracked sales are not flagged");

        flags.watch(1);
        flags.watch(2);
        flags.apply(&[1, 2], &[Some(0), Some(5)]);
        assert!(flags.is_sold_out(1));
        assert!(!flags.is_sold_out(2));

        flags.apply(&[1, 2], &[Some(1), None]);
        assert!(!flags.is_sold_out(1));
        assert!(flags.is_sold_out(2), "a missing key counts as no stock");
    }
}
