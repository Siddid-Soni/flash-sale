//! `GET /v1/orders/status/{ticket}`: what the client polls (only winners do).
//!
//! The answer comes from the read cache. If that entry is missing (the write
//! after a transition was lost, or the entry expired) it falls back to the
//! hold in the inventory Redis, then to the order in PostgreSQL, so a lost
//! cache write never shows a winner the wrong thing.

use actix_web::{get, http::header, web, HttpResponse};
use chrono::SecondsFormat;
use serde::Serialize;

use crate::{
    actions,
    auth::AuthUser,
    cache::{self, ReadCache, RedisConn},
    db::DbPool,
    error::ApiError,
    models::{Order, OrderStatus},
};

#[derive(Debug, Serialize, PartialEq, Eq)]
struct StatusResponse {
    status: OrderStatus,
    /// Place in the queue, while `PENDING`
    #[serde(skip_serializing_if = "Option::is_none")]
    position: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    payment_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    expires_at: Option<String>,
}

impl StatusResponse {
    fn bare(status: OrderStatus) -> Self {
        Self { status, position: None, payment_url: None, expires_at: None }
    }
}

/// The queue position shown for a `PENDING` ticket.
///
/// For now only the message's offset in its partition is known, so the
/// position is that offset plus one: how many messages precede it in the
/// partition, not how many are still unprocessed. Stage 4 replaces this with
/// the position relative to what the processors have finished (their
/// progress per partition), which is why it takes the partition too.
fn queue_position(_partition: Option<i32>, offset: Option<i64>) -> Option<i64> {
    offset.map(|offset| offset + 1)
}

fn order_response(order: Order) -> StatusResponse {
    let awaiting = order.status == OrderStatus::AwaitingPayment;
    StatusResponse {
        status: order.status,
        position: None,
        payment_url: order.checkout_url.filter(|_| awaiting),
        expires_at: order.expires_at.filter(|_| awaiting).map(|at| at.to_rfc3339_opts(SecondsFormat::Millis, true)),
    }
}

/// The status of the caller's ticket: `200 {status, position?, payment_url?,
/// expires_at?}`, `403` for someone else's ticket, `404` if nobody knows it.
/// Never cached: the answer changes every few seconds.
#[get("/orders/status/{ticket}")]
async fn order_status(
    user: AuthUser,
    path: web::Path<String>,
    pool: web::Data<DbPool>,
    redis: web::Data<RedisConn>,
    read_cache: web::Data<ReadCache>,
) -> Result<HttpResponse, ApiError> {
    let ticket = path.into_inner();
    let not_found = || ApiError::NotFound("no such ticket".into());
    let forbidden = || ApiError::Forbidden("this ticket belongs to someone else".into());
    let sale = cache::ticket_sale(&ticket).ok_or_else(not_found)?;

    // 1. The read cache
    let mut cache_con = read_cache.get_ref().clone();
    if let Some(cached) = cache::read_status(&mut cache_con, &ticket).await? {
        if cached.user != user.id {
            return Err(forbidden());
        }
        let pending = cached.status == OrderStatus::Pending;
        return Ok(respond(StatusResponse {
            status: cached.status,
            position: cached.offset.filter(|_| pending).and_then(|o| queue_position(cached.partition, Some(o))),
            payment_url: cached.payment_url,
            expires_at: cached.expires_at,
        }));
    }

    // 2. The hold. It knows the state but not the payment link, so an
    // unpaid or paid hold goes on to the order for the rest
    let mut con = redis.get_ref().clone();
    let mut from_hold = None;
    if let Some(hold) = cache::read_hold(&mut con, sale, &ticket).await? {
        if hold.user != user.id {
            return Err(forbidden());
        }
        match OrderStatus::parse(&hold.state) {
            Some(status @ (OrderStatus::Pending | OrderStatus::Failed | OrderStatus::Expired)) => {
                return Ok(respond(StatusResponse::bare(status)));
            }
            Some(OrderStatus::AwaitingPayment) => from_hold = Some(StatusResponse::bare(OrderStatus::AwaitingPayment)),
            // `PAID`: confirmation is in progress, the order row has the state
            _ => {}
        }
    }

    // 3. PostgreSQL
    let lookup = ticket.clone();
    let order = web::block(move || {
        let mut conn = pool.get()?;
        Ok::<_, ApiError>(actions::orders::find_by_ticket(&mut conn, &lookup)?)
    })
    .await??;
    match order {
        Some(order) if order.user_id != user.id => Err(forbidden()),
        Some(order) => Ok(respond(order_response(order))),
        None => from_hold.map(respond).ok_or_else(not_found),
    }
}

fn respond(body: StatusResponse) -> HttpResponse {
    HttpResponse::Ok().insert_header((header::CACHE_CONTROL, "no-store")).json(body)
}

// GET /v1/orders/status/{ticket}
pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(order_status);
}
