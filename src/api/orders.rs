//! `GET /v1/orders/status/{ticket}`: what the client polls (only winners do).
//!
//! The answer comes from the read cache. If that entry is missing (the write
//! after a transition was lost, or the entry expired) it falls back to the
//! order in PostgreSQL, so a lost cache write never shows a winner the wrong
//! thing. `PENDING` (waiting in Kafka) comes only from the cache: the order row
//! does not exist yet, and it is never stored in PostgreSQL. A ticket with
//! neither a cache entry nor an order is `404` (docs/FAILURE_CASES.md, case 3).

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

use actix_web::{get, http::header, web, HttpResponse};
use chrono::SecondsFormat;
use serde::Serialize;

use crate::{
    actions,
    auth::AuthUser,
    cache::{self, ReadCache},
    config::Config,
    db::DbPool,
    error::ApiError,
    models::{Order, OrderState, OrderStatus},
};

/// How long a replica reuses a partition's processor progress before reading
/// it again, so polling clients do not each cause a Redis read
const PROGRESS_TTL: Duration = Duration::from_millis(500);

/// Per-replica cache of `progress:<topic>:<partition>` (see
/// `cache::progress_key`), refreshed at most every 500 ms.
#[derive(Default)]
pub struct ProgressCache {
    /// partition -> (when it was read, the value)
    entries: RwLock<HashMap<i32, (Instant, Option<i64>)>>,
}

impl ProgressCache {
    /// The highest processed offset of a partition (`None` if none recorded).
    /// A failed read counts as unknown: the position is then an upper bound.
    async fn get(&self, read_cache: &mut ReadCache, topic: &str, partition: i32) -> Option<i64> {
        let fresh = self
            .entries
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&partition)
            .filter(|(read_at, _)| read_at.elapsed() < PROGRESS_TTL)
            .map(|(_, value)| *value);
        if let Some(value) = fresh {
            return value;
        }
        let value = match cache::read_progress(read_cache, topic, partition).await {
            Ok(value) => value,
            Err(e) => {
                log::warn!("progress of partition {partition} unreadable: {e}");
                None
            }
        };
        self.entries.write().unwrap_or_else(|poisoned| poisoned.into_inner()).insert(partition, (Instant::now(), value));
        value
    }
}

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

/// The queue position of a `PENDING` ticket: how many messages of its
/// partition the processors still have to finish up to and including this
/// one, `offset - processed_offset`, where `processed_offset` is the highest
/// offset with everything before it done. 1 means it is next.
///
/// `0` means "processing": a stale cached progress value (read up to 500 ms
/// ago, or written by a processor after the ticket's status was) can put the
/// progress at or past the ticket's offset, so the difference is clamped at 0
/// instead of going negative. With no progress recorded yet the position is
/// `offset + 1`, the number of messages ahead of it plus itself.
///
/// The count includes other sales' messages on the same topic: the topic is
/// shared by all sales, so this is an upper bound on the place in line for
/// this sale. That is accepted; it is a progress indicator, not a promise.
fn queue_position(offset: i64, processed_offset: Option<i64>) -> i64 {
    match processed_offset {
        Some(processed) => (offset - processed).max(0),
        None => offset + 1,
    }
}

fn order_response(order: Order) -> StatusResponse {
    let awaiting = order.status == OrderState::AwaitingPayment;
    StatusResponse {
        status: order.status.into(),
        position: None,
        payment_url: order.checkout_url.filter(|_| awaiting),
        expires_at: order.expires_at.filter(|_| awaiting).map(|at| at.to_rfc3339_opts(SecondsFormat::Millis, true)),
    }
}

/// The status of the caller's ticket: `200 {status, position?, payment_url?,
/// expires_at?}`, `403` for someone else's ticket, `404` if nobody knows it.
/// `position` (only while `PENDING`, and only if the entry knows its
/// partition and offset) is the place in the queue, 1 being next, and `0`
/// while the ticket is being processed (see `queue_position`).
/// Never cached: the answer changes every few seconds.
#[get("/orders/status/{ticket}")]
async fn order_status(
    user: AuthUser,
    path: web::Path<String>,
    pool: web::Data<DbPool>,
    read_cache: web::Data<ReadCache>,
    progress: web::Data<ProgressCache>,
    config: web::Data<Config>,
) -> Result<HttpResponse, ApiError> {
    let ticket = path.into_inner();
    let not_found = || ApiError::NotFound("no such ticket".into());
    let forbidden = || ApiError::Forbidden("this ticket belongs to someone else".into());
    cache::ticket_sale(&ticket).ok_or_else(not_found)?;

    // 1. The read cache
    let mut cache_con = read_cache.get_ref().clone();
    if let Some(cached) = cache::read_status(&mut cache_con, &ticket).await? {
        if cached.user != user.id {
            return Err(forbidden());
        }
        let position = match (cached.status, cached.partition, cached.offset) {
            (OrderStatus::Pending, Some(partition), Some(offset)) => {
                let processed = progress.get(&mut cache_con, &config.checkout_topic, partition).await;
                Some(queue_position(offset, processed))
            }
            _ => None,
        };
        // The link and deadline stay in the entry after payment; only an unpaid order shows them
        let awaiting = cached.status == OrderStatus::AwaitingPayment;
        return Ok(respond(StatusResponse {
            status: cached.status,
            position,
            payment_url: cached.payment_url.filter(|_| awaiting),
            expires_at: cached.expires_at.filter(|_| awaiting),
        }));
    }

    // 2. PostgreSQL
    let lookup = ticket.clone();
    let order = web::block(move || {
        let mut conn = pool.get()?;
        Ok::<_, ApiError>(actions::orders::find_by_ticket(&mut conn, &lookup)?)
    })
    .await??;
    match order {
        Some(order) if order.user_id != user.id => Err(forbidden()),
        Some(order) => Ok(respond(order_response(order))),
        None => Err(not_found()),
    }
}

fn respond(body: StatusResponse) -> HttpResponse {
    HttpResponse::Ok().insert_header((header::CACHE_CONTROL, "no-store")).json(body)
}

// GET /v1/orders/status/{ticket}
pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(order_status);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn position_counts_down_and_clamps_at_zero() {
        // Nothing processed yet: offset 0 is first in line
        assert_eq!(queue_position(0, None), 1);
        assert_eq!(queue_position(41, None), 42);
        // Everything up to offset 9 is done: offset 10 is next
        assert_eq!(queue_position(10, Some(9)), 1);
        assert_eq!(queue_position(15, Some(9)), 6);
        // The progress reached (or, stale, passed) the ticket: being processed
        assert_eq!(queue_position(10, Some(10)), 0);
        assert_eq!(queue_position(10, Some(25)), 0);
    }
}
