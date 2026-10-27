use actix_web::{get, post, web, HttpResponse};
use chrono::{DateTime, Utc};
use serde::Serialize;
use uuid::Uuid;

use crate::{
    auth::AuthUser,
    cache::{self, JoinOutcome, RedisConn, StatusOutcome},
    error::ApiError,
    waiting::SharedQueue,
};

#[derive(Debug, Serialize)]
struct JoinResponse {
    token_id: String,
    /// LOBBY before the sale opens, then QUEUED, ACTIVE, ...
    status: String,
    /// Place in line; absent while in the lobby
    #[serde(skip_serializing_if = "Option::is_none")]
    seq: Option<u64>,
}

#[post("/waiting-room/{product_id}/join")]
async fn join(
    user: AuthUser,
    redis: web::Data<RedisConn>,
    queue: web::Data<SharedQueue>,
    path: web::Path<i32>,
) -> Result<HttpResponse, ApiError> {
    let product_id = path.into_inner();
    let mut con = redis.get_ref().clone();

    let new_token_id = Uuid::new_v4().to_string();
    let token = match cache::join_waiting_room(&mut con, product_id, user.id, &new_token_id).await? {
        JoinOutcome::Closed => return Err(ApiError::Gone(format!("the sale for product {product_id} is closed"))),
        JoinOutcome::ProductNotFound => return Err(ApiError::NotFound(format!("product {product_id} not found"))),
        JoinOutcome::Existing(token) | JoinOutcome::Created(token) => token,
    };

    let seq = ensure_published(&queue, &mut con, product_id, &token.token_id, &token.status, token.published, token.seq)
        .await?;

    Ok(HttpResponse::Accepted().json(JoinResponse { token_id: token.token_id, status: token.status, seq }))
}

/// A queued token must be in Kafka for the admission worker to see it, and its
/// offset there is its place in line. Publishes it if that has not happened yet,
/// which also retries a publish that failed earlier; a duplicate message is
/// harmless because admission skips tokens that are no longer QUEUED. Returns
/// the token's seq.
async fn ensure_published(
    queue: &SharedQueue,
    con: &mut RedisConn,
    product_id: i32,
    token_id: &str,
    status: &str,
    published: bool,
    seq: Option<u64>,
) -> Result<Option<u64>, ApiError> {
    if status != "QUEUED" || published {
        return Ok(seq);
    }
    queue.ensure_product(product_id).await?;
    let offset = queue.publish(product_id, token_id).await?;
    Ok(Some(cache::mark_published(con, token_id, offset).await?))
}

#[derive(Debug, Default, Serialize)]
struct StatusResponse {
    token_id: String,
    /// LOBBY, QUEUED, ACTIVE, USED, CLOSED or EXPIRED
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    sale_opens_at: Option<DateTime<Utc>>,
    /// 1 means next in line; absent until the token is in the queue
    #[serde(skip_serializing_if = "Option::is_none")]
    position: Option<u64>,
    /// From the admission rate over the last minute; absent while admission
    /// is stalled (rate 0) or has not been measured yet
    #[serde(skip_serializing_if = "Option::is_none")]
    estimated_wait_secs: Option<u64>,
    /// Why admission is stalled: `all_units_held` or `sold_out`
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'static str>,
    /// Time left to reserve, while ACTIVE
    #[serde(skip_serializing_if = "Option::is_none")]
    expires_in_ms: Option<u64>,
    /// For USED and EXPIRED tokens: whether joining again would queue a new
    /// token (the turn and any hold are over, and the sale is open)
    #[serde(skip_serializing_if = "Option::is_none")]
    can_rejoin: Option<bool>,
}

/// The caller's place in a product's waiting room. Polled by every waiting
/// client, so it reads Redis only, in one round trip.
#[get("/waiting-room/{product_id}/status")]
async fn get_status(
    user: AuthUser,
    redis: web::Data<RedisConn>,
    queue: web::Data<SharedQueue>,
    path: web::Path<i32>,
) -> Result<HttpResponse, ApiError> {
    let product_id = path.into_inner();
    let mut con = redis.get_ref().clone();

    let t = match cache::waiting_status(&mut con, product_id, user.id).await? {
        StatusOutcome::NotJoined => {
            return Err(ApiError::NotFound(format!("you have not joined the waiting room for product {product_id}")))
        }
        StatusOutcome::Expired { token_id, closed } => {
            return Ok(HttpResponse::Ok().json(StatusResponse {
                token_id,
                status: "EXPIRED".into(),
                can_rejoin: Some(!closed),
                ..Default::default()
            }))
        }
        StatusOutcome::Token(t) => t,
    };

    let waiting = t.status == "LOBBY" || t.status == "QUEUED";
    let mut body = StatusResponse { token_id: t.token_id.clone(), status: t.status.clone(), ..Default::default() };

    if waiting && t.closed {
        body.status = "CLOSED".into();
    } else if t.status == "LOBBY" {
        body.sale_opens_at = t.sale_opens_at;
    } else if t.status == "QUEUED" {
        let seq = ensure_published(&queue, &mut con, product_id, &t.token_id, &t.status, t.published, t.seq).await?;
        // Never below 1: a token can briefly still read QUEUED after the worker moved past it
        body.position = seq.map(|seq| seq.saturating_sub(t.admitted_upto).max(1));
        body.estimated_wait_secs = match (body.position, t.admit_rate) {
            (Some(position), Some(rate)) if rate > 0.0 => Some((position as f64 / rate).ceil() as u64),
            _ => None,
        };
        body.reason = if t.stock_total > 0 && t.sold >= t.stock_total {
            Some("sold_out")
        } else if t.available <= 0 {
            Some("all_units_held")
        } else {
            None
        };
    } else if t.status == "ACTIVE" {
        body.expires_in_ms = t.expires_in_ms;
    } else if t.status == "USED" {
        body.can_rejoin = Some(!t.hold_live && !t.closed);
    }

    Ok(HttpResponse::Ok().json(body))
}

// POST /waiting-room/{product}/join, GET /waiting-room/{product}/status
pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(join).service(get_status);
}
