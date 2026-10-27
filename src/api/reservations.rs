use actix_web::{delete, post, web, HttpResponse};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    auth::AuthUser,
    cache::{self, RedisConn, ReleaseOutcome, ReserveOutcome},
    config::Config,
    error::ApiError,
};

#[derive(Debug, Deserialize)]
struct ReserveRequest {
    product_id: i32,
    #[serde(default = "one")]
    quantity: i32,
}

fn one() -> i32 {
    1
}

#[derive(Debug, Serialize)]
struct ReserveResponse {
    reservation_id: String,
    product_id: i32,
    quantity: i32,
    /// When the hold ends unless payment starts first
    expires_at: DateTime<Utc>,
}

/// Holds units for the caller, whose waiting room token for the product must be
/// ACTIVE (found through `user_token`, like the status endpoint). A refusal
/// (out of stock, limit, invalid quantity) leaves the token ACTIVE to try
/// again; a successful hold uses it up for good. Redis only,
/// all units or none: this is the busiest moment of a sale, so Postgres is not
/// touched until payment starts (the order and its price are recorded then;
/// clients price the units from the product). Retrying after a timeout returns
/// the same hold with `200`.
#[post("/reservations")]
async fn reserve(
    user: AuthUser,
    redis: web::Data<RedisConn>,
    config: web::Data<Config>,
    body: web::Json<ReserveRequest>,
) -> Result<HttpResponse, ApiError> {
    let ReserveRequest { product_id, quantity } = body.into_inner();

    let mut con = redis.get_ref().clone();
    let outcome = cache::reserve_units(
        &mut con,
        product_id,
        user.id,
        &Uuid::new_v4().to_string(),
        quantity,
        config.hold_ttl_secs * 1000,
    )
    .await?;

    match outcome {
        ReserveOutcome::Held { reservation_id, expires_at } => {
            Ok(HttpResponse::Created().json(ReserveResponse { reservation_id, product_id, quantity, expires_at }))
        }
        ReserveOutcome::Existing { reservation_id, expires_at, quantity } => {
            Ok(HttpResponse::Ok().json(ReserveResponse { reservation_id, product_id, quantity, expires_at }))
        }
        ReserveOutcome::Closed => Err(ApiError::Gone(format!("the sale for product {product_id} is closed"))),
        ReserveOutcome::NotAdmitted => {
            Err(ApiError::Forbidden("wait for your turn in the waiting room before reserving".into()))
        }
        ReserveOutcome::TurnUsed => {
            Err(ApiError::Forbidden("your turn was used for an earlier reservation, which has ended".into()))
        }
        ReserveOutcome::InvalidQuantity { max } => {
            Err(ApiError::BadRequest(format!("quantity must be between 1 and {max}")))
        }
        ReserveOutcome::TooMany { held } => Err(ApiError::Conflict(format!(
            "you already have {held} of this product; that and {quantity} more would pass the per-user limit"
        ))),
        ReserveOutcome::OutOfStock { available: 0 } => Err(ApiError::Conflict("sold out".into())),
        ReserveOutcome::OutOfStock { available } => Err(ApiError::Conflict(format!("only {available} left"))),
    }
}

/// Releases the caller's hold (Redis only): the units are back in stock. The
/// caller's turn stays spent: they cannot reserve again with it.
#[delete("/reservations/{id}")]
async fn release(user: AuthUser, redis: web::Data<RedisConn>, path: web::Path<String>) -> Result<HttpResponse, ApiError> {
    let id = Uuid::parse_str(&path.into_inner()).map_err(|_| ApiError::BadRequest("invalid reservation id".into()))?;

    let mut con = redis.get_ref().clone();
    match cache::release_hold(&mut con, &id.to_string(), Some(user.id)).await? {
        ReleaseOutcome::Released => Ok(HttpResponse::NoContent().finish()),
        ReleaseOutcome::NotFound => Err(ApiError::NotFound(format!("no units are held under reservation {id}"))),
        ReleaseOutcome::Forbidden => Err(ApiError::Forbidden("that reservation is not yours".into())),
        ReleaseOutcome::PaymentInProgress => {
            Err(ApiError::Conflict("payment for this reservation has started; it can no longer be released".into()))
        }
    }
}

// POST /reservations, DELETE /reservations/{id}
pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(reserve).service(release);
}
