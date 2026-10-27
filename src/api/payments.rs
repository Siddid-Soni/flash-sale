use actix_web::{http::header, post, web, HttpRequest, HttpResponse};
use chrono::{DateTime, Utc};
use serde::Serialize;
use uuid::Uuid;

use crate::{
    actions,
    auth::AuthUser,
    cache::{self, RedisConn, StartPaymentOutcome},
    config::Config,
    db::DbPool,
    error::ApiError,
    models::OrderStatus,
    payment::SharedProvider,
};

#[derive(Debug, Serialize)]
struct PayResponse {
    /// Same as the reservation id
    order_id: Uuid,
    reservation_id: Uuid,
    status: OrderStatus,
    payment_intent_id: String,
    /// Total in cents, locked now
    amount_cents: i32,
    /// The hold, extended once for payment
    expires_at: DateTime<Utc>,
    /// Poll this for the outcome
    tracking_url: String,
}

/// Starts paying for the caller's hold. Requires an `Idempotency-Key` header.
/// Extends the hold once and makes it unreleasable (Redis), then records the
/// order, the first Postgres write of the purchase, and creates the payment
/// intent. Safe to retry at every step: a repeat returns the same order and
/// intent, and the intent is always created with the order's first key. The
/// outcome arrives later, so the answer is always `202` with the order to poll.
#[post("/reservations/{reservation_id}/pay")]
async fn pay(
    user: AuthUser,
    req: HttpRequest,
    pool: web::Data<DbPool>,
    redis: web::Data<RedisConn>,
    config: web::Data<Config>,
    provider: web::Data<SharedProvider>,
    path: web::Path<String>,
) -> Result<HttpResponse, ApiError> {
    let reservation_id =
        Uuid::parse_str(&path.into_inner()).map_err(|_| ApiError::BadRequest("invalid reservation id".into()))?;
    let key = req
        .headers()
        .get("Idempotency-Key")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|key| (1..=255).contains(&key.len()))
        .ok_or_else(|| ApiError::BadRequest("an Idempotency-Key header (1-255 characters) is required".into()))?
        .to_owned();

    let mut con = redis.get_ref().clone();
    let extension_ms = config.payment_extension_secs * 1000;
    let (hold, expires_at) = match cache::start_payment(&mut con, &reservation_id.to_string(), user.id, extension_ms).await? {
        StartPaymentOutcome::Paying { hold, expires_at } => (hold, expires_at),
        StartPaymentOutcome::Gone => {
            return Err(ApiError::Gone(format!("the hold {reservation_id} has expired or was released")))
        }
        StartPaymentOutcome::Forbidden => return Err(ApiError::Forbidden("that reservation is not yours".into())),
        StartPaymentOutcome::Paid => return Err(ApiError::Conflict(format!("reservation {reservation_id} is already paid"))),
    };

    let provider = provider.get_ref().clone();
    let order = web::block(move || {
        let mut conn = pool.get()?;
        let mut order = actions::orders::create_payment_pending(
            &mut conn,
            reservation_id,
            user.id,
            hold.product,
            hold.quantity,
            expires_at,
            &key,
        )
        .map_err(|err| match err {
            diesel::result::Error::NotFound => {
                ApiError::Internal(format!("hold {reservation_id}: product {} does not exist", hold.product))
            }
            other => other.into(),
        })?;
        if order.payment_intent_id.is_none() {
            let first_key = order.payment_idempotency_key.clone().unwrap_or(key);
            let intent_id = provider
                .create_intent(reservation_id, order.total_cents, &first_key)
                .map_err(|e| ApiError::Internal(e.to_string()))?;
            actions::orders::set_payment_intent(&mut conn, reservation_id, &intent_id)?;
            order.payment_intent_id = Some(intent_id);
        }
        Ok::<_, ApiError>(order)
    })
    .await??;

    let tracking_url = format!("/orders/{}", order.id);
    Ok(HttpResponse::Accepted().insert_header((header::LOCATION, tracking_url.clone())).json(PayResponse {
        order_id: order.id,
        reservation_id,
        status: order.status,
        payment_intent_id: order.payment_intent_id.unwrap_or_default(),
        amount_cents: order.total_cents,
        expires_at: order.expires_at,
        tracking_url,
    }))
}

// POST /reservations/{reservation_id}/pay; POST /webhooks/stripe (step 6.5)
pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(pay);
}
