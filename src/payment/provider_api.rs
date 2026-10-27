//! The fake provider's HTTP API, the part the order processor and the
//! expiration worker call (`crate::payment::client::HttpProvider`). Shaped
//! like Stripe's: it creates, expires and refunds, and every call is
//! idempotent.
//!
//! - `POST /v1/checkout/sessions`: body `{ticket, amount_cents, expires_at}`
//!   (RFC 3339) and an `Idempotency-Key` header (required). Answers `200
//!   {id, url, state}`. The same ticket always gets the same session, as the
//!   same key would: a Kafka message redelivered after a crash finds its session.
//! - `POST /v1/checkout/sessions/{id}/expire`: stops an open session from
//!   accepting payment. `200` whatever the session is (unknown, paid, ...):
//!   there is nothing more to stop.
//! - `POST /v1/refunds`: body `{session_id}`. `200` for a paid session, and
//!   again for one already refunded; `409` for a session that was not paid.
//!
//! These routes are on the provider's own port, reached over the compose
//! network; the gateway proxies only the payment page (`/fake-pay/`).

use actix_web::{post, web, HttpRequest, HttpResponse};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::json;

use super::{FakeProvider, RefundOutcome};
use crate::error::ApiError;

#[derive(Debug, Deserialize)]
struct CreateSession {
    ticket: String,
    amount_cents: i32,
    expires_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
struct Refund {
    session_id: String,
}

/// Create a session (idempotent)
#[post("/checkout/sessions")]
async fn create_session(
    req: HttpRequest,
    body: web::Json<CreateSession>,
    provider: web::Data<FakeProvider>,
) -> Result<HttpResponse, ApiError> {
    let key = req
        .headers()
        .get("Idempotency-Key")
        .and_then(|v| v.to_str().ok())
        .filter(|k| !k.is_empty())
        .ok_or_else(|| ApiError::BadRequest("the Idempotency-Key header is required".into()))?;
    if body.amount_cents < 0 {
        return Err(ApiError::BadRequest("amount_cents must not be negative".into()));
    }
    let session = provider.create_session(&body.ticket, body.amount_cents, body.expires_at, key).await?;
    let state = provider.session(&session.id).await?.map(|s| s.state);
    Ok(HttpResponse::Ok().json(json!({ "id": session.id, "url": session.url, "state": state })))
}

/// Expire a session (idempotent)
#[post("/checkout/sessions/{session_id}/expire")]
async fn expire_session(path: web::Path<String>, provider: web::Data<FakeProvider>) -> Result<HttpResponse, ApiError> {
    let session_id = path.into_inner();
    provider.expire_session(&session_id).await?;
    let state = provider.session(&session_id).await?.map(|s| s.state);
    Ok(HttpResponse::Ok().json(json!({ "id": session_id, "state": state })))
}

/// Refund the payment of a session (idempotent)
#[post("/refunds")]
async fn refund(body: web::Json<Refund>, provider: web::Data<FakeProvider>) -> Result<HttpResponse, ApiError> {
    match provider.refund(&body.session_id).await? {
        RefundOutcome::Refunded | RefundOutcome::AlreadyRefunded => {
            Ok(HttpResponse::Ok().json(json!({ "session_id": body.session_id, "state": "REFUNDED" })))
        }
        // Not paid (or unknown): there is no payment to give back
        RefundOutcome::NotPaid(state) => {
            Err(ApiError::Conflict(format!("cannot refund {}: session is {state}", body.session_id)))
        }
    }
}

// POST /v1/checkout/sessions, POST /v1/checkout/sessions/{id}/expire, POST /v1/refunds
pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::scope("/v1").service(create_session).service(expire_session).service(refund));
}

/// Returns a malformed body as `400 {"message": ...}` like every other error
pub fn json_error(err: actix_web::error::JsonPayloadError, _req: &HttpRequest) -> actix_web::Error {
    let response = HttpResponse::BadRequest().json(json!({ "message": err.to_string() }));
    actix_web::error::InternalError::from_response(err, response).into()
}
