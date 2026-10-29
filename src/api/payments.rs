//! `POST /v1/payments/webhook`: where the payment provider reports outcomes.
//! It is served by the **order processor**, not by the Order
//! Service replicas, and reached through the gateway, which lets only the
//! provider's addresses through (`gateway/payment_provider_ips.conf`).
//!
//! The request is trusted only after the provider has verified its signature
//! over the raw body (the second layer after the address check). A verified
//! event is stored in `payment_inbox` first and then handled at once
//! (`InboxWorker::receive`): confirmed if PostgreSQL says the payment beat the
//! deadline, refunded if not. Once the event is stored the answer is `200`,
//! whether or not handling worked: a failure leaves the row unprocessed and
//! the inbox loop retries it, so the provider need not.

use std::sync::Arc;

use actix_web::{post, web, HttpRequest, HttpResponse};
use serde_json::json;

use crate::{
    error::ApiError,
    inbox::{InboxWorker, Received},
    payment::SharedProvider,
};

/// Provider webhook. `400` if the signature, its timestamp or the event is
/// invalid (nothing is stored); `200` once the event is stored, also for one
/// seen before (providers deliver more than once) and when handling it failed
/// (the inbox loop retries). `500` only if it could not be stored: then the
/// provider's own retry is the only copy of the event.
#[post("/payments/webhook")]
async fn webhook(
    req: HttpRequest,
    body: web::Bytes,
    provider: web::Data<SharedProvider>,
    inbox: web::Data<InboxWorker>,
) -> Result<HttpResponse, ApiError> {
    let event = provider.verify_webhook(req.headers(), &body).map_err(|e| {
        log::warn!("Webhook refused: {e}");
        ApiError::BadRequest("invalid webhook".into())
    })?;
    let inbox: Arc<InboxWorker> = inbox.into_inner();
    match inbox.receive(&event).await {
        Ok(Received::Handled(handled)) => log::debug!("Webhook event {}: {handled:?}", event.id),
        Ok(Received::Duplicate) => log::debug!("Webhook event {} was already processed", event.id),
        Ok(Received::Deferred(reason)) => log::debug!("Webhook event {} deferred to the inbox loop: {reason}", event.id),
        Err(e) => return Err(ApiError::Internal(format!("storing webhook event {}: {e}", event.id))),
    }
    Ok(HttpResponse::Ok().json(json!({ "received": true })))
}

// POST /v1/payments/webhook
pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::scope("/v1").service(webhook));
}
