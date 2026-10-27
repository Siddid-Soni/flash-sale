//! `POST /v1/payments/webhook`: where the payment provider reports outcomes
//! (roadmap step 7). The request is trusted only after the provider has
//! verified its signature over the raw body. A verified event is stored in
//! `payment_inbox` first; for a success the hold is then claimed at once
//! (`confirm.lua`), so a payment that arrives in time is kept however far
//! behind the order processors are. The processors confirm or refund later
//! (`crate::inbox`).

use actix_web::{post, web, HttpRequest, HttpResponse};
use serde_json::json;

use crate::{
    actions,
    cache::{self, ClaimOutcome, RedisConn},
    db::DbPool,
    error::ApiError,
    models::NewPaymentInboxEntry,
    payment::{EventKind, SharedProvider},
};

/// Provider webhook. `400` if the signature, its timestamp or the event is
/// invalid (nothing is stored); `200` once the event is stored, also for one
/// seen before (providers deliver more than once). A failure to claim the hold
/// never fails the request: the event is safely stored and the processor claims
/// again, so the provider need not retry.
#[post("/payments/webhook")]
async fn webhook(
    req: HttpRequest,
    body: web::Bytes,
    provider: web::Data<SharedProvider>,
    pool: web::Data<DbPool>,
    redis: web::Data<RedisConn>,
) -> Result<HttpResponse, ApiError> {
    let event = provider.verify_webhook(req.headers(), &body).map_err(|e| {
        log::warn!("Webhook refused: {e}");
        ApiError::BadRequest("invalid webhook".into())
    })?;

    // The inbox row comes first: if the claim below fails or the process
    // dies, the processor still has the event to claim and confirm
    let (stored, event) = web::block(move || {
        let mut conn = pool.get()?;
        let entry = NewPaymentInboxEntry { provider_event_id: &event.id, event_type: &event.event_type, payload: &event.payload };
        let stored = actions::payments::insert_inbox(&mut conn, &entry)?;
        Ok::<_, ApiError>((stored, event))
    })
    .await??;
    if !stored {
        log::debug!("Webhook event {} was already stored", event.id);
    }

    // Also for an event stored before: the request that stored it may have died before claiming
    if let (EventKind::PaymentSucceeded, Some(sale), Some(payment_id)) =
        (event.kind, cache::ticket_sale(&event.ticket), event.payment_id.as_deref())
    {
        let mut con = redis.get_ref().clone();
        match cache::claim_hold(&mut con, sale, &event.ticket, &event.session_id, payment_id).await {
            Ok(ClaimOutcome::Won) => log::info!("Payment {payment_id} claimed the hold of {}", event.ticket),
            // Past due or released first (or an already confirmed order's
            // duplicate event): the processor refunds or skips it
            Ok(ClaimOutcome::Gone) => log::info!("Payment {payment_id} did not claim the hold of {}", event.ticket),
            Err(e) => log::warn!("Claiming the hold of {} failed, the order processor will: {e}", event.ticket),
        }
    }
    Ok(HttpResponse::Ok().json(json!({ "received": true })))
}

// POST /v1/payments/webhook
pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(webhook);
}
