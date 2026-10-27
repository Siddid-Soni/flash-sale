//! Payment providers. The pay endpoint creates an intent; the provider later
//! reports the outcome, which `record_event` stores in `payment_inbox` and,
//! for a success, uses to claim the hold at once. The payment worker then
//! commits it. Methods are blocking: the worker is a synchronous loop
//! (it holds an inbox row lock across Redis and Postgres calls), and the API
//! calls them inside `web::block`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::json;
use uuid::Uuid;

use crate::{
    actions, cache,
    cache::ClaimOutcome,
    config::Config,
    db::DbPool,
    models::NewPaymentInboxEntry,
};

#[derive(Debug, thiserror::Error)]
#[error("payment provider: {0}")]
pub struct PaymentError(pub String);

pub type PaymentResult<T> = Result<T, PaymentError>;

pub trait PaymentProvider: Send + Sync {
    /// Name stored with inbox events
    fn name(&self) -> &'static str;
    /// Creates (or, for a repeated `idempotency_key`, returns) the intent to
    /// charge `amount_cents` for the reservation, and returns its id.
    fn create_intent(&self, reservation_id: Uuid, amount_cents: i32, idempotency_key: &str) -> PaymentResult<String>;
    /// Refunds a captured payment. Idempotent per intent.
    fn refund(&self, intent_id: &str) -> PaymentResult<()>;
}

pub type SharedProvider = Arc<dyn PaymentProvider>;

/// The provider named by `PAYMENT_PROVIDER`
pub fn from_config(config: &Config, pool: DbPool) -> SharedProvider {
    match config.payment_provider.as_str() {
        "fake" => {
            let redis = redis::Client::open(config.redis_url.as_str()).expect("REDIS_URL should be valid");
            Arc::new(FakeProvider::new(pool, redis, Duration::from_millis(config.fake_payment_delay_ms)))
        }
        other => panic!("PAYMENT_PROVIDER {other} is not supported"),
    }
}

/// Event type for a successful payment, the only one that confirms an order
pub const PAYMENT_SUCCEEDED: &str = "payment_intent.succeeded";

/// What the payment worker needs from a provider event. Fake events use the
/// Stripe shape, so one parser serves both.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaymentEvent {
    pub intent_id: String,
    pub reservation_id: Uuid,
}

/// Reads `data.object.id` and `data.object.metadata.reservation_id`.
pub fn parse_event(payload: &serde_json::Value) -> Option<PaymentEvent> {
    let object = payload.get("data")?.get("object")?;
    Some(PaymentEvent {
        intent_id: object.get("id")?.as_str()?.to_owned(),
        reservation_id: object.get("metadata")?.get("reservation_id")?.as_str()?.parse().ok()?,
    })
}

/// Stores a provider event, then, for a successful payment, claims its hold
/// right away (`confirm.lua`). The outcome of a payment is decided when the
/// provider reports it, not when the payment worker gets to it: a backlog in
/// the worker must never turn a payment that arrived in time into a refund.
///
/// The inbox row comes first. If the claim then fails, the worker claims the
/// hold later as before; the other order could leave a claimed hold with no
/// event to confirm it, holding its units until the claim expires.
/// Returns false if the event was already stored.
pub fn record_event(pool: &DbPool, redis: &ClaimConnection, entry: &NewPaymentInboxEntry) -> PaymentResult<bool> {
    let mut conn = pool.get().map_err(|e| PaymentError(format!("database pool: {e}")))?;
    let stored = actions::payments::insert_inbox(&mut conn, entry).map_err(|e| PaymentError(format!("store event: {e}")))?;
    if !stored || entry.event_type != PAYMENT_SUCCEEDED {
        return Ok(stored);
    }
    let Some(event) = parse_event(entry.payload) else {
        return Ok(stored);
    };
    match redis.claim(&event.reservation_id.to_string(), &event.intent_id) {
        Ok(ClaimOutcome::Won(_)) => {}
        // Expired first: the worker refunds it
        Ok(ClaimOutcome::Gone) => log::info!("Payment {} arrived after its hold ended", event.intent_id),
        Err(e) => log::warn!("Claiming the hold for {} failed, the payment worker will: {e}", event.intent_id),
    }
    Ok(stored)
}

/// One Redis connection shared by every event `record_event` handles. A new
/// connection per event is not an option: at a few hundred payments a second
/// Redis starts resetting them, and each failed claim falls back to the
/// payment worker, which by then may be too late.
pub struct ClaimConnection {
    client: redis::Client,
    con: Mutex<Option<redis::Connection>>,
}

impl ClaimConnection {
    pub fn new(client: redis::Client) -> Self {
        Self { client, con: Mutex::new(None) }
    }

    /// Runs `confirm.lua`, reconnecting and retrying once if the connection broke.
    pub fn claim(&self, reservation_id: &str, intent_id: &str) -> redis::RedisResult<ClaimOutcome> {
        let mut guard = self.con.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut last_error = None;
        for _ in 0..2 {
            if guard.is_none() {
                match self.client.get_connection() {
                    Ok(con) => *guard = Some(con),
                    Err(e) => {
                        last_error = Some(e);
                        continue;
                    }
                }
            }
            let con = guard.as_mut().expect("connected above");
            match cache::claim_hold(con, reservation_id, intent_id) {
                Ok(outcome) => return Ok(outcome),
                Err(e) => {
                    *guard = None;
                    last_error = Some(e);
                }
            }
        }
        Err(last_error.expect("two attempts failed"))
    }
}

/// Succeeds every payment: `delay` after an intent is created it records a
/// `payment_intent.succeeded` event, as a provider webhook would.
pub struct FakeProvider {
    pool: DbPool,
    redis: Arc<ClaimConnection>,
    delay: Duration,
}

impl FakeProvider {
    pub fn new(pool: DbPool, redis: redis::Client, delay: Duration) -> Self {
        Self { pool, redis: Arc::new(ClaimConnection::new(redis)), delay }
    }
}

impl PaymentProvider for FakeProvider {
    fn name(&self) -> &'static str {
        "fake"
    }

    fn create_intent(&self, reservation_id: Uuid, _amount_cents: i32, idempotency_key: &str) -> PaymentResult<String> {
        // Derived from the key, so a repeated call returns the same intent
        // and its event is stored once (provider_event_id is unique)
        let intent_id = format!("fake_pi_{}", Uuid::new_v5(&Uuid::NAMESPACE_OID, idempotency_key.as_bytes()).simple());
        let payload = json!({
            "id": format!("evt_{intent_id}"),
            "type": PAYMENT_SUCCEEDED,
            "data": { "object": { "id": intent_id, "metadata": { "reservation_id": reservation_id.to_string() } } },
        });

        let (pool, redis, delay, event_id) = (self.pool.clone(), self.redis.clone(), self.delay, format!("evt_{intent_id}"));
        std::thread::spawn(move || {
            std::thread::sleep(delay);
            let entry = NewPaymentInboxEntry {
                provider: "fake",
                provider_event_id: &event_id,
                event_type: PAYMENT_SUCCEEDED,
                payload: &payload,
            };
            if let Err(e) = record_event(&pool, &redis, &entry) {
                log::error!("Fake provider could not store {event_id}: {e}");
            }
        });
        Ok(intent_id)
    }

    fn refund(&self, intent_id: &str) -> PaymentResult<()> {
        log::info!("Fake provider refunded {intent_id}");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_stripe_event_shape() {
        let resv = Uuid::new_v4();
        let payload = json!({ "data": { "object": { "id": "pi_1", "metadata": { "reservation_id": resv.to_string() } } } });
        assert_eq!(parse_event(&payload), Some(PaymentEvent { intent_id: "pi_1".into(), reservation_id: resv }));
        assert_eq!(parse_event(&json!({ "data": { "object": { "id": "pi_1" } } })), None);
    }
}
