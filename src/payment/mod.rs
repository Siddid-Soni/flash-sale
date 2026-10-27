//! Payment providers, behind an interface shaped like Stripe Checkout: create
//! a checkout session (an id and a URL the user pays at), expire it, refund
//! it. Stage 5 adds the signed webhooks that report the outcome into
//! `payment_inbox` (see `crate::actions::payments`).
//!
//! Every method is idempotent, because the order processor retries a message
//! after any failure: `create_session` returns the same session for the same
//! idempotency key, `expire_session` and `refund` do nothing the second time.

use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::{cache::RedisConn, config::Config};

#[derive(Debug, thiserror::Error)]
#[error("payment provider: {0}")]
pub struct PaymentError(pub String);

pub type PaymentResult<T> = Result<T, PaymentError>;

/// A checkout session at the provider
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Session {
    /// The provider's id; stored as `orders.payment_session_id`
    pub id: String,
    /// Where the user pays
    pub url: String,
}

#[async_trait]
pub trait PaymentProvider: Send + Sync {
    /// Name stored with inbox events
    fn name(&self) -> &'static str;
    /// Creates a session for `amount_cents` that stops accepting payment at
    /// `expires_at`, or, for a repeated `idempotency_key`, returns the session
    /// the first call made. `ticket` is the queue ticket, for the provider's records.
    async fn create_session(
        &self,
        ticket: &str,
        amount_cents: i32,
        expires_at: DateTime<Utc>,
        idempotency_key: &str,
    ) -> PaymentResult<Session>;
    /// Stops an unpaid session from accepting payment. A session that is
    /// unknown, already expired or already paid is left as it is.
    async fn expire_session(&self, session_id: &str) -> PaymentResult<()>;
    /// Refunds the payment of a session. Idempotent per session.
    async fn refund(&self, session_id: &str) -> PaymentResult<()>;
}

pub type SharedProvider = Arc<dyn PaymentProvider>;

/// The provider named by `PAYMENT_PROVIDER`. The fake provider keeps its
/// sessions in the inventory Redis.
pub fn from_config(config: &Config, inventory: RedisConn) -> SharedProvider {
    match config.payment_provider.as_str() {
        "fake" => Arc::new(FakeProvider::new(inventory, config.public_base_url.clone())),
        other => panic!("PAYMENT_PROVIDER {other} is not supported"),
    }
}

/// Event type for a successful payment, the only one that confirms an order
pub const PAYMENT_SUCCEEDED: &str = "payment_intent.succeeded";

/// How long a fake session and its ticket mapping are kept: 7 days. Far longer
/// than the payment window, so a payment that arrives after the hold was
/// released can still be found and refunded.
pub const FAKE_SESSION_TTL_SECS: u64 = 7 * 24 * 60 * 60;

/// Key of a fake session: the stand-in for the provider's own database,
/// kept in its own namespace. Only `FakeProvider` reads or writes `fakepay:*`
/// keys; the payment page and webhooks of Stage 5 go through its methods.
/// A hash with `ticket`, `amount_cents`, `expires_at` (epoch ms) and `state`
/// (`OPEN`, `EXPIRED`, `PAID`, `REFUNDED`).
fn session_key(session_id: &str) -> String {
    format!("fakepay:session:{session_id}")
}

/// Maps a queue ticket to its session id: what makes creation idempotent by ticket
fn by_ticket_key(ticket: &str) -> String {
    format!("fakepay:by_ticket:{ticket}")
}

static CREATE_SCRIPT: LazyLock<redis::Script> =
    LazyLock::new(|| redis::Script::new(include_str!("../../scripts/fakepay_create.lua")));
static TRANSITION_SCRIPT: LazyLock<redis::Script> =
    LazyLock::new(|| redis::Script::new(include_str!("../../scripts/fakepay_transition.lua")));

/// The session id for an idempotency key (derived from it)
pub fn fake_session_id(idempotency_key: &str) -> String {
    format!("fake_cs_{}", Uuid::new_v5(&Uuid::NAMESPACE_OID, idempotency_key.as_bytes()).simple())
}

/// What the fake provider stores about a session (what its payment page shows)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FakeSession {
    pub id: String,
    pub ticket: String,
    pub amount_cents: i64,
    pub expires_at: DateTime<Utc>,
    /// `OPEN`, `EXPIRED`, `PAID` or `REFUNDED`
    pub state: String,
}

/// A stand-in provider. Its sessions live in the inventory Redis under
/// `fakepay:*`; Stage 5's payment page and webhooks read them through `session`.
/// (A real provider would send its webhooks on its own schedule, apart from
/// the page and with retries; nothing here assumes otherwise.)
pub struct FakeProvider {
    redis: RedisConn,
    public_base_url: String,
}

impl FakeProvider {
    pub fn new(redis: RedisConn, public_base_url: String) -> Self {
        Self { redis, public_base_url: public_base_url.trim_end_matches('/').to_string() }
    }

    fn store_error(e: redis::RedisError) -> PaymentError {
        PaymentError(format!("session store: {e}"))
    }

    fn url(&self, session_id: &str) -> String {
        format!("{}/fake-pay/{session_id}", self.public_base_url)
    }

    async fn transition(&self, session_id: &str, from: &str, to: &str) -> PaymentResult<String> {
        let mut con = self.redis.clone();
        TRANSITION_SCRIPT
            .key(session_key(session_id))
            .arg(from)
            .arg(to)
            .invoke_async(&mut con)
            .await
            .map_err(Self::store_error)
    }

    /// The stored session, for the payment page; `None` if unknown or expired away
    pub async fn session(&self, session_id: &str) -> PaymentResult<Option<FakeSession>> {
        let mut con = self.redis.clone();
        let fields: std::collections::HashMap<String, String> =
            redis::cmd("HGETALL").arg(session_key(session_id)).query_async(&mut con).await.map_err(Self::store_error)?;
        let parsed = (|| {
            Some(FakeSession {
                id: session_id.to_string(),
                ticket: fields.get("ticket")?.clone(),
                amount_cents: fields.get("amount_cents")?.parse().ok()?,
                expires_at: DateTime::from_timestamp_millis(fields.get("expires_at")?.parse().ok()?)?,
                state: fields.get("state")?.clone(),
            })
        })();
        Ok(parsed)
    }

    /// The id of the session made for a ticket, if any
    pub async fn session_id_for_ticket(&self, ticket: &str) -> PaymentResult<Option<String>> {
        let mut con = self.redis.clone();
        redis::cmd("GET").arg(by_ticket_key(ticket)).query_async(&mut con).await.map_err(Self::store_error)
    }
}

#[async_trait]
impl PaymentProvider for FakeProvider {
    fn name(&self) -> &'static str {
        "fake"
    }

    async fn create_session(
        &self,
        ticket: &str,
        amount_cents: i32,
        expires_at: DateTime<Utc>,
        idempotency_key: &str,
    ) -> PaymentResult<Session> {
        let mut con = self.redis.clone();
        let (_, id): (String, String) = CREATE_SCRIPT
            .key(by_ticket_key(ticket))
            .key(session_key(&fake_session_id(idempotency_key)))
            .arg(fake_session_id(idempotency_key))
            .arg(ticket)
            .arg(amount_cents)
            .arg(expires_at.timestamp_millis())
            .arg(FAKE_SESSION_TTL_SECS)
            .invoke_async(&mut con)
            .await
            .map_err(Self::store_error)?;
        let url = self.url(&id);
        Ok(Session { id, url })
    }

    async fn expire_session(&self, session_id: &str) -> PaymentResult<()> {
        match self.transition(session_id, "OPEN", "EXPIRED").await?.as_str() {
            "moved" => log::info!("Fake provider expired {session_id}"),
            // Already expired, paid, refunded or never created: nothing to stop
            other => log::debug!("Fake provider left {session_id} as it was ({other})"),
        }
        Ok(())
    }

    async fn refund(&self, session_id: &str) -> PaymentResult<()> {
        match self.transition(session_id, "PAID", "REFUNDED").await?.as_str() {
            "moved" => log::info!("Fake provider refunded {session_id}"),
            "already" => {}
            other => return Err(PaymentError(format!("cannot refund {session_id}: session is {other}"))),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_key_same_session() {
        let first = fake_session_id("42.abc");
        assert_eq!(fake_session_id("42.abc"), first);
        assert_ne!(fake_session_id("42.def"), first);
        assert!(first.starts_with("fake_cs_"));
    }

    #[test]
    fn session_keys_have_their_own_namespace() {
        assert_eq!(session_key("fake_cs_1"), "fakepay:session:fake_cs_1");
        assert_eq!(by_ticket_key("42.abc"), "fakepay:by_ticket:42.abc");
    }
}
