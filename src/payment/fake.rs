//! The fake provider itself: its sessions and webhook outbox, kept in Redis
//! under `fakepay:*`. Only the `fake_payment_provider` binary (and tests that
//! play the user at the payment page) build a `FakeProvider`; the order
//! processor and the expiration worker reach it over HTTP
//! (`crate::payment::client`), and nothing else touches these keys.

use std::sync::LazyLock;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::{webhook, PaymentError, PaymentResult, Session};
use crate::cache::RedisConn;

/// How long a fake session and its ticket mapping are kept: 7 days. Far longer
/// than the payment window, so a payment that arrives after the order was
/// expired can still be found and refunded.
pub const FAKE_SESSION_TTL_SECS: u64 = 7 * 24 * 60 * 60;

/// Key of a fake session: the stand-in for the provider's own database,
/// kept in its own namespace. Only `FakeProvider` reads or writes `fakepay:*`
/// keys; the payment page and the dispatcher go through its methods.
/// A hash with `ticket`, `amount_cents`, `expires_at` (epoch ms) and `state`
/// (`OPEN`, `EXPIRED`, `PAID`, `CANCELED`, `REFUNDED`).
fn session_key(session_id: &str) -> String {
    format!("fakepay:session:{session_id}")
}

/// Maps a queue ticket to its session id: what makes creation idempotent by ticket
fn by_ticket_key(ticket: &str) -> String {
    format!("fakepay:by_ticket:{ticket}")
}

/// The queue of webhook events not yet delivered: a sorted set of event ids
/// scored by the time of the next attempt (epoch ms)
const OUTBOX_KEY: &str = "fakepay:outbox";

/// Body and delivery record of one queued event: a hash with `body`,
/// `attempts` and `created_ms`
fn event_key(event_id: &str) -> String {
    format!("fakepay:event:{event_id}")
}

/// How long an event record may exist at most: far past the 24 h the
/// dispatcher keeps trying, so a record never outlives its outbox entry for long
const EVENT_TTL_SECS: u64 = 48 * 60 * 60;

static CREATE_SCRIPT: LazyLock<redis::Script> =
    LazyLock::new(|| redis::Script::new(include_str!("../../scripts/fakepay_create.lua")));
static TRANSITION_SCRIPT: LazyLock<redis::Script> =
    LazyLock::new(|| redis::Script::new(include_str!("../../scripts/fakepay_transition.lua")));
static SETTLE_SCRIPT: LazyLock<redis::Script> =
    LazyLock::new(|| redis::Script::new(include_str!("../../scripts/fakepay_settle.lua")));
static CLAIM_EVENTS_SCRIPT: LazyLock<redis::Script> =
    LazyLock::new(|| redis::Script::new(include_str!("../../scripts/fakepay_claim.lua")));

/// The session id for an idempotency key (derived from it)
pub fn fake_session_id(idempotency_key: &str) -> String {
    format!("fake_cs_{}", Uuid::new_v5(&Uuid::NAMESPACE_OID, idempotency_key.as_bytes()).simple())
}

/// The payment id of a fake session's payment: derived from the session, so
/// the one payment a session can have always has the same id
pub fn fake_payment_id(session_id: &str) -> String {
    format!("fake_pi_{}", session_id.strip_prefix("fake_cs_").unwrap_or(session_id))
}

/// The id of the event of `event_type` for a session: deterministic, so
/// queueing it again never makes a second event
fn fake_event_id(session_id: &str, event_type: &str) -> String {
    format!("evt_{}", Uuid::new_v5(&Uuid::NAMESPACE_OID, format!("{session_id}:{event_type}").as_bytes()).simple())
}

/// What the fake provider stores about a session (what its payment page shows)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FakeSession {
    pub id: String,
    pub ticket: String,
    pub amount_cents: i64,
    pub expires_at: DateTime<Utc>,
    /// `OPEN`, `EXPIRED`, `PAID`, `CANCELED` or `REFUNDED`
    pub state: String,
}

/// What the user does at the payment page
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Settle {
    /// `OPEN` to `PAID`, queues `checkout.session.completed`
    Pay,
    /// `OPEN` to `CANCELED`, queues `checkout.session.payment_failed`
    Cancel,
}

/// How a settle attempt ended
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Settled {
    /// The session was open: now settled, its event is queued
    Moved,
    /// It had already been settled the same way (the button pressed twice)
    Already,
    /// No such session
    Missing,
    /// The session is in a state that refuses it: `EXPIRED`, `REFUNDED` or the other outcome
    Refused(String),
}

/// A webhook event waiting in the outbox
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxEvent {
    pub id: String,
    /// The JSON body, exactly as it will be sent and signed
    pub body: String,
    /// Deliveries tried so far
    pub attempts: u32,
    /// When the event was queued (epoch ms)
    pub created_ms: i64,
}

/// A stand-in provider. Its sessions live in the inventory Redis under
/// `fakepay:*`: sessions, and the outbox of webhook events. The payment page
/// settles a session with `settle`; the dispatcher takes events out with
/// `claim_due_events` and signs them with `sign_delivery`.
pub struct FakeProvider {
    redis: RedisConn,
    public_base_url: String,
    /// Signs the events it sends and verifies the ones it receives (`PAYMENT_WEBHOOK_SECRET`)
    webhook_secret: String,
}

impl FakeProvider {
    pub fn new(redis: RedisConn, public_base_url: String, webhook_secret: String) -> Self {
        Self { redis, public_base_url: public_base_url.trim_end_matches('/').to_string(), webhook_secret }
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

    /// The user pays or cancels at the payment page: moves the session out of
    /// `OPEN` and queues its webhook event, atomically (`fakepay_settle.lua`).
    /// The provider checks only its own session: it does not know whether the
    /// merchant's deadline has passed, so a payment after the deadline but
    /// before the session was expired is accepted (and the backend refunds it).
    pub async fn settle(&self, session: &FakeSession, action: Settle) -> PaymentResult<Settled> {
        let (to, event_type) = match action {
            Settle::Pay => ("PAID", webhook::CHECKOUT_COMPLETED),
            Settle::Cancel => ("CANCELED", webhook::CHECKOUT_PAYMENT_FAILED),
        };
        let event_id = fake_event_id(&session.id, event_type);
        let now = Utc::now();
        let payment_id = (action == Settle::Pay).then(|| fake_payment_id(&session.id));
        let body = webhook::event_body(
            &event_id,
            event_type,
            now.timestamp(),
            &session.id,
            &session.ticket,
            session.amount_cents,
            payment_id.as_deref(),
        );
        let mut con = self.redis.clone();
        let reply: String = SETTLE_SCRIPT
            .key(session_key(&session.id))
            .key(OUTBOX_KEY)
            .key(event_key(&event_id))
            .arg("OPEN")
            .arg(to)
            .arg(&event_id)
            .arg(body)
            .arg(now.timestamp_millis())
            .arg(EVENT_TTL_SECS)
            .invoke_async(&mut con)
            .await
            .map_err(Self::store_error)?;
        Ok(match reply.as_str() {
            "moved" => Settled::Moved,
            "already" => Settled::Already,
            "missing" => Settled::Missing,
            other => Settled::Refused(other.to_string()),
        })
    }

    /// Takes up to `limit` events that are due for the caller to deliver. Each
    /// is leased for `lease`: if the caller never reports back, it is due
    /// again then. Records that vanished are dropped from the queue.
    pub async fn claim_due_events(&self, limit: usize, lease: std::time::Duration) -> PaymentResult<Vec<OutboxEvent>> {
        let mut con = self.redis.clone();
        let now = Utc::now().timestamp_millis();
        let ids: Vec<String> = CLAIM_EVENTS_SCRIPT
            .key(OUTBOX_KEY)
            .arg(now)
            .arg(now + lease.as_millis() as i64)
            .arg(limit)
            .invoke_async(&mut con)
            .await
            .map_err(Self::store_error)?;
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut pipe = redis::pipe();
        for id in &ids {
            pipe.cmd("HGETALL").arg(event_key(id));
        }
        let records: Vec<std::collections::HashMap<String, String>> =
            pipe.query_async(&mut con).await.map_err(Self::store_error)?;
        let mut events = Vec::new();
        for (id, record) in ids.into_iter().zip(records) {
            match record.get("body") {
                Some(body) => events.push(OutboxEvent {
                    body: body.clone(),
                    attempts: record.get("attempts").and_then(|v| v.parse().ok()).unwrap_or(0),
                    created_ms: record.get("created_ms").and_then(|v| v.parse().ok()).unwrap_or(now),
                    id,
                }),
                None => {
                    log::warn!("Fake provider: event {id} has no record; removed from the outbox");
                    self.event_done(&id).await?;
                }
            }
        }
        Ok(events)
    }

    /// Removes an event from the outbox: it was delivered, or given up on
    pub async fn event_done(&self, event_id: &str) -> PaymentResult<()> {
        let mut con = self.redis.clone();
        redis::pipe()
            .atomic()
            .zrem(OUTBOX_KEY, event_id)
            .ignore()
            .del(event_key(event_id))
            .ignore()
            .query_async::<()>(&mut con)
            .await
            .map_err(Self::store_error)
    }

    /// Records a failed delivery: `attempts` tries so far, the next due at
    /// `next_attempt_ms`. Does nothing if the event was removed meanwhile.
    pub async fn event_failed(&self, event_id: &str, attempts: u32, next_attempt_ms: i64) -> PaymentResult<()> {
        let mut con = self.redis.clone();
        redis::pipe()
            .atomic()
            .cmd("ZADD")
            .arg(OUTBOX_KEY)
            .arg("XX")
            .arg(next_attempt_ms)
            .arg(event_id)
            .ignore()
            .cmd("HSET")
            .arg(event_key(event_id))
            .arg("attempts")
            .arg(attempts)
            .ignore()
            .query_async::<()>(&mut con)
            .await
            .map_err(Self::store_error)
    }

    /// How many events are queued (delivered ones are gone)
    pub async fn outbox_len(&self) -> PaymentResult<usize> {
        let mut con = self.redis.clone();
        redis::cmd("ZCARD").arg(OUTBOX_KEY).query_async(&mut con).await.map_err(Self::store_error)
    }

    /// The `Fake-Signature` header for one delivery of `body`, signed now
    pub fn sign_delivery(&self, body: &[u8]) -> PaymentResult<String> {
        webhook::sign(&self.webhook_secret, Utc::now().timestamp(), body)
    }

    /// The id of the session made for a ticket, if any
    pub async fn session_id_for_ticket(&self, ticket: &str) -> PaymentResult<Option<String>> {
        let mut con = self.redis.clone();
        redis::cmd("GET").arg(by_ticket_key(ticket)).query_async(&mut con).await.map_err(Self::store_error)
    }

    /// Creates a session for `amount_cents` that stops accepting payment at
    /// `expires_at`, or, for a ticket that has one, returns that session
    /// (idempotent by ticket, whatever the key or state). `idempotency_key`
    /// names the session id. The provider's HTTP API calls this.
    pub async fn create_session(
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

    /// Stops an unpaid session from accepting payment. A session that is
    /// unknown, already expired or already paid is left as it is.
    pub async fn expire_session(&self, session_id: &str) -> PaymentResult<()> {
        match self.transition(session_id, "OPEN", "EXPIRED").await?.as_str() {
            "moved" => log::info!("Fake provider expired {session_id}"),
            // Already expired, paid, refunded or never created: nothing to stop
            other => log::debug!("Fake provider left {session_id} as it was ({other})"),
        }
        Ok(())
    }

    /// Refunds the payment of a session. Idempotent per session; only a paid
    /// session can be refunded.
    pub async fn refund(&self, session_id: &str) -> PaymentResult<RefundOutcome> {
        Ok(match self.transition(session_id, "PAID", "REFUNDED").await?.as_str() {
            "moved" => {
                log::info!("Fake provider refunded {session_id}");
                RefundOutcome::Refunded
            }
            "already" => RefundOutcome::AlreadyRefunded,
            other => RefundOutcome::NotPaid(other.to_string()),
        })
    }
}

/// How a refund request ended
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefundOutcome {
    /// The session was paid: now refunded
    Refunded,
    /// It had been refunded already
    AlreadyRefunded,
    /// There is no payment to refund: the session is in this state (`missing`
    /// for an unknown one)
    NotPaid(String),
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
    fn payment_and_event_ids_are_derived() {
        assert_eq!(fake_payment_id("fake_cs_abc"), "fake_pi_abc");
        let paid = fake_event_id("fake_cs_abc", webhook::CHECKOUT_COMPLETED);
        assert_eq!(fake_event_id("fake_cs_abc", webhook::CHECKOUT_COMPLETED), paid);
        assert_ne!(fake_event_id("fake_cs_abc", webhook::CHECKOUT_PAYMENT_FAILED), paid);
        assert_ne!(fake_event_id("fake_cs_abd", webhook::CHECKOUT_COMPLETED), paid);
    }

    #[test]
    fn session_keys_have_their_own_namespace() {
        assert_eq!(event_key("evt_1"), "fakepay:event:evt_1");
        assert_eq!(session_key("fake_cs_1"), "fakepay:session:fake_cs_1");
        assert_eq!(by_ticket_key("42.abc"), "fakepay:by_ticket:42.abc");
    }
}
