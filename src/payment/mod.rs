//! Payment providers, behind an interface shaped like Stripe Checkout: create
//! a checkout session (an id and a URL the user pays at), expire it, refund
//! it, and verify the signed webhooks that report the outcome (stored in
//! `payment_inbox`, see `crate::actions::payments`).
//!
//! Every method is idempotent, because the order processor retries a message
//! after any failure: `create_session` returns the same session for the same
//! idempotency key, `expire_session` and `refund` do nothing the second time.
//!
//! The backend never links a provider into its own process. `PaymentProvider`
//! is implemented by `client::HttpProvider`, which calls the provider's HTTP
//! API at `PAYMENT_PROVIDER_URL` (and verifies webhook signatures with the
//! shared secret). The fake provider is a service of its own, the
//! `fake_payment_provider` binary:
//!
//! - `fake` is its state: sessions and the webhook outbox under `fakepay:*`
//!   in Redis, which nothing else touches;
//! - `provider_api` is the Stripe-like HTTP API the backend calls (create
//!   session with an `Idempotency-Key`, expire, refund);
//! - `pay_page` is the page the user pays at, `/fake-pay/{session_id}`;
//! - `dispatcher` delivers the webhooks the page queues, signed, with retries,
//!   to `PAYMENT_WEBHOOK_URL` (the order processors, through the gateway).

pub mod client;
pub mod dispatcher;
pub mod fake;
pub mod pay_page;
pub mod provider_api;
pub mod webhook;

pub use fake::{
    fake_payment_id, fake_session_id, FakeProvider, FakeSession, OutboxEvent, RefundOutcome, Settle, Settled, FAKE_SESSION_TTL_SECS,
};
pub use webhook::{EventKind, ProviderEvent};

use std::sync::Arc;

use actix_web::http::header::HeaderMap;
use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::config::Config;

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
    /// Checks the signature of a webhook request (its headers and raw body)
    /// and reads the event. Any error means the request is not from the provider.
    fn verify_webhook(&self, headers: &HeaderMap, body: &[u8]) -> PaymentResult<ProviderEvent>;
}

pub type SharedProvider = Arc<dyn PaymentProvider>;

/// The provider named by `PAYMENT_PROVIDER`, reached over HTTP at `PAYMENT_PROVIDER_URL`
pub fn from_config(config: &Config) -> SharedProvider {
    match config.payment_provider.as_str() {
        "fake" => Arc::new(client::HttpProvider::new(config.payment_provider_url.clone(), config.payment_webhook_secret.clone())),
        other => panic!("PAYMENT_PROVIDER {other} is not supported"),
    }
}
