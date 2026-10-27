//! Payment providers. Stage 1 leaves the minimum that compiles: a provider
//! trait and a fake that hands out deterministic session ids. Stage 4 reshapes
//! it into the roadmap's interface (checkout session with a URL, expire,
//! refund) and Stage 5 adds the signed webhooks that feed `payment_inbox`
//! (see `crate::actions::payments`).

use std::sync::Arc;

use uuid::Uuid;

use crate::config::Config;

#[derive(Debug, thiserror::Error)]
#[error("payment provider: {0}")]
pub struct PaymentError(pub String);

pub type PaymentResult<T> = Result<T, PaymentError>;

pub trait PaymentProvider: Send + Sync {
    /// Name stored with inbox events
    fn name(&self) -> &'static str;
    /// Creates (or, for a repeated `idempotency_key`, returns) a payment
    /// session for `amount_cents` and returns its id.
    fn create_session(&self, amount_cents: i32, idempotency_key: &str) -> PaymentResult<String>;
    /// Refunds a captured payment. Idempotent per session.
    fn refund(&self, session_id: &str) -> PaymentResult<()>;
}

pub type SharedProvider = Arc<dyn PaymentProvider>;

/// The provider named by `PAYMENT_PROVIDER`
pub fn from_config(config: &Config) -> SharedProvider {
    match config.payment_provider.as_str() {
        "fake" => Arc::new(FakeProvider),
        other => panic!("PAYMENT_PROVIDER {other} is not supported"),
    }
}

/// Event type for a successful payment, the only one that confirms an order
pub const PAYMENT_SUCCEEDED: &str = "payment_intent.succeeded";

/// A stand-in provider with no state of its own
pub struct FakeProvider;

impl PaymentProvider for FakeProvider {
    fn name(&self) -> &'static str {
        "fake"
    }

    fn create_session(&self, _amount_cents: i32, idempotency_key: &str) -> PaymentResult<String> {
        // Derived from the key, so a repeated call returns the same session
        Ok(format!("fake_cs_{}", Uuid::new_v5(&Uuid::NAMESPACE_OID, idempotency_key.as_bytes()).simple()))
    }

    fn refund(&self, session_id: &str) -> PaymentResult<()> {
        log::info!("Fake provider refunded {session_id}");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_key_same_session() {
        let provider = FakeProvider;
        let first = provider.create_session(2500, "42.abc").unwrap();
        assert_eq!(provider.create_session(2500, "42.abc").unwrap(), first);
        assert_ne!(provider.create_session(2500, "42.def").unwrap(), first);
    }
}
