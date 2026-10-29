//! The hook for a proof-of-work or CAPTCHA check on session creation
//! (`POST /v1/session`, the one unauthenticated write). The gateway already
//! rate-limits that route per IP and refuses obvious bots; a challenge makes
//! each new session cost the client something, which is what stops a script
//! that rotates IPs.
//!
//! Only the seam exists today: `Challenge::verify` receives the optional
//! `X-Challenge` request header (the solved puzzle or CAPTCHA token) and
//! decides. `SESSION_CHALLENGE=off` (the default) selects `AlwaysPass`. A real
//! implementation (a hashcash-style proof of work whose difficulty rises with
//! load, a Turnstile/reCAPTCHA token check) is another type implementing the
//! trait, selected by another value in `from_config`; it is not built yet.
//! The handler needs no change.

use std::sync::Arc;

use async_trait::async_trait;

use crate::{config::Config, error::ApiError};

#[async_trait]
pub trait Challenge: Send + Sync {
    /// Checks the `X-Challenge` header (`None` when the client sent none).
    /// `Err` is returned to the client as is, typically `ApiError::Forbidden`.
    async fn verify(&self, header: Option<&str>) -> Result<(), ApiError>;
}

/// Accepts every request; the default (`SESSION_CHALLENGE=off`).
pub struct AlwaysPass;

#[async_trait]
impl Challenge for AlwaysPass {
    async fn verify(&self, _header: Option<&str>) -> Result<(), ApiError> {
        Ok(())
    }
}

pub type SharedChallenge = Arc<dyn Challenge>;

/// Picks the implementation named by `SESSION_CHALLENGE`; panics on an unknown name at startup.
pub fn from_config(config: &Config) -> SharedChallenge {
    match config.session_challenge.as_str() {
        "off" => Arc::new(AlwaysPass),
        other => panic!("SESSION_CHALLENGE has an unknown value: {other} (supported: off)"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[actix_web::test]
    async fn always_pass_accepts_with_and_without_a_header() {
        assert!(AlwaysPass.verify(None).await.is_ok());
        assert!(AlwaysPass.verify(Some("anything")).await.is_ok());
    }
}
