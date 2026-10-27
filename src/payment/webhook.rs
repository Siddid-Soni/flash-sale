//! Webhook events and their signature, the part of the provider abstraction
//! that is the same however the events travel.
//!
//! The fake provider signs like Stripe does: the header
//! `Fake-Signature: t=<unix secs>,v1=<hex HMAC-SHA256(secret, "<t>.<raw body>")>`.
//! The timestamp is part of what is signed, so a captured request cannot be
//! replayed later (the receiver refuses one older than `TOLERANCE_SECS`), and
//! the provider signs every delivery attempt afresh, so a retry hours later is
//! still accepted. The body is signed as received, byte for byte.

use hmac::{Hmac, Mac};
use serde_json::{json, Value};
use sha2::Sha256;

use super::{PaymentError, PaymentResult};

/// Header carrying the signature
pub const SIGNATURE_HEADER: &str = "Fake-Signature";

/// How far a signature's timestamp may be from the receiver's clock: 5 minutes
pub const TOLERANCE_SECS: i64 = 300;

/// Event type: the session was paid
pub const CHECKOUT_COMPLETED: &str = "checkout.session.completed";
/// Event type: the user cancelled at the payment page
pub const CHECKOUT_PAYMENT_FAILED: &str = "checkout.session.payment_failed";

type HmacSha256 = Hmac<Sha256>;

fn mac(secret: &str, timestamp: i64, body: &[u8]) -> PaymentResult<HmacSha256> {
    if secret.is_empty() {
        return Err(PaymentError("webhook secret is not set".into()));
    }
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).map_err(|e| PaymentError(format!("webhook secret: {e}")))?;
    mac.update(timestamp.to_string().as_bytes());
    mac.update(b".");
    mac.update(body);
    Ok(mac)
}

/// The `Fake-Signature` header value for `body`, signed at `timestamp` (unix seconds)
pub fn sign(secret: &str, timestamp: i64, body: &[u8]) -> PaymentResult<String> {
    let tag = mac(secret, timestamp, body)?.finalize().into_bytes();
    Ok(format!("t={timestamp},v1={}", hex::encode(tag)))
}

/// Checks a `Fake-Signature` header against the raw body. Refuses a missing or
/// malformed header, a timestamp more than `TOLERANCE_SECS` from `now` (either
/// way), and a signature that does not match; the comparison is constant-time
/// (`Mac::verify_slice`). Several `v1=` entries are allowed (key rotation):
/// any one matching is enough.
pub fn verify_signature(secret: &str, header: &str, body: &[u8], now: i64) -> PaymentResult<()> {
    let mut timestamp = None;
    let mut candidates = Vec::new();
    for part in header.split(',') {
        match part.trim().split_once('=') {
            Some(("t", value)) => timestamp = value.parse::<i64>().ok(),
            Some(("v1", value)) => candidates.push(value),
            _ => {}
        }
    }
    let timestamp = timestamp.ok_or_else(|| PaymentError("signature has no timestamp".into()))?;
    if (now - timestamp).abs() > TOLERANCE_SECS {
        return Err(PaymentError("signature timestamp is outside the tolerance".into()));
    }
    for candidate in candidates {
        let Ok(tag) = hex::decode(candidate) else { continue };
        if mac(secret, timestamp, body)?.verify_slice(&tag).is_ok() {
            return Ok(());
        }
    }
    Err(PaymentError("signature does not match".into()))
}

/// What an event means to us
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    /// The user paid
    PaymentSucceeded,
    /// The user cancelled
    PaymentFailed,
    /// Anything else the provider may send; stored, needs no action
    Other,
}

/// A verified provider event, reduced to what the backend uses. `payload` is
/// the whole event, as stored in `payment_inbox`.
#[derive(Debug, Clone, PartialEq)]
pub struct ProviderEvent {
    /// The provider's event id; unique, and the same on every redelivery
    pub id: String,
    pub kind: EventKind,
    /// The provider's event type, as sent
    pub event_type: String,
    pub session_id: String,
    /// Our queue ticket, which the session was created for
    pub ticket: String,
    pub amount_cents: i64,
    /// The payment to refund; set for a success
    pub payment_id: Option<String>,
    pub payload: Value,
}

impl ProviderEvent {
    /// Reads an event from its JSON (`{id, type, created, data: {object:
    /// {session_id, ticket, amount_cents, payment_id}}}`); `None` if a
    /// required field is missing.
    pub fn from_json(payload: Value) -> Option<Self> {
        let object = payload.get("data")?.get("object")?;
        let event_type = payload.get("type")?.as_str()?.to_owned();
        let kind = match event_type.as_str() {
            CHECKOUT_COMPLETED => EventKind::PaymentSucceeded,
            CHECKOUT_PAYMENT_FAILED => EventKind::PaymentFailed,
            _ => EventKind::Other,
        };
        let payment_id = object.get("payment_id").and_then(Value::as_str).map(str::to_owned);
        if kind == EventKind::PaymentSucceeded && payment_id.is_none() {
            return None;
        }
        Some(Self {
            id: payload.get("id")?.as_str()?.to_owned(),
            kind,
            event_type,
            session_id: object.get("session_id")?.as_str()?.to_owned(),
            ticket: object.get("ticket")?.as_str()?.to_owned(),
            amount_cents: object.get("amount_cents")?.as_i64()?,
            payment_id,
            payload,
        })
    }
}

/// The JSON body of an event the fake provider sends
pub fn event_body(
    id: &str,
    event_type: &str,
    created: i64,
    session_id: &str,
    ticket: &str,
    amount_cents: i64,
    payment_id: Option<&str>,
) -> String {
    json!({
        "id": id,
        "type": event_type,
        "created": created,
        "data": { "object": {
            "session_id": session_id,
            "ticket": ticket,
            "amount_cents": amount_cents,
            "payment_id": payment_id,
        } },
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "whsec_test";
    const BODY: &[u8] = br#"{"id":"evt_1"}"#;

    #[test]
    fn a_valid_signature_verifies() {
        let header = sign(SECRET, 1_000, BODY).unwrap();
        assert!(header.starts_with("t=1000,v1="));
        verify_signature(SECRET, &header, BODY, 1_000).unwrap();
        verify_signature(SECRET, &header, BODY, 1_000 + TOLERANCE_SECS).unwrap();
    }

    #[test]
    fn a_tampered_body_or_wrong_secret_fails() {
        let header = sign(SECRET, 1_000, BODY).unwrap();
        assert!(verify_signature(SECRET, &header, br#"{"id":"evt_2"}"#, 1_000).is_err());
        assert!(verify_signature("other", &header, BODY, 1_000).is_err());
        // Changing the timestamp in the header changes what was signed
        let moved = header.replace("t=1000", "t=1001");
        assert!(verify_signature(SECRET, &moved, BODY, 1_001).is_err());
    }

    #[test]
    fn a_stale_or_future_timestamp_is_refused() {
        let header = sign(SECRET, 1_000, BODY).unwrap();
        assert!(verify_signature(SECRET, &header, BODY, 1_000 + TOLERANCE_SECS + 1).is_err());
        assert!(verify_signature(SECRET, &header, BODY, 1_000 - TOLERANCE_SECS - 1).is_err());
    }

    #[test]
    fn malformed_headers_and_empty_secrets_fail() {
        for header in ["", "garbage", "t=abc,v1=00", "v1=00", "t=1000", "t=1000,v1=zz"] {
            assert!(verify_signature(SECRET, header, BODY, 1_000).is_err(), "{header}");
        }
        assert!(sign("", 1_000, BODY).is_err());
        let header = sign(SECRET, 1_000, BODY).unwrap();
        assert!(verify_signature("", &header, BODY, 1_000).is_err());
    }

    #[test]
    fn rotation_accepts_any_matching_signature() {
        let good = sign(SECRET, 1_000, BODY).unwrap();
        let tag = good.split_once("v1=").unwrap().1;
        let header = format!("t=1000,v1={},v1={tag}", "00".repeat(32));
        verify_signature(SECRET, &header, BODY, 1_000).unwrap();
    }

    #[test]
    fn events_round_trip() {
        let body = event_body("evt_1", CHECKOUT_COMPLETED, 5, "cs_1", "7.abc", 100, Some("pi_1"));
        let event = ProviderEvent::from_json(serde_json::from_str(&body).unwrap()).unwrap();
        assert_eq!(
            (event.kind, event.session_id.as_str(), event.ticket.as_str(), event.amount_cents, event.payment_id.as_deref()),
            (EventKind::PaymentSucceeded, "cs_1", "7.abc", 100, Some("pi_1"))
        );

        let body = event_body("evt_2", CHECKOUT_PAYMENT_FAILED, 5, "cs_1", "7.abc", 100, None);
        let event = ProviderEvent::from_json(serde_json::from_str(&body).unwrap()).unwrap();
        assert_eq!((event.kind, event.payment_id), (EventKind::PaymentFailed, None));

        // A success without a payment id, or a payload of another shape, is not an event
        let body = event_body("evt_3", CHECKOUT_COMPLETED, 5, "cs_1", "7.abc", 100, None);
        assert!(ProviderEvent::from_json(serde_json::from_str(&body).unwrap()).is_none());
        assert!(ProviderEvent::from_json(json!({"id": "x"})).is_none());
    }
}
