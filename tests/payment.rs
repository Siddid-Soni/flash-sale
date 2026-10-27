//! The fake payment provider against a real Redis (database 15).

mod common;

use chrono::{Duration, Utc};
use flash_sale::payment::{fake_session_id, FakeProvider, PaymentProvider};

async fn provider() -> FakeProvider {
    let env = common::env().await;
    FakeProvider::new(env.redis.clone(), "http://pay.test/".into(), "secret".into())
}

fn ticket() -> String {
    format!("1.{}", uuid::Uuid::new_v4())
}

#[tokio::test]
async fn a_session_has_a_url_and_is_stored_open() {
    let provider = provider().await;
    let ticket = ticket();
    let expires_at = Utc::now() + Duration::minutes(5);
    let session = provider.create_session(&ticket, 59_900, expires_at, &ticket).await.unwrap();

    assert_eq!(session.id, fake_session_id(&ticket));
    assert_eq!(session.url, format!("http://pay.test/fake-pay/{}", session.id));
    let stored = provider.session(&session.id).await.unwrap().unwrap();
    assert_eq!((stored.ticket.as_str(), stored.amount_cents, stored.state.as_str()), (ticket.as_str(), 59_900, "OPEN"));
    assert_eq!(stored.expires_at.timestamp_millis(), expires_at.timestamp_millis());
    assert!(provider.session("fake_cs_unknown").await.unwrap().is_none());
}

/// A redelivered Kafka message must not create a second session
#[tokio::test]
async fn creation_is_idempotent_by_ticket() {
    let provider = provider().await;
    let ticket = ticket();
    let expires_at = Utc::now() + Duration::minutes(5);

    let first = provider.create_session(&ticket, 100, expires_at, &ticket).await.unwrap();
    let second = provider.create_session(&ticket, 100, expires_at + Duration::minutes(1), &ticket).await.unwrap();
    assert_eq!(first, second);
    // Even under a different idempotency key the ticket keeps its one session
    let third = provider.create_session(&ticket, 100, expires_at, "another-key").await.unwrap();
    assert_eq!(third, first);
    assert!(provider.session(&fake_session_id("another-key")).await.unwrap().is_none(), "no second session exists");
    assert_eq!(provider.session_id_for_ticket(&ticket).await.unwrap(), Some(first.id.clone()));

    // The first call's data stands, and an expired session stays the same session
    assert_eq!(provider.session(&first.id).await.unwrap().unwrap().expires_at.timestamp_millis(), expires_at.timestamp_millis());
    provider.expire_session(&first.id).await.unwrap();
    assert_eq!(provider.create_session(&ticket, 100, expires_at, &ticket).await.unwrap(), first);
}

#[tokio::test]
async fn expire_is_idempotent_and_refund_needs_a_payment() {
    let provider = provider().await;
    let ticket = ticket();
    let session = provider.create_session(&ticket, 100, Utc::now() + Duration::minutes(5), &ticket).await.unwrap();

    provider.expire_session(&session.id).await.unwrap();
    provider.expire_session(&session.id).await.unwrap();
    provider.expire_session("fake_cs_unknown").await.unwrap();
    assert_eq!(provider.session(&session.id).await.unwrap().unwrap().state, "EXPIRED");

    // Nothing was paid, so there is nothing to refund
    assert!(provider.refund(&session.id).await.is_err());
    assert!(provider.refund("fake_cs_unknown").await.is_err());
}
