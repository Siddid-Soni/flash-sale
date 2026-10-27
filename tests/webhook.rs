//! `POST /v1/payments/webhook`: signature, inbox, claim on receipt.

mod common;

use actix_web::{http::StatusCode, test};
use chrono::Utc;
use diesel::prelude::*;

use common::sale::SaleFixture;
use flash_sale::{
    models::OrderStatus,
    payment::webhook::{self, SIGNATURE_HEADER},
    schema::payment_inbox,
};

/// Posts a body with an optional `Fake-Signature` and returns the status
macro_rules! post {
    ($app:expr, $body:expr, $signature:expr) => {{
        let mut req = test::TestRequest::post().uri("/v1/payments/webhook").set_payload($body.to_string());
        if let Some(signature) = $signature as Option<String> {
            req = req.insert_header((SIGNATURE_HEADER, signature));
        }
        test::call_service(&$app, req.to_request()).await.status()
    }};
}

fn stored(fx: &SaleFixture, event_id: &str) -> i64 {
    payment_inbox::table
        .filter(payment_inbox::provider_event_id.eq(event_id))
        .count()
        .get_result(&mut fx.env.pool.get().unwrap())
        .unwrap()
}

#[actix_web::test]
async fn a_signed_success_is_stored_and_claims_the_hold() {
    let fx = SaleFixture::new(3).await;
    let a = fx.awaiting().await;
    let wire = fx.pay(&a).await;
    let app = test_app!(fx.env);

    assert_eq!(post!(app, &wire.body, Some(wire.signature())), StatusCode::OK);

    let row = fx.inbox_row(&wire.event.id).expect("the event is in the inbox");
    assert_eq!(row.event_type, "checkout.session.completed");
    assert!(row.processed_at.is_none());
    assert_eq!(row.payload["data"]["object"]["ticket"], a.event.ticket.as_str());
    // Claimed on receipt, before any processor ran: PAID, out of the expiry set
    assert_eq!(fx.hold_state(&a.event.ticket).await, "PAID");
    let still_queued: Option<f64> = redis::AsyncCommands::zscore(
        &mut fx.env.redis.clone(),
        flash_sale::cache::holds_expiry_key(fx.sale),
        &a.event.ticket,
    )
    .await
    .unwrap();
    assert!(still_queued.is_none());
    // The order is not confirmed yet: that is the processor's job
    assert_eq!(fx.order(&a.event.ticket).unwrap().status, OrderStatus::AwaitingPayment);
    fx.cleanup().await;
}

#[actix_web::test]
async fn a_duplicate_event_is_answered_200_and_stored_once() {
    let fx = SaleFixture::new(3).await;
    let a = fx.awaiting().await;
    let wire = fx.pay(&a).await;
    let app = test_app!(fx.env);

    assert_eq!(post!(app, &wire.body, Some(wire.signature())), StatusCode::OK);
    assert_eq!(post!(app, &wire.body, Some(wire.signature())), StatusCode::OK);
    assert_eq!(stored(&fx, &wire.event.id), 1);
    assert_eq!(fx.hold_state(&a.event.ticket).await, "PAID");
    fx.cleanup().await;
}

#[actix_web::test]
async fn a_bad_signature_is_refused_and_nothing_is_stored() {
    let fx = SaleFixture::new(3).await;
    let a = fx.awaiting().await;
    let wire = fx.pay(&a).await;
    let app = test_app!(fx.env);

    // No header, a wrong secret, a tampered body, a stale timestamp, garbage
    let wrong_secret = webhook::sign("another-secret", Utc::now().timestamp(), wire.body.as_bytes()).unwrap();
    let tampered = wire.body.replace("59900", "1");
    let stale = webhook::sign(common::WEBHOOK_SECRET, Utc::now().timestamp() - 301, wire.body.as_bytes()).unwrap();
    assert_eq!(post!(app, &wire.body, None), StatusCode::BAD_REQUEST);
    assert_eq!(post!(app, &wire.body, Some(wrong_secret)), StatusCode::BAD_REQUEST);
    assert_eq!(post!(app, &tampered, Some(wire.signature())), StatusCode::BAD_REQUEST);
    assert_eq!(post!(app, &wire.body, Some(stale)), StatusCode::BAD_REQUEST);
    assert_eq!(post!(app, &wire.body, Some("garbage".to_string())), StatusCode::BAD_REQUEST);

    assert_eq!(stored(&fx, &wire.event.id), 0);
    assert_eq!(fx.hold_state(&a.event.ticket).await, "AWAITING_PAYMENT", "an unverified event claims nothing");
    fx.cleanup().await;
}

#[actix_web::test]
async fn a_signed_body_that_is_not_an_event_is_refused() {
    let fx = SaleFixture::new(1).await;
    let app = test_app!(fx.env);
    let body = r#"{"hello":"world"}"#;
    let signature = webhook::sign(common::WEBHOOK_SECRET, Utc::now().timestamp(), body.as_bytes()).unwrap();
    assert_eq!(post!(app, body, Some(signature)), StatusCode::BAD_REQUEST);
    fx.cleanup().await;
}

/// A payment that arrives after the deadline is stored (the processor will
/// refund it) but the hold is not claimed
#[actix_web::test]
async fn a_late_payment_is_stored_without_claiming() {
    let fx = SaleFixture::new(3).await;
    let a = fx.awaiting().await;
    let wire = fx.pay(&a).await;
    // The deadline passes and the hold is released
    let released = flash_sale::cache::release_hold(
        &mut fx.env.redis.clone(),
        fx.sale,
        &a.event.ticket,
        flash_sale::cache::HoldState::AwaitingPayment,
        flash_sale::cache::MaxScore::Any,
        flash_sale::cache::ReleasedAs::Expired,
        false,
    )
    .await
    .unwrap();
    assert_eq!(released, flash_sale::cache::ReleaseOutcome::Released);
    let app = test_app!(fx.env);

    assert_eq!(post!(app, &wire.body, Some(wire.signature())), StatusCode::OK);
    assert_eq!(stored(&fx, &wire.event.id), 1);
    assert_eq!(fx.hold_state(&a.event.ticket).await, "EXPIRED");
    fx.cleanup().await;
}

#[actix_web::test]
async fn a_failure_event_is_stored_and_changes_nothing() {
    let fx = SaleFixture::new(3).await;
    let a = fx.awaiting().await;
    let wire = fx.cancel(&a).await;
    let app = test_app!(fx.env);

    assert_eq!(post!(app, &wire.body, Some(wire.signature())), StatusCode::OK);
    assert_eq!(stored(&fx, &wire.event.id), 1);
    assert_eq!(fx.hold_state(&a.event.ticket).await, "AWAITING_PAYMENT");
    fx.cleanup().await;
}
