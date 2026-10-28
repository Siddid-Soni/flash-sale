//! `POST /v1/payments/webhook` on the order processor: signature, inbox, and
//! the decision on receipt.

mod common;

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use actix_web::http::{header::HeaderMap, StatusCode};
use actix_web::test;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use diesel::prelude::*;

use common::sale::{clear_inbox, SaleFixture};
use flash_sale::{
    inbox::InboxWorker,
    models::{OrderState, OrderStatus},
    payment::{
        webhook::{self, SIGNATURE_HEADER},
        PaymentError, PaymentProvider, PaymentResult, ProviderEvent, Session, SharedProvider,
    },
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

async fn assert_not_decided(fx: &SaleFixture, ticket: &str) {
    assert_eq!(fx.order(ticket).unwrap().status, OrderState::AwaitingPayment);
    assert_eq!(fx.stock().await, 2);
}

#[actix_web::test]
async fn a_signed_success_is_stored_and_confirms_the_order_at_once() {
    let fx = SaleFixture::new(3).await;
    let a = fx.awaiting().await;
    let wire = fx.pay(&a).await;
    let app = processor_app!(fx.env, fx.inbox_worker());

    assert_eq!(post!(app, &wire.body, Some(wire.signature())), StatusCode::OK);

    let row = fx.inbox_row(&wire.event.id).expect("the event is in the inbox");
    assert_eq!(row.event_type, "checkout.session.completed");
    assert!(row.processed_at.is_some(), "handled on receipt");
    assert_eq!(row.payload["data"]["object"]["ticket"], a.event.ticket.as_str());
    // Decided by PostgreSQL, not by a claim in Redis
    let order = fx.order(&a.event.ticket).unwrap();
    assert_eq!(order.status, OrderState::Confirmed);
    assert_eq!(fx.product().sold, 1);
    assert_eq!(fx.status(&a.event.ticket).await.unwrap().status, OrderStatus::Confirmed);
    // The payment path touches no inventory key
    assert_eq!(fx.stock().await, 2);
    assert_eq!(fx.user_live(a.event.user).await.as_deref(), Some(a.event.ticket.as_str()));
    fx.cleanup().await;
}

#[actix_web::test]
async fn a_duplicate_event_is_answered_200_and_stored_once() {
    let fx = SaleFixture::new(3).await;
    let a = fx.awaiting().await;
    let wire = fx.pay(&a).await;
    let app = processor_app!(fx.env, fx.inbox_worker());

    assert_eq!(post!(app, &wire.body, Some(wire.signature())), StatusCode::OK);
    assert_eq!(post!(app, &wire.body, Some(wire.signature())), StatusCode::OK);
    assert_eq!(stored(&fx, &wire.event.id), 1);
    assert_eq!(fx.order(&a.event.ticket).unwrap().status, OrderState::Confirmed);
    assert_eq!(fx.product().sold, 1, "counted once");
    fx.cleanup().await;
}

#[actix_web::test]
async fn a_bad_signature_is_refused_and_nothing_is_stored() {
    let fx = SaleFixture::new(3).await;
    let a = fx.awaiting().await;
    let wire = fx.pay(&a).await;
    let app = processor_app!(fx.env, fx.inbox_worker());

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
    assert_not_decided(&fx, &a.event.ticket).await;
    fx.cleanup().await;
}

#[actix_web::test]
async fn a_signed_body_that_is_not_an_event_is_refused() {
    let fx = SaleFixture::new(1).await;
    let app = processor_app!(fx.env, fx.inbox_worker());
    let body = r#"{"hello":"world"}"#;
    let signature = webhook::sign(common::WEBHOOK_SECRET, Utc::now().timestamp(), body.as_bytes()).unwrap();
    assert_eq!(post!(app, body, Some(signature)), StatusCode::BAD_REQUEST);
    fx.cleanup().await;
}

/// A payment that arrives after PostgreSQL's deadline, before the worker has
/// expired the order, is stored and answered 200 but decided by nobody: the
/// inbox never decides a deadline. Once the worker has expired the order (and
/// returned the unit) the provider's redelivery is refunded, moving no stock.
#[actix_web::test]
async fn a_late_payment_waits_for_the_worker_and_is_then_refunded() {
    let _due = common::sale::due_guard().await;
    let fx = SaleFixture::new(3).await;
    let a = fx.awaiting().await;
    let wire = fx.pay(&a).await;
    fx.make_due(&a.event.ticket);
    let worker = fx.inbox_worker();
    let app = processor_app!(fx.env, worker);

    assert_eq!(post!(app, &wire.body, Some(wire.signature())), StatusCode::OK);
    assert_eq!(stored(&fx, &wire.event.id), 1);
    assert!(fx.inbox_row(&wire.event.id).unwrap().processed_at.is_none(), "left for the inbox loop");
    assert_eq!(fx.order(&a.event.ticket).unwrap().status, OrderState::AwaitingPayment, "nothing decided");
    assert_eq!(fx.fake().session(&a.session.id).await.unwrap().unwrap().state, "PAID");
    assert_eq!((fx.product().sold, fx.stock().await), (0, 2));

    // The worker expires the order and returns the unit...
    assert!(fx.expire_due().await >= 1);
    assert_eq!(fx.order(&a.event.ticket).unwrap().status, OrderState::Expired);
    assert_eq!(fx.stock().await, 3);

    // ...and the redelivery is refunded with no stock moving
    assert_eq!(post!(app, &wire.body, Some(wire.signature())), StatusCode::OK);
    assert!(fx.inbox_row(&wire.event.id).unwrap().processed_at.is_some());
    assert_eq!(fx.order(&a.event.ticket).unwrap().status, OrderState::Refunded);
    assert_eq!(fx.fake().session(&a.session.id).await.unwrap().unwrap().state, "REFUNDED");
    assert_eq!(fx.status(&a.event.ticket).await.unwrap().status, OrderStatus::Refunded);
    assert_eq!((fx.product().sold, fx.stock().await), (0, 3));
    fx.cleanup().await;
}

#[actix_web::test]
async fn a_failure_event_is_stored_and_changes_nothing() {
    let fx = SaleFixture::new(3).await;
    let a = fx.awaiting().await;
    let wire = fx.cancel(&a).await;
    let app = processor_app!(fx.env, fx.inbox_worker());

    assert_eq!(post!(app, &wire.body, Some(wire.signature())), StatusCode::OK);
    assert_eq!(stored(&fx, &wire.event.id), 1);
    assert!(fx.inbox_row(&wire.event.id).unwrap().processed_at.is_some());
    assert_not_decided(&fx, &a.event.ticket).await;
    fx.cleanup().await;
}

/// Delegates to the real provider, but its refunds fail while `fail` is set
struct FailingRefund {
    inner: SharedProvider,
    fail: AtomicBool,
}

#[async_trait]
impl PaymentProvider for FailingRefund {
    fn name(&self) -> &'static str {
        "failing"
    }

    async fn create_session(&self, t: &str, amount: i32, at: DateTime<Utc>, key: &str) -> PaymentResult<Session> {
        self.inner.create_session(t, amount, at, key).await
    }

    async fn expire_session(&self, id: &str) -> PaymentResult<()> {
        self.inner.expire_session(id).await
    }

    async fn refund(&self, id: &str) -> PaymentResult<()> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(PaymentError("provider is down".into()));
        }
        self.inner.refund(id).await
    }

    fn verify_webhook(&self, headers: &HeaderMap, body: &[u8]) -> PaymentResult<ProviderEvent> {
        self.inner.verify_webhook(headers, body)
    }
}

/// Once the event is stored the answer is 200 even if handling fails: the row
/// stays unprocessed with the error recorded, and the inbox loop finishes it
#[actix_web::test]
async fn a_handling_error_after_the_insert_still_answers_200_and_the_loop_retries() {
    let _due = common::sale::due_guard().await;
    let fx = SaleFixture::new(3).await;
    clear_inbox(&fx.env);
    let a = fx.awaiting().await;
    let wire = fx.pay(&a).await;
    fx.make_due(&a.event.ticket);
    // The worker has expired the order, so the payment is refunded on receipt
    fx.expire_due().await;
    let failing = Arc::new(FailingRefund { inner: fx.env.shared_provider(), fail: AtomicBool::new(true) });
    let provider: SharedProvider = failing.clone();
    let inbox = Arc::new(InboxWorker::new(fx.env.pool.clone(), fx.env.read_cache.clone(), provider.clone()));
    let app = test::init_service(
        actix_web::App::new()
            .app_data(actix_web::web::Data::new(provider))
            .app_data(actix_web::web::Data::from(inbox.clone()))
            .configure(flash_sale::api::payments::configure),
    )
    .await;

    assert_eq!(post!(app, &wire.body, Some(wire.signature())), StatusCode::OK);
    let row = fx.inbox_row(&wire.event.id).unwrap();
    assert!(row.processed_at.is_none());
    assert!(row.last_error.unwrap().contains("provider is down"));
    // The decision was made and recorded before the refund was tried
    assert_eq!(fx.order(&a.event.ticket).unwrap().status, OrderState::Refunded);
    assert_eq!(fx.status(&a.event.ticket).await.unwrap().status, OrderStatus::Expired, "not told before the refund");

    // The provider's redelivery meanwhile is answered 200 too; then the loop gets there
    assert_eq!(post!(app, &wire.body, Some(wire.signature())), StatusCode::OK);
    failing.fail.store(false, Ordering::SeqCst);
    assert!(inbox.process_next().await.unwrap());
    assert!(fx.inbox_row(&wire.event.id).unwrap().processed_at.is_some());
    assert_eq!(fx.fake().session(&a.session.id).await.unwrap().unwrap().state, "REFUNDED");
    assert_eq!(fx.status(&a.event.ticket).await.unwrap().status, OrderStatus::Refunded);
    fx.cleanup().await;
}
