//! Handling payment events (`flash_sale::inbox`) against real Postgres and
//! Redis: confirm, refund, crash recovery, retries, several workers.
//!
//! The inbox is shared by every test in the database, so these tests take a
//! lock and start by marking leftover rows processed: a worker here handles
//! only the events its own test stored.

mod common;

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

use actix_web::http::header::HeaderMap;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use diesel::prelude::*;

use common::sale::{clear_inbox, drain, Awaiting, SaleFixture};
use flash_sale::{
    actions::{self, orders::Settlement},
    cache::{self, HoldState, MaxScore, ReleaseOutcome, ReleasedAs},
    inbox::{Handled, InboxWorker},
    models::OrderStatus,
    payment::{
        fake_payment_id,
        webhook::{self, CHECKOUT_COMPLETED},
        PaymentError, PaymentProvider, PaymentResult, ProviderEvent, Session, Settle, SharedProvider,
    },
    schema::products,
};

static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn setup(stock: i32) -> (tokio::sync::MutexGuard<'static, ()>, SaleFixture) {
    let guard = LOCK.lock().await;
    let fx = SaleFixture::new(stock).await;
    clear_inbox(&fx.env);
    (guard, fx)
}

/// What must agree once every event is processed: the payment path counts a
/// sale in PostgreSQL only; `sold:{sale}` is the worker's, still 0
async fn assert_consistent(fx: &SaleFixture) {
    let confirmed = fx.count_orders(OrderStatus::Confirmed);
    assert_eq!(fx.product().sold as i64, confirmed, "products.sold == CONFIRMED orders");
    assert_eq!(fx.sold_counter().await, 0, "sold:{{sale}} is counted by the expiration worker");
}

async fn assert_confirmed(fx: &SaleFixture, a: &Awaiting, event_id: &str) {
    let order = fx.order(&a.event.ticket).unwrap();
    assert_eq!(order.status, OrderStatus::Confirmed);
    assert_eq!(fx.hold_state(&a.event.ticket).await, "AWAITING_PAYMENT", "the worker settles the hold");
    let status = fx.status(&a.event.ticket).await.unwrap();
    assert_eq!((status.status, status.user), (OrderStatus::Confirmed, a.event.user));
    assert!(fx.inbox_row(event_id).unwrap().processed_at.is_some());
}

async fn assert_refunded(fx: &SaleFixture, a: &Awaiting, event_id: &str) {
    assert_eq!(fx.order(&a.event.ticket).unwrap().status, OrderStatus::Refunded);
    assert_eq!(fx.fake().session(&a.session.id).await.unwrap().unwrap().state, "REFUNDED");
    assert_eq!(fx.status(&a.event.ticket).await.unwrap().status, OrderStatus::Refunded);
    assert!(fx.inbox_row(event_id).unwrap().processed_at.is_some());
}

fn settle_by_hand(fx: &SaleFixture, a: &Awaiting) -> Settlement {
    actions::orders::settle_payment(&mut fx.env.pool.get().unwrap(), &a.event.ticket, &a.session.id, fx.sale).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_payment_in_time_confirms_the_order() {
    let (_guard, fx) = setup(3).await;
    let a = fx.awaiting().await;
    let wire = fx.pay(&a).await;
    fx.store(&wire);

    let worker = fx.inbox_worker();
    assert_eq!(drain(&worker).await, 1);

    assert_confirmed(&fx, &a, &wire.event.id).await;
    assert_eq!(fx.product().sold, 1);
    assert_eq!(fx.stock().await, 2, "the unit stays taken");
    assert_eq!(fx.expiry_len().await, 1, "the hold waits for the worker");
    assert_eq!(fx.fake().session(&a.session.id).await.unwrap().unwrap().state, "PAID");
    let row = fx.inbox_row(&wire.event.id).unwrap();
    assert_eq!(row.last_error, None);
    assert_consistent(&fx).await;

    // Running it again changes nothing
    assert_eq!(drain(&worker).await, 0);
    assert_consistent(&fx).await;
    fx.cleanup().await;
}

/// PostgreSQL's clock decides: a payment after the deadline is refunded even
/// though the hold was never released
#[tokio::test(flavor = "multi_thread")]
async fn a_payment_after_the_deadline_but_before_the_release_is_refunded() {
    let (_guard, fx) = setup(3).await;
    let a = fx.awaiting().await;
    fx.make_due(&a.event.ticket).await;
    let wire = fx.pay(&a).await;
    fx.store(&wire);

    assert_eq!(drain(&fx.inbox_worker()).await, 1);

    assert_refunded(&fx, &a, &wire.event.id).await;
    assert_eq!(fx.product().sold, 0);
    // The hold is still the worker's: it will find the order REFUNDED and return the unit
    assert_eq!(fx.hold_state(&a.event.ticket).await, "AWAITING_PAYMENT");
    assert_eq!(fx.expiry_len().await, 1);
    assert_eq!(fx.stock().await, 2);
    assert_consistent(&fx).await;
    fx.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_late_payment_after_the_worker_expired_the_order_is_refunded() {
    let (_guard, fx) = setup(3).await;
    let a = fx.awaiting().await;
    // The deadline passed and the expiration worker expired the order and released the hold ...
    fx.make_due(&a.event.ticket).await;
    let expired = actions::orders::expire_due(&mut fx.env.pool.get().unwrap(), std::slice::from_ref(&a.event.ticket)).unwrap();
    assert_eq!(expired, vec![a.event.ticket.clone()]);
    let released = cache::release_hold(
        &mut fx.env.redis.clone(),
        fx.sale,
        &a.event.ticket,
        HoldState::AwaitingPayment,
        MaxScore::Now,
        ReleasedAs::Expired,
        false,
    )
    .await
    .unwrap();
    assert_eq!(released, ReleaseOutcome::Released);
    // ... but the provider's session was still open when the user paid
    let wire = fx.pay(&a).await;
    fx.store(&wire);

    assert_eq!(drain(&fx.inbox_worker()).await, 1);

    assert_refunded(&fx, &a, &wire.event.id).await;
    assert_eq!(fx.hold_state(&a.event.ticket).await, "EXPIRED", "the released hold is left as it is");
    assert_eq!(fx.product().sold, 0);
    assert_eq!(fx.stock().await, 3, "the release already returned the unit; the refund adds none");
    assert_consistent(&fx).await;
    fx.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_guard_rolls_back_and_refunds() {
    let (_guard, fx) = setup(3).await;
    let a = fx.awaiting().await;
    // PostgreSQL believes everything is sold although Redis let this one through
    diesel::update(products::table.find(fx.sale))
        .set(products::sold.eq(products::total_inventory))
        .execute(&mut fx.env.pool.get().unwrap())
        .unwrap();
    let wire = fx.pay(&a).await;
    fx.store(&wire);

    assert_eq!(drain(&fx.inbox_worker()).await, 1);

    assert_refunded(&fx, &a, &wire.event.id).await;
    assert_eq!(fx.product().sold, 3, "sold was not raised");
    assert_eq!(fx.sold_counter().await, 0);
    // Redis is not touched: the worker drops the hold when it is due
    assert_eq!(fx.hold_state(&a.event.ticket).await, "AWAITING_PAYMENT");
    assert_eq!(fx.expiry_len().await, 1);
    fx.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cancelled_payment_needs_no_action() {
    let (_guard, fx) = setup(3).await;
    let a = fx.awaiting().await;
    let wire = fx.cancel(&a).await;
    fx.store(&wire);

    assert_eq!(drain(&fx.inbox_worker()).await, 1);

    assert!(fx.inbox_row(&wire.event.id).unwrap().processed_at.is_some());
    assert_eq!(fx.order(&a.event.ticket).unwrap().status, OrderStatus::AwaitingPayment);
    assert_eq!(fx.hold_state(&a.event.ticket).await, "AWAITING_PAYMENT", "the hold runs out by itself");
    assert_eq!(fx.status(&a.event.ticket).await.unwrap().status, OrderStatus::AwaitingPayment);
    fx.cleanup().await;
}

/// A payment the backend has no order for, or on a session the order does not
/// own, is given back and no order is touched
#[tokio::test(flavor = "multi_thread")]
async fn a_payment_without_an_order_or_on_a_foreign_session_is_refunded() {
    let (_guard, fx) = setup(3).await;
    let worker = fx.inbox_worker();
    let paid_event = |event_id: &str, session_id: &str, ticket: &str| {
        let body = webhook::event_body(
            event_id,
            CHECKOUT_COMPLETED,
            Utc::now().timestamp(),
            session_id,
            ticket,
            100,
            Some(&fake_payment_id(session_id)),
        );
        ProviderEvent::from_json(serde_json::from_str(&body).unwrap()).unwrap()
    };
    let paid_session = |ticket: String| {
        let fx = &fx;
        async move {
            let session = fx.fake().create_session(&ticket, 100, Utc::now() + chrono::Duration::minutes(5), &ticket).await.unwrap();
            let stored = fx.fake().session(&session.id).await.unwrap().unwrap();
            fx.fake().settle(&stored, Settle::Pay).await.unwrap();
            session
        }
    };

    // No order for the ticket
    let ticket = cache::new_ticket(fx.sale);
    let session = paid_session(ticket.clone()).await;
    let event = paid_event("evt_no_order", &session.id, &ticket);
    assert_eq!(worker.handle_event(&event).await.unwrap(), Handled::Refunded);
    assert_eq!(fx.fake().session(&session.id).await.unwrap().unwrap().state, "REFUNDED");

    // An order exists, but the paid session is not its session
    let a = fx.awaiting().await;
    let stray = paid_session(format!("stray-{}", a.event.ticket)).await;
    let event = paid_event("evt_foreign", &stray.id, &a.event.ticket);
    assert_eq!(worker.handle_event(&event).await.unwrap(), Handled::Refunded);
    assert_eq!(fx.fake().session(&stray.id).await.unwrap().unwrap().state, "REFUNDED");
    assert_eq!(fx.order(&a.event.ticket).unwrap().status, OrderStatus::AwaitingPayment, "the order keeps its own session");
    assert_eq!(fx.fake().session(&a.session.id).await.unwrap().unwrap().state, "OPEN");
    fx.cleanup().await;
}

/// The decision committed, the process died before the provider refund: the
/// order is already REFUNDED, the row is not processed, the rerun refunds
#[tokio::test(flavor = "multi_thread")]
async fn a_crash_after_the_decision_before_the_refund_converges() {
    let (_guard, fx) = setup(3).await;
    let a = fx.awaiting().await;
    fx.make_due(&a.event.ticket).await;
    let wire = fx.pay(&a).await;
    fx.store(&wire);

    assert_eq!(settle_by_hand(&fx, &a), Settlement::Refund);
    assert_eq!(fx.order(&a.event.ticket).unwrap().status, OrderStatus::Refunded);
    assert_eq!(fx.fake().session(&a.session.id).await.unwrap().unwrap().state, "PAID", "not refunded yet");
    assert!(fx.inbox_row(&wire.event.id).unwrap().processed_at.is_none());

    assert_eq!(drain(&fx.inbox_worker()).await, 1);
    assert_refunded(&fx, &a, &wire.event.id).await;
    assert_consistent(&fx).await;
    fx.cleanup().await;
}

/// The refund went through, the process died before the row was marked
/// processed: the provider's refund is idempotent, so the rerun just finishes
#[tokio::test(flavor = "multi_thread")]
async fn a_crash_after_the_refund_before_the_row_is_marked_converges() {
    let (_guard, fx) = setup(3).await;
    let a = fx.awaiting().await;
    fx.make_due(&a.event.ticket).await;
    let wire = fx.pay(&a).await;
    fx.store(&wire);

    assert_eq!(settle_by_hand(&fx, &a), Settlement::Refund);
    fx.fake().refund(&a.session.id).await.unwrap();
    assert!(fx.inbox_row(&wire.event.id).unwrap().processed_at.is_none());
    assert!(fx.status(&a.event.ticket).await.unwrap().status != OrderStatus::Refunded);

    assert_eq!(drain(&fx.inbox_worker()).await, 1);
    assert_refunded(&fx, &a, &wire.event.id).await;
    // And once more, as after a second crash
    let worker = fx.inbox_worker();
    let event = ProviderEvent::from_json(wire.event.payload.clone()).unwrap();
    assert_eq!(worker.handle_event(&event).await.unwrap(), Handled::Refunded);
    assert_consistent(&fx).await;
    fx.cleanup().await;
}

/// The confirmation committed, the process died before the status write: the
/// rerun finds the order CONFIRMED and writes only the status, counting nothing again
#[tokio::test(flavor = "multi_thread")]
async fn a_crash_after_the_confirmation_before_the_status_converges() {
    let (_guard, fx) = setup(3).await;
    let a = fx.awaiting().await;
    let wire = fx.pay(&a).await;
    fx.store(&wire);

    assert_eq!(settle_by_hand(&fx, &a), Settlement::Confirmed);
    assert_eq!(fx.product().sold, 1);
    assert_eq!(fx.status(&a.event.ticket).await.unwrap().status, OrderStatus::AwaitingPayment);
    assert!(fx.inbox_row(&wire.event.id).unwrap().processed_at.is_none());

    assert_eq!(drain(&fx.inbox_worker()).await, 1);
    assert_confirmed(&fx, &a, &wire.event.id).await;
    assert_eq!(fx.product().sold, 1, "counted once");
    assert_consistent(&fx).await;
    fx.cleanup().await;
}

/// A guard failure decided before a crash is refunded on the rerun, not decided again
#[tokio::test(flavor = "multi_thread")]
async fn a_crash_after_a_guard_failure_converges() {
    let (_guard, fx) = setup(3).await;
    let a = fx.awaiting().await;
    diesel::update(products::table.find(fx.sale))
        .set(products::sold.eq(products::total_inventory))
        .execute(&mut fx.env.pool.get().unwrap())
        .unwrap();
    let wire = fx.pay(&a).await;
    fx.store(&wire);

    assert_eq!(settle_by_hand(&fx, &a), Settlement::SoldOut);
    assert_eq!(fx.order(&a.event.ticket).unwrap().status, OrderStatus::Refunded);
    assert_eq!(settle_by_hand(&fx, &a), Settlement::AlreadyRefunded);

    assert_eq!(drain(&fx.inbox_worker()).await, 1);
    assert_refunded(&fx, &a, &wire.event.id).await;
    assert_eq!(fx.product().sold, 3);
    fx.cleanup().await;
}

/// A refund that fails leaves the event in the inbox with the error recorded,
/// is skipped for a moment, and succeeds on a later try; the order is
/// already REFUNDED by then (the decision does not wait for the provider)
#[tokio::test(flavor = "multi_thread")]
async fn a_failing_event_is_recorded_backed_off_and_retried() {
    let (_guard, fx) = setup(3).await;
    let a = fx.awaiting().await;
    fx.make_due(&a.event.ticket).await;
    let wire = fx.pay(&a).await;
    fx.store(&wire);

    let failing = Arc::new(FailingRefund { inner: fx.env.shared_provider(), fail: AtomicBool::new(true) });
    let worker = Arc::new(InboxWorker::new(fx.env.pool.clone(), fx.env.read_cache.clone(), failing.clone()));

    assert!(worker.process_next().await.is_err());
    let row = fx.inbox_row(&wire.event.id).unwrap();
    assert!(row.processed_at.is_none());
    assert_eq!(row.attempts, 1);
    assert!(row.last_error.unwrap().contains("provider is down"));
    assert_eq!(fx.order(&a.event.ticket).unwrap().status, OrderStatus::Refunded, "the decision stands");
    assert_eq!(fx.fake().session(&a.session.id).await.unwrap().unwrap().state, "PAID");
    assert_eq!(fx.status(&a.event.ticket).await.unwrap().status, OrderStatus::AwaitingPayment, "not told before the refund");

    // Backing off: this processor passes the event over for now
    failing.fail.store(false, Ordering::SeqCst);
    assert!(!worker.process_next().await.unwrap());
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    assert!(worker.process_next().await.unwrap());
    assert_refunded(&fx, &a, &wire.event.id).await;
    fx.cleanup().await;
}

/// Several processors at once: each event handled once, the counts agree
#[tokio::test(flavor = "multi_thread")]
async fn several_workers_share_the_inbox() {
    let (_guard, fx) = setup(12).await;
    let mut wires = Vec::new();
    let mut orders = Vec::new();
    for _ in 0..12 {
        let a = fx.awaiting().await;
        let wire = fx.pay(&a).await;
        fx.store(&wire);
        wires.push(wire);
        orders.push(a);
    }

    let workers: Vec<_> = (0..4).map(|_| fx.inbox_worker()).collect();
    let handled: Vec<usize> = futures_join(workers.iter().cloned().map(|w| async move { drain(&w).await })).await;
    assert_eq!(handled.iter().sum::<usize>(), 12);

    for (a, wire) in orders.iter().zip(&wires) {
        assert_confirmed(&fx, a, &wire.event.id).await;
    }
    assert_eq!(fx.product().sold, 12);
    assert_eq!(fx.count_orders(OrderStatus::Confirmed), 12);
    assert_consistent(&fx).await;
    fx.cleanup().await;
}

/// The webhook request and the inbox loop (or two replicas) may handle one
/// event at the same time: one decides, the others see the result
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_handlers_of_one_event_decide_once() {
    let (_guard, fx) = setup(3).await;
    let a = fx.awaiting().await;
    let wire = fx.pay(&a).await;
    fx.store(&wire);

    let tasks: Vec<_> = (0..8)
        .map(|_| {
            let (worker, event) = (fx.inbox_worker(), wire.event.clone());
            tokio::spawn(async move { worker.handle_event(&event).await.unwrap() })
        })
        .collect();
    for task in tasks {
        assert_eq!(task.await.unwrap(), Handled::Confirmed);
    }
    assert_eq!(fx.order(&a.event.ticket).unwrap().status, OrderStatus::Confirmed);
    assert_eq!(fx.product().sold, 1);
    assert_eq!(fx.status(&a.event.ticket).await.unwrap().status, OrderStatus::Confirmed);
    fx.cleanup().await;
}

/// `handle_event` on an event for an order that is already CONFIRMED (a
/// duplicate success event under another id) repeats only the idempotent status write
#[tokio::test(flavor = "multi_thread")]
async fn a_second_success_event_for_a_confirmed_order_changes_nothing() {
    let (_guard, fx) = setup(3).await;
    let a = fx.awaiting().await;
    let wire = fx.pay(&a).await;
    fx.store(&wire);
    let worker = fx.inbox_worker();
    assert_eq!(drain(&worker).await, 1);

    let again = fx.wire(&a, CHECKOUT_COMPLETED);
    assert_eq!(worker.handle_event(&again.event).await.unwrap(), Handled::Confirmed);
    assert_eq!(fx.product().sold, 1);
    assert_eq!(fx.count_orders(OrderStatus::Confirmed), 1);
    fx.cleanup().await;
}

/// Runs futures at the same time and collects their outputs
async fn futures_join<F: std::future::Future<Output = usize> + Send + 'static>(
    futures: impl Iterator<Item = F>,
) -> Vec<usize> {
    let tasks: Vec<_> = futures.map(tokio::spawn).collect();
    let mut out = Vec::new();
    for task in tasks {
        out.push(task.await.unwrap());
    }
    out
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
