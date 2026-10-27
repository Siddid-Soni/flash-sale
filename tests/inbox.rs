//! The payment inbox loop (`flash_sale::inbox`) against real Postgres and
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
    cache::{self, ClaimOutcome, HoldState, MaxScore, ReleaseOutcome, ReleasedAs},
    inbox::{Handled, InboxWorker},
    models::OrderStatus,
    payment::{PaymentError, PaymentProvider, PaymentResult, ProviderEvent, Session, SharedProvider},
    schema::products,
};

static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn setup(stock: i32) -> (tokio::sync::MutexGuard<'static, ()>, SaleFixture) {
    let guard = LOCK.lock().await;
    let fx = SaleFixture::new(stock).await;
    clear_inbox(&fx.env);
    (guard, fx)
}

/// Everything that must agree once an event is processed
async fn assert_consistent(fx: &SaleFixture) {
    let confirmed = fx.count_orders(OrderStatus::Confirmed);
    assert_eq!(fx.product().sold as i64, confirmed, "products.sold == CONFIRMED orders");
    assert_eq!(fx.sold_counter().await, confirmed, "sold:{{sale}} == CONFIRMED orders");
}

async fn assert_confirmed(fx: &SaleFixture, a: &Awaiting, event_id: &str) {
    let order = fx.order(&a.event.ticket).unwrap();
    assert_eq!(order.status, OrderStatus::Confirmed);
    assert_eq!(fx.hold_state(&a.event.ticket).await, "CONFIRMED");
    let status = fx.status(&a.event.ticket).await.unwrap();
    assert_eq!((status.status, status.user), (OrderStatus::Confirmed, a.event.user));
    assert!(fx.inbox_row(event_id).unwrap().processed_at.is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_payment_in_time_confirms_the_order() {
    let (_guard, fx) = setup(3).await;
    let a = fx.awaiting().await;
    let wire = fx.pay(&a).await;
    fx.store(&wire);
    // The webhook claimed the hold on receipt
    let claim = cache::claim_hold(&mut fx.env.redis.clone(), fx.sale, &a.event.ticket, &a.session.id, "pi").await.unwrap();
    assert_eq!(claim, ClaimOutcome::Won);

    let worker = fx.inbox_worker();
    assert_eq!(drain(&worker).await, 1);

    assert_confirmed(&fx, &a, &wire.event.id).await;
    assert_eq!(fx.product().sold, 1);
    assert_eq!(fx.sold_counter().await, 1);
    assert_eq!(fx.stock().await, 2, "the unit stays taken");
    assert_eq!(fx.fake().session(&a.session.id).await.unwrap().unwrap().state, "PAID");
    let row = fx.inbox_row(&wire.event.id).unwrap();
    assert_eq!(row.last_error, None);
    assert_consistent(&fx).await;

    // Running it again changes nothing
    assert_eq!(drain(&worker).await, 0);
    assert_consistent(&fx).await;
    fx.cleanup().await;
}

/// The webhook's claim failed (Redis was down): the processor claims for the first time
#[tokio::test(flavor = "multi_thread")]
async fn an_unclaimed_payment_in_time_is_claimed_by_the_processor() {
    let (_guard, fx) = setup(3).await;
    let a = fx.awaiting().await;
    let wire = fx.pay(&a).await;
    fx.store(&wire);
    assert_eq!(fx.hold_state(&a.event.ticket).await, "AWAITING_PAYMENT");

    assert_eq!(drain(&fx.inbox_worker()).await, 1);
    assert_confirmed(&fx, &a, &wire.event.id).await;
    assert_consistent(&fx).await;
    fx.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_late_payment_after_release_is_refunded() {
    let (_guard, fx) = setup(3).await;
    let a = fx.awaiting().await;
    // The deadline passed and the expiration worker released the hold ...
    let released = cache::release_hold(
        &mut fx.env.redis.clone(),
        fx.sale,
        &a.event.ticket,
        HoldState::AwaitingPayment,
        MaxScore::Any,
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

    assert_eq!(fx.order(&a.event.ticket).unwrap().status, OrderStatus::Refunded);
    assert_eq!(fx.fake().session(&a.session.id).await.unwrap().unwrap().state, "REFUNDED");
    assert_eq!(fx.status(&a.event.ticket).await.unwrap().status, OrderStatus::Refunded);
    assert_eq!(fx.hold_state(&a.event.ticket).await, "EXPIRED", "the released hold is left as it is");
    assert_eq!(fx.product().sold, 0);
    assert_eq!(fx.sold_counter().await, 0);
    assert_eq!(fx.stock().await, 3, "the release already returned the unit; the refund adds none");
    assert!(fx.inbox_row(&wire.event.id).unwrap().processed_at.is_some());
    assert_consistent(&fx).await;
    fx.cleanup().await;
}

/// The race of HANDOFF: the deadline passes, the user pays before the worker
/// releases the hold, and the claim finds it past due
#[tokio::test(flavor = "multi_thread")]
async fn a_payment_after_the_deadline_but_before_the_release_is_refunded() {
    let (_guard, fx) = setup(3).await;
    let a = fx.awaiting().await;
    // Move the deadline into the past without releasing anything
    let _: () = redis::AsyncCommands::zadd(
        &mut fx.env.redis.clone(),
        cache::holds_expiry_key(fx.sale),
        &a.event.ticket,
        Utc::now().timestamp_millis() - 1_000,
    )
    .await
    .unwrap();
    let wire = fx.pay(&a).await;
    fx.store(&wire);

    assert_eq!(drain(&fx.inbox_worker()).await, 1);

    assert_eq!(fx.order(&a.event.ticket).unwrap().status, OrderStatus::Refunded);
    assert_eq!(fx.fake().session(&a.session.id).await.unwrap().unwrap().state, "REFUNDED");
    // The hold is still the worker's to release, and releases cleanly
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
    assert_eq!(fx.stock().await, 3);
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

    let order = fx.order(&a.event.ticket).unwrap();
    assert_eq!(order.status, OrderStatus::Refunded, "the confirmation rolled back, the order was refunded");
    assert_eq!(fx.product().sold, 3, "sold was not raised");
    assert_eq!(fx.sold_counter().await, 0, "and not counted in Redis");
    assert_eq!(fx.fake().session(&a.session.id).await.unwrap().unwrap().state, "REFUNDED");
    assert_eq!(fx.status(&a.event.ticket).await.unwrap().status, OrderStatus::Refunded);
    assert_eq!(fx.hold_state(&a.event.ticket).await, "REFUNDED", "the hold leaves PAID");
    assert!(fx.inbox_row(&wire.event.id).unwrap().processed_at.is_some());
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

/// Stops after the commit, before the Redis finalize, as a crash would, then
/// reruns the event
#[tokio::test(flavor = "multi_thread")]
async fn a_crash_after_the_commit_converges_with_the_sale_counted_once() {
    let (_guard, fx) = setup(3).await;
    let a = fx.awaiting().await;
    let wire = fx.pay(&a).await;
    fx.store(&wire);
    let worker = fx.inbox_worker();

    // Phases 1 and 2, then "the process dies"
    assert_eq!(worker.claim(&a.order, &wire.event).await.unwrap(), ClaimOutcome::Won);
    assert_eq!(worker.commit(&a.order).await.unwrap(), flash_sale::actions::orders::Confirm::Confirmed);
    assert_eq!(fx.order(&a.event.ticket).unwrap().status, OrderStatus::Confirmed);
    assert_eq!(fx.product().sold, 1);
    assert_eq!(fx.sold_counter().await, 0, "not counted in Redis yet");
    assert_eq!(fx.hold_state(&a.event.ticket).await, "PAID");
    assert!(fx.inbox_row(&wire.event.id).unwrap().processed_at.is_none());

    // The rerun
    assert_eq!(drain(&worker).await, 1);
    assert_confirmed(&fx, &a, &wire.event.id).await;
    assert_eq!(fx.sold_counter().await, 1);
    assert_consistent(&fx).await;
    fx.cleanup().await;
}

/// Crashes after the finalize but before the event is marked processed, and
/// again after only the claim
#[tokio::test(flavor = "multi_thread")]
async fn a_rerun_after_any_phase_counts_once() {
    let (_guard, fx) = setup(4).await;
    let worker = fx.inbox_worker();

    // After the finalize
    let a = fx.awaiting().await;
    let wire = fx.pay(&a).await;
    fx.store(&wire);
    worker.claim(&a.order, &wire.event).await.unwrap();
    worker.commit(&a.order).await.unwrap();
    worker.finalize(&a.order).await.unwrap();
    assert_eq!(fx.sold_counter().await, 1);
    assert!(fx.inbox_row(&wire.event.id).unwrap().processed_at.is_none());
    assert_eq!(drain(&worker).await, 1);
    assert_eq!(fx.sold_counter().await, 1, "the finalize did not count twice");
    assert_confirmed(&fx, &a, &wire.event.id).await;

    // After the claim only
    let b = fx.awaiting().await;
    let wire_b = fx.pay(&b).await;
    fx.store(&wire_b);
    worker.claim(&b.order, &wire_b.event).await.unwrap();
    assert_eq!(drain(&worker).await, 1);
    assert_confirmed(&fx, &b, &wire_b.event.id).await;

    // After the provider refund of a lost claim, before the order update
    let c = fx.awaiting().await;
    cache::release_hold(
        &mut fx.env.redis.clone(),
        fx.sale,
        &c.event.ticket,
        HoldState::AwaitingPayment,
        MaxScore::Any,
        ReleasedAs::Expired,
        false,
    )
    .await
    .unwrap();
    let wire_c = fx.pay(&c).await;
    fx.store(&wire_c);
    flash_sale::payment::PaymentProvider::refund(fx.fake().as_ref(), &c.session.id).await.unwrap();
    assert_eq!(drain(&worker).await, 1);
    assert_eq!(fx.order(&c.event.ticket).unwrap().status, OrderStatus::Refunded);

    assert_eq!(fx.sold_counter().await, 2);
    assert_eq!(fx.product().sold, 2);
    assert_consistent(&fx).await;
    fx.cleanup().await;
}

/// A refund that fails leaves the event in the inbox with the error recorded,
/// is skipped for a moment, and succeeds on a later try
#[tokio::test(flavor = "multi_thread")]
async fn a_failing_event_is_recorded_backed_off_and_retried() {
    let (_guard, fx) = setup(3).await;
    let a = fx.awaiting().await;
    cache::release_hold(
        &mut fx.env.redis.clone(),
        fx.sale,
        &a.event.ticket,
        HoldState::AwaitingPayment,
        MaxScore::Any,
        ReleasedAs::Expired,
        false,
    )
    .await
    .unwrap();
    let wire = fx.pay(&a).await;
    fx.store(&wire);

    let failing = Arc::new(FailingRefund { inner: fx.env.shared_provider(), fail: AtomicBool::new(true) });
    let worker = Arc::new(InboxWorker::new(
        fx.env.pool.clone(),
        fx.env.redis.clone(),
        fx.env.read_cache.clone(),
        failing.clone(),
    ));

    assert!(worker.process_next().await.is_err());
    let row = fx.inbox_row(&wire.event.id).unwrap();
    assert!(row.processed_at.is_none());
    assert_eq!(row.attempts, 1);
    assert!(row.last_error.unwrap().contains("provider is down"));
    assert_eq!(fx.order(&a.event.ticket).unwrap().status, OrderStatus::AwaitingPayment, "nothing half-done");

    // Backing off: this processor passes the event over for now
    failing.fail.store(false, Ordering::SeqCst);
    assert!(!worker.process_next().await.unwrap());
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    assert!(worker.process_next().await.unwrap());
    assert_eq!(fx.order(&a.event.ticket).unwrap().status, OrderStatus::Refunded);
    assert!(fx.inbox_row(&wire.event.id).unwrap().processed_at.is_some());
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
    assert_eq!(fx.sold_counter().await, 12);
    assert_eq!(fx.count_orders(OrderStatus::Confirmed), 12);
    fx.cleanup().await;
}

/// `handle` on an event for an order that is already CONFIRMED (a duplicate
/// success event under another id) repeats only the idempotent Redis writes
#[tokio::test(flavor = "multi_thread")]
async fn a_second_success_event_for_a_confirmed_order_changes_nothing() {
    let (_guard, fx) = setup(3).await;
    let a = fx.awaiting().await;
    let wire = fx.pay(&a).await;
    fx.store(&wire);
    let worker = fx.inbox_worker();
    assert_eq!(drain(&worker).await, 1);

    let again = fx.wire(&a, flash_sale::payment::webhook::CHECKOUT_COMPLETED);
    assert_eq!(worker.handle(&again.event).await.unwrap(), Handled::Confirmed);
    assert_eq!(fx.sold_counter().await, 1);
    assert_eq!(fx.product().sold, 1);
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
