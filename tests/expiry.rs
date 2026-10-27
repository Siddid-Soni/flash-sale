//! The expiration worker's passes (`flash_sale::expiry`) against real
//! Postgres and Redis, and real Kafka where lag matters (a topic of its own
//! per test). Tests call the passes directly instead of running the binary.
//!
//! One lock serialises the tests: they share the database (the payment inbox
//! and the sweep look at every order) and each opens its own connection pool.

mod common;

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::Duration as StdDuration;

use actix_web::http::header::HeaderMap;
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use diesel::prelude::*;
use rdkafka::{
    consumer::{BaseConsumer, CommitMode, Consumer},
    ClientConfig, Offset, TopicPartitionList,
};
use redis::AsyncCommands;

use common::sale::{clear_inbox, drain, Awaiting, SaleFixture, PRICE_CENTS};
use flash_sale::{
    actions,
    cache::{self, CheckoutOutcome, HoldState, MaxScore, ReleaseOutcome, ReleasedAs},
    expiry::{Expirer, ExpiryError, KafkaLag, LagSource, PartitionLag, Settings},
    models::{NewOrder, OrderState, OrderStatus},
    payment::{PaymentError, PaymentProvider, PaymentResult, ProviderEvent, Session, SharedProvider},
    processor::GROUP_ID,
    queue::{ensure_topic, CheckoutEvent, CheckoutQueue, KafkaQueue},
    schema::orders,
};

static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn setup(stock: i32) -> (tokio::sync::MutexGuard<'static, ()>, SaleFixture) {
    let guard = LOCK.lock().await;
    let fx = SaleFixture::new(stock).await;
    clear_inbox(&fx.env);
    (guard, fx)
}

/// A `LagSource` the test controls
struct StubLag(Mutex<Vec<PartitionLag>>);

impl StubLag {
    fn new(partitions: &[(i32, bool)]) -> Arc<Self> {
        let stub = Arc::new(Self(Mutex::new(Vec::new())));
        stub.set(partitions);
        stub
    }

    fn set(&self, partitions: &[(i32, bool)]) {
        *self.0.lock().unwrap() =
            partitions.iter().map(|&(partition, lagging)| PartitionLag { partition, lagging }).collect();
    }
}

#[async_trait]
impl LagSource for StubLag {
    async fn partitions(&self) -> Result<Vec<PartitionLag>, ExpiryError> {
        Ok(self.0.lock().unwrap().clone())
    }
}

fn settings(batch: usize, delivery_timeout_ms: i64) -> Settings {
    Settings {
        batch,
        delivery_timeout_ms,
        stale_after: Duration::minutes(10),
        sale_lookback: Duration::days(3),
        expired_recheck: Duration::minutes(30),
    }
}

fn expirer_with(fx: &SaleFixture, provider: SharedProvider, lag: Arc<dyn LagSource>, settings: Settings) -> Expirer {
    Expirer::new(fx.env.pool.clone(), fx.env.redis.clone(), fx.env.read_cache.clone(), provider, lag, settings)
}

fn expirer(fx: &SaleFixture, lag: Arc<dyn LagSource>, batch: usize, delivery_timeout_ms: i64) -> Expirer {
    expirer_with(fx, fx.env.shared_provider(), lag, settings(batch, delivery_timeout_ms))
}

/// An expirer whose Kafka is quiet: one partition, no lag
fn quiet(fx: &SaleFixture) -> Expirer {
    expirer(fx, StubLag::new(&[(0, false)]), 100, 30_000)
}

/// The hold's deadline has passed, for PostgreSQL and for Redis
async fn make_due(fx: &SaleFixture, ticket: &str) {
    fx.make_due(ticket).await;
}

async fn session_state(fx: &SaleFixture, session_id: &str) -> String {
    fx.fake().session(session_id).await.unwrap().unwrap().state
}

async fn user_live(fx: &SaleFixture, user: i32) -> Option<String> {
    fx.env.redis.clone().get(cache::user_live_key(fx.sale, user)).await.unwrap()
}

async fn raise_progress(fx: &SaleFixture, partition: i32, checkout_ms: i64) {
    cache::raise_to(&mut fx.env.redis.clone(), &cache::processed_upto_key(fx.sale, partition), checkout_ms)
        .await
        .unwrap();
}

async fn zcard(fx: &SaleFixture, key: String) -> i64 {
    fx.env.redis.clone().zcard(key).await.unwrap()
}

/// What a processor leaves when it dies after inserting the order but before
/// linking the hold: an `AWAITING_PAYMENT` order with its session, the hold
/// still `PENDING`
async fn unlinked_order(fx: &SaleFixture, event: &CheckoutEvent) -> Session {
    let expires_at = Utc::now() + Duration::minutes(5);
    let session = fx.fake().create_session(&event.ticket, PRICE_CENTS, expires_at, &event.ticket).await.unwrap();
    let order = NewOrder {
        user_id: event.user,
        product_id: fx.sale,
        queue_ticket_id: &event.ticket,
        idempotency_key: &format!("key-{}", event.user),
        status: OrderState::AwaitingPayment,
        amount_cents: PRICE_CENTS,
        payment_session_id: Some(&session.id),
        checkout_url: Some(&session.url),
        expires_at: Some(expires_at),
    };
    actions::orders::insert_awaiting_payment(&mut fx.env.pool.get().unwrap(), &order).unwrap().unwrap();
    session
}

/// Backdates an order, as if it had been left alone for `minutes`
fn age_order(fx: &SaleFixture, ticket: &str, minutes: i64) {
    let then = Utc::now() - Duration::minutes(minutes);
    diesel::update(orders::table.filter(orders::queue_ticket_id.eq(ticket)))
        .set((orders::created_at.eq(then), orders::expires_at.eq(Some(then))))
        .execute(&mut fx.env.pool.get().unwrap())
        .unwrap();
}

/// What the Order Service does between the script and the publish when it
/// crashes there: a hold, no message, no order
async fn leaked_hold(fx: &SaleFixture) -> CheckoutEvent {
    fx.winner().await
}

async fn release_by_hand(fx: &SaleFixture, ticket: &str, expected: HoldState, released_as: ReleasedAs) {
    let outcome = cache::release_hold(
        &mut fx.env.redis.clone(),
        fx.sale,
        ticket,
        expected,
        MaxScore::Any,
        released_as,
        false,
    )
    .await
    .unwrap();
    assert_eq!(outcome, ReleaseOutcome::Released);
}

// ---- The unpaid-hold loop ----

#[tokio::test(flavor = "multi_thread")]
async fn an_unpaid_hold_past_its_deadline_is_released_and_its_order_expired() {
    let (_guard, fx) = setup(5).await;
    let late = fx.awaiting().await;
    let early = fx.awaiting().await;
    make_due(&fx, &late.event.ticket).await;
    assert_eq!(fx.stock().await, 3);

    let worker = quiet(&fx);
    let pass = worker.unpaid_pass(&[fx.sale]).await.unwrap();
    assert_eq!((pass.released, pass.more), (1, false));

    let ticket = &late.event.ticket;
    assert_eq!(fx.order(ticket).unwrap().status, OrderState::Expired);
    assert_eq!(fx.hold_state(ticket).await, "EXPIRED");
    let status = fx.status(ticket).await.unwrap();
    assert_eq!((status.status, status.user), (OrderStatus::Expired, late.event.user));
    assert_eq!(session_state(&fx, &late.session.id).await, "EXPIRED");
    assert_eq!(fx.stock().await, 4, "the unit is back");
    assert_eq!(user_live(&fx, late.event.user).await, None, "the user may check out again");

    // The hold whose deadline is ahead is untouched
    let ticket = &early.event.ticket;
    assert_eq!(fx.order(ticket).unwrap().status, OrderState::AwaitingPayment);
    assert_eq!(fx.hold_state(ticket).await, "AWAITING_PAYMENT");
    assert_eq!(session_state(&fx, &early.session.id).await, "OPEN");

    // Nothing more to do, and the user can buy again
    assert_eq!(worker.unpaid_pass(&[fx.sale]).await.unwrap().released, 0);
    let again = cache::checkout(&mut fx.env.redis.clone(), fx.sale, late.event.user, "second-try", &cache::new_ticket(fx.sale))
        .await
        .unwrap();
    assert!(matches!(again, CheckoutOutcome::Held { .. }), "{again:?}");
    fx.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_batch_that_comes_back_full_asks_for_another_pass_at_once() {
    let (_guard, fx) = setup(5).await;
    let mut held = Vec::new();
    for _ in 0..5 {
        let a = fx.awaiting().await;
        make_due(&fx, &a.event.ticket).await;
        held.push(a);
    }
    let worker = expirer(&fx, StubLag::new(&[(0, false)]), 2, 30_000);
    let mut released = 0;
    let mut passes = 0;
    loop {
        let pass = worker.unpaid_pass(&[fx.sale]).await.unwrap();
        released += pass.released;
        passes += 1;
        if !pass.more {
            break;
        }
    }
    assert_eq!((released, passes), (5, 3), "2 + 2 + 1");
    assert_eq!(fx.stock().await, 5);
    fx.cleanup().await;
}

/// A payment that slipped in after the release is refunded by the inbox; the
/// worker's order update (`WHERE status = 'AWAITING_PAYMENT'`) must not undo it
#[tokio::test(flavor = "multi_thread")]
async fn a_refunded_order_is_never_overwritten_with_expired() {
    let (_guard, fx) = setup(5).await;
    let a = fx.awaiting().await;
    let ticket = a.event.ticket.clone();

    // The order is expired and the hold released ...
    make_due(&fx, &ticket).await;
    assert_eq!(actions::orders::expire_due(&mut fx.env.pool.get().unwrap(), std::slice::from_ref(&ticket)).unwrap().len(), 1);
    release_by_hand(&fx, &ticket, HoldState::AwaitingPayment, ReleasedAs::Expired).await;
    // ... the user pays before the session is expired, and the inbox refunds it
    let wire = fx.pay(&a).await;
    fx.store(&wire);
    assert_eq!(drain(&fx.inbox_worker()).await, 1);
    assert_eq!(fx.order(&ticket).unwrap().status, OrderState::Refunded);
    assert_eq!(fx.status(&ticket).await.unwrap().status, OrderStatus::Refunded);

    // Only now the worker gets to the order
    let changed = quiet(&fx).finish_expired(fx.sale, &ticket).await.unwrap();
    assert!(!changed);
    assert_eq!(fx.order(&ticket).unwrap().status, OrderState::Refunded);
    assert_eq!(fx.status(&ticket).await.unwrap().status, OrderStatus::Refunded);
    assert_eq!(session_state(&fx, &a.session.id).await, "REFUNDED", "a paid session is not expired");
    assert_eq!(fx.stock().await, 5, "released once, refunded without returning stock again");
    fx.cleanup().await;
}

/// The worker died between PostgreSQL's expiry and the release: the hold is
/// still in `holds:expiry`, its order is EXPIRED, and the next pass finishes it
#[tokio::test(flavor = "multi_thread")]
async fn a_crash_between_the_order_update_and_the_release_converges() {
    let (_guard, fx) = setup(3).await;
    let a = fx.awaiting().await;
    make_due(&fx, &a.event.ticket).await;
    let expired = actions::orders::expire_due(&mut fx.env.pool.get().unwrap(), std::slice::from_ref(&a.event.ticket)).unwrap();
    assert_eq!(expired.len(), 1);
    assert_eq!(fx.stock().await, 2);

    let pass = quiet(&fx).unpaid_pass(&[fx.sale]).await.unwrap();
    assert_eq!((pass.released, pass.confirmed, pass.dropped), (1, 0, 0));
    assert_eq!(fx.hold_state(&a.event.ticket).await, "EXPIRED");
    assert_eq!(fx.stock().await, 3);
    assert_eq!(fx.status(&a.event.ticket).await.unwrap().status, OrderStatus::Expired);
    assert_eq!(session_state(&fx, &a.session.id).await, "EXPIRED");
    fx.cleanup().await;
}

// ---- Paid holds ----

/// A paid order's hold waits in `holds:expiry` until its deadline; then the
/// worker counts the sale once and returns no stock
#[tokio::test(flavor = "multi_thread")]
async fn a_paid_hold_is_counted_as_sold_when_it_is_due_and_returns_no_stock() {
    let (_guard, fx) = setup(5).await;
    let paid = fx.awaiting().await;
    let wire = fx.pay(&paid).await;
    fx.store(&wire);
    assert_eq!(drain(&fx.inbox_worker()).await, 1);
    let unpaid = fx.awaiting().await;
    assert_eq!(fx.stock().await, 3);
    let worker = quiet(&fx);

    // Not due yet: the payment path wrote no inventory key, and the worker has nothing to do
    let pass = worker.unpaid_pass(&[fx.sale]).await.unwrap();
    assert_eq!((pass.released, pass.confirmed, pass.dropped, pass.more), (0, 0, 0, false));
    assert_eq!(fx.sold_counter().await, 0);
    assert_eq!(fx.hold_state(&paid.event.ticket).await, "AWAITING_PAYMENT");

    make_due(&fx, &paid.event.ticket).await;
    let pass = worker.unpaid_pass(&[fx.sale]).await.unwrap();
    assert_eq!((pass.released, pass.confirmed, pass.dropped), (0, 1, 0));
    assert_eq!(fx.hold_state(&paid.event.ticket).await, "CONFIRMED");
    assert_eq!(fx.sold_counter().await, 1);
    assert_eq!(fx.stock().await, 3, "the unit is sold, not returned");
    assert_eq!(fx.order(&paid.event.ticket).unwrap().status, OrderState::Confirmed);
    assert_eq!(fx.status(&paid.event.ticket).await.unwrap().status, OrderStatus::Confirmed);
    assert_eq!(session_state(&fx, &paid.session.id).await, "PAID", "a paid session is left alone");
    assert_eq!(user_live(&fx, paid.event.user).await.as_deref(), Some(paid.event.ticket.as_str()), "the user bought one");
    // The unpaid one is untouched
    assert_eq!(fx.order(&unpaid.event.ticket).unwrap().status, OrderState::AwaitingPayment);

    // Counted once, however often the worker looks
    for _ in 0..3 {
        assert_eq!(worker.unpaid_pass(&[fx.sale]).await.unwrap().confirmed, 0);
    }
    assert_eq!(fx.sold_counter().await, 1);
    assert_eq!(fx.product().sold, 1);
    fx.cleanup().await;
}

/// A payment that came after the deadline was refunded by the processor; the
/// hold is still in `holds:expiry`, so the worker returns the unit
#[tokio::test(flavor = "multi_thread")]
async fn a_hold_whose_late_payment_was_refunded_is_released() {
    let (_guard, fx) = setup(3).await;
    let a = fx.awaiting().await;
    make_due(&fx, &a.event.ticket).await;
    let wire = fx.pay(&a).await;
    fx.store(&wire);
    assert_eq!(drain(&fx.inbox_worker()).await, 1);
    assert_eq!(fx.order(&a.event.ticket).unwrap().status, OrderState::Refunded);
    assert_eq!(fx.stock().await, 2);

    let pass = quiet(&fx).unpaid_pass(&[fx.sale]).await.unwrap();
    assert_eq!((pass.released, pass.confirmed, pass.dropped), (1, 0, 0));
    assert_eq!(fx.stock().await, 3, "the unit is back");
    assert_eq!(fx.hold_state(&a.event.ticket).await, "EXPIRED");
    assert_eq!(fx.order(&a.event.ticket).unwrap().status, OrderState::Refunded);
    assert_eq!(fx.status(&a.event.ticket).await.unwrap().status, OrderStatus::Refunded, "REFUNDED is not overwritten");
    assert_eq!(session_state(&fx, &a.session.id).await, "REFUNDED", "a paid session is not expired");
    assert_eq!(fx.sold_counter().await, 0);
    assert_eq!(user_live(&fx, a.event.user).await, None);
    fx.cleanup().await;
}

/// PostgreSQL refused the payment because it had no unit left although Redis
/// held one: the hold is dropped without stock and without a count
#[tokio::test(flavor = "multi_thread")]
async fn a_hold_whose_payment_failed_the_guard_is_dropped_without_stock_or_count() {
    let (_guard, fx) = setup(3).await;
    let a = fx.awaiting().await;
    diesel::update(flash_sale::schema::products::table.find(fx.sale))
        .set(flash_sale::schema::products::sold.eq(flash_sale::schema::products::total_inventory))
        .execute(&mut fx.env.pool.get().unwrap())
        .unwrap();
    let wire = fx.pay(&a).await;
    fx.store(&wire);
    assert_eq!(drain(&fx.inbox_worker()).await, 1);
    assert_eq!(fx.order(&a.event.ticket).unwrap().status, OrderState::Refunded);
    make_due(&fx, &a.event.ticket).await;

    let worker = quiet(&fx);
    let pass = worker.unpaid_pass(&[fx.sale]).await.unwrap();
    assert_eq!((pass.released, pass.confirmed, pass.dropped), (0, 0, 1));
    assert_eq!(fx.stock().await, 2, "no unit came back");
    assert_eq!(fx.sold_counter().await, 0);
    assert_eq!(fx.hold_state(&a.event.ticket).await, "REFUNDED");
    assert_eq!(fx.expiry_len().await, 0);
    assert_eq!(fx.status(&a.event.ticket).await.unwrap().status, OrderStatus::Refunded);
    // Nothing more to do
    let pass = worker.unpaid_pass(&[fx.sale]).await.unwrap();
    assert_eq!((pass.released, pass.confirmed, pass.dropped), (0, 0, 0));
    fx.cleanup().await;
}

/// Redis' clock says due, PostgreSQL's says not yet: nothing is released or
/// counted, the pass does not ask for another at once, and a later pass
/// expires the order when PostgreSQL agrees
#[tokio::test(flavor = "multi_thread")]
async fn a_hold_due_by_redis_but_not_by_postgres_is_left_for_a_later_pass() {
    let (_guard, fx) = setup(3).await;
    let a = fx.awaiting().await;
    fx.set_redis_deadline(&a.event.ticket, Utc::now().timestamp_millis() - 1_000).await;
    let worker = expirer(&fx, StubLag::new(&[(0, false)]), 1, 30_000);

    for _ in 0..2 {
        let pass = worker.unpaid_pass(&[fx.sale]).await.unwrap();
        assert_eq!((pass.released, pass.confirmed, pass.dropped, pass.more), (0, 0, 0, false));
    }
    assert_eq!(fx.order(&a.event.ticket).unwrap().status, OrderState::AwaitingPayment);
    assert_eq!(fx.hold_state(&a.event.ticket).await, "AWAITING_PAYMENT");
    assert_eq!(fx.expiry_len().await, 1);
    assert_eq!(fx.stock().await, 2);
    assert_eq!(session_state(&fx, &a.session.id).await, "OPEN");

    // PostgreSQL's clock catches up
    fx.set_pg_deadline(&a.event.ticket, Utc::now().timestamp_millis() - 1_000);
    assert_eq!(worker.unpaid_pass(&[fx.sale]).await.unwrap().released, 1);
    assert_eq!(fx.order(&a.event.ticket).unwrap().status, OrderState::Expired);
    assert_eq!(fx.hold_state(&a.event.ticket).await, "EXPIRED");
    assert_eq!(fx.stock().await, 3);
    fx.cleanup().await;
}

/// A payment in that window is decided by PostgreSQL's deadline too: it is
/// ahead, so the payment wins, and the worker then counts the sale at once
/// (a confirmed order never turns back, whatever the clocks say)
#[tokio::test(flavor = "multi_thread")]
async fn a_payment_in_the_clock_skew_window_wins_and_is_counted() {
    let (_guard, fx) = setup(3).await;
    let a = fx.awaiting().await;
    fx.set_redis_deadline(&a.event.ticket, Utc::now().timestamp_millis() - 1_000).await;
    let wire = fx.pay(&a).await;
    fx.store(&wire);
    assert_eq!(drain(&fx.inbox_worker()).await, 1);
    assert_eq!(fx.order(&a.event.ticket).unwrap().status, OrderState::Confirmed);

    let worker = quiet(&fx);
    assert_eq!(worker.unpaid_pass(&[fx.sale]).await.unwrap().confirmed, 1);
    assert_eq!(fx.sold_counter().await, 1);
    assert_eq!(fx.hold_state(&a.event.ticket).await, "CONFIRMED");
    fx.cleanup().await;
}

/// Holds are linked only after their order is updated, so a due hold without
/// an order should not exist; if it does the worker leaves it alone
#[tokio::test(flavor = "multi_thread")]
async fn a_due_hold_without_an_order_is_left_alone() {
    let (_guard, fx) = setup(3).await;
    let event = fx.winner().await;
    let linked = cache::link_ready(
        &mut fx.env.redis.clone(),
        fx.sale,
        &event.ticket,
        "sess-x",
        Utc::now().timestamp_millis() - 1_000,
    )
    .await
    .unwrap();
    assert_eq!(linked, cache::LinkOutcome::Ready);

    let pass = quiet(&fx).unpaid_pass(&[fx.sale]).await.unwrap();
    assert_eq!((pass.released, pass.confirmed, pass.dropped, pass.more), (0, 0, 0, false));
    assert_eq!(fx.hold_state(&event.ticket).await, "AWAITING_PAYMENT");
    assert_eq!(fx.stock().await, 2);
    fx.cleanup().await;
}

/// The release succeeded but a later step failed: the hold is remembered and
/// finished before the next pass
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_follow_up_is_retried_by_the_next_pass() {
    let (_guard, fx) = setup(3).await;
    let a = fx.awaiting().await;
    make_due(&fx, &a.event.ticket).await;
    let provider = Arc::new(FailingExpire { inner: fx.env.shared_provider(), fail: AtomicBool::new(true) });
    let worker = expirer_with(&fx, provider.clone(), StubLag::new(&[(0, false)]), settings(100, 30_000));

    let error = worker.unpaid_pass(&[fx.sale]).await.unwrap_err();
    assert!(matches!(error, ExpiryError::Payment(_)), "{error:?}");
    // The unit is back and the order and status are done; only the session is not
    assert_eq!(fx.hold_state(&a.event.ticket).await, "EXPIRED");
    assert_eq!(fx.stock().await, 3);
    assert_eq!(fx.order(&a.event.ticket).unwrap().status, OrderState::Expired);
    assert_eq!(session_state(&fx, &a.session.id).await, "OPEN");

    provider.fail.store(false, Ordering::SeqCst);
    let pass = worker.unpaid_pass(&[fx.sale]).await.unwrap();
    assert_eq!(pass.released, 0, "nothing left to release; the old release is finished");
    assert_eq!(session_state(&fx, &a.session.id).await, "EXPIRED");
    assert_eq!(fx.stock().await, 3, "the unit was returned once");
    fx.cleanup().await;
}

// ---- The leaked-hold loop ----

/// Roadmap step 24: the Order Service crashed between the script and the
/// publish. The hold stays while progress is behind it and is released once
/// progress passes it by the delivery timeout.
#[tokio::test(flavor = "multi_thread")]
async fn a_hold_without_a_message_is_released_once_progress_passes_it() {
    const TIMEOUT: i64 = 300;
    let (_guard, fx) = setup(3).await;
    let leaked = leaked_hold(&fx).await;
    assert_eq!(fx.stock().await, 2);
    let lag = StubLag::new(&[(0, true)]);
    let worker = expirer(&fx, lag.clone(), 100, TIMEOUT);

    // No progress reported for the lagging partition: release nothing
    assert_eq!(worker.leaked_pass(&[fx.sale]).await.unwrap().released, 0);
    // Progress behind the hold
    raise_progress(&fx, 0, leaked.checkout_ms - 1).await;
    assert_eq!(worker.leaked_pass(&[fx.sale]).await.unwrap().released, 0);
    // Progress exactly one delivery timeout past the hold: its message could still be on its way
    raise_progress(&fx, 0, leaked.checkout_ms + TIMEOUT).await;
    assert_eq!(worker.leaked_pass(&[fx.sale]).await.unwrap().released, 0);
    assert_eq!(fx.hold_state(&leaked.ticket).await, "PENDING");
    assert_eq!(fx.stock().await, 2);

    // One millisecond more: it cannot arrive any more
    raise_progress(&fx, 0, leaked.checkout_ms + TIMEOUT + 1).await;
    let pass = worker.leaked_pass(&[fx.sale]).await.unwrap();
    assert_eq!(pass.released, 1);

    assert_eq!(fx.hold_state(&leaked.ticket).await, "FAILED");
    assert_eq!(fx.stock().await, 3, "the unit is back");
    assert_eq!(user_live(&fx, leaked.user).await, None);
    let status = fx.status(&leaked.ticket).await.unwrap();
    assert_eq!((status.status, status.user), (OrderStatus::Failed, leaked.user));
    assert!(fx.order(&leaked.ticket).is_none(), "there never was an order");
    // The user can check out again
    let again = cache::checkout(&mut fx.env.redis.clone(), fx.sale, leaked.user, "retry", &cache::new_ticket(fx.sale))
        .await
        .unwrap();
    assert!(matches!(again, CheckoutOutcome::Held { .. }), "{again:?}");
    fx.cleanup().await;
}

/// With no lag at all the partitions count as caught up to now, so time alone
/// releases a hold whose message never came (real Kafka, an empty topic)
#[tokio::test(flavor = "multi_thread")]
async fn with_no_lag_time_alone_releases_a_leaked_hold() {
    const TIMEOUT: i64 = 300;
    let (_guard, mut fx) = setup(3).await;
    fx.env.config.checkout_topic = format!("test.expiry.{}", uuid::Uuid::new_v4().simple());
    fx.env.config.checkout_partitions = 3;
    ensure_topic(&fx.env.config).await.unwrap();
    let lag = Arc::new(KafkaLag::new(&fx.env.config).unwrap());
    let worker = expirer(&fx, lag, 100, TIMEOUT);

    let leaked = leaked_hold(&fx).await;
    assert_eq!(worker.leaked_pass(&[fx.sale]).await.unwrap().released, 0, "too recent");
    tokio::time::sleep(StdDuration::from_millis(TIMEOUT as u64 + 150)).await;
    assert_eq!(worker.leaked_pass(&[fx.sale]).await.unwrap().released, 1);
    assert_eq!(fx.hold_state(&leaked.ticket).await, "FAILED");
    assert_eq!(fx.stock().await, 3);
    fx.cleanup().await;
}

/// A processor that crashed after it made an order but before the hold was
/// linked: the leaked hold is released, the order failed and its session expired
#[tokio::test(flavor = "multi_thread")]
async fn a_leaked_hold_fails_the_order_the_processor_left_behind() {
    let (_guard, fx) = setup(3).await;
    let event = fx.winner().await;
    let session = unlinked_order(&fx, &event).await;

    let lag = StubLag::new(&[(0, false)]);
    let worker = expirer(&fx, lag, 100, 50);
    tokio::time::sleep(StdDuration::from_millis(120)).await;
    assert_eq!(worker.leaked_pass(&[fx.sale]).await.unwrap().released, 1);

    assert_eq!(fx.order(&event.ticket).unwrap().status, OrderState::Failed);
    assert_eq!(session_state(&fx, &session.id).await, "EXPIRED");
    assert_eq!(fx.status(&event.ticket).await.unwrap().status, OrderStatus::Failed);
    assert_eq!(fx.hold_state(&event.ticket).await, "FAILED");
    assert_eq!(fx.stock().await, 3);
    fx.cleanup().await;
}

/// Publishes the events of `holds` to a fresh topic nobody consumes
async fn publish_unconsumed(fx: &mut SaleFixture, holds: &[CheckoutEvent]) -> Vec<(CheckoutEvent, i32, i64)> {
    fx.env.config.checkout_topic = format!("test.expiry.{}", uuid::Uuid::new_v4().simple());
    fx.env.config.checkout_partitions = 3;
    ensure_topic(&fx.env.config).await.unwrap();
    let queue = KafkaQueue::new(&fx.env.config).unwrap();
    let mut published = Vec::new();
    for event in holds {
        let at = queue.publish(event).await.unwrap();
        published.push((event.clone(), at.partition, at.offset));
    }
    published
}

/// Waits far longer than the delivery timeout, then checks that no hold was released
async fn assert_nothing_released(fx: &SaleFixture, worker: &Expirer, holds: &[CheckoutEvent], what: &str) {
    tokio::time::sleep(StdDuration::from_millis(400)).await;
    assert_eq!(worker.leaked_pass(&[fx.sale]).await.unwrap().released, 0, "{what}");
    for hold in holds {
        assert_eq!(fx.hold_state(&hold.ticket).await, "PENDING", "{what}");
    }
    assert_eq!(fx.stock().await, 0, "{what}");
}

/// Roadmap step 24: a slow processor (messages in Kafka, not consumed) releases
/// nothing, however much time passes; once progress passes the holds it does
#[tokio::test(flavor = "multi_thread")]
async fn a_slow_processor_releases_nothing() {
    const TIMEOUT: i64 = 100;
    let (_guard, mut fx) = setup(6).await;
    let mut holds = Vec::new();
    for _ in 0..6 {
        holds.push(fx.winner().await);
    }
    let published = publish_unconsumed(&mut fx, &holds).await;
    let lagging: std::collections::BTreeSet<i32> = published.iter().map(|(_, partition, _)| *partition).collect();
    let oldest = holds.iter().map(|h| h.checkout_ms).min().unwrap();
    let newest = holds.iter().map(|h| h.checkout_ms).max().unwrap();

    let lag = Arc::new(KafkaLag::new(&fx.env.config).unwrap());
    let reported = lag.partitions().await.unwrap();
    assert_eq!(reported.len(), 3);
    for p in &reported {
        assert_eq!(p.lagging, lagging.contains(&p.partition), "partition {}", p.partition);
    }
    let worker = expirer(&fx, lag, 100, TIMEOUT);

    // The processors have reported nothing for the sale yet
    assert_nothing_released(&fx, &worker, &holds, "no progress").await;
    // Progress, far behind the holds
    for &p in &lagging {
        raise_progress(&fx, p, oldest - 10_000).await;
    }
    assert_nothing_released(&fx, &worker, &holds, "progress behind").await;
    // One lagging partition has not reported (a processor that has not started)
    let silent = *lagging.iter().next().unwrap();
    let _: () = fx.env.redis.clone().del(cache::processed_upto_key(fx.sale, silent)).await.unwrap();
    for &p in lagging.iter().filter(|&&p| p != silent) {
        raise_progress(&fx, p, newest + 60_000).await;
    }
    assert_nothing_released(&fx, &worker, &holds, "one partition silent").await;

    // The slowest processor catches up (its messages are still in Kafka's lag
    // until committed, but its progress is past every hold)
    raise_progress(&fx, silent, newest + TIMEOUT + 1).await;
    let pass = worker.leaked_pass(&[fx.sale]).await.unwrap();
    assert_eq!(pass.released, 6);
    assert_eq!(fx.stock().await, 6);
    fx.cleanup().await;
}

/// The lag source against a real broker: committed offsets and watermarks
#[tokio::test(flavor = "multi_thread")]
async fn the_lag_of_a_partition_is_its_committed_offset_against_its_watermark() {
    let (_guard, mut fx) = setup(7).await;
    let mut holds = Vec::new();
    for _ in 0..6 {
        holds.push(fx.winner().await);
    }
    let published = publish_unconsumed(&mut fx, &holds).await;
    let topic = fx.env.config.checkout_topic.clone();
    let lag = KafkaLag::new(&fx.env.config).unwrap();

    let state = |parts: Vec<PartitionLag>| parts.into_iter().map(|p| (p.partition, p.lagging)).collect::<Vec<_>>();
    let mut with_messages: std::collections::BTreeMap<i32, i64> = Default::default();
    for (_, partition, offset) in &published {
        let end = with_messages.entry(*partition).or_insert(0);
        *end = (*end).max(offset + 1);
    }
    let expected: Vec<(i32, bool)> = (0..3).map(|p| (p, with_messages.contains_key(&p))).collect();
    assert_eq!(state(lag.partitions().await.unwrap()), expected);

    // The order processors' group commits everything
    let committer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &fx.env.config.kafka_brokers)
        .set("group.id", GROUP_ID)
        .create()
        .unwrap();
    let mut list = TopicPartitionList::new();
    for (&partition, &end) in &with_messages {
        list.add_partition_offset(&topic, partition, Offset::Offset(end)).unwrap();
    }
    committer.commit(&list, CommitMode::Sync).unwrap();
    let caught_up: Vec<(i32, bool)> = (0..3).map(|p| (p, false)).collect();
    assert_eq!(state(lag.partitions().await.unwrap()), caught_up);

    // One more message behind the committed offset: lag again
    let queue = KafkaQueue::new(&fx.env.config).unwrap();
    let extra = fx.winner().await;
    let at = queue.publish(&extra).await.unwrap();
    let expected: Vec<(i32, bool)> = (0..3).map(|p| (p, p == at.partition)).collect();
    assert_eq!(state(lag.partitions().await.unwrap()), expected);
    fx.cleanup().await;
}

// ---- Payment and expiry racing ----

/// Roadmap step 24: many holds just past due against payments handled at the
/// same moment (concurrent webhook handlers) and the expiration worker. Both
/// sides decide in PostgreSQL, on its one clock: exactly one wins each order,
/// every losing payment is refunded, and once the worker has settled every hold
/// the counts add up, `sold:{sale}` counted once per sale.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn payment_and_expiry_racing_leave_exactly_one_winner_per_order() {
    const N: usize = 40;
    let (_guard, fx) = setup(N as i32).await;
    let mut holds: Vec<(Awaiting, Option<flash_sale::payment::ProviderEvent>)> = Vec::new();
    for i in 0..N {
        let a = fx.awaiting().await;
        let event = if i % 2 == 0 { Some(fx.pay(&a).await.event) } else { None };
        holds.push((a, event));
    }

    // The deadlines fall about a millisecond apart; the payments and the
    // expiration workers start a few milliseconds before the first, so the
    // decisions meet around the deadlines
    let workers: Vec<Arc<Expirer>> = (0..2).map(|_| Arc::new(quiet(&fx))).collect();
    let t0 = workers[0].redis_now_ms().await.unwrap();
    for (i, (a, _)) in holds.iter().enumerate() {
        fx.set_deadline(&a.event.ticket, t0 + 150 + (i / 2) as i64).await;
    }
    let stop = Arc::new(AtomicBool::new(false));
    let until_first_deadline = t0 + 150 - workers[0].redis_now_ms().await.unwrap();
    let start_after = StdDuration::from_millis((until_first_deadline - 25).max(0) as u64);
    let mut tasks = Vec::new();
    for worker in &workers {
        let (worker, stop, sale) = (worker.clone(), stop.clone(), fx.sale);
        tasks.push(tokio::spawn(async move {
            tokio::time::sleep(start_after).await;
            while !stop.load(Ordering::SeqCst) {
                worker.unpaid_pass(&[sale]).await.unwrap();
                tokio::time::sleep(StdDuration::from_millis(2)).await;
            }
        }));
    }
    // The payments arrive as webhook requests do: three handlers, each event handled on receipt
    let events: Vec<_> = holds.iter().filter_map(|(_, e)| e.clone()).collect();
    let next = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let events = Arc::new(events);
    for _ in 0..3 {
        let (inbox, events, next) = (fx.inbox_worker(), events.clone(), next.clone());
        tasks.push(tokio::spawn(async move {
            tokio::time::sleep(start_after).await;
            loop {
                let i = next.fetch_add(1, Ordering::SeqCst);
                let Some(event) = events.get(i) else { return };
                inbox.receive(event).await.unwrap();
                // A pause after each event keeps the decisions spread over the deadlines
                tokio::time::sleep(StdDuration::from_millis(3)).await;
            }
        }));
    }

    // Until every hold has left the expiry queue (settled by a worker)
    let give_up = tokio::time::Instant::now() + StdDuration::from_secs(20);
    while fx.expiry_len().await > 0 {
        assert!(tokio::time::Instant::now() < give_up, "the race did not settle");
        tokio::time::sleep(StdDuration::from_millis(20)).await;
    }
    stop.store(true, Ordering::SeqCst);
    for task in tasks {
        task.await.unwrap();
    }
    for worker in &workers {
        worker.unpaid_pass(&[fx.sale]).await.unwrap();
    }

    let unprocessed: i64 = {
        use flash_sale::schema::payment_inbox;
        payment_inbox::table
            .filter(payment_inbox::processed_at.is_null())
            .count()
            .get_result(&mut fx.env.pool.get().unwrap())
            .unwrap()
    };
    assert_eq!(unprocessed, 0, "every payment was handled on receipt");

    let (mut confirmed, mut refunded, mut expired) = (0i64, 0i64, 0i64);
    for (a, event) in &holds {
        let pays = event.is_some();
        let ticket = &a.event.ticket;
        let order = fx.order(ticket).unwrap();
        let status = fx.status(ticket).await.unwrap().status;
        let hold = fx.hold_state(ticket).await;
        let session = session_state(&fx, &a.session.id).await;
        match (pays, order.status) {
            // The payment beat PostgreSQL's deadline: the worker counted the sale
            (true, OrderState::Confirmed) => {
                assert_eq!((status, hold.as_str(), session.as_str()), (OrderStatus::Confirmed, "CONFIRMED", "PAID"));
                confirmed += 1;
            }
            // The deadline won; the payment was refunded and the worker returned the unit
            (true, OrderState::Refunded) => {
                assert_eq!((status, hold.as_str(), session.as_str()), (OrderStatus::Refunded, "EXPIRED", "REFUNDED"));
                refunded += 1;
            }
            // Nobody paid
            (false, OrderState::Expired) => {
                assert_eq!((status, hold.as_str(), session.as_str()), (OrderStatus::Expired, "EXPIRED", "EXPIRED"));
                expired += 1;
            }
            (pays, other) => panic!("{ticket}: paid {pays}, order {other:?}, hold {hold}"),
        }
    }
    println!("confirmed {confirmed}, refunded {refunded}, expired {expired}");
    assert_eq!(confirmed + refunded, (N / 2) as i64);
    assert_eq!(expired, (N / 2) as i64);

    // stock + live holds + sold == total
    let live = fx.expiry_len().await + zcard(&fx, cache::holds_pending_key(fx.sale)).await;
    assert_eq!(live, 0);
    assert_eq!(fx.sold_counter().await, confirmed, "counted once per sale, by the workers");
    assert_eq!(fx.product().sold as i64, confirmed);
    assert_eq!(fx.stock().await + live + fx.sold_counter().await, N as i64);
    fx.cleanup().await;
}

// ---- Several workers ----

/// Roadmap step 24: concurrent workers over the same due holds. Each unit is
/// returned once, each paid hold is counted once, each hold is finished by one
/// worker, the stock is exact.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn several_workers_release_each_hold_once() {
    const UNPAID: usize = 28;
    const PAID: usize = 14;
    const LEAKED: usize = 20;
    const TIMEOUT: i64 = 100;
    let (_guard, fx) = setup((UNPAID + PAID + LEAKED) as i32).await;
    let mut leaked = Vec::new();
    for _ in 0..LEAKED {
        leaked.push(leaked_hold(&fx).await);
    }
    let mut unpaid = Vec::new();
    for _ in 0..UNPAID {
        let a = fx.awaiting().await;
        make_due(&fx, &a.event.ticket).await;
        unpaid.push(a);
    }
    let mut paid = Vec::new();
    let worker_inbox = fx.inbox_worker();
    for _ in 0..PAID {
        let a = fx.awaiting().await;
        let wire = fx.pay(&a).await;
        fx.store(&wire);
        paid.push(a);
    }
    assert_eq!(drain(&worker_inbox).await, PAID);
    for a in &paid {
        make_due(&fx, &a.event.ticket).await;
    }
    assert_eq!(fx.stock().await, 0);
    tokio::time::sleep(StdDuration::from_millis(TIMEOUT as u64 + 100)).await;

    let tasks: Vec<_> = (0..4)
        .map(|_| {
            // A small batch, so there are several passes to race over
            let worker = expirer(&fx, StubLag::new(&[(0, false), (1, false)]), 7, TIMEOUT);
            let sale = fx.sale;
            tokio::spawn(async move {
                let (mut released, mut confirmed) = (0, 0);
                loop {
                    let pass = worker.round(&[sale]).await.unwrap();
                    released += pass.released;
                    confirmed += pass.confirmed;
                    if !pass.more {
                        return (released, confirmed);
                    }
                }
            })
        })
        .collect();
    let (mut released, mut confirmed) = (0, 0);
    for task in tasks {
        let (r, c) = task.await.unwrap();
        released += r;
        confirmed += c;
    }
    assert_eq!(released, UNPAID + LEAKED, "every hold was released by exactly one worker");
    assert_eq!(confirmed, PAID, "every paid hold was counted by exactly one worker");
    assert_eq!(fx.stock().await, (UNPAID + LEAKED) as i64, "every unit returned once; the paid ones stay taken");
    assert_eq!(fx.sold_counter().await, PAID as i64, "sold:{{sale}} counted once per paid hold");
    assert_eq!(fx.product().sold as usize, PAID);
    assert_eq!(fx.expiry_len().await, 0);
    for a in &paid {
        assert_eq!(fx.hold_state(&a.event.ticket).await, "CONFIRMED");
        assert_eq!(fx.order(&a.event.ticket).unwrap().status, OrderState::Confirmed);
    }

    for a in &unpaid {
        assert_eq!(fx.order(&a.event.ticket).unwrap().status, OrderState::Expired);
        assert_eq!(fx.hold_state(&a.event.ticket).await, "EXPIRED");
        assert_eq!(fx.status(&a.event.ticket).await.unwrap().status, OrderStatus::Expired);
        assert_eq!(session_state(&fx, &a.session.id).await, "EXPIRED");
    }
    for hold in &leaked {
        assert_eq!(fx.hold_state(&hold.ticket).await, "FAILED");
        assert_eq!(fx.status(&hold.ticket).await.unwrap().status, OrderStatus::Failed);
    }
    assert_eq!(fx.count_orders(OrderState::Expired), UNPAID as i64);
    fx.cleanup().await;
}

// ---- The stale-order sweep ----

#[tokio::test(flavor = "multi_thread")]
async fn the_sweep_finishes_orders_whose_hold_ended_and_leaves_live_ones() {
    let (_guard, fx) = setup(10).await;
    let worker = quiet(&fx);

    // AWAITING_PAYMENT orders, long past their deadline
    let crashed = fx.awaiting().await; // the worker died after the release
    release_by_hand(&fx, &crashed.event.ticket, HoldState::AwaitingPayment, ReleasedAs::Expired).await;
    let gone = fx.awaiting().await; // the hold has vanished
    release_by_hand(&fx, &gone.event.ticket, HoldState::AwaitingPayment, ReleasedAs::Expired).await;
    let _: () = fx.env.redis.clone().del(cache::hold_key(fx.sale, &gone.event.ticket)).await.unwrap();
    let live = fx.awaiting().await; // a live hold: never touched
    let recent = fx.awaiting().await; // hold ended, but only just
    release_by_hand(&fx, &recent.event.ticket, HoldState::AwaitingPayment, ReleasedAs::Expired).await;
    for a in [&crashed, &gone, &live] {
        age_order(&fx, &a.event.ticket, 20);
    }
    age_order(&fx, &recent.event.ticket, 5);

    // Orders whose processor died before linking the hold, long past their deadline
    let failed = fx.winner().await;
    unlinked_order(&fx, &failed).await;
    release_by_hand(&fx, &failed.ticket, HoldState::Pending, ReleasedAs::Failed).await;
    let vanished = fx.winner().await; // hold gone: no way to tell whether it was leaked
    unlinked_order(&fx, &vanished).await;
    release_by_hand(&fx, &vanished.ticket, HoldState::Pending, ReleasedAs::Failed).await;
    let _: () = fx.env.redis.clone().del(cache::hold_key(fx.sale, &vanished.ticket)).await.unwrap();
    let waiting = fx.winner().await; // PENDING hold still live: the leaked loop's business
    unlinked_order(&fx, &waiting).await;
    for event in [&failed, &vanished, &waiting] {
        age_order(&fx, &event.ticket, 20);
    }

    let swept = worker.sweep(Utc::now()).await.unwrap();
    assert!(swept.expired >= 3 && swept.failed >= 1, "{swept:?}");

    for a in [&crashed, &gone] {
        assert_eq!(fx.order(&a.event.ticket).unwrap().status, OrderState::Expired);
        assert_eq!(fx.status(&a.event.ticket).await.unwrap().status, OrderStatus::Expired);
        assert_eq!(session_state(&fx, &a.session.id).await, "EXPIRED");
    }
    // The hold was released as leaked: FAILED. One that vanished cannot be told
    // from an unpaid one: EXPIRED
    assert_eq!(fx.order(&failed.ticket).unwrap().status, OrderState::Failed);
    assert_eq!(fx.status(&failed.ticket).await.unwrap().status, OrderStatus::Failed);
    assert_eq!(fx.order(&vanished.ticket).unwrap().status, OrderState::Expired);
    assert_eq!(fx.status(&vanished.ticket).await.unwrap().status, OrderStatus::Expired);
    // Live holds and fresh orders are left alone
    assert_eq!(fx.order(&live.event.ticket).unwrap().status, OrderState::AwaitingPayment);
    assert_eq!(fx.hold_state(&live.event.ticket).await, "AWAITING_PAYMENT");
    assert_eq!(session_state(&fx, &live.session.id).await, "OPEN");
    assert_eq!(fx.order(&recent.event.ticket).unwrap().status, OrderState::AwaitingPayment);
    assert_eq!(fx.order(&waiting.ticket).unwrap().status, OrderState::AwaitingPayment);
    assert_eq!(fx.hold_state(&waiting.ticket).await, "PENDING");

    // Idempotent
    let again = worker.sweep(Utc::now()).await.unwrap();
    assert_eq!((again.expired, again.failed), (0, 0));
    fx.cleanup().await;
}

/// The unpaid loop expires the order first, so a worker that dies after that
/// (before the status or the session) leaves an EXPIRED order: the sweep finishes it
#[tokio::test(flavor = "multi_thread")]
async fn the_sweep_finishes_an_expired_order_whose_status_and_session_were_missed() {
    let (_guard, fx) = setup(5).await;
    let worker = quiet(&fx);
    let missed = fx.awaiting().await;
    let done = fx.awaiting().await;
    for a in [&missed, &done] {
        make_due(&fx, &a.event.ticket).await;
    }
    // The worker died right after the release of `missed`
    let ticket = missed.event.ticket.clone();
    assert_eq!(actions::orders::expire_due(&mut fx.env.pool.get().unwrap(), std::slice::from_ref(&ticket)).unwrap().len(), 1);
    release_by_hand(&fx, &ticket, HoldState::AwaitingPayment, ReleasedAs::Expired).await;
    assert_eq!(worker.unpaid_pass(&[fx.sale]).await.unwrap().released, 1, "the other one is done normally");
    assert_eq!(fx.status(&ticket).await.unwrap().status, OrderStatus::AwaitingPayment);
    assert_eq!(session_state(&fx, &missed.session.id).await, "OPEN");

    // Too recent for the sweep
    worker.sweep(Utc::now()).await.unwrap();
    assert_eq!(fx.status(&ticket).await.unwrap().status, OrderStatus::AwaitingPayment);

    // Later: the missed one is finished, the finished one is left alone
    let later = Utc::now() + Duration::minutes(15);
    // (Other tests leave EXPIRED orders behind in the shared database, so only this one is checked)
    let swept = worker.sweep(later).await.unwrap();
    assert!(swept.repaired >= 1, "{swept:?}");
    assert_eq!(fx.status(&ticket).await.unwrap().status, OrderStatus::Expired);
    assert_eq!(session_state(&fx, &missed.session.id).await, "EXPIRED");
    assert_eq!(fx.status(&done.event.ticket).await.unwrap().status, OrderStatus::Expired);
    worker.sweep(later).await.unwrap();
    assert_eq!(fx.status(&ticket).await.unwrap().status, OrderStatus::Expired, "idempotent");
    fx.cleanup().await;
}

// ---- Which sales ----

#[tokio::test(flavor = "multi_thread")]
async fn sales_are_read_from_postgres_within_the_lookback() {
    let (_guard, fx) = setup(1).await;
    let worker = quiet(&fx);
    assert!(worker.load_sales().await.unwrap().contains(&fx.sale));

    let old = actions::products::create_product(
        &mut fx.env.pool.get().unwrap(),
        &flash_sale::models::NewProduct {
            name: "Old",
            price_cents: 100,
            total_inventory: 1,
            sale_start_time: Utc::now() - Duration::days(30),
        },
    )
    .unwrap();
    let upcoming = actions::products::create_product(
        &mut fx.env.pool.get().unwrap(),
        &flash_sale::models::NewProduct {
            name: "Upcoming",
            price_cents: 100,
            total_inventory: 1,
            sale_start_time: Utc::now() + Duration::days(1),
        },
    )
    .unwrap();
    let sales = worker.load_sales().await.unwrap();
    assert!(!sales.contains(&old.id));
    assert!(sales.contains(&upcoming.id));
    fx.cleanup().await;
}

/// Delegates to the real provider, but its `expire_session` fails while `fail` is set
struct FailingExpire {
    inner: SharedProvider,
    fail: AtomicBool,
}

#[async_trait]
impl PaymentProvider for FailingExpire {
    fn name(&self) -> &'static str {
        "failing"
    }

    async fn create_session(&self, t: &str, amount: i32, at: DateTime<Utc>, key: &str) -> PaymentResult<Session> {
        self.inner.create_session(t, amount, at, key).await
    }

    async fn expire_session(&self, id: &str) -> PaymentResult<()> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(PaymentError("provider is down".into()));
        }
        self.inner.expire_session(id).await
    }

    async fn refund(&self, id: &str) -> PaymentResult<()> {
        self.inner.refund(id).await
    }

    fn verify_webhook(&self, headers: &HeaderMap, body: &[u8]) -> PaymentResult<ProviderEvent> {
        self.inner.verify_webhook(headers, body)
    }
}
