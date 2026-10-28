//! The expiration worker's passes (`flash_sale::expiry`) against real
//! Postgres and Redis. Tests call the passes directly instead of running the
//! binary. The worker expires every due order in the database, so one lock
//! serialises these tests (and they hold the binary's `due_guard` too).

mod common;

use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration as StdDuration;

use actix_web::http::header::HeaderMap;
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use diesel::prelude::*;
use redis::AsyncCommands;

use common::sale::{clear_inbox, drain, Awaiting, SaleFixture};
use flash_sale::{
    actions,
    cache::{self, CheckoutOutcome},
    expiry::{Expirer, Settings},
    models::{OrderState, OrderStatus},
    payment::{PaymentError, PaymentProvider, PaymentResult, ProviderEvent, Session, SharedProvider},
    schema::orders,
};

static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct Guards(#[allow(dead_code)] tokio::sync::MutexGuard<'static, ()>, #[allow(dead_code)] tokio::sync::MutexGuard<'static, ()>);

async fn setup(stock: i32) -> (Guards, SaleFixture) {
    let guards = Guards(LOCK.lock().await, common::sale::due_guard().await);
    let fx = SaleFixture::new(stock).await;
    clear_inbox(&fx.env);
    (guards, fx)
}

fn settings(batch: usize) -> Settings {
    Settings { batch, stale_after: Duration::minutes(10), expired_recheck: Duration::minutes(30) }
}

fn expirer_with(fx: &SaleFixture, provider: SharedProvider, batch: usize) -> Expirer {
    Expirer::new(fx.env.pool.clone(), fx.env.redis.clone(), fx.env.read_cache.clone(), provider, settings(batch))
}

async fn session_state(fx: &SaleFixture, session_id: &str) -> String {
    fx.fake().session(session_id).await.unwrap().unwrap().state
}

/// `n` orders awaiting payment, all past their deadline
async fn due_orders(fx: &SaleFixture, n: usize) -> Vec<Awaiting> {
    let mut orders = Vec::new();
    for _ in 0..n {
        let a = fx.awaiting().await;
        fx.make_due(&a.event.ticket);
        orders.push(a);
    }
    orders
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unpaid_order_past_its_deadline_is_expired_and_its_unit_returned() {
    let (_guard, fx) = setup(3).await;
    let a = fx.awaiting().await;
    let ticket = &a.event.ticket;
    let kept = fx.awaiting().await; // not due: must be left alone
    assert_eq!(fx.stock().await, 1);

    // Not due yet: nothing happens to it
    fx.expire_due().await;
    assert_eq!(fx.order(ticket).unwrap().status, OrderState::AwaitingPayment);
    assert_eq!(fx.stock().await, 1);

    fx.make_due(ticket);
    assert!(fx.expire_due().await >= 1);

    let order = fx.order(ticket).unwrap();
    assert_eq!(order.status, OrderState::Expired);
    assert!(order.updated_at > a.order.updated_at);
    assert_eq!(fx.stock().await, 2, "the unit is back");
    assert_eq!(fx.user_live(a.event.user).await, None, "the buyer may try again");
    assert_eq!(fx.status(ticket).await.unwrap().status, OrderStatus::Expired);
    assert_eq!(session_state(&fx, &a.session.id).await, "EXPIRED");
    // The one that is not due stays as it was
    assert_eq!(fx.order(&kept.event.ticket).unwrap().status, OrderState::AwaitingPayment);
    assert_eq!(fx.user_live(kept.event.user).await.as_deref(), Some(kept.event.ticket.as_str()));

    // Once only: another pass returns nothing more
    fx.expire_due().await;
    assert_eq!(fx.stock().await, 2);

    // The user checks out again, with a new key (the old key still names the old ticket)
    let again = cache::new_ticket(fx.sale);
    let outcome = cache::checkout(&mut fx.env.redis.clone(), fx.sale, a.event.user, "second-try", &again).await.unwrap();
    assert!(matches!(outcome, CheckoutOutcome::Reserved { .. }), "{outcome:?}");
    assert_eq!(fx.stock().await, 1);
    fx.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_batch_that_comes_back_full_asks_for_another_pass_at_once() {
    let (_guard, fx) = setup(8).await;
    let worker = expirer_with(&fx, fx.env.shared_provider(), 3);
    let orders = due_orders(&fx, 7).await;
    assert_eq!(fx.stock().await, 1);

    // The database may hold other due orders (earlier runs), so only the shape is checked
    let first = worker.expire_pass().await.unwrap();
    assert_eq!((first.expired, first.more), (3, true));
    let mut passes = 1;
    loop {
        let pass = worker.expire_pass().await.unwrap();
        passes += 1;
        if !pass.more {
            break;
        }
    }
    assert!(passes >= 3);
    for a in &orders {
        assert_eq!(fx.order(&a.event.ticket).unwrap().status, OrderState::Expired);
    }
    assert_eq!(fx.stock().await, 8);
    fx.cleanup().await;
}

/// Only an unpaid order is expired, and only an expired one returns its unit:
/// a confirmed buyer keeps the marker and the stock stays taken
#[tokio::test(flavor = "multi_thread")]
async fn confirmed_refunded_and_failed_orders_never_return_stock() {
    let (_guard, fx) = setup(4).await;
    let worker = fx.inbox_worker();

    // Paid in time and confirmed
    let confirmed = fx.awaiting().await;
    let wire = fx.pay(&confirmed).await;
    fx.store(&wire);
    assert_eq!(drain(&worker).await, 1);
    assert_eq!(fx.order(&confirmed.event.ticket).unwrap().status, OrderState::Confirmed);

    // Refused by the guard (PostgreSQL has no unit) and refunded
    let refused = fx.awaiting().await;
    diesel::update(flash_sale::schema::products::table.find(fx.sale))
        .set(flash_sale::schema::products::sold.eq(flash_sale::schema::products::total_inventory))
        .execute(&mut fx.env.pool.get().unwrap())
        .unwrap();
    let wire = fx.pay(&refused).await;
    fx.store(&wire);
    assert_eq!(drain(&worker).await, 1);
    assert_eq!(fx.order(&refused.event.ticket).unwrap().status, OrderState::Refunded);

    // A FAILED order (a publish that failed; its unit went back at once)
    let event = fx.winner().await;
    actions::orders::insert_failed(&mut fx.env.pool.get().unwrap(), event.user, fx.sale, &event.ticket).unwrap();

    let stock_before = fx.stock().await;
    let live_before = (fx.user_live(confirmed.event.user).await, fx.user_live(refused.event.user).await);
    // Every deadline passes (the confirmed and refunded orders have one; the failed has none)
    fx.make_due(&confirmed.event.ticket);
    fx.make_due(&refused.event.ticket);
    fx.expire_due().await;

    assert_eq!(fx.order(&confirmed.event.ticket).unwrap().status, OrderState::Confirmed);
    assert_eq!(fx.order(&refused.event.ticket).unwrap().status, OrderState::Refunded);
    assert_eq!(fx.order(&event.ticket).unwrap().status, OrderState::Failed);
    assert_eq!(fx.stock().await, stock_before, "no unit came back");
    assert_eq!((fx.user_live(confirmed.event.user).await, fx.user_live(refused.event.user).await), live_before);
    assert_eq!(fx.status(&confirmed.event.ticket).await.unwrap().status, OrderStatus::Confirmed);
    fx.cleanup().await;
}

/// A Redis error on the return is logged and not retried: the order stays
/// expired and everything else about it is finished (accepted failure case 2)
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_return_is_not_retried_and_the_rest_is_finished() {
    let (_guard, fx) = setup(3).await;
    let a = fx.awaiting().await;
    fx.make_due(&a.event.ticket);
    // INCR on a hash fails with WRONGTYPE
    let mut con = fx.env.redis.clone();
    let _: () = con.del(cache::stock_key(fx.sale)).await.unwrap();
    let _: () = con.hset(cache::stock_key(fx.sale), "f", "v").await.unwrap();

    let worker = fx.expirer();
    let mut lost = 0;
    loop {
        let pass = worker.expire_pass().await.unwrap();
        lost += pass.lost;
        if !pass.more {
            break;
        }
    }
    assert!(lost >= 1);
    assert_eq!(fx.order(&a.event.ticket).unwrap().status, OrderState::Expired);
    assert_eq!(fx.status(&a.event.ticket).await.unwrap().status, OrderStatus::Expired);
    assert_eq!(session_state(&fx, &a.session.id).await, "EXPIRED");
    // The script failed as a whole: the user is still marked, and nothing retries
    assert_eq!(fx.user_live(a.event.user).await.as_deref(), Some(a.event.ticket.as_str()));
    let _: () = con.del(cache::stock_key(fx.sale)).await.unwrap();
    fx.expire_due().await;
    assert_eq!(fx.stock().await, 0, "no second attempt");
    fx.cleanup().await;
}

/// The unit is back and the order expired, but the session step failed: the
/// next pass finishes it, without returning the unit again
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_follow_up_is_retried_by_the_next_pass() {
    let (_guard, fx) = setup(3).await;
    let a = fx.awaiting().await;
    fx.make_due(&a.event.ticket);
    let failing = Arc::new(FailingExpire { inner: fx.env.shared_provider(), fail: AtomicBool::new(true) });
    let worker = expirer_with(&fx, failing.clone(), 200);

    let mut failed = false;
    loop {
        match worker.expire_pass().await {
            Err(e) => {
                assert!(e.to_string().contains("provider is down"), "{e}");
                failed = true;
                break;
            }
            Ok(pass) if !pass.more => break,
            Ok(_) => {}
        }
    }
    assert!(failed);
    assert_eq!(fx.order(&a.event.ticket).unwrap().status, OrderState::Expired);
    assert_eq!(fx.stock().await, 3, "the unit is back already");
    assert_eq!(session_state(&fx, &a.session.id).await, "OPEN");

    failing.fail.store(false, Ordering::SeqCst);
    worker.expire_pass().await.unwrap();
    assert_eq!(session_state(&fx, &a.session.id).await, "EXPIRED");
    assert_eq!(fx.stock().await, 3, "returned once");
    fx.cleanup().await;
}

/// Several workers at once over the same due orders: each order is expired by
/// one of them and each unit comes back exactly once
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn several_workers_return_each_expired_unit_exactly_once() {
    const N: usize = 60;
    let (_guard, fx) = setup(N as i32).await;
    let orders = due_orders(&fx, N).await;
    assert_eq!(fx.stock().await, 0);

    let workers: Vec<Arc<Expirer>> = (0..4).map(|_| Arc::new(expirer_with(&fx, fx.env.shared_provider(), 7))).collect();
    let tasks: Vec<_> = workers
        .into_iter()
        .map(|worker| {
            tokio::spawn(async move {
                let mut expired = 0;
                loop {
                    let pass = worker.expire_pass().await.unwrap();
                    expired += pass.expired;
                    if !pass.more {
                        return expired;
                    }
                }
            })
        })
        .collect();
    let mut total = 0;
    for task in tasks {
        total += task.await.unwrap();
    }
    assert!(total >= N, "{total} expired in all");

    for a in &orders {
        assert_eq!(fx.order(&a.event.ticket).unwrap().status, OrderState::Expired);
        assert_eq!(fx.user_live(a.event.user).await, None);
        assert_eq!(session_state(&fx, &a.session.id).await, "EXPIRED");
    }
    assert_eq!(fx.stock().await, N as i64, "N units back, none twice");
    assert_eq!(fx.count_orders(OrderState::Expired), N as i64);
    fx.cleanup().await;
}

// ---- Payment and expiry racing ----

/// Many orders just past due against payments handled at the same moment
/// (concurrent webhook handlers) and two expiration workers. Both sides decide
/// in PostgreSQL, on its one clock: exactly one wins each order. A payment
/// that lands after the deadline but before the worker is left for the inbox
/// loop; every losing payment ends refunded, with no stock moved by the inbox.
/// Once settled the counts add up: `stock + products.sold == total`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn payment_and_expiry_racing_leave_exactly_one_winner_per_order() {
    const N: usize = 40;
    let (_guard, fx) = setup(N as i32).await;
    let mut all: Vec<(Awaiting, Option<ProviderEvent>)> = Vec::new();
    for i in 0..N {
        let a = fx.awaiting().await;
        let event = if i % 2 == 0 { Some(fx.pay(&a).await.event) } else { None };
        all.push((a, event));
    }

    // The deadlines fall about a millisecond apart; the payments and the
    // expiration workers start a few milliseconds before the first, so the
    // decisions meet around the deadlines
    let workers: Vec<Arc<Expirer>> = (0..2).map(|_| Arc::new(fx.expirer())).collect();
    let t0 = Utc::now().timestamp_millis();
    for (i, (a, _)) in all.iter().enumerate() {
        fx.set_deadline(&a.event.ticket, t0 + 150 + (i / 2) as i64);
    }
    let stop = Arc::new(AtomicBool::new(false));
    let until_first_deadline = t0 + 150 - Utc::now().timestamp_millis();
    let start_after = StdDuration::from_millis((until_first_deadline - 25).max(0) as u64);
    let mut tasks = Vec::new();
    for worker in &workers {
        let (worker, stop) = (worker.clone(), stop.clone());
        tasks.push(tokio::spawn(async move {
            tokio::time::sleep(start_after).await;
            while !stop.load(Ordering::SeqCst) {
                worker.expire_pass().await.unwrap();
                tokio::time::sleep(StdDuration::from_millis(2)).await;
            }
        }));
    }
    // The payments arrive as webhook requests do: three handlers, each event handled on receipt
    let events: Arc<Vec<ProviderEvent>> = Arc::new(all.iter().filter_map(|(_, e)| e.clone()).collect());
    let next = Arc::new(AtomicUsize::new(0));
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

    // Until every order has been decided (confirmed, or expired by a worker)
    let give_up = tokio::time::Instant::now() + StdDuration::from_secs(20);
    while fx.count_orders(OrderState::AwaitingPayment) > 0 {
        assert!(tokio::time::Instant::now() < give_up, "the race did not settle");
        tokio::time::sleep(StdDuration::from_millis(20)).await;
    }
    stop.store(true, Ordering::SeqCst);
    for task in tasks {
        task.await.unwrap();
    }

    // The payments the workers beat were postponed: the inbox loop refunds them
    // now that their orders are expired (a fresh worker has no backoff to wait out)
    assert_eq!(fx.count_orders(OrderState::AwaitingPayment), 0);
    drain(&fx.inbox_worker()).await;
    let unprocessed: i64 = {
        use flash_sale::schema::payment_inbox;
        payment_inbox::table
            .filter(payment_inbox::processed_at.is_null())
            .count()
            .get_result(&mut fx.env.pool.get().unwrap())
            .unwrap()
    };
    assert_eq!(unprocessed, 0, "every payment was decided");

    let (mut confirmed, mut refunded, mut expired) = (0i64, 0i64, 0i64);
    for (a, event) in &all {
        let pays = event.is_some();
        let ticket = &a.event.ticket;
        let order = fx.order(ticket).unwrap();
        let status = fx.status(ticket).await.unwrap().status;
        let session = session_state(&fx, &a.session.id).await;
        let live = fx.user_live(a.event.user).await;
        match (pays, order.status) {
            // The payment beat PostgreSQL's deadline: the buyer keeps the unit
            (true, OrderState::Confirmed) => {
                assert_eq!((status, session.as_str(), live.as_deref()), (OrderStatus::Confirmed, "PAID", Some(ticket.as_str())));
                confirmed += 1;
            }
            // The deadline won; the worker returned the unit and the payment was refunded
            (true, OrderState::Refunded) => {
                assert_eq!((status, session.as_str(), live), (OrderStatus::Refunded, "REFUNDED", None));
                refunded += 1;
            }
            // Nobody paid
            (false, OrderState::Expired) => {
                assert_eq!((status, session.as_str(), live), (OrderStatus::Expired, "EXPIRED", None));
                expired += 1;
            }
            (pays, other) => panic!("{ticket}: paid {pays}, order {other:?}"),
        }
    }
    println!("confirmed {confirmed}, refunded {refunded}, expired {expired}");
    assert_eq!(confirmed + refunded, (N / 2) as i64);
    assert_eq!(expired, (N / 2) as i64);

    // Every unit is either back in stock or sold, none twice
    assert_eq!(fx.product().sold as i64, confirmed);
    assert_eq!(fx.stock().await + fx.product().sold as i64, N as i64);
    fx.cleanup().await;
}

// ---- The sweep ----

/// The worker died after the update, before the status or the session: the
/// order is EXPIRED, the sweep finishes it and never touches stock
#[tokio::test(flavor = "multi_thread")]
async fn the_sweep_finishes_an_expired_order_whose_status_and_session_were_missed() {
    let (_guard, fx) = setup(5).await;
    let worker = fx.expirer();
    let missed = fx.awaiting().await;
    let done = fx.awaiting().await;
    for a in [&missed, &done] {
        fx.make_due(&a.event.ticket);
    }
    // The worker died right after the update of `missed`
    diesel::update(orders::table.filter(orders::queue_ticket_id.eq(&missed.event.ticket)))
        .set((orders::status.eq(OrderState::Expired), orders::updated_at.eq(diesel::dsl::now)))
        .execute(&mut fx.env.pool.get().unwrap())
        .unwrap();
    // `done` is expired normally
    fx.expire_due().await;
    assert_eq!(fx.status(&done.event.ticket).await.unwrap().status, OrderStatus::Expired);
    assert_eq!(fx.status(&missed.event.ticket).await.unwrap().status, OrderStatus::AwaitingPayment);
    assert_eq!(session_state(&fx, &missed.session.id).await, "OPEN");
    let stock = fx.stock().await;

    // Too recent for the sweep
    worker.sweep(Utc::now()).await.unwrap();
    assert_eq!(fx.status(&missed.event.ticket).await.unwrap().status, OrderStatus::AwaitingPayment);

    // Later: the missed one is finished, the finished one is left alone
    let later = Utc::now() + Duration::minutes(15);
    // (Other tests leave EXPIRED orders behind in the shared database, so only these are checked)
    let swept = worker.sweep(later).await.unwrap();
    assert!(swept.repaired >= 1, "{swept:?}");
    assert_eq!(fx.status(&missed.event.ticket).await.unwrap().status, OrderStatus::Expired);
    assert_eq!(session_state(&fx, &missed.session.id).await, "EXPIRED");
    assert_eq!(fx.stock().await, stock, "the sweep never returns stock");
    worker.sweep(later).await.unwrap();
    assert_eq!(fx.status(&missed.event.ticket).await.unwrap().status, OrderStatus::Expired, "idempotent");
    assert_eq!(fx.stock().await, stock);
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
