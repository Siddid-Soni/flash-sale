//! `confirm.lua` against a real Redis (database 15): claim, finalize, and the
//! payment-versus-expiry race. Each test uses its own random sale id.

use std::collections::HashSet;

use chrono::{Duration, Utc};
use redis::AsyncCommands;

use flash_sale::cache::{
    self, CheckoutOutcome, ClaimOutcome, FinalizeOutcome, HoldState, LinkOutcome, MaxScore, RedisConn,
    ReleaseOutcome, ReleasedAs,
};

async fn connect() -> RedisConn {
    dotenvy::dotenv().ok();
    let url = std::env::var("TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/15".into());
    let client = redis::Client::open(url).unwrap();
    redis::aio::ConnectionManager::new(client).await.expect("Redis should be running for these tests")
}

async fn open_sale(con: &mut RedisConn, stock: i32) -> i32 {
    let sale = 1_000_000 + (uuid::Uuid::new_v4().as_u128() % 1_000_000_000) as i32;
    cache::init_sale(con, sale, stock, Utc::now() - Duration::seconds(60)).await.unwrap();
    sale
}

/// A user's hold, linked to session `sess-<user>` with its deadline `deadline_in_ms` from now
async fn awaiting(con: &mut RedisConn, sale: i32, user: i32, deadline_in_ms: i64) -> String {
    let ticket = cache::new_ticket(sale);
    let outcome = cache::checkout(con, sale, user, &format!("k{user}"), &ticket).await.unwrap();
    assert!(matches!(outcome, CheckoutOutcome::Held { .. }), "{outcome:?}");
    let deadline = Utc::now().timestamp_millis() + deadline_in_ms;
    let linked = cache::link_ready(con, sale, &ticket, &format!("sess-{user}"), deadline).await.unwrap();
    assert_eq!(linked, LinkOutcome::Ready);
    ticket
}

async fn get_i64(con: &mut RedisConn, key: String) -> i64 {
    con.get::<_, Option<i64>>(key).await.unwrap().unwrap_or(0)
}

async fn state(con: &mut RedisConn, sale: i32, ticket: &str) -> String {
    con.hget(cache::hold_key(sale, ticket), "state").await.unwrap()
}

async fn in_expiry_set(con: &mut RedisConn, sale: i32, ticket: &str) -> bool {
    con.zscore::<_, _, Option<f64>>(cache::holds_expiry_key(sale), ticket).await.unwrap().is_some()
}

async fn cleanup(con: &mut RedisConn, sale: i32) {
    let keys: Vec<String> = con.keys(format!("*{{{sale}}}*")).await.unwrap();
    if !keys.is_empty() {
        let _: () = con.del(keys).await.unwrap();
    }
}

#[tokio::test]
async fn a_payment_in_time_wins_the_hold_and_a_retry_finds_its_own_claim() {
    let mut con = connect().await;
    let sale = open_sale(&mut con, 3).await;
    let ticket = awaiting(&mut con, sale, 1, 60_000).await;

    assert_eq!(cache::claim_hold(&mut con, sale, &ticket, "sess-1", "pi-1").await.unwrap(), ClaimOutcome::Won);
    assert_eq!(state(&mut con, sale, &ticket).await, "PAID");
    let paid_by: String = con.hget(cache::hold_key(sale, &ticket), "paid_by").await.unwrap();
    assert_eq!(paid_by, "pi-1");
    assert!(!in_expiry_set(&mut con, sale, &ticket).await, "expiry can no longer take it");
    let ttl: i64 = con.ttl(cache::hold_key(sale, &ticket)).await.unwrap();
    assert_eq!(ttl, -1, "a claimed hold has no TTL");

    // The same payment again (the processor after the webhook): still won
    assert_eq!(cache::claim_hold(&mut con, sale, &ticket, "sess-1", "pi-1").await.unwrap(), ClaimOutcome::Won);
    // Another payment session does not get it
    assert_eq!(cache::claim_hold(&mut con, sale, &ticket, "sess-2", "pi-2").await.unwrap(), ClaimOutcome::Gone);
    // A claimed hold is never released
    let outcome = cache::release_hold(
        &mut con, sale, &ticket, HoldState::AwaitingPayment, MaxScore::Any, ReleasedAs::Expired, false,
    )
    .await
    .unwrap();
    assert_eq!(outcome, ReleaseOutcome::NotReleased);
    assert_eq!(get_i64(&mut con, cache::stock_key(sale)).await, 2, "no stock came back");
    cleanup(&mut con, sale).await;
}

#[tokio::test]
async fn a_claim_is_gone_when_the_hold_is_past_due_released_or_someone_elses() {
    let mut con = connect().await;
    let sale = open_sale(&mut con, 5).await;

    // Past due counts as expired even before the worker releases it
    let past_due = awaiting(&mut con, sale, 1, -1_000).await;
    assert_eq!(cache::claim_hold(&mut con, sale, &past_due, "sess-1", "pi-1").await.unwrap(), ClaimOutcome::Gone);
    assert_eq!(state(&mut con, sale, &past_due).await, "AWAITING_PAYMENT", "a lost claim changes nothing");
    assert!(in_expiry_set(&mut con, sale, &past_due).await, "the worker still releases it");

    // Released
    let released = awaiting(&mut con, sale, 2, 60_000).await;
    let outcome = cache::release_hold(
        &mut con, sale, &released, HoldState::AwaitingPayment, MaxScore::Any, ReleasedAs::Expired, false,
    )
    .await
    .unwrap();
    assert_eq!(outcome, ReleaseOutcome::Released);
    assert_eq!(cache::claim_hold(&mut con, sale, &released, "sess-2", "pi-2").await.unwrap(), ClaimOutcome::Gone);
    assert_eq!(state(&mut con, sale, &released).await, "EXPIRED");

    // Another payment session than the hold's
    let other = awaiting(&mut con, sale, 3, 60_000).await;
    assert_eq!(cache::claim_hold(&mut con, sale, &other, "sess-9", "pi-9").await.unwrap(), ClaimOutcome::Gone);
    assert_eq!(state(&mut con, sale, &other).await, "AWAITING_PAYMENT");

    // Still PENDING (no link yet), and no hold at all
    let pending = cache::new_ticket(sale);
    cache::checkout(&mut con, sale, 4, "k4", &pending).await.unwrap();
    assert_eq!(cache::claim_hold(&mut con, sale, &pending, "sess-4", "pi-4").await.unwrap(), ClaimOutcome::Gone);
    let missing = cache::new_ticket(sale);
    assert_eq!(cache::claim_hold(&mut con, sale, &missing, "sess-5", "pi-5").await.unwrap(), ClaimOutcome::Gone);
    cleanup(&mut con, sale).await;
}

#[tokio::test]
async fn finalize_counts_the_sale_once() {
    let mut con = connect().await;
    let sale = open_sale(&mut con, 3).await;
    let ticket = awaiting(&mut con, sale, 1, 60_000).await;

    // Nothing to finalize before the claim
    assert_eq!(cache::finalize_hold(&mut con, sale, &ticket, "sess-1").await.unwrap(), FinalizeOutcome::Noop);
    assert_eq!(get_i64(&mut con, cache::sold_key(sale)).await, 0);

    assert_eq!(cache::claim_hold(&mut con, sale, &ticket, "sess-1", "pi-1").await.unwrap(), ClaimOutcome::Won);
    // Someone else's session cannot finalize it
    assert_eq!(cache::finalize_hold(&mut con, sale, &ticket, "sess-2").await.unwrap(), FinalizeOutcome::Noop);

    assert_eq!(cache::finalize_hold(&mut con, sale, &ticket, "sess-1").await.unwrap(), FinalizeOutcome::Finalized);
    assert_eq!(cache::finalize_hold(&mut con, sale, &ticket, "sess-1").await.unwrap(), FinalizeOutcome::Noop);
    assert_eq!(cache::finalize_hold(&mut con, sale, &ticket, "sess-1").await.unwrap(), FinalizeOutcome::Noop);
    assert_eq!(get_i64(&mut con, cache::sold_key(sale)).await, 1, "counted once");

    // The hold stays readable for the status fallback, for a day; the user keeps their marker
    assert_eq!(state(&mut con, sale, &ticket).await, "CONFIRMED");
    let ttl: i64 = con.ttl(cache::hold_key(sale, &ticket)).await.unwrap();
    assert!(ttl > 23 * 3600 && ttl <= 24 * 3600, "{ttl}");
    let live: Option<String> = con.get(cache::user_live_key(sale, 1)).await.unwrap();
    assert_eq!(live.as_deref(), Some(ticket.as_str()));
    assert_eq!(get_i64(&mut con, cache::stock_key(sale)).await, 2);
    // A claim after confirmation is gone: the processor never claims an order that is already CONFIRMED
    assert_eq!(cache::claim_hold(&mut con, sale, &ticket, "sess-1", "pi-1").await.unwrap(), ClaimOutcome::Gone);
    cleanup(&mut con, sale).await;
}

/// Finalize running twice at the same moment still counts once
#[tokio::test]
async fn concurrent_finalizes_count_once() {
    let mut con = connect().await;
    let sale = open_sale(&mut con, 3).await;
    let ticket = awaiting(&mut con, sale, 1, 60_000).await;
    cache::claim_hold(&mut con, sale, &ticket, "sess-1", "pi-1").await.unwrap();

    let mut tasks = Vec::new();
    for _ in 0..16 {
        let (mut con, ticket) = (con.clone(), ticket.clone());
        tasks.push(tokio::spawn(async move { cache::finalize_hold(&mut con, sale, &ticket, "sess-1").await.unwrap() }));
    }
    let mut finalized = 0;
    for task in tasks {
        finalized += (task.await.unwrap() == FinalizeOutcome::Finalized) as i32;
    }
    assert_eq!(finalized, 1);
    assert_eq!(get_i64(&mut con, cache::sold_key(sale)).await, 1);
    cleanup(&mut con, sale).await;
}

#[tokio::test]
async fn a_refunded_claim_closes_the_hold_without_counting() {
    let mut con = connect().await;
    let sale = open_sale(&mut con, 3).await;
    let ticket = awaiting(&mut con, sale, 1, 60_000).await;
    assert!(!cache::close_refunded_hold(&mut con, sale, &ticket, "sess-1").await.unwrap(), "not claimed yet");
    cache::claim_hold(&mut con, sale, &ticket, "sess-1", "pi-1").await.unwrap();

    assert!(cache::close_refunded_hold(&mut con, sale, &ticket, "sess-1").await.unwrap());
    assert!(!cache::close_refunded_hold(&mut con, sale, &ticket, "sess-1").await.unwrap());
    assert_eq!(state(&mut con, sale, &ticket).await, "REFUNDED");
    assert_eq!(cache::finalize_hold(&mut con, sale, &ticket, "sess-1").await.unwrap(), FinalizeOutcome::Noop);
    assert_eq!(get_i64(&mut con, cache::sold_key(sale)).await, 0);
    cleanup(&mut con, sale).await;
}

/// Payment and expiry racing on many holds whose deadlines fall inside the
/// test: every hold ends in exactly one of PAID (the payment won) or EXPIRED
/// (the release won), never both, and stock matches the releases.
#[tokio::test]
async fn payment_and_expiry_race_has_one_winner_per_hold() {
    let mut con = connect().await;
    const HOLDS: i32 = 200;
    let sale = open_sale(&mut con, HOLDS).await;

    // Deadlines spread over the next ~600 ms, so claims and releases cross them
    let mut tickets = Vec::new();
    for user in 0..HOLDS {
        let deadline_in = 100 + (user as i64 * 3) % 600;
        tickets.push(awaiting(&mut con, sale, user, deadline_in).await);
    }

    let releasers: Vec<_> = (0..4)
        .map(|_| {
            let (mut con, tickets) = (con.clone(), tickets.clone());
            tokio::spawn(async move {
                // Sweep the whole set repeatedly for a while, as expiration workers do
                let mut released = HashSet::new();
                let until = tokio::time::Instant::now() + std::time::Duration::from_millis(1_200);
                while tokio::time::Instant::now() < until {
                    for ticket in &tickets {
                        let outcome = cache::release_hold(
                            &mut con, sale, ticket, HoldState::AwaitingPayment, MaxScore::Now, ReleasedAs::Expired, false,
                        )
                        .await
                        .unwrap();
                        if outcome == ReleaseOutcome::Released {
                            released.insert(ticket.clone());
                        }
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
                released
            })
        })
        .collect();

    // Each payment arrives once, at a time spread across the same window
    let payers: Vec<_> = tickets
        .iter()
        .enumerate()
        .map(|(user, ticket)| {
            let (mut con, ticket) = (con.clone(), ticket.clone());
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis((user as u64 * 7) % 700)).await;
                let won = cache::claim_hold(&mut con, sale, &ticket, &format!("sess-{user}"), &format!("pi-{user}"))
                    .await
                    .unwrap();
                (ticket, won == ClaimOutcome::Won)
            })
        })
        .collect();

    let mut won = HashSet::new();
    for payer in payers {
        let (ticket, claimed) = payer.await.unwrap();
        if claimed {
            won.insert(ticket);
        }
    }
    let mut released_total = 0;
    let mut released_all = HashSet::new();
    for releaser in releasers {
        let released = releaser.await.unwrap();
        released_total += released.len();
        released_all.extend(released);
    }

    assert_eq!(released_total, released_all.len(), "no hold was released by two workers");
    assert!(won.is_disjoint(&released_all), "a hold was both paid and released");
    assert_eq!(won.len() + released_all.len(), HOLDS as usize, "every hold has exactly one winner");
    assert!(!won.is_empty() && !released_all.is_empty(), "the race was not one-sided: {} paid, {} expired", won.len(), released_all.len());
    for ticket in &tickets {
        let expected = if won.contains(ticket) { "PAID" } else { "EXPIRED" };
        assert_eq!(state(&mut con, sale, ticket).await, expected);
    }
    assert_eq!(get_i64(&mut con, cache::stock_key(sale)).await, released_all.len() as i64, "stock came back only for released holds");
    cleanup(&mut con, sale).await;
}
