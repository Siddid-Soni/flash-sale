//! `settle_hold.lua` against a real Redis (database 15): closing a due hold
//! whose order PostgreSQL settled (it is deleted, nothing else changes), and
//! the race with `release.lua`. Each test uses its own random sale id.

use std::collections::HashSet;

use chrono::{Duration, Utc};
use redis::AsyncCommands;

use flash_sale::cache::{
    self, CheckoutOutcome, HoldState, LinkOutcome, MaxScore, RedisConn, ReleaseOutcome, ReleasedAs,
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
async fn a_settled_hold_is_deleted_and_nothing_else_changes() {
    let mut con = connect().await;
    let sale = open_sale(&mut con, 3).await;
    let ticket = awaiting(&mut con, sale, 1, 60_000).await;
    assert_eq!(get_i64(&mut con, cache::stock_key(sale)).await, 2);

    // The deadline is far away: settling does not look at it, PostgreSQL has decided
    assert!(cache::settle_hold(&mut con, sale, &ticket).await.unwrap());
    assert!(cache::read_hold(&mut con, sale, &ticket).await.unwrap().is_none(), "the hold is deleted");
    assert!(!in_expiry_set(&mut con, sale, &ticket).await);
    assert_eq!(get_i64(&mut con, cache::stock_key(sale)).await, 2, "the unit is sold, not free");
    let keys: Vec<String> = con.keys(format!("*{{{sale}}}*")).await.unwrap();
    assert!(!keys.iter().any(|k| k.starts_with("sold:")), "Redis does not count sales: {keys:?}");
    let live: Option<String> = con.get(cache::user_live_key(sale, 1)).await.unwrap();
    assert_eq!(live.as_deref(), Some(ticket.as_str()), "the user bought one");

    // Settling again does nothing
    assert!(!cache::settle_hold(&mut con, sale, &ticket).await.unwrap());
    assert_eq!(get_i64(&mut con, cache::stock_key(sale)).await, 2);
    cleanup(&mut con, sale).await;
}

#[tokio::test]
async fn only_a_hold_in_the_expiry_set_can_be_settled() {
    let mut con = connect().await;
    let sale = open_sale(&mut con, 5).await;

    // Released: no longer in the set, state EXPIRED
    let released = awaiting(&mut con, sale, 1, 60_000).await;
    let outcome =
        cache::release_hold(&mut con, sale, &released, HoldState::AwaitingPayment, MaxScore::Any, ReleasedAs::Expired, false)
            .await
            .unwrap();
    assert_eq!(outcome, ReleaseOutcome::Released);
    assert!(!cache::settle_hold(&mut con, sale, &released).await.unwrap());
    assert_eq!(state(&mut con, sale, &released).await, "EXPIRED");

    // PENDING: not linked yet
    let pending = cache::new_ticket(sale);
    cache::checkout(&mut con, sale, 2, "k2", &pending).await.unwrap();
    assert!(!cache::settle_hold(&mut con, sale, &pending).await.unwrap());
    assert_eq!(state(&mut con, sale, &pending).await, "PENDING");

    // Unknown
    let unknown = cache::new_ticket(sale);
    assert!(!cache::settle_hold(&mut con, sale, &unknown).await.unwrap());

    cleanup(&mut con, sale).await;
}

#[tokio::test]
async fn concurrent_settles_of_one_hold_succeed_once() {
    let mut con = connect().await;
    let sale = open_sale(&mut con, 1).await;
    let ticket = awaiting(&mut con, sale, 1, 60_000).await;

    let tasks: Vec<_> = (0..16)
        .map(|_| {
            let (mut con, ticket) = (con.clone(), ticket.clone());
            tokio::spawn(async move { cache::settle_hold(&mut con, sale, &ticket).await.unwrap() })
        })
        .collect();
    let mut settled = 0;
    for task in tasks {
        settled += usize::from(task.await.unwrap());
    }
    assert_eq!(settled, 1);
    cleanup(&mut con, sale).await;
}

/// Settling and releasing racing on many holds: each ends in exactly one of
/// settled (the hold is deleted, no stock back) or released (state EXPIRED,
/// stock back), never both
#[tokio::test]
async fn settle_and_release_race_has_one_winner_per_hold() {
    let mut con = connect().await;
    const HOLDS: i32 = 200;
    let sale = open_sale(&mut con, HOLDS).await;
    let mut tickets = Vec::new();
    for user in 0..HOLDS {
        tickets.push(awaiting(&mut con, sale, user, 60_000).await);
    }

    let releasers: Vec<_> = (0..4)
        .map(|_| {
            let (mut con, tickets) = (con.clone(), tickets.clone());
            tokio::spawn(async move {
                let mut released = HashSet::new();
                for ticket in &tickets {
                    let outcome = cache::release_hold(
                        &mut con, sale, ticket, HoldState::AwaitingPayment, MaxScore::Any, ReleasedAs::Expired, false,
                    )
                    .await
                    .unwrap();
                    if outcome == ReleaseOutcome::Released {
                        released.insert(ticket.clone());
                    }
                }
                released
            })
        })
        .collect();
    let settlers: Vec<_> = (0..4)
        .map(|_| {
            let (mut con, tickets) = (con.clone(), tickets.clone());
            tokio::spawn(async move {
                let mut settled = HashSet::new();
                // The other direction, so both sides reach every hold first sometimes
                for ticket in tickets.iter().rev() {
                    if cache::settle_hold(&mut con, sale, ticket).await.unwrap() {
                        settled.insert(ticket.clone());
                    }
                }
                settled
            })
        })
        .collect();

    let (mut released_total, mut released_all) = (0, HashSet::new());
    for task in releasers {
        let released = task.await.unwrap();
        released_total += released.len();
        released_all.extend(released);
    }
    let (mut settled_total, mut settled_all) = (0, HashSet::new());
    for task in settlers {
        let settled = task.await.unwrap();
        settled_total += settled.len();
        settled_all.extend(settled);
    }

    assert_eq!((released_total, settled_total), (released_all.len(), settled_all.len()), "no hold was won twice");
    assert!(released_all.is_disjoint(&settled_all), "a hold was both released and settled");
    assert_eq!(released_all.len() + settled_all.len(), HOLDS as usize, "every hold has exactly one winner");
    for ticket in &tickets {
        if settled_all.contains(ticket) {
            assert!(cache::read_hold(&mut con, sale, ticket).await.unwrap().is_none());
        } else {
            assert_eq!(state(&mut con, sale, ticket).await, "EXPIRED");
        }
    }
    assert_eq!(get_i64(&mut con, cache::stock_key(sale)).await, released_all.len() as i64);
    cleanup(&mut con, sale).await;
}
