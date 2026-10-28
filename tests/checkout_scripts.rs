//! `checkout.lua`, `return_unit.lua` and `set_status.lua` against a real Redis
//! (`docker compose up -d redis`), in database 15 (`TEST_REDIS_URL`). Each
//! test uses its own random sale id, and the read cache is the same instance.

use std::collections::HashSet;

use chrono::{Duration, Utc};
use redis::AsyncCommands;

use flash_sale::{
    cache::{self, CheckoutOutcome, ReadCache, RedisConn, StatusUpdate},
    models::OrderStatus,
};

async fn connect() -> RedisConn {
    dotenvy::dotenv().ok();
    let url = std::env::var("TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/15".into());
    let client = redis::Client::open(url).unwrap();
    redis::aio::ConnectionManager::new(client).await.expect("Redis should be running for these tests")
}

/// A sale with `stock` units that opened `opened_ago_ms` ago (negative: opens in the future)
async fn sale(con: &mut RedisConn, stock: i32, opened_ago_ms: i64) -> i32 {
    let sale = 1_000_000 + (uuid::Uuid::new_v4().as_u128() % 1_000_000_000) as i32;
    cache::init_sale(con, sale, stock, Utc::now() - Duration::milliseconds(opened_ago_ms)).await.unwrap();
    sale
}

async fn open_sale(con: &mut RedisConn, stock: i32) -> i32 {
    sale(con, stock, 60_000).await
}

/// Checks out with a fresh candidate ticket; returns the outcome and that ticket
async fn checkout(con: &mut RedisConn, sale: i32, user: i32, key: &str) -> (CheckoutOutcome, String) {
    let ticket = cache::new_ticket(sale);
    (cache::checkout(con, sale, user, key, &ticket).await.unwrap(), ticket)
}

async fn get_i64(con: &mut RedisConn, key: String) -> i64 {
    con.get::<_, Option<i64>>(key).await.unwrap().unwrap_or(0)
}

async fn stock(con: &mut RedisConn, sale: i32) -> i64 {
    get_i64(con, cache::stock_key(sale)).await
}

async fn cleanup(con: &mut RedisConn, sale: i32) {
    for pattern in [format!("*{{{sale}}}*"), format!("status:{sale}.*")] {
        let keys: Vec<String> = con.keys(pattern).await.unwrap();
        if !keys.is_empty() {
            let _: () = con.del(keys).await.unwrap();
        }
    }
}

#[tokio::test]
async fn refuses_before_the_start_and_for_unknown_sales() {
    let mut con = connect().await;
    let future = sale(&mut con, 5, -60_000).await;
    let (outcome, _) = checkout(&mut con, future, 1, "k").await;
    assert!(matches!(outcome, CheckoutOutcome::NotStarted { sale_start_ms } if sale_start_ms > Utc::now().timestamp_millis()));
    assert_eq!(stock(&mut con, future).await, 5);
    assert!(!con.exists::<_, bool>(cache::user_live_key(future, 1)).await.unwrap());
    // Nothing was bound to the key, so it works once the sale opens
    redis::cmd("SET")
        .arg(cache::sale_start_key(future))
        .arg(Utc::now().timestamp_millis() - 1)
        .query_async::<()>(&mut con)
        .await
        .unwrap();
    assert!(matches!(checkout(&mut con, future, 1, "k").await.0, CheckoutOutcome::Reserved { .. }));

    let unknown = 2_000_000_000 - (uuid::Uuid::new_v4().as_u128() % 1_000_000) as i32;
    assert_eq!(checkout(&mut con, unknown, 1, "k").await.0, CheckoutOutcome::NotFound);
    cleanup(&mut con, future).await;
}

#[tokio::test]
async fn a_checkout_reserves_a_unit_and_keeps_only_the_gating_keys() {
    let mut con = connect().await;
    let sale = open_sale(&mut con, 3).await;
    let (outcome, ticket) = checkout(&mut con, sale, 7, "key-1").await;
    let CheckoutOutcome::Reserved { checkout_ms } = outcome else { panic!("{outcome:?}") };
    assert!((Utc::now().timestamp_millis() - checkout_ms).abs() < 5_000);

    assert_eq!(stock(&mut con, sale).await, 2);
    let live: String = con.get(cache::user_live_key(sale, 7)).await.unwrap();
    assert_eq!(live, ticket);
    let idem: String = con.get(cache::idem_key(sale, 7, "key-1")).await.unwrap();
    assert_eq!(idem, ticket);
    let ttl: i64 = con.ttl(cache::idem_key(sale, 7, "key-1")).await.unwrap();
    assert!((86_300..=86_400).contains(&ttl), "ttl {ttl}");
    // No record of the ticket beyond those two keys: the queue is Kafka, the outcome PostgreSQL's
    let mut keys: Vec<String> = con.keys(format!("*{{{sale}}}*")).await.unwrap();
    keys.sort();
    let mut expected =
        vec![cache::idem_key(sale, 7, "key-1"), cache::sale_start_key(sale), cache::stock_key(sale), cache::user_live_key(sale, 7)];
    expected.sort();
    assert_eq!(keys, expected);
    // The status entry is Rust's to write, not the script's
    assert!(!con.exists::<_, bool>(cache::status_key(&ticket)).await.unwrap());
    cleanup(&mut con, sale).await;
}

/// `user_live` gets a TTL from the script: the seconds until the sale's start
/// plus a day, at least a day. A checkout happens only after the start, so it
/// is the day; a start a few seconds ago must not shorten it.
#[tokio::test]
async fn user_live_expires_a_day_after_the_sale_not_never_and_not_early() {
    let mut con = connect().await;
    for opened_ago_ms in [60_000, 5_000, 1] {
        let sale = sale(&mut con, 3, opened_ago_ms).await;
        let (outcome, ticket) = checkout(&mut con, sale, 7, "k").await;
        assert!(matches!(outcome, CheckoutOutcome::Reserved { .. }), "{outcome:?}");
        let ttl: i64 = con.ttl(cache::user_live_key(sale, 7)).await.unwrap();
        assert!((86_398..=86_400).contains(&ttl), "opened {opened_ago_ms} ms ago: ttl {ttl}");
        let live: String = con.get(cache::user_live_key(sale, 7)).await.unwrap();
        assert_eq!(live, ticket);
        cleanup(&mut con, sale).await;
    }
}

#[tokio::test]
async fn a_retry_with_the_same_key_gets_the_same_ticket() {
    let mut con = connect().await;
    let sale = open_sale(&mut con, 3).await;
    let (first, ticket) = checkout(&mut con, sale, 7, "k1").await;
    assert!(matches!(first, CheckoutOutcome::Reserved { .. }));

    // The retry is answered before the one-per-user check, which would say 409
    let (retry, _) = checkout(&mut con, sale, 7, "k1").await;
    assert_eq!(retry, CheckoutOutcome::Existing { ticket: ticket.clone() });
    assert_eq!(stock(&mut con, sale).await, 2, "the retry took no unit");

    // A different key is a different request: the user already holds one
    assert_eq!(checkout(&mut con, sale, 7, "k2").await.0, CheckoutOutcome::AlreadyLive);
    assert_eq!(stock(&mut con, sale).await, 2);

    // Keys are per user
    assert!(matches!(checkout(&mut con, sale, 8, "k1").await.0, CheckoutOutcome::Reserved { .. }));
    assert_eq!(stock(&mut con, sale).await, 1);
    cleanup(&mut con, sale).await;
}

#[tokio::test]
async fn sold_out_is_refused_and_stock_never_goes_negative() {
    let mut con = connect().await;
    let sale = open_sale(&mut con, 1).await;
    let (winner, ticket) = checkout(&mut con, sale, 1, "k").await;
    assert!(matches!(winner, CheckoutOutcome::Reserved { .. }));
    assert_eq!(checkout(&mut con, sale, 2, "k").await.0, CheckoutOutcome::SoldOut);
    assert_eq!(checkout(&mut con, sale, 3, "k").await.0, CheckoutOutcome::SoldOut);
    assert_eq!(stock(&mut con, sale).await, 0);
    // The winner's retry still gets the ticket, even with nothing left
    assert_eq!(checkout(&mut con, sale, 1, "k").await.0, CheckoutOutcome::Existing { ticket });
    // A missing stock key counts as sold out
    let _: () = con.del(cache::stock_key(sale)).await.unwrap();
    assert_eq!(checkout(&mut con, sale, 4, "k").await.0, CheckoutOutcome::SoldOut);
    cleanup(&mut con, sale).await;
}

/// Runs `return_unit.lua` the way the worker does (no idempotency key) or the
/// Order Service does after a failed publish (with it)
async fn return_unit(con: &mut RedisConn, sale: i32, user: i32, ticket: &str, idem: Option<&str>) {
    cache::return_unit(con, sale, user, ticket, idem).await.unwrap();
}

#[tokio::test]
async fn returning_a_unit_gives_it_back_and_frees_the_user() {
    let mut con = connect().await;
    let sale = open_sale(&mut con, 2).await;
    let (_, ticket) = checkout(&mut con, sale, 1, "k").await;
    assert_eq!(stock(&mut con, sale).await, 1);

    // The worker's call: no idempotency key
    return_unit(&mut con, sale, 1, &ticket, None).await;
    assert_eq!(stock(&mut con, sale).await, 2);
    assert!(!con.exists::<_, bool>(cache::user_live_key(sale, 1)).await.unwrap());
    // The key keeps naming the old ticket: a retry with it gets that ticket back
    assert_eq!(checkout(&mut con, sale, 1, "k").await.0, CheckoutOutcome::Existing { ticket });
    // The user may check out again with another key
    assert!(matches!(checkout(&mut con, sale, 1, "k-new").await.0, CheckoutOutcome::Reserved { .. }));
    assert_eq!(stock(&mut con, sale).await, 1);
    cleanup(&mut con, sale).await;
}

#[tokio::test]
async fn compensation_forgets_the_key_so_a_retry_makes_a_new_ticket() {
    let mut con = connect().await;
    let sale = open_sale(&mut con, 2).await;
    let (_, first) = checkout(&mut con, sale, 1, "same-key").await;
    return_unit(&mut con, sale, 1, &first, Some("same-key")).await;
    assert!(!con.exists::<_, bool>(cache::idem_key(sale, 1, "same-key")).await.unwrap());
    assert!(!con.exists::<_, bool>(cache::user_live_key(sale, 1)).await.unwrap());
    assert_eq!(stock(&mut con, sale).await, 2);

    let (retry, second) = checkout(&mut con, sale, 1, "same-key").await;
    assert!(matches!(retry, CheckoutOutcome::Reserved { .. }));
    assert_ne!(first, second);
    assert_eq!(stock(&mut con, sale).await, 1);
    cleanup(&mut con, sale).await;
}

#[tokio::test]
async fn returning_a_unit_leaves_markers_that_name_another_ticket() {
    let mut con = connect().await;
    let sale = open_sale(&mut con, 2).await;
    let (_, ticket) = checkout(&mut con, sale, 1, "k").await;
    let _: () = con.set(cache::user_live_key(sale, 1), "someone-elses").await.unwrap();
    let _: () = con.set(cache::idem_key(sale, 1, "k"), "someone-elses").await.unwrap();
    return_unit(&mut con, sale, 1, &ticket, Some("k")).await;
    let live: String = con.get(cache::user_live_key(sale, 1)).await.unwrap();
    assert_eq!(live, "someone-elses");
    let idem: String = con.get(cache::idem_key(sale, 1, "k")).await.unwrap();
    assert_eq!(idem, "someone-elses");
    assert_eq!(stock(&mut con, sale).await, 2, "the unit itself came back");
    cleanup(&mut con, sale).await;
}

#[tokio::test]
async fn set_status_only_moves_forward() {
    let mut con = connect().await;
    let mut cache_con = ReadCache(con.clone());
    let sale = open_sale(&mut con, 1).await;
    let ticket = cache::new_ticket(sale);
    let write = |status, extra: fn(&mut StatusUpdate)| {
        let mut update = StatusUpdate::new(status, 7);
        extra(&mut update);
        update
    };

    assert_eq!(cache::read_status(&mut cache_con, &ticket).await.unwrap(), None);

    let pending = write(OrderStatus::Pending, |u| {
        u.partition = Some(2);
        u.offset = Some(41);
        u.sale = Some(9);
    });
    assert!(cache::set_status(&mut cache_con, &ticket, &pending).await.unwrap());
    let cached = cache::read_status(&mut cache_con, &ticket).await.unwrap().unwrap();
    assert_eq!((cached.status, cached.user, cached.partition, cached.offset), (OrderStatus::Pending, 7, Some(2), Some(41)));
    let ttl: i64 = con.ttl(cache::status_key(&ticket)).await.unwrap();
    assert!((86_300..=86_400).contains(&ttl), "ttl {ttl}");

    let link = write(OrderStatus::AwaitingPayment, |u| {
        u.payment_url = Some("http://pay/1".into());
        u.expires_at = Some("2030-01-01T00:05:00.000Z".into());
    });
    assert!(cache::set_status(&mut cache_con, &ticket, &link).await.unwrap());
    // A delayed PENDING write (the Order Service was slow) cannot undo it
    assert!(!cache::set_status(&mut cache_con, &ticket, &pending).await.unwrap());
    let cached = cache::read_status(&mut cache_con, &ticket).await.unwrap().unwrap();
    assert_eq!(cached.status, OrderStatus::AwaitingPayment);
    assert_eq!(cached.payment_url.as_deref(), Some("http://pay/1"));
    assert_eq!(cached.expires_at.as_deref(), Some("2030-01-01T00:05:00.000Z"));
    // The same rank refreshes
    assert!(cache::set_status(&mut cache_con, &ticket, &link).await.unwrap());

    // The first final state wins over the other final states...
    assert!(cache::set_status(&mut cache_con, &ticket, &write(OrderStatus::Expired, |_| {})).await.unwrap());
    for later in [OrderStatus::Pending, OrderStatus::AwaitingPayment, OrderStatus::Confirmed, OrderStatus::Failed, OrderStatus::Expired] {
        assert!(!cache::set_status(&mut cache_con, &ticket, &write(later, |_| {})).await.unwrap(), "{later:?}");
    }
    assert_eq!(cache::read_status(&mut cache_con, &ticket).await.unwrap().unwrap().status, OrderStatus::Expired);
    // ...except that a late payment's refund replaces EXPIRED, and then nothing replaces it
    assert!(cache::set_status(&mut cache_con, &ticket, &write(OrderStatus::Refunded, |_| {})).await.unwrap());
    for later in [OrderStatus::Confirmed, OrderStatus::Expired, OrderStatus::Failed, OrderStatus::Refunded] {
        assert!(!cache::set_status(&mut cache_con, &ticket, &write(later, |_| {})).await.unwrap(), "{later:?}");
    }
    assert_eq!(cache::read_status(&mut cache_con, &ticket).await.unwrap().unwrap().status, OrderStatus::Refunded);
    // A CONFIRMED entry is never replaced, not even by REFUNDED
    let confirmed = cache::new_ticket(sale);
    assert!(cache::set_status(&mut cache_con, &confirmed, &write(OrderStatus::Confirmed, |_| {})).await.unwrap());
    assert!(!cache::set_status(&mut cache_con, &confirmed, &write(OrderStatus::Refunded, |_| {})).await.unwrap());
    cleanup(&mut con, sale).await;
}

/// The roadmap's test: a flood of checkouts for 500 units. 10,000 distinct
/// users; 500 of them also fire a second request with another key, 500 retry
/// with the same key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ten_thousand_concurrent_checkouts_sell_exactly_500() {
    const UNITS: i32 = 500;
    const USERS: i32 = 10_000;
    let mut con = connect().await;
    let sale = open_sale(&mut con, UNITS).await;

    // Stock must never be seen below zero while the flood runs
    let monitor = {
        let mut con = con.clone();
        tokio::spawn(async move {
            let mut lowest = i64::MAX;
            loop {
                let level: Option<i64> = con.get(cache::stock_key(sale)).await.unwrap();
                lowest = lowest.min(level.unwrap());
                if level == Some(0) {
                    return lowest;
                }
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        })
    };

    let mut tasks = tokio::task::JoinSet::new();
    for user in 1..=USERS {
        let attempts: Vec<&'static str> = match user {
            1..=500 => vec!["a", "b"],
            501..=1000 => vec!["a", "a"],
            _ => vec!["a"],
        };
        for key in attempts {
            let mut con = con.clone();
            tasks.spawn(async move { (user, key, checkout(&mut con, sale, user, key).await) });
        }
    }
    let (mut held, mut existing, mut already, mut sold_out) = (Vec::new(), 0, 0, 0);
    while let Some(done) = tasks.join_next().await {
        let (user, _key, (outcome, ticket)) = done.unwrap();
        match outcome {
            CheckoutOutcome::Reserved { .. } => held.push((user, ticket)),
            CheckoutOutcome::Existing { .. } => existing += 1,
            CheckoutOutcome::AlreadyLive => already += 1,
            CheckoutOutcome::SoldOut => sold_out += 1,
            other => panic!("unexpected {other:?}"),
        }
    }
    let lowest = tokio::time::timeout(std::time::Duration::from_secs(5), monitor).await.unwrap().unwrap();
    assert!(lowest >= 0, "stock dipped to {lowest}");

    assert_eq!(held.len(), UNITS as usize, "exactly 500 winners");
    assert_eq!(stock(&mut con, sale).await, 0);
    assert_eq!(existing + already + sold_out + held.len(), (USERS + 1000) as usize);
    assert!(sold_out > 0 && existing + already > 0);

    // One live marker per winner, naming that winner's ticket
    let live: Vec<String> = con.keys(format!("user_live:{{{sale}}}:*")).await.unwrap();
    assert_eq!(live.len(), UNITS as usize, "one live ticket per user");
    let mut users = HashSet::new();
    for (user, ticket) in &held {
        let named: String = con.get(cache::user_live_key(sale, *user)).await.unwrap();
        assert_eq!(&named, ticket);
        users.insert(*user);
    }
    assert_eq!(users.len(), UNITS as usize);
    // Losers left no idempotency key behind
    let idem: Vec<String> = con.keys(format!("idem:{{{sale}}}:*")).await.unwrap();
    assert_eq!(idem.len(), UNITS as usize);
    cleanup(&mut con, sale).await;
}
