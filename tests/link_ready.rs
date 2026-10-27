//! `link_ready.lua` (and `set_max.lua`) against a real Redis, database 15
//! (`TEST_REDIS_URL`). Each test uses its own random sale id.

use chrono::{Duration, Utc};
use redis::AsyncCommands;

use flash_sale::cache::{
    self, CheckoutOutcome, HoldState, LinkOutcome, MaxScore, ReleaseOutcome, ReleasedAs, RedisConn,
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

/// A user's `PENDING` hold; returns its ticket
async fn hold(con: &mut RedisConn, sale: i32, user: i32) -> String {
    let ticket = cache::new_ticket(sale);
    let outcome = cache::checkout(con, sale, user, &format!("k{user}"), &ticket).await.unwrap();
    assert!(matches!(outcome, CheckoutOutcome::Held { .. }), "{outcome:?}");
    ticket
}

async fn stock(con: &mut RedisConn, sale: i32) -> i64 {
    con.get::<_, Option<i64>>(cache::stock_key(sale)).await.unwrap().unwrap_or(0)
}

async fn cleanup(con: &mut RedisConn, sale: i32) {
    let keys: Vec<String> = con.keys(format!("*{{{sale}}}*")).await.unwrap();
    if !keys.is_empty() {
        let _: () = con.del(keys).await.unwrap();
    }
}

#[tokio::test]
async fn ready_moves_the_hold_from_pending_to_the_expiry_set() {
    let mut con = connect().await;
    let sale = open_sale(&mut con, 3).await;
    let ticket = hold(&mut con, sale, 1).await;
    let expires_at = Utc::now().timestamp_millis() + 300_000;

    let outcome = cache::link_ready(&mut con, sale, &ticket, "sess-1", expires_at).await.unwrap();
    assert_eq!(outcome, LinkOutcome::Ready);

    let state: String = con.hget(cache::hold_key(sale, &ticket), "state").await.unwrap();
    let session: String = con.hget(cache::hold_key(sale, &ticket), "payment_session").await.unwrap();
    assert_eq!((state.as_str(), session.as_str()), ("AWAITING_PAYMENT", "sess-1"));
    assert!(con.zscore::<_, _, Option<f64>>(cache::holds_pending_key(sale), &ticket).await.unwrap().is_none());
    let score: f64 = con.zscore(cache::holds_expiry_key(sale), &ticket).await.unwrap();
    assert_eq!(score as i64, expires_at);
    assert_eq!(stock(&mut con, sale).await, 2, "linking does not touch stock");
    cleanup(&mut con, sale).await;
}

#[tokio::test]
async fn a_redelivery_with_the_same_session_is_already_done() {
    let mut con = connect().await;
    let sale = open_sale(&mut con, 3).await;
    let ticket = hold(&mut con, sale, 1).await;
    let first = Utc::now().timestamp_millis() + 300_000;
    assert_eq!(cache::link_ready(&mut con, sale, &ticket, "sess-1", first).await.unwrap(), LinkOutcome::Ready);

    // The same session again, even with a later deadline: success, nothing changes
    let again = cache::link_ready(&mut con, sale, &ticket, "sess-1", first + 5_000).await.unwrap();
    assert_eq!(again, LinkOutcome::Already);
    let score: f64 = con.zscore(cache::holds_expiry_key(sale), &ticket).await.unwrap();
    assert_eq!(score as i64, first);

    // Another session is not this message's link
    assert_eq!(cache::link_ready(&mut con, sale, &ticket, "sess-2", first).await.unwrap(), LinkOutcome::Gone);

    // A hold the worker has settled (sold) is past linking
    let _: () = con.hset(cache::hold_key(sale, &ticket), "state", "CONFIRMED").await.unwrap();
    assert_eq!(cache::link_ready(&mut con, sale, &ticket, "sess-1", first).await.unwrap(), LinkOutcome::Gone);
    cleanup(&mut con, sale).await;
}

#[tokio::test]
async fn a_released_or_unknown_hold_is_gone() {
    let mut con = connect().await;
    let sale = open_sale(&mut con, 3).await;
    let ticket = hold(&mut con, sale, 1).await;
    let released = cache::release_hold(
        &mut con, sale, &ticket, HoldState::Pending, MaxScore::Any, ReleasedAs::Failed, false,
    )
    .await
    .unwrap();
    assert_eq!(released, ReleaseOutcome::Released);

    let expires_at = Utc::now().timestamp_millis() + 300_000;
    assert_eq!(cache::link_ready(&mut con, sale, &ticket, "sess-1", expires_at).await.unwrap(), LinkOutcome::Gone);
    assert!(con.zscore::<_, _, Option<f64>>(cache::holds_expiry_key(sale), &ticket).await.unwrap().is_none());
    assert_eq!(stock(&mut con, sale).await, 3);

    let unknown = cache::new_ticket(sale);
    assert_eq!(cache::link_ready(&mut con, sale, &unknown, "sess-1", expires_at).await.unwrap(), LinkOutcome::Gone);
    cleanup(&mut con, sale).await;
}

/// A link and a release race on each hold: exactly one wins, and the unit
/// goes back to stock only if the release did
#[tokio::test]
async fn link_and_release_race_has_one_winner() {
    let mut con = connect().await;
    let n = 100;
    let sale = open_sale(&mut con, n).await;
    let mut tickets = Vec::new();
    for user in 0..n {
        tickets.push(hold(&mut con, sale, user + 1).await);
    }
    assert_eq!(stock(&mut con, sale).await, 0);

    let expires_at = Utc::now().timestamp_millis() + 300_000;
    let mut tasks = Vec::new();
    for ticket in &tickets {
        for linking in [true, false] {
            let (mut con, ticket) = (con.clone(), ticket.clone());
            tasks.push(tokio::spawn(async move {
                if linking {
                    let outcome = cache::link_ready(&mut con, sale, &ticket, "sess", expires_at).await.unwrap();
                    (ticket, linking, outcome == LinkOutcome::Ready)
                } else {
                    let outcome = cache::release_hold(
                        &mut con, sale, &ticket, HoldState::Pending, MaxScore::Any, ReleasedAs::Failed, false,
                    )
                    .await
                    .unwrap();
                    (ticket, linking, outcome == ReleaseOutcome::Released)
                }
            }));
        }
    }
    let mut linked = 0;
    let mut released = 0;
    let mut wins: std::collections::HashMap<String, u32> = Default::default();
    for task in tasks {
        let (ticket, linking, won) = task.await.unwrap();
        if won {
            *wins.entry(ticket).or_default() += 1;
            if linking { linked += 1 } else { released += 1 }
        }
    }
    assert!(tickets.iter().all(|t| wins.get(t) == Some(&1)), "exactly one winner per hold");
    assert_eq!(linked + released, n);
    // Stock is back for the released holds only, and the sets agree with the outcomes
    assert_eq!(stock(&mut con, sale).await, released as i64);
    assert_eq!(con.zcard::<_, i64>(cache::holds_expiry_key(sale)).await.unwrap(), linked as i64);
    assert_eq!(con.zcard::<_, i64>(cache::holds_pending_key(sale)).await.unwrap(), 0);
    cleanup(&mut con, sale).await;
}

#[tokio::test]
async fn raise_to_never_lowers() {
    let mut con = connect().await;
    let key = format!("test:set_max:{}", uuid::Uuid::new_v4());
    assert!(cache::raise_to(&mut con, &key, 100).await.unwrap());
    assert!(!cache::raise_to(&mut con, &key, 90).await.unwrap());
    assert!(!cache::raise_to(&mut con, &key, 100).await.unwrap());
    assert!(cache::raise_to(&mut con, &key, 101).await.unwrap());
    assert_eq!(con.get::<_, i64>(&key).await.unwrap(), 101);
    let _: () = con.del(&key).await.unwrap();
}

/// `processed_upto` and the queue-position progress are written together and
/// neither goes backwards
#[tokio::test]
async fn recorded_progress_only_rises() {
    let mut con = connect().await;
    let mut read_cache = cache::ReadCache(con.clone());
    let sale = open_sale(&mut con, 1).await;
    let topic = format!("test.progress.{}", uuid::Uuid::new_v4().simple());

    cache::record_progress(&mut con, &mut read_cache, &topic, 2, 10, &[(sale, 5_000)]).await.unwrap();
    // Checkout times within a partition are not monotonic: a later prefix may end at an earlier time
    cache::record_progress(&mut con, &mut read_cache, &topic, 2, 12, &[(sale, 4_000)]).await.unwrap();
    assert_eq!(cache::read_processed_upto(&mut con, sale, 2).await.unwrap(), Some(5_000));
    assert_eq!(cache::read_progress(&mut read_cache, &topic, 2).await.unwrap(), Some(12));
    // A stale write (e.g. a previous owner of the partition) changes neither
    cache::record_progress(&mut con, &mut read_cache, &topic, 2, 8, &[(sale, 3_000)]).await.unwrap();
    assert_eq!(cache::read_processed_upto(&mut con, sale, 2).await.unwrap(), Some(5_000));
    assert_eq!(cache::read_progress(&mut read_cache, &topic, 2).await.unwrap(), Some(12));
    assert_eq!(cache::read_processed_upto(&mut con, sale, 1).await.unwrap(), None);

    let _: () = con.del(cache::progress_key(&topic, 2)).await.unwrap();
    cleanup(&mut con, sale).await;
}
