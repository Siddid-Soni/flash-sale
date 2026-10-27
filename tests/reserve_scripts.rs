//! Runs the reserve and release scripts against Redis (`docker compose up -d
//! redis`), in database 15 so the tests never see, or are slowed down by, the
//! data in the database the API uses. Override with `TEST_REDIS_URL`. Each test
//! uses its own random product id.

use chrono::Utc;
use redis::AsyncCommands;

use flash_sale::cache::{self, JoinOutcome, ReleaseOutcome, ReserveOutcome, RedisConn};

const HOLD_MS: u64 = 60_000;
const MAX_PER_USER: u32 = 4;

async fn connect() -> RedisConn {
    dotenvy::dotenv().ok();
    let url = std::env::var("TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/15".into());
    let client = redis::Client::open(url).unwrap();
    redis::aio::ConnectionManager::new(client).await.expect("Redis should be running for these tests")
}

/// A product with `stock` units, and a helper that admits a user (an ACTIVE token)
async fn product(con: &mut RedisConn, stock: u32, max_per_user: u32) -> i32 {
    let product_id = 1_000_000 + (uuid::Uuid::new_v4().as_u128() % 1_000_000_000) as i32;
    cache::init_product(con, product_id, stock, max_per_user, Utc::now()).await.unwrap();
    product_id
}

async fn admit(con: &mut RedisConn, product_id: i32, user_id: i32) -> String {
    let token_id = uuid::Uuid::new_v4().to_string();
    let key = cache::token_key(&token_id);
    let _: () = redis::pipe()
        .hset_multiple(&key, &[("user", user_id.to_string()), ("product", product_id.to_string()), ("status", "ACTIVE".into())])
        .pexpire(&key, 600_000)
        .set(format!("user_token:{product_id}:{user_id}"), &token_id)
        .zadd(format!("active_tokens:{product_id}"), &token_id, Utc::now().timestamp_millis() + 600_000)
        .query_async(con)
        .await
        .unwrap();
    token_id
}

async fn reserve(con: &mut RedisConn, product_id: i32, user_id: i32, quantity: i32) -> ReserveOutcome {
    let resv = uuid::Uuid::new_v4().to_string();
    cache::reserve_units(con, product_id, user_id, &resv, quantity, HOLD_MS).await.unwrap()
}

async fn get_i64(con: &mut RedisConn, key: String) -> i64 {
    con.get::<_, Option<i64>>(key).await.unwrap().unwrap_or(0)
}

async fn cleanup(con: &mut RedisConn, product_id: i32) {
    let keys: Vec<String> = con.keys(format!("*:{product_id}*")).await.unwrap();
    if !keys.is_empty() {
        let _: () = con.del(keys).await.unwrap();
    }
}

#[tokio::test]
async fn holding_takes_units_from_stock_and_out_of_stock_is_refused() {
    let mut con = connect().await;
    let p = product(&mut con, 10, MAX_PER_USER).await;
    for user in 1..=4 {
        admit(&mut con, p, user).await;
    }

    assert!(matches!(reserve(&mut con, p, 1, 4).await, ReserveOutcome::Held { .. }));
    assert_eq!(get_i64(&mut con, cache::stock_key(p)).await, 6);
    assert_eq!(get_i64(&mut con, format!("user_units:{p}:1")).await, 4);
    assert!(matches!(reserve(&mut con, p, 2, 4).await, ReserveOutcome::Held { .. }));

    // All or nothing: 2 units are left, so asking for 3 holds none of them
    assert_eq!(reserve(&mut con, p, 3, 3).await, ReserveOutcome::OutOfStock { available: 2 });
    assert_eq!(get_i64(&mut con, cache::stock_key(p)).await, 2);
    // Losing the race does not cost the turn: the loser asks for fewer
    assert!(matches!(reserve(&mut con, p, 3, 2).await, ReserveOutcome::Held { .. }));
    assert_eq!(get_i64(&mut con, cache::stock_key(p)).await, 0);

    assert_eq!(reserve(&mut con, p, 4, 1).await, ReserveOutcome::OutOfStock { available: 0 });
    let snapshot = cache::stock_snapshot(&mut con, p).await.unwrap().unwrap();
    assert_eq!((snapshot.available, snapshot.sold, snapshot.total, snapshot.closed), (0, 0, 10, false));
    cleanup(&mut con, p).await;
}

#[tokio::test]
async fn retry_returns_the_same_reservation() {
    let mut con = connect().await;
    let p = product(&mut con, 10, MAX_PER_USER).await;
    let token = admit(&mut con, p, 1).await;

    let ReserveOutcome::Held { reservation_id, expires_at } = reserve(&mut con, p, 1, 2).await else { panic!("not held") };
    // Whatever the retry asks for, it gets the hold it made, and nothing more is taken
    assert_eq!(
        reserve(&mut con, p, 1, 3).await,
        ReserveOutcome::Existing { reservation_id, expires_at, quantity: 2 }
    );
    assert_eq!(get_i64(&mut con, cache::stock_key(p)).await, 8);
    // The token is used and its active slot freed for the next person
    let status: String = con.hget(cache::token_key(&token), "status").await.unwrap();
    assert_eq!(status, "USED");
    assert_eq!(con.zscore::<_, _, Option<f64>>(format!("active_tokens:{p}"), &token).await.unwrap(), None);
    cleanup(&mut con, p).await;
}

#[tokio::test]
async fn refuses_unadmitted_invalid_quantity_unknown_product_and_closed() {
    let mut con = connect().await;
    let p = product(&mut con, 10, MAX_PER_USER).await;
    assert_eq!(reserve(&mut con, p, 1, 1).await, ReserveOutcome::NotAdmitted);

    admit(&mut con, p, 1).await;
    let invalid = ReserveOutcome::InvalidQuantity { max: MAX_PER_USER };
    assert_eq!(reserve(&mut con, p, 1, 0).await, invalid);
    assert_eq!(reserve(&mut con, p, 1, -1).await, invalid);
    assert_eq!(reserve(&mut con, p, 1, MAX_PER_USER as i32 + 1).await, invalid);
    // Refusals cost nothing
    assert_eq!(get_i64(&mut con, cache::stock_key(p)).await, 10);

    // Redis does not know this product: nothing to sell, and no hold can be made
    let unknown = p + 1;
    admit(&mut con, unknown, 1).await;
    assert_eq!(reserve(&mut con, unknown, 1, 1).await, ReserveOutcome::OutOfStock { available: 0 });

    let _: () = con.set(cache::closed_key(p), 1).await.unwrap();
    assert_eq!(reserve(&mut con, p, 1, 1).await, ReserveOutcome::Closed);
    cleanup(&mut con, p).await;
    cleanup(&mut con, unknown).await;
}

#[tokio::test]
async fn release_returns_units_but_the_turn_stays_used() {
    let mut con = connect().await;
    let p = product(&mut con, 10, MAX_PER_USER).await;
    let token = admit(&mut con, p, 1).await;
    let ReserveOutcome::Held { reservation_id, .. } = reserve(&mut con, p, 1, 3).await else { panic!("not held") };
    assert_eq!(get_i64(&mut con, cache::stock_key(p)).await, 7);

    assert_eq!(cache::release_hold(&mut con, &reservation_id, Some(2)).await.unwrap(), ReleaseOutcome::Forbidden);
    assert_eq!(cache::release_hold(&mut con, &reservation_id, Some(1)).await.unwrap(), ReleaseOutcome::Released);
    assert_eq!(cache::release_hold(&mut con, &reservation_id, Some(1)).await.unwrap(), ReleaseOutcome::NotFound);

    assert_eq!(get_i64(&mut con, cache::stock_key(p)).await, 10);
    assert_eq!(get_i64(&mut con, format!("user_units:{p}:1")).await, 0);
    assert_eq!(con.zscore::<_, _, Option<f64>>(cache::HOLDS_EXPIRY_KEY, &reservation_id).await.unwrap(), None);
    // The turn was spent on the reservation
    let status: String = con.hget(cache::token_key(&token), "status").await.unwrap();
    assert_eq!(status, "USED");
    assert_eq!(con.zscore::<_, _, Option<f64>>(format!("active_tokens:{p}"), &token).await.unwrap(), None);
    assert_eq!(reserve(&mut con, p, 1, 1).await, ReserveOutcome::TurnUsed);
    cleanup(&mut con, p).await;
}

/// An expired hold gives its units back through the cleanup worker's script
#[tokio::test]
async fn expiry_returns_units_to_stock() {
    let mut con = connect().await;
    let p = product(&mut con, 10, MAX_PER_USER).await;
    admit(&mut con, p, 1).await;
    let mut sync = redis::Client::open(std::env::var("TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/15".into()))
        .unwrap()
        .get_connection()
        .unwrap();

    let resv = uuid::Uuid::new_v4().to_string();
    assert!(matches!(cache::reserve_units(&mut con, p, 1, &resv, 3, 200).await.unwrap(), ReserveOutcome::Held { .. }));
    assert_eq!(get_i64(&mut con, cache::stock_key(p)).await, 7);

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let (_, released) = cache::claim_expired(&mut sync, 500).unwrap();
    assert!(released.iter().any(|hold| hold.reservation_id == resv && hold.state == "held"));
    assert_eq!(get_i64(&mut con, cache::stock_key(p)).await, 10);
    assert_eq!(get_i64(&mut con, format!("user_units:{p}:1")).await, 0);
    cleanup(&mut con, p).await;
}

/// Many admitted users reserve the last units at once: the script is atomic,
/// so exactly the stock is held, never more, and the rest are refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_reserves_never_hold_more_than_the_stock() {
    let mut con = connect().await;
    let p = product(&mut con, 10, 1).await;
    for user in 1..=200 {
        admit(&mut con, p, user).await;
    }

    let mut tasks = tokio::task::JoinSet::new();
    for user in 1..=200 {
        let mut con = con.clone();
        tasks.spawn(async move { reserve(&mut con, p, user, 1).await });
    }
    let (mut held, mut out_of_stock) = (0, 0);
    while let Some(outcome) = tasks.join_next().await {
        match outcome.unwrap() {
            ReserveOutcome::Held { .. } => held += 1,
            ReserveOutcome::OutOfStock { .. } => out_of_stock += 1,
            other => panic!("unexpected outcome {other:?}"),
        }
    }

    assert_eq!((held, out_of_stock), (10, 190));
    assert_eq!(get_i64(&mut con, cache::stock_key(p)).await, 0);
    cleanup(&mut con, p).await;
}

async fn join(con: &mut RedisConn, product_id: i32, user_id: i32) -> JoinOutcome {
    cache::join_waiting_room(con, product_id, user_id, &uuid::Uuid::new_v4().to_string()).await.unwrap()
}

#[tokio::test]
async fn rejoin_only_after_the_hold_ends() {
    let mut con = connect().await;
    let p = product(&mut con, 10, MAX_PER_USER).await;
    let _: () = con.set(format!("lobby_flushed:{p}"), 1).await.unwrap();
    let token = admit(&mut con, p, 1).await;
    let ReserveOutcome::Held { reservation_id, .. } = reserve(&mut con, p, 1, 1).await else { panic!("not held") };

    // Holding units: joining returns the used token, not a new place in line
    let JoinOutcome::Existing(existing) = join(&mut con, p, 1).await else { panic!("expected the existing token") };
    assert_eq!((existing.token_id.as_str(), existing.status.as_str()), (token.as_str(), "USED"));

    cache::release_hold(&mut con, &reservation_id, Some(1)).await.unwrap();
    let JoinOutcome::Created(new) = join(&mut con, p, 1).await else { panic!("expected a new token") };
    assert_ne!(new.token_id, token);
    assert_eq!(new.status, "QUEUED");
    // Idempotent again for the new token
    let JoinOutcome::Existing(again) = join(&mut con, p, 1).await else { panic!("expected the new token") };
    assert_eq!(again.token_id, new.token_id);
    cleanup(&mut con, p).await;
}

#[tokio::test]
async fn rejoining_keeps_the_unit_limit() {
    let mut con = connect().await;
    let p = product(&mut con, 10, MAX_PER_USER).await;
    let _: () = con.set(format!("lobby_flushed:{p}"), 1).await.unwrap();
    admit(&mut con, p, 1).await;
    let ReserveOutcome::Held { reservation_id, .. } = reserve(&mut con, p, 1, 2).await else { panic!("not held") };
    // Paid, as the payment worker will do: the hold ends, the units stay counted
    let _: () = con.del(format!("resv:{reservation_id}")).await.unwrap();

    assert!(matches!(join(&mut con, p, 1).await, JoinOutcome::Created(_)));
    admit(&mut con, p, 1).await;
    assert_eq!(reserve(&mut con, p, 1, 3).await, ReserveOutcome::TooMany { held: 2 });
    assert!(matches!(reserve(&mut con, p, 1, 2).await, ReserveOutcome::Held { .. }));
    cleanup(&mut con, p).await;
}

/// A payment reported while its hold is live wins even if the payment worker
/// only gets to it after the hold would have expired: the hold is claimed on
/// receipt, so expiry cannot take it and the worker's own claim still wins.
/// A payment claimed only after the hold ended is refunded (the control).
#[tokio::test]
async fn payment_claimed_on_receipt_survives_a_worker_backlog() {
    let mut con = connect().await;
    let p = product(&mut con, 10, MAX_PER_USER).await;
    admit(&mut con, p, 1).await;
    admit(&mut con, p, 2).await;
    let mut sync = redis::Client::open(std::env::var("TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/15".into()))
        .unwrap()
        .get_connection()
        .unwrap();

    // Two holds of 300 ms, both paying (extended by 200 ms)
    let (paid, late) = (uuid::Uuid::new_v4().to_string(), uuid::Uuid::new_v4().to_string());
    for (resv, user, quantity) in [(&paid, 1, 1), (&late, 2, 2)] {
        let held = cache::reserve_units(&mut con, p, user, resv, quantity, 300).await.unwrap();
        assert!(matches!(held, ReserveOutcome::Held { .. }));
        assert!(matches!(cache::start_payment(&mut con, resv, user, 200).await.unwrap(), cache::StartPaymentOutcome::Paying { .. }));
    }
    assert_eq!(get_i64(&mut con, cache::stock_key(p)).await, 7);

    // The first payment's success event arrives in time and is claimed at once
    assert!(matches!(cache::claim_hold(&mut sync, &paid, "pi_paid").unwrap(), cache::ClaimOutcome::Won(_)));

    // The worker is behind: both holds pass their expiry, and cleanup runs
    tokio::time::sleep(std::time::Duration::from_millis(800)).await;
    cache::claim_expired(&mut sync, 500).unwrap();

    // The claimed hold kept its unit and is still there for the worker...
    assert!(con.exists::<_, bool>(format!("resv:{paid}")).await.unwrap());
    // ...whose claim, with the same intent, wins
    assert!(matches!(cache::claim_hold(&mut sync, &paid, "pi_paid").unwrap(), cache::ClaimOutcome::Won(_)));
    // It is kept far longer than the hold's own expiry plus grace
    let ttl: i64 = con.pttl(format!("resv:{paid}")).await.unwrap();
    assert!(ttl > 60 * 60 * 1000, "claimed hold TTL {ttl} ms");

    // The unclaimed one expired and was released: its units are back, and its payment is refunded
    assert_eq!(get_i64(&mut con, cache::stock_key(p)).await, 9);
    assert_eq!(cache::claim_hold(&mut sync, &late, "pi_late").unwrap(), cache::ClaimOutcome::Gone);
    // Another payment for the claimed hold does not win it
    assert_eq!(cache::claim_hold(&mut sync, &paid, "pi_other").unwrap(), cache::ClaimOutcome::Gone);

    // The worker commits and finalizes: the unit counts as sold, and stays out of stock
    assert!(cache::finalize_hold(&mut sync, &paid, true).unwrap());
    assert_eq!(get_i64(&mut con, cache::sold_key(p)).await, 1);
    assert_eq!(get_i64(&mut con, cache::stock_key(p)).await, 9);

    let _: () = con.del(&[format!("resv:{paid}"), format!("resv:{late}")]).await.unwrap();
    cleanup(&mut con, p).await;
}
