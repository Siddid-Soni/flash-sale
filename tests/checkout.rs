//! Checkout and order status over HTTP, with a fake queue instead of Kafka.
//! Needs Postgres and Redis (see tests/common).

mod common;

use actix_web::{
    http::StatusCode,
    test::{self, TestRequest},
    web,
};
use chrono::{Duration, Utc};
use diesel::prelude::*;
use redis::AsyncCommands;
use serde_json::{json, Value};

use flash_sale::{
    actions,
    api::checkout::SoldOutFlags,
    auth,
    cache,
    models::{NewOrder, NewProduct, OrderStatus},
    schema::orders,
};

/// A sale in PostgreSQL and Redis; `opened_ago_secs` negative means it opens later
async fn sale(env: &common::Env, stock: i32, opened_ago_secs: i64) -> i32 {
    let product = actions::products::create_product(
        &mut env.pool.get().unwrap(),
        &NewProduct {
            name: "Smartphone",
            price_cents: 59_900,
            total_inventory: stock,
            sale_start_time: Utc::now() - Duration::seconds(opened_ago_secs),
        },
    )
    .unwrap();
    cache::init_sale(&mut env.redis.clone(), product.id, stock, product.sale_start_time).await.unwrap();
    product.id
}

struct Guest {
    id: i32,
    token: String,
}

fn guest(env: &common::Env) -> Guest {
    let user = actions::users::create_guest(&mut env.pool.get().unwrap()).unwrap();
    let (token, _) = auth::issue_token(user.id, &env.config.jwt_secret, 600).unwrap();
    Guest { id: user.id, token }
}

fn checkout_request(sale: i32, who: &Guest, key: Option<&str>) -> TestRequest {
    let req = TestRequest::post()
        .uri(&format!("/v1/sales/{sale}/checkout"))
        .insert_header(("Authorization", format!("Bearer {}", who.token)));
    match key {
        Some(key) => req.insert_header(("Idempotency-Key", key)),
        None => req,
    }
}

fn status_request(ticket: &str, who: &Guest) -> TestRequest {
    TestRequest::get()
        .uri(&format!("/v1/orders/status/{ticket}"))
        .insert_header(("Authorization", format!("Bearer {}", who.token)))
}

async fn stock(env: &common::Env, sale: i32) -> i64 {
    env.redis.clone().get::<_, Option<i64>>(cache::stock_key(sale)).await.unwrap().unwrap_or(0)
}

async fn cleanup(env: &common::Env, sale: i32) {
    let mut con = env.redis.clone();
    for pattern in [format!("*{{{sale}}}*"), format!("status:{sale}.*")] {
        let keys: Vec<String> = con.keys(pattern).await.unwrap();
        if !keys.is_empty() {
            let _: () = con.del(keys).await.unwrap();
        }
    }
}

#[actix_web::test]
async fn a_winner_gets_202_and_a_retry_gets_the_same_ticket_with_200() {
    let env = common::env().await;
    let (fake, queue) = common::fake_queue();
    let app = test_app!(env, queue);
    let sale = sale(&env, 5, 60).await;
    let user = guest(&env);

    let resp = test::call_service(&app, checkout_request(sale, &user, Some("attempt-1")).to_request()).await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let location = resp.headers().get("Location").unwrap().to_str().unwrap().to_string();
    let body: Value = test::read_body_json(resp).await;
    let ticket = body["queue_ticket_id"].as_str().unwrap().to_string();
    assert!(ticket.starts_with(&format!("{sale}.")));
    assert_eq!(cache::ticket_sale(&ticket), Some(sale));
    assert_eq!(body["status_url"], format!("/v1/orders/status/{ticket}"));
    assert_eq!(location, body["status_url"].as_str().unwrap());
    assert_eq!(stock(&env, sale).await, 4);

    // One event, keyed by the ticket's sale and user
    let events = fake.events();
    assert_eq!(events.len(), 1);
    assert_eq!((events[0].ticket.as_str(), events[0].sale, events[0].user), (ticket.as_str(), sale, user.id));
    // The hold remembers the key for the processor
    let hold = cache::read_hold(&mut env.redis.clone(), sale, &ticket).await.unwrap().unwrap();
    assert_eq!((hold.state.as_str(), hold.user, hold.idem.as_str()), ("PENDING", user.id, "attempt-1"));
    // The queue position is stored with the PENDING status
    let cached = cache::read_status(&mut env.read_cache.clone(), &ticket).await.unwrap().unwrap();
    assert_eq!((cached.status, cached.partition, cached.offset), (OrderStatus::Pending, Some(3), Some(0)));

    // The retry: 200, the same body, no second event, no second unit
    let retry = test::call_service(&app, checkout_request(sale, &user, Some("attempt-1")).to_request()).await;
    assert_eq!(retry.status(), StatusCode::OK);
    assert_eq!(test::read_body_json::<Value, _>(retry).await, body);
    assert_eq!(fake.events().len(), 1);
    assert_eq!(stock(&env, sale).await, 4);

    // Another key from the same user: 409
    let again = test::call_service(&app, checkout_request(sale, &user, Some("attempt-2")).to_request()).await;
    assert_eq!(again.status(), StatusCode::CONFLICT);
    assert_eq!(stock(&env, sale).await, 4);
    cleanup(&env, sale).await;
}

#[actix_web::test]
async fn refusals_have_their_status_codes() {
    let env = common::env().await;
    let app = test_app!(env);
    let open = sale(&env, 5, 60).await;
    let not_open = sale(&env, 5, -3600).await;
    let user = guest(&env);

    // Before the start
    let resp = test::call_service(&app, checkout_request(not_open, &user, Some("k")).to_request()).await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    assert_eq!(stock(&env, not_open).await, 5);

    // Idempotency-Key: required, 1-255 characters
    let long = "x".repeat(256);
    for key in [None, Some(""), Some(long.as_str())] {
        let resp = test::call_service(&app, checkout_request(open, &user, key).to_request()).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "key {:?}", key.map(str::len));
    }
    let longest = "x".repeat(255);
    let resp = test::call_service(&app, checkout_request(open, &user, Some(&longest)).to_request()).await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    // No token, a sale nobody created
    let resp = test::call_service(
        &app,
        TestRequest::post().uri(&format!("/v1/sales/{open}/checkout")).insert_header(("Idempotency-Key", "k")).to_request(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let other = guest(&env);
    let resp = test::call_service(&app, checkout_request(2_000_000_000, &other, Some("k")).to_request()).await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    cleanup(&env, open).await;
    cleanup(&env, not_open).await;
}

#[actix_web::test]
async fn sold_out_answers_410_and_the_flag_follows_stock() {
    let env = common::env().await;
    let flags = web::Data::new(SoldOutFlags::default());
    let app = test_app!(env, common::fake_queue().1, flags);
    let sale = sale(&env, 1, 60).await;
    let (winner, loser, late) = (guest(&env), guest(&env), guest(&env));

    let resp = test::call_service(&app, checkout_request(sale, &winner, Some("k")).to_request()).await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let resp = test::call_service(&app, checkout_request(sale, &loser, Some("k")).to_request()).await;
    assert_eq!(resp.status(), StatusCode::GONE);
    assert_eq!(stock(&env, sale).await, 0);
    // The winner's retry still reaches Redis while the flag is not set
    let resp = test::call_service(&app, checkout_request(sale, &winner, Some("k")).to_request()).await;
    assert_eq!(resp.status(), StatusCode::OK);

    // The refresher sets the flag: now the answer is given without Redis. Proof:
    // put a unit back behind its back and see 410 until the next refresh
    flags.refresh(&mut env.redis.clone()).await.unwrap();
    assert!(flags.is_sold_out(sale));
    let _: () = env.redis.clone().set(cache::stock_key(sale), 1).await.unwrap();
    let resp = test::call_service(&app, checkout_request(sale, &late, Some("k")).to_request()).await;
    assert_eq!(resp.status(), StatusCode::GONE);
    assert_eq!(stock(&env, sale).await, 1, "the flagged request never touched Redis");

    // Stock came back (a released hold): the flag clears on the next refresh
    flags.refresh(&mut env.redis.clone()).await.unwrap();
    assert!(!flags.is_sold_out(sale));
    let resp = test::call_service(&app, checkout_request(sale, &late, Some("k")).to_request()).await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    cleanup(&env, sale).await;
}

#[actix_web::test]
async fn a_failed_publish_returns_the_unit_and_the_retry_succeeds() {
    let env = common::env().await;
    let (fake, queue) = common::fake_queue();
    let app = test_app!(env, queue);
    let sale = sale(&env, 2, 60).await;
    let user = guest(&env);

    fake.set_failing(true);
    let resp = test::call_service(&app, checkout_request(sale, &user, Some("k")).to_request()).await;
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(resp.headers().get("Retry-After").unwrap(), "1");
    assert_eq!(stock(&env, sale).await, 2, "the unit came back at once");
    let mut con = env.redis.clone();
    assert_eq!(con.zcard::<_, i64>(cache::holds_pending_key(sale)).await.unwrap(), 0);
    assert!(!con.exists::<_, bool>(cache::user_live_key(sale, user.id)).await.unwrap());
    let held: Vec<String> = con.keys(format!("hold:{{{sale}}}:*")).await.unwrap();
    assert_eq!(held.len(), 1);
    let failed_ticket = held[0].rsplit(':').next().unwrap().to_string();
    assert_eq!(cache::read_hold(&mut con, sale, &failed_ticket).await.unwrap().unwrap().state, "FAILED");

    // The ticket the failed attempt used reads as FAILED (hold fallback)
    let resp = test::call_service(&app, status_request(&failed_ticket, &user).to_request()).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(test::read_body_json::<Value, _>(resp).await, json!({ "status": "FAILED" }));

    // The retry with the same key makes a new hold
    fake.set_failing(false);
    let resp = test::call_service(&app, checkout_request(sale, &user, Some("k")).to_request()).await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let body: Value = test::read_body_json(resp).await;
    assert_ne!(body["queue_ticket_id"], failed_ticket.as_str());
    assert_eq!(stock(&env, sale).await, 1);
    assert_eq!(fake.events().len(), 1);
    cleanup(&env, sale).await;
}

#[actix_web::test]
async fn status_is_for_the_owner_only() {
    let env = common::env().await;
    let app = test_app!(env);
    let sale = sale(&env, 5, 60).await;
    let (owner, stranger) = (guest(&env), guest(&env));

    let body: Value =
        test::call_and_read_body_json(&app, checkout_request(sale, &owner, Some("k")).to_request()).await;
    let ticket = body["queue_ticket_id"].as_str().unwrap();

    let resp = test::call_service(&app, status_request(ticket, &owner).to_request()).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers().get("Cache-Control").unwrap(), "no-store");
    // The fake queue stored offset 0: first in its partition
    assert_eq!(test::read_body_json::<Value, _>(resp).await, json!({ "status": "PENDING", "position": 1 }));

    let resp = test::call_service(&app, status_request(ticket, &stranger).to_request()).await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    let unknown = cache::new_ticket(sale);
    for missing in [unknown.as_str(), "garbage", &format!("{sale}.not-a-uuid")] {
        let resp = test::call_service(&app, status_request(missing, &owner).to_request()).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND, "{missing}");
    }
    let resp = test::call_service(&app, TestRequest::get().uri(&format!("/v1/orders/status/{ticket}")).to_request()).await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // A later state in the cache is what the poll returns
    let mut update = cache::StatusUpdate::new(OrderStatus::AwaitingPayment, owner.id);
    update.payment_url = Some("http://localhost/fake-pay/abc".into());
    update.expires_at = Some("2030-01-01T00:05:00.000Z".into());
    assert!(cache::set_status(&mut env.read_cache.clone(), ticket, &update).await.unwrap());
    let body: Value = test::call_and_read_body_json(&app, status_request(ticket, &owner).to_request()).await;
    assert_eq!(
        body,
        json!({ "status": "AWAITING_PAYMENT", "payment_url": "http://localhost/fake-pay/abc", "expires_at": "2030-01-01T00:05:00.000Z" })
    );
    cleanup(&env, sale).await;
}

#[actix_web::test]
async fn status_falls_back_to_the_hold_and_then_to_postgres() {
    let env = common::env().await;
    let app = test_app!(env);
    let sale = sale(&env, 5, 60).await;
    let (owner, stranger) = (guest(&env), guest(&env));
    let mut redis = env.redis.clone();

    let body: Value =
        test::call_and_read_body_json(&app, checkout_request(sale, &owner, Some("k")).to_request()).await;
    let ticket = body["queue_ticket_id"].as_str().unwrap().to_string();

    // The status write was lost: the hold says PENDING (no position known)
    let _: () = redis.del(cache::status_key(&ticket)).await.unwrap();
    let resp = test::call_service(&app, status_request(&ticket, &owner).to_request()).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(test::read_body_json::<Value, _>(resp).await, json!({ "status": "PENDING" }));
    let resp = test::call_service(&app, status_request(&ticket, &stranger).to_request()).await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    // Both Redis sources are gone: the order row answers, with the payment link
    let _: () = redis.del(cache::hold_key(sale, &ticket)).await.unwrap();
    let resp = test::call_service(&app, status_request(&ticket, &owner).to_request()).await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND, "no order yet");

    let mut conn = env.pool.get().unwrap();
    diesel::insert_into(orders::table)
        .values(&NewOrder {
            user_id: owner.id,
            product_id: sale,
            queue_ticket_id: &ticket,
            idempotency_key: "k",
            status: OrderStatus::Pending,
            amount_cents: 59_900,
        })
        .execute(&mut conn)
        .unwrap();
    let resp = test::call_service(&app, status_request(&ticket, &owner).to_request()).await;
    assert_eq!(test::read_body_json::<Value, _>(resp).await, json!({ "status": "PENDING" }));

    let expires = Utc::now() + Duration::minutes(5);
    diesel::update(orders::table.filter(orders::queue_ticket_id.eq(&ticket)))
        .set((
            orders::status.eq(OrderStatus::AwaitingPayment),
            orders::checkout_url.eq("http://localhost/fake-pay/xyz"),
            orders::expires_at.eq(expires),
        ))
        .execute(&mut conn)
        .unwrap();
    let body: Value = test::call_and_read_body_json(&app, status_request(&ticket, &owner).to_request()).await;
    assert_eq!(body["status"], "AWAITING_PAYMENT");
    assert_eq!(body["payment_url"], "http://localhost/fake-pay/xyz");
    assert_eq!(
        body["expires_at"],
        expires.to_rfc3339_opts(chrono::SecondsFormat::Millis, true).as_str()
    );
    let resp = test::call_service(&app, status_request(&ticket, &stranger).to_request()).await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    // An AWAITING_PAYMENT hold with no order row yet still answers
    let _: () = redis
        .hset_multiple(cache::hold_key(sale, &ticket), &[("state", "AWAITING_PAYMENT"), ("user", &owner.id.to_string())])
        .await
        .unwrap();
    diesel::delete(orders::table.filter(orders::queue_ticket_id.eq(&ticket))).execute(&mut conn).unwrap();
    let body: Value = test::call_and_read_body_json(&app, status_request(&ticket, &owner).to_request()).await;
    assert_eq!(body, json!({ "status": "AWAITING_PAYMENT" }));
    cleanup(&env, sale).await;
}

#[actix_web::test]
async fn the_position_follows_processor_progress() {
    let mut env = common::env().await;
    // A topic of its own, so the progress key is not shared with other tests
    env.config.checkout_topic = format!("test.position.{}", uuid::Uuid::new_v4().simple());
    let topic = env.config.checkout_topic.clone();
    let (_fake, queue) = common::fake_queue();
    let app = test_app!(env, queue);
    let sale = sale(&env, 5, 60).await;
    let owner = guest(&env);

    // The fake queue stores partition 3, offsets 0, 1, ...; make this offset 7
    let ticket = {
        let body: Value = test::call_and_read_body_json(&app, checkout_request(sale, &owner, Some("k")).to_request()).await;
        body["queue_ticket_id"].as_str().unwrap().to_string()
    };
    let mut status = cache::StatusUpdate::new(OrderStatus::Pending, owner.id);
    status.partition = Some(3);
    status.offset = Some(7);
    let mut read_cache = env.read_cache.clone();
    cache::set_status(&mut read_cache, &ticket, &status).await.unwrap();

    macro_rules! position {
        () => {{
            let resp = test::call_service(&app, status_request(&ticket, &owner).to_request()).await;
            assert_eq!(resp.status(), StatusCode::OK);
            test::read_body_json::<Value, _>(resp).await["position"].clone()
        }};
    }

    // No progress recorded: offset + 1
    assert_eq!(position!(), json!(8));
    cache::raise_to(&mut read_cache.0, &cache::progress_key(&topic, 3), 4).await.unwrap();
    // Cached for ~500 ms per replica: still the old answer
    assert_eq!(position!(), json!(8));
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;
    assert_eq!(position!(), json!(3));
    // Progress past the ticket (stale reads): clamped to 0, not negative
    cache::raise_to(&mut read_cache.0, &cache::progress_key(&topic, 3), 9).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;
    assert_eq!(position!(), json!(0));
    let _: () = read_cache.0.del(cache::progress_key(&topic, 3)).await.unwrap();
    cleanup(&env, sale).await;
}
