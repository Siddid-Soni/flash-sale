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
    models::{NewOrder, NewProduct, OrderState, OrderStatus},
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

    // One event, carrying everything the processor needs (Redis keeps no hold)
    let events = fake.events();
    assert_eq!(events.len(), 1);
    assert_eq!((events[0].ticket.as_str(), events[0].sale, events[0].user), (ticket.as_str(), sale, user.id));
    assert_eq!(events[0].idempotency_key, "attempt-1");
    assert!((Utc::now().timestamp_millis() - events[0].checkout_ms).abs() < 5_000);
    let live: Option<String> = env.redis.clone().get(cache::user_live_key(sale, user.id)).await.unwrap();
    assert_eq!(live.as_deref(), Some(ticket.as_str()));
    // The PENDING status is written to the read cache
    let cached = cache::read_status(&mut env.read_cache.clone(), &ticket).await.unwrap().unwrap();
    assert_eq!(cached.status, OrderStatus::Pending);

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

    // Stock came back (an expired order): the flag clears on the next refresh
    flags.refresh(&mut env.redis.clone()).await.unwrap();
    assert!(!flags.is_sold_out(sale));
    let resp = test::call_service(&app, checkout_request(sale, &late, Some("k")).to_request()).await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    cleanup(&env, sale).await;
}

fn orders_of(env: &common::Env, sale: i32) -> Vec<flash_sale::models::Order> {
    use flash_sale::models::Order;
    orders::table
        .filter(orders::product_id.eq(sale))
        .select(Order::as_select())
        .load(&mut env.pool.get().unwrap())
        .unwrap()
}

#[actix_web::test]
async fn a_failed_publish_records_a_failed_order_returns_the_unit_and_the_retry_succeeds() {
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
    assert!(!con.exists::<_, bool>(cache::user_live_key(sale, user.id)).await.unwrap());
    assert!(!con.exists::<_, bool>(cache::idem_key(sale, user.id, "k")).await.unwrap());

    // PostgreSQL has the ticket as a FAILED order, with the product's price
    let rows = orders_of(&env, sale);
    assert_eq!(rows.len(), 1);
    let failed = &rows[0];
    assert_eq!((failed.status, failed.user_id, failed.amount_cents), (OrderState::Failed, user.id, 59_900));
    assert_eq!((failed.payment_session_id.as_deref(), failed.checkout_url.as_deref(), failed.expires_at), (None, None, None));
    let failed_ticket = failed.queue_ticket_id.clone();
    assert_eq!(cache::ticket_sale(&failed_ticket), Some(sale));

    // The ticket the failed attempt used reads as FAILED (the Order Service wrote it to the read cache)
    let resp = test::call_service(&app, status_request(&failed_ticket, &user).to_request()).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(test::read_body_json::<Value, _>(resp).await, json!({ "status": "FAILED" }));

    // The retry with the same key makes a new ticket; its order can be stored
    // under that key although the FAILED row came first
    fake.set_failing(false);
    let resp = test::call_service(&app, checkout_request(sale, &user, Some("k")).to_request()).await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let body: Value = test::read_body_json(resp).await;
    assert_ne!(body["queue_ticket_id"], failed_ticket.as_str());
    assert_eq!(stock(&env, sale).await, 1);
    let events = fake.events();
    assert_eq!(events.len(), 1);
    let processed = common::sale::SaleFixture { env: common::env().await, sale }.processor().handle(&events[0]).await.unwrap();
    assert_eq!(processed, flash_sale::processor::Outcome::Linked);
    assert_eq!(orders_of(&env, sale).len(), 2);
    cleanup(&env, sale).await;
}

/// A queue whose publish fails after the message "reached Kafka": the
/// processor makes the order before the Order Service learns of the failure
struct ArrivesThenFails {
    pool: flash_sale::db::DbPool,
}

#[async_trait::async_trait]
impl flash_sale::queue::CheckoutQueue for ArrivesThenFails {
    async fn publish(
        &self,
        event: &flash_sale::queue::CheckoutEvent,
    ) -> flash_sale::queue::QueueResult<()> {
        let session = format!("session-{}", uuid::Uuid::new_v4().simple());
        diesel::insert_into(orders::table)
            .values(&NewOrder {
                user_id: event.user,
                product_id: event.sale,
                queue_ticket_id: &event.ticket,
                idempotency_key: &event.idempotency_key,
                status: OrderState::AwaitingPayment,
                amount_cents: 59_900,
                payment_session_id: Some(&session),
                checkout_url: Some(&format!("http://pay.test/fake-pay/{session}")),
                expires_at: Some(Utc::now() + Duration::minutes(5)),
            })
            .execute(&mut self.pool.get().unwrap())
            .unwrap();
        Err(flash_sale::queue::QueueError("timed out after the broker stored it".into()))
    }
}

#[actix_web::test]
async fn a_failed_publish_whose_message_made_an_order_is_a_successful_checkout() {
    let env = common::env().await;
    let queue: flash_sale::queue::SharedQueue = std::sync::Arc::new(ArrivesThenFails { pool: env.pool.clone() });
    let app = test_app!(env, queue);
    let sale = sale(&env, 2, 60).await;
    let user = guest(&env);

    let resp = test::call_service(&app, checkout_request(sale, &user, Some("k")).to_request()).await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let body: Value = test::read_body_json(resp).await;
    let ticket = body["queue_ticket_id"].as_str().unwrap().to_string();

    // The FAILED insert lost: the order is the processor's, and no compensation ran
    let rows = orders_of(&env, sale);
    assert_eq!(rows.len(), 1);
    assert_eq!((rows[0].queue_ticket_id.as_str(), rows[0].status), (ticket.as_str(), OrderState::AwaitingPayment));
    assert_eq!(stock(&env, sale).await, 1, "the unit stays reserved");
    let live: Option<String> = env.redis.clone().get(cache::user_live_key(sale, user.id)).await.unwrap();
    assert_eq!(live.as_deref(), Some(ticket.as_str()));
    let idem: Option<String> = env.redis.clone().get(cache::idem_key(sale, user.id, "k")).await.unwrap();
    assert_eq!(idem.as_deref(), Some(ticket.as_str()), "a retry gets this ticket");

    // The status reads PENDING until the processor writes its link (forward-only)
    let body: Value = test::call_and_read_body_json(&app, status_request(&ticket, &user).to_request()).await;
    assert_eq!(body["status"], "PENDING");
    cleanup(&env, sale).await;
}

#[actix_web::test]
async fn a_failed_publish_with_postgres_unable_to_record_it_keeps_the_unit_reserved() {
    let env = common::env().await;
    let (fake, queue) = common::fake_queue();
    let app = test_app!(env, queue);
    // Redis knows a sale PostgreSQL does not: the FAILED row cannot be written
    let sale = 2_000_000_000 - (uuid::Uuid::new_v4().as_u128() % 1_000_000) as i32;
    cache::init_sale(&mut env.redis.clone(), sale, 2, Utc::now() - Duration::seconds(60)).await.unwrap();
    let user = guest(&env);

    fake.set_failing(true);
    let resp = test::call_service(&app, checkout_request(sale, &user, Some("k")).to_request()).await;
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    // No PostgreSQL decision, no compensation (accepted: docs/FAILURE_CASES.md, case 1)
    assert_eq!(stock(&env, sale).await, 1);
    assert!(env.redis.clone().exists::<_, bool>(cache::user_live_key(sale, user.id)).await.unwrap());
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
    assert_eq!(test::read_body_json::<Value, _>(resp).await, json!({ "status": "PENDING" }));

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
async fn status_answers_from_the_read_cache_only() {
    let env = common::env().await;
    let app = test_app!(env);
    let sale = sale(&env, 5, 60).await;
    let (owner, stranger) = (guest(&env), guest(&env));
    let mut redis = env.redis.clone();

    let body: Value =
        test::call_and_read_body_json(&app, checkout_request(sale, &owner, Some("k")).to_request()).await;
    let ticket = body["queue_ticket_id"].as_str().unwrap().to_string();

    // An order that exists only in PostgreSQL is not looked up: the endpoint
    // never touches the database, so made-up tickets cannot reach it
    let _: () = redis.del(cache::status_key(&ticket)).await.unwrap();
    let expires = Utc::now() + Duration::minutes(5);
    let mut conn = env.pool.get().unwrap();
    let session = format!("xyz-{}", uuid::Uuid::new_v4().simple());
    let url = format!("http://localhost/fake-pay/{session}");
    diesel::insert_into(orders::table)
        .values(&NewOrder {
            user_id: owner.id,
            product_id: sale,
            queue_ticket_id: &ticket,
            idempotency_key: "k",
            status: OrderState::AwaitingPayment,
            amount_cents: 59_900,
            payment_session_id: Some(&session),
            checkout_url: Some(&url),
            expires_at: Some(expires),
        })
        .execute(&mut conn)
        .unwrap();
    let resp = test::call_service(&app, status_request(&ticket, &owner).to_request()).await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND, "no cache entry, no answer");

    // Once the entry is written, it is the answer (and only for its owner)
    let mut update = cache::StatusUpdate::new(OrderStatus::AwaitingPayment, owner.id);
    update.payment_url = Some(url.clone());
    update.expires_at = Some(expires.to_rfc3339_opts(chrono::SecondsFormat::Millis, true));
    cache::set_status(&mut env.read_cache.clone(), &ticket, &update).await.unwrap();
    let body: Value = test::call_and_read_body_json(&app, status_request(&ticket, &owner).to_request()).await;
    assert_eq!(body["status"], "AWAITING_PAYMENT");
    assert_eq!(body["payment_url"], url.as_str());
    let resp = test::call_service(&app, status_request(&ticket, &stranger).to_request()).await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    // PostgreSQL moving on changes nothing until the cache is written
    diesel::update(orders::table.filter(orders::queue_ticket_id.eq(&ticket)))
        .set(orders::status.eq(OrderState::Confirmed))
        .execute(&mut conn)
        .unwrap();
    let body: Value = test::call_and_read_body_json(&app, status_request(&ticket, &owner).to_request()).await;
    assert_eq!(body["status"], "AWAITING_PAYMENT");
    cleanup(&env, sale).await;
}

