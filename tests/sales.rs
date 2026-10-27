//! Admin sale creation and the public sale details. Needs Postgres and Redis
//! (see tests/common).

mod common;

use actix_web::{http::StatusCode, test::{self, TestRequest}};
use diesel::prelude::*;
use serde_json::{json, Value};

use flash_sale::{cache, schema::products};

fn sale_body() -> Value {
    json!({
        "name": "Smartphone",
        "price_cents": 59_900,
        "total_inventory": 500,
        "sale_start_time": "2030-01-01T20:00:00Z",
    })
}

fn create_request(token: Option<&str>, body: &Value) -> TestRequest {
    let req = TestRequest::post().uri("/v1/admin/sales").set_json(body);
    match token {
        Some(token) => req.insert_header(("X-Admin-Token", token)),
        None => req,
    }
}

async fn redis_get(env: &common::Env, key: String) -> Option<String> {
    redis::cmd("GET").arg(key).query_async(&mut env.redis.clone()).await.unwrap()
}

#[actix_web::test]
async fn admin_routes_need_the_admin_token() {
    let env = common::env().await;
    let app = test_app!(env);

    // Other tests create sales too, so look for this one by name
    let body = json!({ "name": "should-not-exist", "price_cents": 100, "total_inventory": 1,
                       "sale_start_time": "2030-01-01T00:00:00Z" });

    for token in [None, Some("wrong"), Some(""), Some("test-admin-token-and-more"), Some("test-admin-toke")] {
        let resp = test::call_service(&app, create_request(token, &body).to_request()).await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN, "token {token:?}");
    }
    let created: i64 = products::table
        .filter(products::name.eq("should-not-exist"))
        .count()
        .get_result(&mut env.pool.get().unwrap())
        .unwrap();
    assert_eq!(created, 0);
}

#[actix_web::test]
async fn creating_a_sale_stores_the_product_and_loads_redis() {
    let env = common::env().await;
    let app = test_app!(env);

    let resp = test::call_service(&app, create_request(Some(common::ADMIN_TOKEN), &sale_body()).to_request()).await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    let product: Value = test::read_body_json(resp).await;
    let id = product["id"].as_i64().unwrap() as i32;
    assert_eq!(product["name"], "Smartphone");
    assert_eq!(product["price_cents"], 59_900);
    assert_eq!(product["total_inventory"], 500);
    assert_eq!(product["sold"], 0);

    // The row
    let (name, total, sold): (String, i32, i32) = products::table
        .find(id)
        .select((products::name, products::total_inventory, products::sold))
        .first(&mut env.pool.get().unwrap())
        .unwrap();
    assert_eq!((name.as_str(), total, sold), ("Smartphone", 500, 0));

    // The keys, with the literal hash tag, and the start time in epoch ms
    assert_eq!(redis_get(&env, format!("stock:{{{id}}}")).await.as_deref(), Some("500"));
    assert_eq!(redis_get(&env, format!("sold:{{{id}}}")).await.as_deref(), Some("0"));
    assert_eq!(redis_get(&env, cache::sale_start_key(id)).await.as_deref(), Some("1893528000000"));
}

#[actix_web::test]
async fn invalid_sales_are_rejected() {
    let env = common::env().await;
    let app = test_app!(env);
    let cases = [
        json!({ "name": "  ", "price_cents": 100, "total_inventory": 5, "sale_start_time": "2030-01-01T00:00:00Z" }),
        json!({ "name": "x", "price_cents": 0, "total_inventory": 5, "sale_start_time": "2030-01-01T00:00:00Z" }),
        json!({ "name": "x", "price_cents": 100, "total_inventory": 0, "sale_start_time": "2030-01-01T00:00:00Z" }),
        json!({ "name": "x", "price_cents": 100, "total_inventory": -3, "sale_start_time": "2030-01-01T00:00:00Z" }),
        json!({ "name": "x", "price_cents": 100, "total_inventory": 5, "sale_start_time": "not a time" }),
        json!({ "name": "x", "price_cents": 100 }),
    ];
    for body in cases {
        let resp = test::call_service(&app, create_request(Some(common::ADMIN_TOKEN), &body).to_request()).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "{body}");
    }
}

#[actix_web::test]
async fn sale_details_body_headers_and_not_found() {
    let env = common::env().await;
    let app = test_app!(env);
    let created: Value = test::call_and_read_body_json(
        &app,
        create_request(Some(common::ADMIN_TOKEN), &sale_body()).to_request(),
    )
    .await;
    let id = created["id"].as_i64().unwrap();

    let resp = test::call_service(&app, TestRequest::get().uri(&format!("/v1/sales/{id}")).to_request()).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers().get("Cache-Control").unwrap(), "public, max-age=1");
    let body: Value = test::read_body_json(resp).await;
    assert_eq!(
        body,
        json!({
            "id": id,
            "name": "Smartphone",
            "price_cents": 59_900,
            "sale_start_time": "2030-01-01T20:00:00Z",
            "total_inventory": 500,
            "sold_out": false,
        })
    );

    // sold_out follows stock in Redis at once (it is not part of the product cache)
    redis::cmd("SET").arg(format!("stock:{{{id}}}")).arg(0).query_async::<()>(&mut env.redis.clone()).await.unwrap();
    let body: Value =
        test::call_and_read_body_json(&app, TestRequest::get().uri(&format!("/v1/sales/{id}")).to_request()).await;
    assert_eq!(body["sold_out"], true);

    let missing = test::call_service(&app, TestRequest::get().uri("/v1/sales/2000000000").to_request()).await;
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    let not_a_number = test::call_service(&app, TestRequest::get().uri("/v1/sales/abc").to_request()).await;
    assert_eq!(not_a_number.status(), StatusCode::NOT_FOUND);
}
