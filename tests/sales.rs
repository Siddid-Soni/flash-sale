//! The site service's sale routes: admin sale creation, the public sale
//! details, the list and search; and that the Order Service no longer serves
//! them. Needs Postgres and Redis (see tests/common).

mod common;

use actix_web::{http::StatusCode, test::{self, TestRequest}};
use chrono::{Duration, Utc};
use diesel::prelude::*;
use serde_json::{json, Value};

use flash_sale::{cache, schema::products};

fn sale_body() -> Value {
    json!({
        "name": "Smartphone",
        "price_cents": 59_900,
        "total_inventory": 500,
        "sale_start_time": "2025-01-01T20:00:00Z",
    })
}

/// A start time one second ago, whole seconds (as the API prints it): a sale
/// that has opened and is newer than any other test's, so it is among the 50
/// latest the list returns
fn just_now() -> String {
    (Utc::now() - Duration::seconds(1)).format("%Y-%m-%dT%H:%M:%SZ").to_string()
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
    let app = site_app!(env);

    // Other tests create sales too, so look for this one by name
    let body = json!({ "name": "should-not-exist", "price_cents": 100, "total_inventory": 1,
                       "sale_start_time": "2025-01-01T00:00:00Z" });

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
    let app = site_app!(env);

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
    assert_eq!(redis_get(&env, format!("sold:{{{id}}}")).await, None, "Redis does not count sales");
    assert_eq!(redis_get(&env, cache::sale_start_key(id)).await.as_deref(), Some("1735761600000"));
}

#[actix_web::test]
async fn invalid_sales_are_rejected() {
    let env = common::env().await;
    let app = site_app!(env);
    let cases = [
        json!({ "name": "  ", "price_cents": 100, "total_inventory": 5, "sale_start_time": "2025-01-01T00:00:00Z" }),
        json!({ "name": "x", "price_cents": 0, "total_inventory": 5, "sale_start_time": "2025-01-01T00:00:00Z" }),
        json!({ "name": "x", "price_cents": 100, "total_inventory": 0, "sale_start_time": "2025-01-01T00:00:00Z" }),
        json!({ "name": "x", "price_cents": 100, "total_inventory": -3, "sale_start_time": "2025-01-01T00:00:00Z" }),
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
    let app = site_app!(env);
    let created: Value = test::call_and_read_body_json(
        &app,
        create_request(Some(common::ADMIN_TOKEN), &sale_body()).to_request(),
    )
    .await;
    let id = created["id"].as_i64().unwrap();

    let resp = test::call_service(&app, TestRequest::get().uri(&format!("/v1/sales/{id}")).to_request()).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers().get("Cache-Control").unwrap(), "public, max-age=1, stale-while-revalidate=5");
    let body: Value = test::read_body_json(resp).await;
    assert_eq!(
        body,
        json!({
            "id": id,
            "name": "Smartphone",
            "price_cents": 59_900,
            "sale_start_time": "2025-01-01T20:00:00Z",
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

#[actix_web::test]
async fn sale_list_has_the_details_newest_start_first() {
    let env = common::env().await;
    let app = site_app!(env);
    // Started a second ago, so it is among the 50 latest whatever other tests created
    let start = just_now();
    let mut body = sale_body();
    body["name"] = json!("list-me");
    body["sale_start_time"] = json!(start);
    let created: Value =
        test::call_and_read_body_json(&app, create_request(Some(common::ADMIN_TOKEN), &body).to_request()).await;
    let id = created["id"].as_i64().unwrap();
    redis::cmd("SET").arg(format!("stock:{{{id}}}")).arg(0).query_async::<()>(&mut env.redis.clone()).await.unwrap();

    let resp = test::call_service(&app, TestRequest::get().uri("/v1/sales").to_request()).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers().get("Cache-Control").unwrap(), "public, max-age=5, stale-while-revalidate=30");
    let list: Vec<Value> = test::read_body_json(resp).await;
    assert!(list.len() <= 50);
    let starts: Vec<&str> = list.iter().map(|s| s["sale_start_time"].as_str().unwrap()).collect();
    assert!(starts.windows(2).all(|w| w[0] >= w[1]), "newest start first: {starts:?}");
    let mine = list.iter().find(|s| s["id"] == id).expect("the new sale is listed");
    assert_eq!(
        *mine,
        json!({
            "id": id,
            "name": "list-me",
            "price_cents": 59_900,
            "sale_start_time": start,
            "total_inventory": 500,
            "sold_out": true,
        })
    );
}

/// A name no other test uses, lowercase so a search for it needs no care
fn unique_token() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

fn percent_encode(text: &str) -> String {
    text.bytes()
        .map(|b| if b.is_ascii_alphanumeric() { (b as char).to_string() } else { format!("%{b:02X}") })
        .collect()
}

/// Creates a sale through the admin route; its id. (Macros, because the app's
/// service type cannot be named.)
macro_rules! create_named {
    ($app:expr, $name:expr, $start:expr) => {{
        let mut body = sale_body();
        body["name"] = json!($name);
        body["sale_start_time"] = json!($start);
        let created: Value =
            test::call_and_read_body_json(&$app, create_request(Some(common::ADMIN_TOKEN), &body).to_request()).await;
        created["id"].as_i64().unwrap()
    }};
}

/// `GET /v1/sales/search?q=<q>` (percent-encoded here): status, `Cache-Control` and the parsed body
macro_rules! search {
    ($app:expr, $q:expr) => {{
        let uri = format!("/v1/sales/search?q={}", percent_encode(&$q));
        let resp = test::call_service(&$app, TestRequest::get().uri(&uri).to_request()).await;
        let status = resp.status();
        let cache_control = resp.headers().get("Cache-Control").map(|v| v.to_str().unwrap().to_string());
        let body = test::read_body(resp).await;
        (status, cache_control, serde_json::from_slice::<Vec<Value>>(&body).unwrap_or_default())
    }};
}

fn ids(sales: &[Value]) -> Vec<i64> {
    sales.iter().map(|s| s["id"].as_i64().unwrap()).collect()
}

#[actix_web::test]
async fn search_matches_names_in_any_case_with_the_list_shape() {
    let env = common::env().await;
    let app = site_app!(env);
    let token = unique_token();
    let old = create_named!(app, &format!("Blue PHONE {token}"), "2020-01-01T00:00:00Z");
    let new = create_named!(app, &format!("Red phone {token}"), "2020-06-01T00:00:00Z");
    create_named!(app, &format!("Laptop {token}"), "2020-03-01T00:00:00Z");
    redis::cmd("SET").arg(format!("stock:{{{old}}}")).arg(0).query_async::<()>(&mut env.redis.clone()).await.unwrap();

    // Case does not matter on either side; surrounding blanks are trimmed
    let (status, cache_control, found) = search!(app, &format!("  PhOnE {token} "));
    assert_eq!(status, StatusCode::OK);
    assert_eq!(cache_control.as_deref(), Some("public, max-age=5, stale-while-revalidate=30"));
    assert_eq!(ids(&found), vec![new, old], "newest start first, laptop excluded");
    assert_eq!(found[1]["name"], format!("Blue PHONE {token}"));
    assert_eq!(found[1]["sold_out"], true);
    assert_eq!(found[0]["sold_out"], false);
    assert_eq!(found[0]["price_cents"], 59_900);
    assert_eq!(found[0]["total_inventory"], 500);
    assert_eq!(found[0]["sale_start_time"], "2020-06-01T00:00:00Z");

    // A part of the name, and nothing found
    assert!(!ids(&search!(app, "lap").2).contains(&old));
    assert_eq!(search!(app, &format!("{token}-nothing")).2, Vec::<Value>::new());
}

#[actix_web::test]
async fn search_treats_wildcards_and_backslashes_as_text() {
    let env = common::env().await;
    let app = site_app!(env);
    let token = unique_token();
    let percent = create_named!(app, &format!("100% cotton {token}"), "2021-01-01T00:00:00Z");
    let decoy_percent = create_named!(app, &format!("100x cotton {token}"), "2021-01-02T00:00:00Z");
    let underscore = create_named!(app, &format!("a_b {token}"), "2021-01-03T00:00:00Z");
    let decoy_underscore = create_named!(app, &format!("axb {token}"), "2021-01-04T00:00:00Z");
    let backslash = create_named!(app, &format!("c\\d {token}"), "2021-01-05T00:00:00Z");
    let decoy_backslash = create_named!(app, &format!("cd {token}"), "2021-01-06T00:00:00Z");

    assert_eq!(ids(&search!(app, &format!("100% cotton {token}")).2), vec![percent]);
    assert_eq!(ids(&search!(app, &format!("a_b {token}")).2), vec![underscore]);
    assert_eq!(ids(&search!(app, &format!("c\\d {token}")).2), vec![backslash]);
    // A bare wildcard is a literal too, not "everything"
    let (_, _, bare) = search!(app, "%");
    assert!(bare.iter().all(|s| s["name"].as_str().unwrap().contains('%')));
    assert!(!ids(&bare).contains(&decoy_percent));
    let (_, _, bare) = search!(app, "_");
    assert!(bare.iter().all(|s| s["name"].as_str().unwrap().contains('_')));
    assert!(!ids(&bare).contains(&decoy_underscore) && !ids(&bare).contains(&decoy_backslash));
}

#[actix_web::test]
async fn a_sale_with_every_unit_paid_for_is_not_listed() {
    let env = common::env().await;
    let app = site_app!(env);
    let token = unique_token();
    let over = create_named!(app, &format!("over {token}"), just_now());
    let reserved = create_named!(app, &format!("reserved {token}"), just_now());
    let mut conn = env.pool.get().unwrap();
    diesel::update(products::table.find(over as i32))
        .set(products::sold.eq(products::total_inventory))
        .execute(&mut conn)
        .unwrap();
    // Stock at 0 with units only reserved: still selling (shown sold out)
    redis::cmd("SET").arg(format!("stock:{{{reserved}}}")).arg(0).query_async::<()>(&mut env.redis.clone()).await.unwrap();

    let list: Vec<Value> = test::call_and_read_body_json(&app, TestRequest::get().uri("/v1/sales").to_request()).await;
    assert!(!ids(&list).contains(&over), "a sale that is over is not listed");
    assert!(ids(&list).contains(&reserved));
    let (_, _, found) = search!(app, &token);
    assert_eq!(ids(&found), vec![reserved]);
}

#[actix_web::test]
async fn an_empty_search_answers_like_the_list() {
    let env = common::env().await;
    let app = site_app!(env);
    let id = create_named!(app, "empty-search", just_now());

    for uri in ["/v1/sales/search", "/v1/sales/search?q=", "/v1/sales/search?q=%20%20%09", "/v1/sales/search?x=1"] {
        let resp = test::call_service(&app, TestRequest::get().uri(uri).to_request()).await;
        assert_eq!(resp.status(), StatusCode::OK, "{uri}");
        assert_eq!(resp.headers().get("Cache-Control").unwrap(), "public, max-age=5, stale-while-revalidate=30");
        let list: Vec<Value> = test::read_body_json(resp).await;
        assert!(list.len() <= 50);
        assert!(ids(&list).contains(&id), "{uri} lists the latest sale");
        let starts: Vec<&str> = list.iter().map(|s| s["sale_start_time"].as_str().unwrap()).collect();
        assert!(starts.windows(2).all(|w| w[0] >= w[1]), "newest start first: {starts:?}");
    }
}

#[actix_web::test]
async fn search_returns_at_most_fifty_and_cuts_long_queries() {
    let env = common::env().await;
    let app = site_app!(env);
    let token = unique_token();
    let mut created = Vec::new();
    for i in 0..51 {
        created.push(create_named!(app, &format!("many {token} {i}"), &format!("2022-01-01T00:{i:02}:00Z")));
    }
    let (_, _, found) = search!(app, &format!("many {token}"));
    assert_eq!(found.len(), 50);
    assert_eq!(ids(&found), created[1..].iter().rev().copied().collect::<Vec<_>>(), "the oldest of 51 is left out");

    // Only the first 64 characters of the text are used, so a longer one still finds the name
    let long = create_named!(app, &format!("{}b", "a".repeat(64)), "2023-01-01T00:00:00Z");
    assert!(ids(&search!(app, &"a".repeat(80)).2).contains(&long));
}

#[actix_web::test]
async fn the_order_service_no_longer_serves_the_site_routes() {
    let env = common::env().await;
    let orders = test_app!(env);
    let site = site_app!(env);
    let id = create_named!(site, "moved", just_now());

    for (method, uri) in [
        ("POST", "/v1/session".to_string()),
        ("GET", "/v1/sales".to_string()),
        ("GET", format!("/v1/sales/{id}")),
        ("GET", "/v1/sales/search?q=moved".to_string()),
        ("POST", "/v1/admin/sales".to_string()),
    ] {
        let req = if method == "POST" { TestRequest::post() } else { TestRequest::get() };
        let resp = test::call_service(&orders, req.uri(&uri).insert_header(("X-Admin-Token", common::ADMIN_TOKEN)).to_request()).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND, "{method} {uri} on the Order Service");
    }

    // And the site serves neither checkout nor status
    for (method, uri) in [("POST", format!("/v1/sales/{id}/checkout")), ("GET", "/v1/orders/status/1.x".to_string())] {
        let req = if method == "POST" { TestRequest::post() } else { TestRequest::get() };
        let resp = test::call_service(&site, req.uri(&uri).to_request()).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND, "{method} {uri} on the site service");
    }
}
