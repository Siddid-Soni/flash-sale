//! The fake payment provider against a real Redis (database 15): its state
//! (`FakeProvider`), its HTTP API, and the `HttpProvider` client the order
//! processor and the expiration worker use against a real server.

mod common;

use actix_web::{http::StatusCode, test};
use chrono::{Duration, SecondsFormat, Utc};
use serde_json::{json, Value};

use flash_sale::payment::{
    client::HttpProvider, fake_session_id, FakeProvider, PaymentProvider, RefundOutcome, Settle, Settled,
};

async fn provider() -> FakeProvider {
    let env = common::env().await;
    FakeProvider::new(env.redis.clone(), "http://pay.test/".into(), "secret".into())
}

fn ticket() -> String {
    format!("1.{}", uuid::Uuid::new_v4())
}

// ---- The provider's state ----

#[tokio::test]
async fn a_session_has_a_url_and_is_stored_open() {
    let provider = provider().await;
    let ticket = ticket();
    let expires_at = Utc::now() + Duration::minutes(5);
    let session = provider.create_session(&ticket, 59_900, expires_at, &ticket).await.unwrap();

    assert_eq!(session.id, fake_session_id(&ticket));
    assert_eq!(session.url, format!("http://pay.test/fake-pay/{}", session.id));
    let stored = provider.session(&session.id).await.unwrap().unwrap();
    assert_eq!((stored.ticket.as_str(), stored.amount_cents, stored.state.as_str()), (ticket.as_str(), 59_900, "OPEN"));
    assert_eq!(stored.expires_at.timestamp_millis(), expires_at.timestamp_millis());
    assert!(provider.session("fake_cs_unknown").await.unwrap().is_none());
}

/// A redelivered Kafka message must not create a second session
#[tokio::test]
async fn creation_is_idempotent_by_ticket() {
    let provider = provider().await;
    let ticket = ticket();
    let expires_at = Utc::now() + Duration::minutes(5);

    let first = provider.create_session(&ticket, 100, expires_at, &ticket).await.unwrap();
    let second = provider.create_session(&ticket, 100, expires_at + Duration::minutes(1), &ticket).await.unwrap();
    assert_eq!(first, second);
    // Even under a different idempotency key the ticket keeps its one session
    let third = provider.create_session(&ticket, 100, expires_at, "another-key").await.unwrap();
    assert_eq!(third, first);
    assert!(provider.session(&fake_session_id("another-key")).await.unwrap().is_none(), "no second session exists");
    assert_eq!(provider.session_id_for_ticket(&ticket).await.unwrap(), Some(first.id.clone()));

    // The first call's data stands, and an expired session stays the same session
    assert_eq!(provider.session(&first.id).await.unwrap().unwrap().expires_at.timestamp_millis(), expires_at.timestamp_millis());
    provider.expire_session(&first.id).await.unwrap();
    assert_eq!(provider.create_session(&ticket, 100, expires_at, &ticket).await.unwrap(), first);
}

#[tokio::test]
async fn expire_is_idempotent_and_refund_needs_a_payment() {
    let provider = provider().await;
    let ticket = ticket();
    let session = provider.create_session(&ticket, 100, Utc::now() + Duration::minutes(5), &ticket).await.unwrap();

    provider.expire_session(&session.id).await.unwrap();
    provider.expire_session(&session.id).await.unwrap();
    provider.expire_session("fake_cs_unknown").await.unwrap();
    assert_eq!(provider.session(&session.id).await.unwrap().unwrap().state, "EXPIRED");

    // Nothing was paid, so there is nothing to refund
    assert_eq!(provider.refund(&session.id).await.unwrap(), RefundOutcome::NotPaid("EXPIRED".into()));
    assert_eq!(provider.refund("fake_cs_unknown").await.unwrap(), RefundOutcome::NotPaid("missing".into()));
}

// ---- The HTTP API, without the network ----

fn create_request(ticket: &str, amount: i64, key: Option<&str>) -> test::TestRequest {
    let req = test::TestRequest::post().uri("/v1/checkout/sessions").set_json(json!({
        "ticket": ticket,
        "amount_cents": amount,
        "expires_at": (Utc::now() + Duration::minutes(5)).to_rfc3339_opts(SecondsFormat::Millis, true),
    }));
    match key {
        Some(key) => req.insert_header(("Idempotency-Key", key)),
        None => req,
    }
}

#[actix_web::test]
async fn the_api_creates_a_session_once_per_ticket() {
    let env = common::env().await;
    let app = provider_app!(env);
    let ticket = ticket();

    let first: Value = test::call_and_read_body_json(&app, create_request(&ticket, 59_900, Some(&ticket)).to_request()).await;
    assert_eq!(first["id"], fake_session_id(&ticket).as_str());
    assert_eq!(first["url"], format!("http://pay.test/fake-pay/{}", first["id"].as_str().unwrap()).as_str());
    assert_eq!(first["state"], "OPEN");

    // The same key, and the same ticket under another key: the same session
    let again: Value = test::call_and_read_body_json(&app, create_request(&ticket, 59_900, Some(&ticket)).to_request()).await;
    assert_eq!(again, first);
    let other_key: Value = test::call_and_read_body_json(&app, create_request(&ticket, 59_900, Some("another")).to_request()).await;
    assert_eq!(other_key["id"], first["id"]);
    assert!(env.provider.session(&fake_session_id("another")).await.unwrap().is_none());
}

#[actix_web::test]
async fn the_api_refuses_bad_creates() {
    let env = common::env().await;
    let app = provider_app!(env);
    let ticket = ticket();

    // No Idempotency-Key, an empty one, a negative amount
    for request in [
        create_request(&ticket, 100, None),
        create_request(&ticket, 100, Some("")),
        create_request(&ticket, -1, Some(&ticket)),
    ] {
        let resp = test::call_service(&app, request.to_request()).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body: Value = test::read_body_json(resp).await;
        assert!(body["message"].is_string());
    }
    // A body that is not a session, and a deadline that is not a time
    for body in [json!({"hello": "world"}), json!({"ticket": "t", "amount_cents": 1, "expires_at": "tomorrow"})] {
        let req = test::TestRequest::post().uri("/v1/checkout/sessions").insert_header(("Idempotency-Key", "k")).set_json(body);
        let resp = test::call_service(&app, req.to_request()).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body: Value = test::read_body_json(resp).await;
        assert!(body["message"].is_string());
    }
    assert!(env.provider.session_id_for_ticket(&ticket).await.unwrap().is_none(), "nothing was created");
}

#[actix_web::test]
async fn the_api_expires_and_refunds_idempotently() {
    let env = common::env().await;
    let app = provider_app!(env);
    let (open, paid) = (ticket(), ticket());
    let expires_at = Utc::now() + Duration::minutes(5);
    let open = env.provider.create_session(&open, 100, expires_at, &open).await.unwrap();
    let paid_ticket = paid.clone();
    let paid = env.provider.create_session(&paid_ticket, 100, expires_at, &paid_ticket).await.unwrap();

    // Expire: any number of times, also for sessions that are unknown or paid
    for _ in 0..2 {
        let req = test::TestRequest::post().uri(&format!("/v1/checkout/sessions/{}/expire", open.id)).to_request();
        let body: Value = test::call_and_read_body_json(&app, req).await;
        assert_eq!(body["state"], "EXPIRED");
    }
    let req = test::TestRequest::post().uri("/v1/checkout/sessions/fake_cs_unknown/expire").to_request();
    assert_eq!(test::call_service(&app, req).await.status(), StatusCode::OK);

    // Refund: nothing to give back for an expired or unknown session
    for session_id in [open.id.as_str(), "fake_cs_unknown"] {
        let req = test::TestRequest::post().uri("/v1/refunds").set_json(json!({ "session_id": session_id })).to_request();
        assert_eq!(test::call_service(&app, req).await.status(), StatusCode::CONFLICT, "{session_id}");
    }
    // A paid session is refunded, and a repeat is still 200
    let session = env.provider.session(&paid.id).await.unwrap().unwrap();
    assert_eq!(env.provider.settle(&session, Settle::Pay).await.unwrap(), Settled::Moved);
    let req = || test::TestRequest::post().uri("/v1/refunds").set_json(json!({ "session_id": paid.id })).to_request();
    for _ in 0..2 {
        let body: Value = test::call_and_read_body_json(&app, req()).await;
        assert_eq!(body["state"], "REFUNDED");
    }
    assert_eq!(env.provider.session(&paid.id).await.unwrap().unwrap().state, "REFUNDED");
    // A refunded session cannot be paid or expired back
    let req = test::TestRequest::post().uri(&format!("/v1/checkout/sessions/{}/expire", paid.id)).to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    assert_eq!(body["state"], "REFUNDED");
}

// ---- The client against a real server ----

#[tokio::test]
async fn the_client_creates_expires_and_refunds_over_http() {
    let env = common::env().await;
    let client = env.shared_provider();
    let ticket = ticket();
    let expires_at = Utc::now() + Duration::minutes(5);

    let session = client.create_session(&ticket, 59_900, expires_at, &ticket).await.unwrap();
    assert_eq!(session.id, fake_session_id(&ticket));
    assert_eq!(session.url, format!("{}/fake-pay/{}", common::PAGE_BASE_URL, session.id));
    assert_eq!(client.create_session(&ticket, 59_900, expires_at, &ticket).await.unwrap(), session, "idempotent");
    let stored = env.provider.session(&session.id).await.unwrap().unwrap();
    assert_eq!((stored.ticket.as_str(), stored.amount_cents, stored.state.as_str()), (ticket.as_str(), 59_900, "OPEN"));
    assert_eq!(stored.expires_at.timestamp_millis(), expires_at.timestamp_millis());

    // Not paid: no refund, and the error says why
    let error = client.refund(&session.id).await.unwrap_err();
    assert!(error.0.contains("409"), "{error}");
    env.provider.settle(&stored, Settle::Pay).await.unwrap();
    client.refund(&session.id).await.unwrap();
    client.refund(&session.id).await.unwrap();
    assert_eq!(env.provider.session(&session.id).await.unwrap().unwrap().state, "REFUNDED");

    let other = format!("1.{}", uuid::Uuid::new_v4());
    let session = client.create_session(&other, 100, expires_at, &other).await.unwrap();
    client.expire_session(&session.id).await.unwrap();
    client.expire_session(&session.id).await.unwrap();
    client.expire_session("fake_cs_unknown").await.unwrap();
    assert_eq!(env.provider.session(&session.id).await.unwrap().unwrap().state, "EXPIRED");
}

/// The client is shared by many tasks of a multi-thread runtime
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_client_serves_concurrent_callers() {
    let env = common::env().await;
    let client = env.shared_provider();
    let tickets: Vec<String> = (0..40).map(|_| ticket()).collect();
    let tasks: Vec<_> = tickets
        .iter()
        .cloned()
        .map(|t| {
            let client = client.clone();
            tokio::spawn(async move { client.create_session(&t, 100, Utc::now() + Duration::minutes(5), &t).await.unwrap() })
        })
        .collect();
    for (task, ticket) in tasks.into_iter().zip(&tickets) {
        assert_eq!(task.await.unwrap().id, fake_session_id(ticket));
    }
}

#[tokio::test]
async fn the_client_reports_a_provider_that_is_not_there() {
    // Nothing listens on port 9 (discard) here
    let client = HttpProvider::new("http://127.0.0.1:9".into(), "secret".into());
    let error = client.create_session("1.x", 100, Utc::now(), "k").await.unwrap_err();
    assert!(error.0.contains("request failed"), "{error}");
    assert!(client.refund("fake_cs_x").await.is_err());
}
