//! Guest sessions and the `AuthUser` extractor. Needs Postgres (see tests/common).

mod common;

use actix_web::{http::StatusCode, test, web, App};
use diesel::prelude::*;
use serde_json::Value;

use flash_sale::{auth::{self, AuthUser}, schema::users};

#[actix_web::test]
async fn session_creates_a_guest_and_returns_its_token() {
    let env = common::env().await;
    let app = site_app!(env);

    let first: Value = test::call_and_read_body_json(&app, test::TestRequest::post().uri("/v1/session").to_request()).await;
    let second: Value = test::call_and_read_body_json(&app, test::TestRequest::post().uri("/v1/session").to_request()).await;

    let id = first["user_id"].as_i64().unwrap() as i32;
    assert_ne!(first["user_id"], second["user_id"]);
    // The token names the new user and expires after JWT_TTL_SECS
    assert_eq!(auth::verify_token(first["token"].as_str().unwrap(), &env.config.jwt_secret).unwrap(), id);
    assert!(first["expires_at"].as_str().is_some());

    // A guest has neither username nor email
    let (username, email): (Option<String>, Option<String>) = users::table
        .find(id)
        .select((users::username, users::email))
        .first(&mut env.pool.get().unwrap())
        .unwrap();
    assert_eq!((username, email), (None, None));

    let resp = test::call_service(&app, test::TestRequest::post().uri("/v1/session").to_request()).await;
    assert_eq!(resp.status(), StatusCode::CREATED);
}

#[actix_web::test]
async fn auth_user_comes_from_the_token_only() {
    let env = common::env().await;
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(env.config.clone()))
            .route("/whoami", web::get().to(|user: AuthUser| async move { user.id.to_string() })),
    )
    .await;

    let (token, _) = auth::issue_token(42, &env.config.jwt_secret, 60).unwrap();
    let ok = test::TestRequest::get().uri("/whoami").insert_header(("Authorization", format!("Bearer {token}"))).to_request();
    assert_eq!(test::call_and_read_body(&app, ok).await, "42");

    let missing = test::call_service(&app, test::TestRequest::get().uri("/whoami").to_request()).await;
    assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);

    let forged = test::TestRequest::get().uri("/whoami").insert_header(("Authorization", "Bearer nope")).to_request();
    assert_eq!(test::call_service(&app, forged).await.status(), StatusCode::UNAUTHORIZED);

    let (other, _) = auth::issue_token(42, "another-secret", 60).unwrap();
    let wrong_key = test::TestRequest::get().uri("/whoami").insert_header(("Authorization", format!("Bearer {other}"))).to_request();
    assert_eq!(test::call_service(&app, wrong_key).await.status(), StatusCode::UNAUTHORIZED);
}
