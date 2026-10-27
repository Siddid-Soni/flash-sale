//! HTTP layer. Each file owns one group of routes and exposes `configure`,
//! which `configure` below mounts on the Order Service under `/v1`, except
//! `payments`, the provider's webhook, which the order processor serves (see
//! `bin/order_processor.rs`). Handlers return `Result<_, ApiError>`; they call
//! `crate::actions` inside `web::block` for database work.

use actix_web::{error, web, HttpResponse};

use crate::error::ErrorBody;

pub mod admin;
pub mod checkout;
pub mod orders;
pub mod payments;
pub mod sales;
pub mod session;

/// The Order Service's routes, all under `/v1`
pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/v1")
            .configure(session::configure)
            .configure(sales::configure)
            .configure(checkout::configure)
            .configure(orders::configure)
            .configure(admin::configure),
    );
}

/// Returns malformed JSON bodies as `400 {"message": ...}` like every other error.
pub fn json_config() -> web::JsonConfig {
    web::JsonConfig::default().error_handler(|err, _req| {
        let response = match &err {
            error::JsonPayloadError::ContentType => {
                HttpResponse::UnsupportedMediaType().json(ErrorBody { message: "expected application/json".into() })
            }
            error::JsonPayloadError::Deserialize(inner) => {
                HttpResponse::BadRequest().json(ErrorBody { message: inner.to_string() })
            }
            other => HttpResponse::BadRequest().json(ErrorBody { message: other.to_string() }),
        };
        error::InternalError::from_response(err, response).into()
    })
}

/// Returns malformed query strings as `400 {"message": ...}` too.
pub fn query_config() -> web::QueryConfig {
    web::QueryConfig::default().error_handler(|err, _req| {
        let response = HttpResponse::BadRequest().json(ErrorBody { message: err.to_string() });
        error::InternalError::from_response(err, response).into()
    })
}
