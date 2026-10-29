//! `GET /v1/orders/status/{ticket}`: what the client polls (only winners do).
//!
//! The answer comes from the read cache only; this endpoint never touches
//! PostgreSQL, so a flood of made-up tickets costs one Redis read each. Every
//! transition writes the entry (and the writers retry until it lands), and an
//! entry outlives its payment window by far (24 h), so a winner's entry is
//! missing only if the read cache lost its data. A ticket without an entry is
//! `404` (docs/FAILURE_CASES.md, case 3).

use actix_web::{get, http::header, web, HttpResponse};
use serde::Serialize;

use crate::{
    auth::AuthUser,
    cache::{self, ReadCache},
    error::ApiError,
    models::OrderStatus,
};

#[derive(Debug, Serialize, PartialEq, Eq)]
struct StatusResponse {
    status: OrderStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    payment_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    expires_at: Option<String>,
}

/// The status of the caller's ticket: `200 {status, payment_url?, expires_at?}`,
/// `403` for someone else's ticket, `404` if the read cache has no entry for it.
/// Never cached: the answer changes every few seconds.
#[get("/orders/status/{ticket}")]
async fn order_status(
    user: AuthUser,
    path: web::Path<String>,
    read_cache: web::Data<ReadCache>,
) -> Result<HttpResponse, ApiError> {
    let ticket = path.into_inner();
    let not_found = || ApiError::NotFound("no such ticket".into());
    let forbidden = || ApiError::Forbidden("this ticket belongs to someone else".into());
    cache::ticket_sale(&ticket).ok_or_else(not_found)?;

    let mut cache_con = read_cache.get_ref().clone();
    if let Some(cached) = cache::read_status(&mut cache_con, &ticket).await? {
        if cached.user != user.id {
            return Err(forbidden());
        }
        // The link and deadline stay in the entry after payment; only an unpaid order shows them
        let awaiting = cached.status == OrderStatus::AwaitingPayment;
        return Ok(respond(StatusResponse {
            status: cached.status,
            payment_url: cached.payment_url.filter(|_| awaiting),
            expires_at: cached.expires_at.filter(|_| awaiting),
        }));
    }

    Err(not_found())
}

fn respond(body: StatusResponse) -> HttpResponse {
    HttpResponse::Ok().insert_header((header::CACHE_CONTROL, "no-store")).json(body)
}

// GET /v1/orders/status/{ticket}
pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(order_status);
}

