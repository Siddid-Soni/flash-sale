use actix_web::{get, http::header, web, HttpResponse};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    actions,
    auth::AuthUser,
    db::DbPool,
    error::ApiError,
    models::{Order, OrderStatus},
};

/// An order as clients see it (without the payment keys)
#[derive(Debug, Serialize)]
struct OrderResponse {
    id: Uuid,
    product_id: i32,
    quantity: i32,
    unit_price_cents: i32,
    total_cents: i32,
    status: OrderStatus,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
}

impl From<Order> for OrderResponse {
    fn from(order: Order) -> Self {
        Self {
            id: order.id,
            product_id: order.product_id,
            quantity: order.quantity,
            unit_price_cents: order.unit_price_cents,
            total_cents: order.total_cents,
            status: order.status,
            created_at: order.created_at,
            updated_at: order.updated_at,
            expires_at: order.expires_at,
        }
    }
}

/// The caller's order. Polled until it leaves `PAYMENT_PENDING`, so it is never
/// cached, and says how often to ask while the payment is in flight.
#[get("/orders/{id}")]
async fn get_order(user: AuthUser, pool: web::Data<DbPool>, path: web::Path<String>) -> Result<HttpResponse, ApiError> {
    let id = Uuid::parse_str(&path.into_inner()).map_err(|_| ApiError::BadRequest("invalid order id".into()))?;

    let order = web::block(move || {
        let mut conn = pool.get()?;
        actions::orders::find_order(&mut conn, id).map_err(|err| match err {
            diesel::result::Error::NotFound => ApiError::NotFound(format!("order {id} not found")),
            other => other.into(),
        })
    })
    .await??;
    if order.user_id != user.id {
        return Err(ApiError::Forbidden("that order is not yours".into()));
    }

    let mut response = HttpResponse::Ok();
    response.insert_header((header::CACHE_CONTROL, "no-store"));
    if order.status == OrderStatus::PaymentPending {
        response.insert_header((header::RETRY_AFTER, "1"));
    }
    Ok(response.json(OrderResponse::from(order)))
}

#[derive(Debug, Deserialize)]
struct ListQuery {
    limit: Option<i64>,
}

#[derive(Debug, Serialize)]
struct ListResponse {
    orders: Vec<OrderResponse>,
}

/// The caller's orders, newest first
#[get("/orders")]
async fn list_orders(user: AuthUser, pool: web::Data<DbPool>, query: web::Query<ListQuery>) -> Result<HttpResponse, ApiError> {
    let limit = query.limit.unwrap_or(20).clamp(1, 100);

    let orders = web::block(move || {
        let mut conn = pool.get()?;
        Ok::<_, ApiError>(actions::orders::list_orders(&mut conn, user.id, limit)?)
    })
    .await??;

    Ok(HttpResponse::Ok()
        .insert_header((header::CACHE_CONTROL, "no-store"))
        .json(ListResponse { orders: orders.into_iter().map(OrderResponse::from).collect() }))
}

// GET /orders/{id}, GET /orders
pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(get_order).service(list_orders);
}
