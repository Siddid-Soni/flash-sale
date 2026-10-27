use chrono::{DateTime, Utc};
use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use crate::schema::{orders, payment_inbox, products, users};

/// Where an order is in its life; mirrors the hold states in Redis (see ROADMAP.md, Holds).
#[derive(Debug, Clone, Copy, PartialEq, Eq, diesel_derive_enum::DbEnum, Serialize, Deserialize)]
#[ExistingTypePath = "crate::schema::sql_types::OrderStatus"]
#[DbValueStyle = "SCREAMING_SNAKE_CASE"]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderStatus {
    Pending,
    AwaitingPayment,
    Confirmed,
    Expired,
    Failed,
    Refunded,
}

impl OrderStatus {
    /// The name used in Redis (`status:<ticket>`, holds) and in API responses
    pub fn as_str(self) -> &'static str {
        match self {
            OrderStatus::Pending => "PENDING",
            OrderStatus::AwaitingPayment => "AWAITING_PAYMENT",
            OrderStatus::Confirmed => "CONFIRMED",
            OrderStatus::Expired => "EXPIRED",
            OrderStatus::Failed => "FAILED",
            OrderStatus::Refunded => "REFUNDED",
        }
    }

    /// The inverse of `as_str`; `None` for anything else
    pub fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "PENDING" => OrderStatus::Pending,
            "AWAITING_PAYMENT" => OrderStatus::AwaitingPayment,
            "CONFIRMED" => OrderStatus::Confirmed,
            "EXPIRED" => OrderStatus::Expired,
            "FAILED" => OrderStatus::Failed,
            "REFUNDED" => OrderStatus::Refunded,
            _ => return None,
        })
    }
}

/// Guests only: `username` and `email` are NULL until registration exists.
#[derive(Debug, Clone, Queryable, Selectable, Identifiable)]
#[diesel(table_name = users)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct User {
    pub id: i32,
    pub username: Option<String>,
    pub email: Option<String>,
    pub created_at: DateTime<Utc>,
}

/// A product is a sale: `id` is the sale id in every Redis key.
#[derive(Debug, Clone, Queryable, Selectable, Identifiable, Serialize)]
#[diesel(table_name = products)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Product {
    pub id: i32,
    pub name: String,
    pub price_cents: i32,
    pub total_inventory: i32,
    /// Units paid for and confirmed; only the confirming transaction raises it
    pub sold: i32,
    pub sale_start_time: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = products)]
pub struct NewProduct<'a> {
    pub name: &'a str,
    pub price_cents: i32,
    pub total_inventory: i32,
    pub sale_start_time: DateTime<Utc>,
}

#[derive(Debug, Clone, Queryable, Selectable, Identifiable, Associations, Serialize)]
#[diesel(belongs_to(User))]
#[diesel(belongs_to(Product))]
#[diesel(table_name = orders)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Order {
    pub id: i64,
    pub user_id: i32,
    pub product_id: i32,
    /// `<sale>.<uuid>`
    pub queue_ticket_id: String,
    pub idempotency_key: String,
    pub status: OrderStatus,
    pub amount_cents: i32,
    pub payment_session_id: Option<String>,
    /// With `expires_at`, set when the order becomes `AWAITING_PAYMENT`
    pub checkout_url: Option<String>,
    pub expires_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Inserted `PENDING` by an order processor when it consumes a checkout event.
#[derive(Debug, Insertable)]
#[diesel(table_name = orders)]
pub struct NewOrder<'a> {
    pub user_id: i32,
    pub product_id: i32,
    pub queue_ticket_id: &'a str,
    pub idempotency_key: &'a str,
    pub status: OrderStatus,
    pub amount_cents: i32,
}

#[derive(Debug, Clone, Queryable, Selectable, Identifiable)]
#[diesel(table_name = payment_inbox)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct PaymentInboxEntry {
    pub id: i64,
    pub provider_event_id: String,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub received_at: DateTime<Utc>,
    pub processed_at: Option<DateTime<Utc>>,
    pub attempts: i32,
    pub last_error: Option<String>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = payment_inbox)]
pub struct NewPaymentInboxEntry<'a> {
    pub provider_event_id: &'a str,
    pub event_type: &'a str,
    pub payload: &'a serde_json::Value,
}
