use chrono::{DateTime, Utc};
use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::schema::{inventory, orders, payment_inbox, products, users};

#[derive(Debug, Clone, Copy, PartialEq, Eq, diesel_derive_enum::DbEnum, Serialize, Deserialize)]
#[ExistingTypePath = "crate::schema::sql_types::OrderStatus"]
#[DbValueStyle = "SCREAMING_SNAKE_CASE"]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderStatus {
    PaymentPending,
    Confirmed,
    Expired,
    Canceled,
    Refunded,
}

// Not Serialize, so the password hash can never end up in a response. Guests
// (`POST /session`) have neither a username nor a password.
#[derive(Debug, Clone, Queryable, Selectable, Identifiable)]
#[diesel(table_name = users)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct User {
    pub id: i32,
    pub username: Option<String>,
    pub password_hash: Option<String>,
    pub is_guest: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = users)]
pub struct NewUser<'a> {
    pub username: &'a str,
    pub password_hash: &'a str,
}

// status is one of SCHEDULED, ON_SALE, CLOSED, CANCELED (checked by the database)
#[derive(Debug, Clone, Queryable, Selectable, Identifiable, Serialize)]
#[diesel(table_name = products)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Product {
    pub id: i32,
    pub sku: String,
    pub name: String,
    pub description: String,
    pub image_url: Option<String>,
    pub price_cents: i32,
    pub max_per_user: i32,
    pub sale_opens_at: DateTime<Utc>,
    pub sale_ends_at: DateTime<Utc>,
    pub status: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = products)]
pub struct NewProduct<'a> {
    pub sku: &'a str,
    pub name: &'a str,
    pub description: &'a str,
    pub image_url: Option<&'a str>,
    pub price_cents: i32,
    pub max_per_user: i32,
    pub sale_opens_at: DateTime<Utc>,
    pub sale_ends_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Queryable, Selectable, Identifiable, Associations, Serialize)]
#[diesel(belongs_to(Product))]
#[diesel(table_name = inventory)]
#[diesel(primary_key(product_id))]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Inventory {
    pub product_id: i32,
    pub stock_total: i32,
    pub stock: i32,
    pub version: i32,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = inventory)]
pub struct NewInventory {
    pub product_id: i32,
    pub stock_total: i32,
    pub stock: i32,
}

#[derive(Debug, Clone, Queryable, Selectable, Identifiable, Associations, Serialize)]
#[diesel(belongs_to(User))]
#[diesel(belongs_to(Product))]
#[diesel(table_name = orders)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Order {
    pub id: Uuid,
    pub user_id: i32,
    pub product_id: i32,
    pub quantity: i32,
    pub unit_price_cents: i32,
    pub total_cents: i32,
    pub status: OrderStatus,
    pub payment_idempotency_key: Option<String>,
    pub payment_intent_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// The id is the reservation id from Redis. Created when payment starts, so
// its status is PAYMENT_PENDING: holds live in Redis, not in orders.
#[derive(Debug, Insertable)]
#[diesel(table_name = orders)]
pub struct NewOrder<'a> {
    pub id: Uuid,
    pub user_id: i32,
    pub product_id: i32,
    pub quantity: i32,
    pub unit_price_cents: i32,
    pub total_cents: i32,
    pub status: OrderStatus,
    pub payment_idempotency_key: &'a str,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Queryable, Selectable, Identifiable)]
#[diesel(table_name = payment_inbox)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct PaymentInboxEntry {
    pub id: i64,
    pub provider: String,
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
    pub provider: &'a str,
    pub provider_event_id: &'a str,
    pub event_type: &'a str,
    pub payload: &'a serde_json::Value,
}

