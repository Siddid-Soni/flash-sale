// @generated automatically by Diesel CLI.

pub mod sql_types {
    #[derive(diesel::query_builder::QueryId, diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "order_status"))]
    pub struct OrderStatus;
}

diesel::table! {
    inventory (product_id) {
        product_id -> Int4,
        stock_total -> Int4,
        stock -> Int4,
        version -> Int4,
        updated_at -> Timestamptz,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::OrderStatus;

    orders (id) {
        id -> Uuid,
        user_id -> Int4,
        product_id -> Int4,
        quantity -> Int4,
        unit_price_cents -> Int4,
        total_cents -> Int4,
        status -> OrderStatus,
        #[max_length = 255]
        payment_idempotency_key -> Nullable<Varchar>,
        #[max_length = 255]
        payment_intent_id -> Nullable<Varchar>,
        created_at -> Timestamptz,
        expires_at -> Timestamptz,
        updated_at -> Timestamptz,
    }
}

diesel::table! {
    payment_inbox (id) {
        id -> Int8,
        #[max_length = 50]
        provider -> Varchar,
        #[max_length = 255]
        provider_event_id -> Varchar,
        #[max_length = 100]
        event_type -> Varchar,
        payload -> Jsonb,
        received_at -> Timestamptz,
        processed_at -> Nullable<Timestamptz>,
        attempts -> Int4,
        last_error -> Nullable<Text>,
    }
}

diesel::table! {
    products (id) {
        id -> Int4,
        #[max_length = 64]
        sku -> Varchar,
        #[max_length = 255]
        name -> Varchar,
        description -> Text,
        #[max_length = 1024]
        image_url -> Nullable<Varchar>,
        price_cents -> Int4,
        max_per_user -> Int4,
        sale_opens_at -> Timestamptz,
        sale_ends_at -> Timestamptz,
        #[max_length = 20]
        status -> Varchar,
        created_at -> Timestamptz,
    }
}

diesel::table! {
    users (id) {
        id -> Int4,
        #[max_length = 255]
        username -> Nullable<Varchar>,
        #[max_length = 255]
        password_hash -> Nullable<Varchar>,
        is_guest -> Bool,
        created_at -> Timestamptz,
    }
}

diesel::joinable!(inventory -> products (product_id));
diesel::joinable!(orders -> products (product_id));
diesel::joinable!(orders -> users (user_id));

diesel::allow_tables_to_appear_in_same_query!(inventory, orders, payment_inbox, products, users,);
