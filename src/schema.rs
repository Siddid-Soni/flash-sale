// @generated automatically by Diesel CLI.

pub mod sql_types {
    #[derive(diesel::query_builder::QueryId, diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "order_status"))]
    pub struct OrderStatus;
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::OrderStatus;

    orders (id) {
        id -> Int8,
        user_id -> Int4,
        product_id -> Int4,
        queue_ticket_id -> Text,
        idempotency_key -> Text,
        status -> OrderStatus,
        amount_cents -> Int4,
        payment_session_id -> Nullable<Text>,
        checkout_url -> Nullable<Text>,
        expires_at -> Nullable<Timestamptz>,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
    }
}

diesel::table! {
    payment_inbox (id) {
        id -> Int8,
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
        #[max_length = 255]
        name -> Varchar,
        price_cents -> Int4,
        total_inventory -> Int4,
        sold -> Int4,
        sale_start_time -> Timestamptz,
        created_at -> Timestamptz,
    }
}

diesel::table! {
    users (id) {
        id -> Int4,
        #[max_length = 255]
        username -> Nullable<Varchar>,
        #[max_length = 255]
        email -> Nullable<Varchar>,
        created_at -> Timestamptz,
    }
}

diesel::joinable!(orders -> products (product_id));
diesel::joinable!(orders -> users (user_id));

diesel::allow_tables_to_appear_in_same_query!(orders, payment_inbox, products, users,);
