use chrono::{DateTime, Utc};
use diesel::prelude::*;

use crate::models::{NewOrder, Order, OrderStatus};
use crate::schema::orders;

/// The order for a queue ticket, if the processor has created it. The last
/// fallback of the status endpoint, after the read cache and the hold.
pub fn find_by_ticket(conn: &mut PgConnection, ticket: &str) -> QueryResult<Option<Order>> {
    orders::table
        .filter(orders::queue_ticket_id.eq(ticket))
        .select(Order::as_select())
        .first(conn)
        .optional()
}

/// Inserts the order for a checkout event, then returns the row stored for its
/// ticket. The insert is `ON CONFLICT DO NOTHING` (no target), so a redelivered
/// message finds its order, and any other unique constraint that fires (the
/// user already has a live order) inserts nothing: then there is no row for the
/// ticket and the result is `None`.
pub fn insert_pending(conn: &mut PgConnection, new_order: &NewOrder) -> QueryResult<Option<Order>> {
    diesel::insert_into(orders::table).values(new_order).on_conflict_do_nothing().execute(conn)?;
    find_by_ticket(conn, new_order.queue_ticket_id).map(|order| order.filter(|o| o.user_id == new_order.user_id))
}

/// Records the payment session on the order and moves it to `AWAITING_PAYMENT`.
/// Only from `PENDING` or `AWAITING_PAYMENT` (a redelivery repeats this step);
/// returns false if the order is in any other state and was left alone.
pub fn mark_awaiting_payment(
    conn: &mut PgConnection,
    ticket: &str,
    session_id: &str,
    checkout_url: &str,
    expires_at: DateTime<Utc>,
) -> QueryResult<bool> {
    let updated = diesel::update(
        orders::table
            .filter(orders::queue_ticket_id.eq(ticket))
            .filter(orders::status.eq_any([OrderStatus::Pending, OrderStatus::AwaitingPayment])),
    )
    .set((
        orders::status.eq(OrderStatus::AwaitingPayment),
        orders::payment_session_id.eq(session_id),
        orders::checkout_url.eq(checkout_url),
        orders::expires_at.eq(expires_at),
    ))
    .execute(conn)?;
    Ok(updated == 1)
}

/// Marks an order that never got its payment link `FAILED` (only from
/// `PENDING` or `AWAITING_PAYMENT`, so a final state is never overwritten).
pub fn mark_failed(conn: &mut PgConnection, ticket: &str) -> QueryResult<bool> {
    let updated = diesel::update(
        orders::table
            .filter(orders::queue_ticket_id.eq(ticket))
            .filter(orders::status.eq_any([OrderStatus::Pending, OrderStatus::AwaitingPayment])),
    )
    .set(orders::status.eq(OrderStatus::Failed))
    .execute(conn)?;
    Ok(updated == 1)
}
