use chrono::{DateTime, Utc};
use diesel::prelude::*;

use crate::models::{NewOrder, Order, OrderStatus};
use crate::schema::{orders, products};

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

/// How the confirming transaction ended
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Confirm {
    /// The order is now `CONFIRMED` and the product's `sold` went up by one
    Confirmed,
    /// The order was not `AWAITING_PAYMENT` (another processor confirmed or
    /// refunded it first); nothing changed
    NotAwaiting,
    /// The product has no unit left (`sold = total_inventory`): the guard
    /// refused, the transaction rolled back and nothing changed
    SoldOut,
}

/// The confirming transaction (roadmap step 8): the order goes
/// `AWAITING_PAYMENT` to `CONFIRMED` and the backstop
/// `UPDATE products SET sold = sold + 1 WHERE id = $1 AND sold < total_inventory`
/// runs, both or neither. The order row is locked first, so two processors
/// handling the same event cannot both count it, and every transaction takes
/// the order lock before the product's, so they cannot deadlock.
pub fn confirm_order(conn: &mut PgConnection, ticket: &str, product_id: i32) -> QueryResult<Confirm> {
    let result = conn.transaction(|conn| {
        let moved = diesel::update(
            orders::table
                .filter(orders::queue_ticket_id.eq(ticket))
                .filter(orders::status.eq(OrderStatus::AwaitingPayment)),
        )
        .set(orders::status.eq(OrderStatus::Confirmed))
        .execute(conn)?;
        if moved == 0 {
            return Ok(Confirm::NotAwaiting);
        }
        let sold = diesel::update(
            products::table.filter(products::id.eq(product_id)).filter(products::sold.lt(products::total_inventory)),
        )
        .set(products::sold.eq(products::sold + 1))
        .execute(conn)?;
        if sold == 0 {
            // Rolls the order update back too
            return Err(diesel::result::Error::RollbackTransaction);
        }
        Ok(Confirm::Confirmed)
    });
    match result {
        Err(diesel::result::Error::RollbackTransaction) => Ok(Confirm::SoldOut),
        other => other,
    }
}

/// Marks an order whose payment was refunded `REFUNDED`: from any state that
/// is not `CONFIRMED` (a late payment finds the order `AWAITING_PAYMENT`, or
/// already `EXPIRED` or `FAILED`). Returns false if it was left alone.
pub fn mark_refunded(conn: &mut PgConnection, ticket: &str) -> QueryResult<bool> {
    let updated = diesel::update(
        orders::table.filter(orders::queue_ticket_id.eq(ticket)).filter(orders::status.eq_any([
            OrderStatus::Pending,
            OrderStatus::AwaitingPayment,
            OrderStatus::Expired,
            OrderStatus::Failed,
        ])),
    )
    .set(orders::status.eq(OrderStatus::Refunded))
    .execute(conn)?;
    Ok(updated == 1)
}

/// Marks an order whose hold the unpaid-hold loop released `EXPIRED`, only
/// from `AWAITING_PAYMENT`: a `REFUNDED` the payment inbox wrote for a payment
/// that slipped in after the release must never be overwritten. Returns
/// whether the order was changed.
pub fn mark_expired(conn: &mut PgConnection, ticket: &str) -> QueryResult<bool> {
    let updated = diesel::update(
        orders::table
            .filter(orders::queue_ticket_id.eq(ticket))
            .filter(orders::status.eq(OrderStatus::AwaitingPayment)),
    )
    .set(orders::status.eq(OrderStatus::Expired))
    .execute(conn)?;
    Ok(updated == 1)
}

/// `AWAITING_PAYMENT` orders whose deadline passed before `expired_before`, in
/// id order after `after_id`: the stale-order sweep checks each against its
/// hold. A page at a time, so rows that stay (their hold is still live) cannot
/// hide the ones behind them.
pub fn stale_awaiting_payment(
    conn: &mut PgConnection,
    expired_before: DateTime<Utc>,
    after_id: i64,
    limit: i64,
) -> QueryResult<Vec<Order>> {
    orders::table
        .filter(orders::status.eq(OrderStatus::AwaitingPayment))
        .filter(orders::expires_at.lt(expired_before))
        .filter(orders::id.gt(after_id))
        .order(orders::id.asc())
        .limit(limit)
        .select(Order::as_select())
        .load(conn)
}

/// `PENDING` orders created before `created_before`, in id order after `after_id`
pub fn stale_pending(
    conn: &mut PgConnection,
    created_before: DateTime<Utc>,
    after_id: i64,
    limit: i64,
) -> QueryResult<Vec<Order>> {
    orders::table
        .filter(orders::status.eq(OrderStatus::Pending))
        .filter(orders::created_at.lt(created_before))
        .filter(orders::id.gt(after_id))
        .order(orders::id.asc())
        .limit(limit)
        .select(Order::as_select())
        .load(conn)
}
