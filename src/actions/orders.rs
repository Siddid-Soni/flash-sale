use chrono::{DateTime, Utc};
use diesel::prelude::*;

use crate::models::{NewOrder, Order, OrderState};
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

/// Inserts the order for a checkout event (born `AWAITING_PAYMENT`, with its
/// session, link and deadline), then returns the row stored for its ticket.
/// The insert is `ON CONFLICT DO NOTHING` (no target), so a redelivered
/// message finds its order, and any other unique constraint that fires (the
/// user already has a live order) inserts nothing: then there is no row for the
/// ticket and the result is `None`.
pub fn insert_awaiting_payment(conn: &mut PgConnection, new_order: &NewOrder) -> QueryResult<Option<Order>> {
    diesel::insert_into(orders::table).values(new_order).on_conflict_do_nothing().execute(conn)?;
    find_by_ticket(conn, new_order.queue_ticket_id).map(|order| order.filter(|o| o.user_id == new_order.user_id))
}

/// Marks an order whose hold was released before its link was ready `FAILED`
/// (only from `AWAITING_PAYMENT`, so a final state is never overwritten).
pub fn mark_failed(conn: &mut PgConnection, ticket: &str) -> QueryResult<bool> {
    let updated = diesel::update(
        orders::table
            .filter(orders::queue_ticket_id.eq(ticket))
            .filter(orders::status.eq(OrderState::AwaitingPayment)),
    )
    .set(orders::status.eq(OrderState::Failed))
    .execute(conn)?;
    Ok(updated == 1)
}

/// What the decision transaction for a payment came to
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Settlement {
    /// The order is now `CONFIRMED` and the product's `sold` went up by one
    Confirmed,
    /// The order was already `CONFIRMED` (a rerun or a duplicate event)
    AlreadyConfirmed,
    /// The payment cannot be kept: the deadline passed, or the order was
    /// already `EXPIRED` or `FAILED`. The order is now `REFUNDED`; the
    /// payment still has to be refunded at the provider.
    Refund,
    /// The deadline was ahead but the product has no unit left (`sold =
    /// total_inventory`): Redis and PostgreSQL disagree. Nothing was counted;
    /// the order is now `REFUNDED`, the payment still to be refunded.
    SoldOut,
    /// The order was already `REFUNDED` by an earlier run: only the provider
    /// refund and the status write may be missing
    AlreadyRefunded,
    /// The order is in a state that should be impossible for this payment
    Unexpected(OrderState),
    /// There is no order for the ticket
    Missing,
}

/// PostgreSQL's decision between a payment and the deadline (one transaction).
/// The order is confirmed only if, at PostgreSQL's own `now()`, it is still
/// `AWAITING_PAYMENT` for this session and its deadline is ahead:
///
/// ```sql
/// UPDATE orders SET status = 'CONFIRMED' WHERE queue_ticket_id = $1
///   AND status = 'AWAITING_PAYMENT' AND payment_session_id = $2 AND expires_at > now()
/// UPDATE products SET sold = sold + 1 WHERE id = $3 AND sold < total_inventory
/// ```
///
/// If the guard matches no row the confirmation is rolled back (to a
/// savepoint). The expiration worker's `expire_due` is the mirror image
/// (`status = 'AWAITING_PAYMENT' AND expires_at <= now()`); both need the
/// order's row lock and see the other's result after it, so exactly one wins.
///
/// When the order cannot be confirmed it is set `REFUNDED` in the same
/// transaction (from `AWAITING_PAYMENT`, `EXPIRED` or `FAILED`, never from
/// `CONFIRMED`), which records the decision before the provider is asked to
/// refund; a rerun finds `REFUNDED` and goes straight to the refund.
pub fn settle_payment(conn: &mut PgConnection, ticket: &str, session_id: &str, product_id: i32) -> QueryResult<Settlement> {
    conn.transaction(|conn| {
        let confirmed = conn.transaction(|conn| {
            let moved = diesel::update(
                orders::table
                    .filter(orders::queue_ticket_id.eq(ticket))
                    .filter(orders::status.eq(OrderState::AwaitingPayment))
                    .filter(orders::payment_session_id.eq(session_id))
                    .filter(orders::expires_at.gt(diesel::dsl::now)),
            )
            .set(orders::status.eq(OrderState::Confirmed))
            .execute(conn)?;
            if moved == 0 {
                return Ok(None);
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
            Ok(Some(()))
        });
        let sold_out = match confirmed {
            Ok(Some(())) => return Ok(Settlement::Confirmed),
            Ok(None) => false,
            Err(diesel::result::Error::RollbackTransaction) => true,
            Err(e) => return Err(e),
        };

        let refunded = diesel::update(
            orders::table
                .filter(orders::queue_ticket_id.eq(ticket))
                .filter(orders::payment_session_id.eq(session_id))
                .filter(orders::status.eq_any([OrderState::AwaitingPayment, OrderState::Expired, OrderState::Failed])),
        )
        .set(orders::status.eq(OrderState::Refunded))
        .execute(conn)?;
        if refunded == 1 {
            return Ok(if sold_out { Settlement::SoldOut } else { Settlement::Refund });
        }
        let status: Option<OrderState> = orders::table
            .filter(orders::queue_ticket_id.eq(ticket))
            .select(orders::status)
            .first(conn)
            .optional()?;
        Ok(match status {
            Some(OrderState::Confirmed) => Settlement::AlreadyConfirmed,
            Some(OrderState::Refunded) => Settlement::AlreadyRefunded,
            Some(other) => Settlement::Unexpected(other),
            None => Settlement::Missing,
        })
    })
}

/// Marks the due, unpaid orders among `tickets` `EXPIRED` in one statement and
/// returns the tickets it changed (the expiration worker's half of the race
/// with `settle_payment`):
///
/// ```sql
/// UPDATE orders SET status = 'EXPIRED' WHERE queue_ticket_id = ANY($1)
///   AND status = 'AWAITING_PAYMENT' AND expires_at <= now() RETURNING queue_ticket_id
/// ```
pub fn expire_due(conn: &mut PgConnection, tickets: &[String]) -> QueryResult<Vec<String>> {
    diesel::update(
        orders::table
            .filter(orders::queue_ticket_id.eq_any(tickets))
            .filter(orders::status.eq(OrderState::AwaitingPayment))
            .filter(orders::expires_at.le(diesel::dsl::now)),
    )
    .set(orders::status.eq(OrderState::Expired))
    .returning(orders::queue_ticket_id)
    .get_results(conn)
}

/// The orders of `tickets`: after `expire_due`, the worker reads what
/// PostgreSQL decided for the tickets it did not expire
pub fn find_by_tickets(conn: &mut PgConnection, tickets: &[String]) -> QueryResult<Vec<Order>> {
    orders::table
        .filter(orders::queue_ticket_id.eq_any(tickets))
        .select(Order::as_select())
        .load(conn)
}

/// Whether the product still has a unit PostgreSQL could sell
/// (`sold < total_inventory`). When a hold is live this is true unless Redis
/// and PostgreSQL disagree, which is how the worker tells a payment refunded
/// after its deadline from one refunded after the guard failed.
pub fn has_free_unit(conn: &mut PgConnection, product_id: i32) -> QueryResult<bool> {
    products::table
        .filter(products::id.eq(product_id))
        .filter(products::sold.lt(products::total_inventory))
        .count()
        .get_result::<i64>(conn)
        .map(|n| n > 0)
}

/// Marks an order whose payment was refunded `REFUNDED`: from `AWAITING_PAYMENT`,
/// `EXPIRED` or `FAILED`, never from `CONFIRMED`. Returns false if it was left alone.
/// (`settle_payment` does this as part of its decision; this is the standalone form.)
pub fn mark_refunded(conn: &mut PgConnection, ticket: &str) -> QueryResult<bool> {
    let updated = diesel::update(
        orders::table.filter(orders::queue_ticket_id.eq(ticket)).filter(orders::status.eq_any([
            OrderState::AwaitingPayment,
            OrderState::Expired,
            OrderState::Failed,
        ])),
    )
    .set(orders::status.eq(OrderState::Refunded))
    .execute(conn)?;
    Ok(updated == 1)
}

/// Marks an order whose hold the worker released `EXPIRED`, only from
/// `AWAITING_PAYMENT`: a `CONFIRMED` or `REFUNDED` order is never overwritten.
/// The unpaid loop does this in batches (`expire_due`); this is the form for the
/// stale-order sweep. Returns whether the order was changed.
pub fn mark_expired(conn: &mut PgConnection, ticket: &str) -> QueryResult<bool> {
    let updated = diesel::update(
        orders::table
            .filter(orders::queue_ticket_id.eq(ticket))
            .filter(orders::status.eq(OrderState::AwaitingPayment)),
    )
    .set(orders::status.eq(OrderState::Expired))
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
        .filter(orders::status.eq(OrderState::AwaitingPayment))
        .filter(orders::expires_at.lt(expired_before))
        .filter(orders::id.gt(after_id))
        .order(orders::id.asc())
        .limit(limit)
        .select(Order::as_select())
        .load(conn)
}

/// `EXPIRED` orders last changed between `since` and `before`, in id order
/// after `after_id`: the sweep re-checks that the status reached the read
/// cache and the payment session was stopped (a worker may have died after
/// the order update).
pub fn recently_expired(
    conn: &mut PgConnection,
    since: DateTime<Utc>,
    before: DateTime<Utc>,
    after_id: i64,
    limit: i64,
) -> QueryResult<Vec<Order>> {
    orders::table
        .filter(orders::status.eq(OrderState::Expired))
        .filter(orders::updated_at.ge(since))
        .filter(orders::updated_at.lt(before))
        .filter(orders::id.gt(after_id))
        .order(orders::id.asc())
        .limit(limit)
        .select(Order::as_select())
        .load(conn)
}
