use chrono::{DateTime, Utc};
use diesel::prelude::*;

use crate::models::{NewOrder, Order, OrderState};
use crate::schema::{orders, products};

/// The order for a queue ticket, if one exists. The status endpoint's fallback
/// after the read cache.
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

/// What `insert_failed` found
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailedInsert {
    /// This call created the `FAILED` row: the ticket will never become an order
    Created,
    /// A row for the ticket already existed (the message reached Kafka after
    /// all and a processor made the order)
    Existed,
}

/// Records a ticket whose publish failed as a `FAILED` order, so the decision
/// is PostgreSQL's: the row is inserted `ON CONFLICT DO NOTHING` on the ticket,
/// and only a call that created it may return the unit to stock. A message that
/// reached Kafka anyway finds the row and stops at it (the processor), or made
/// the order first (then the row `Existed`).
///
/// The row needs an amount (the product's price, read here: this path is rare)
/// and an idempotency key. It is stored under the ticket itself, not the
/// client's key, because `(user_id, idempotency_key)` is unique and the
/// client's retry with the same key (compensation forgets it in Redis) must
/// still be able to make its order. Fails if the product does not exist.
pub fn insert_failed(conn: &mut PgConnection, user_id: i32, product_id: i32, ticket: &str) -> QueryResult<FailedInsert> {
    let amount_cents: i32 = products::table.find(product_id).select(products::price_cents).first(conn)?;
    let created = diesel::insert_into(orders::table)
        .values(&NewOrder {
            user_id,
            product_id,
            queue_ticket_id: ticket,
            idempotency_key: ticket,
            status: OrderState::Failed,
            amount_cents,
            payment_session_id: None,
            checkout_url: None,
            expires_at: None,
        })
        .on_conflict(orders::queue_ticket_id)
        .do_nothing()
        .execute(conn)?;
    Ok(if created == 1 { FailedInsert::Created } else { FailedInsert::Existed })
}

/// What the decision transaction for a payment came to
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Settlement {
    /// The order is now `CONFIRMED` and the product's `sold` went up by one
    Confirmed,
    /// The order was already `CONFIRMED` (a rerun or a duplicate event)
    AlreadyConfirmed,
    /// The payment cannot be kept: the order was already `EXPIRED` or `FAILED`.
    /// The order is now `REFUNDED`; the payment still has to be refunded at the
    /// provider. No stock moves (the expiration worker returned the unit when it
    /// expired the order, or nothing was reserved).
    Refund,
    /// The order is still `AWAITING_PAYMENT` but its deadline has passed. The
    /// inbox never decides a deadline: nothing was changed. The event must stay
    /// unprocessed and be tried again; once the expiration worker has expired
    /// the order (and returned the unit) the retry finds it `EXPIRED`.
    Postponed,
    /// The payment was in time but the product has no unit left (`sold =
    /// total_inventory`): Redis and PostgreSQL disagree. Nothing was counted;
    /// the order is now `REFUNDED` (no stock moves), the payment still to be
    /// refunded.
    SoldOut,
    /// The order was already `REFUNDED` by an earlier run: only the provider
    /// refund and the status write may be missing
    AlreadyRefunded,
    /// The order is in a state that should be impossible for this payment
    Unexpected(OrderState),
    /// There is no order for the ticket
    Missing,
}

/// PostgreSQL's decision on a payment (one transaction). The order is
/// confirmed only if, at PostgreSQL's own `now()`, it is still
/// `AWAITING_PAYMENT` for this session and its deadline is ahead:
///
/// ```sql
/// UPDATE orders SET status = 'CONFIRMED' WHERE queue_ticket_id = $1
///   AND status = 'AWAITING_PAYMENT' AND payment_session_id = $2 AND expires_at > now()
/// UPDATE products SET sold = sold + 1 WHERE id = $3 AND sold < total_inventory
/// ```
///
/// If the guard matches no row the confirmation is rolled back (to a
/// savepoint) and the order is `REFUNDED`. The expiration worker's
/// `expire_batch` is the mirror image (`status = 'AWAITING_PAYMENT' AND
/// expires_at <= now()`); both need the order's row lock and see the other's
/// result after it, so exactly one wins.
///
/// **The inbox never decides a deadline.** An order that is `EXPIRED` or
/// `FAILED` is set `REFUNDED` (never from `CONFIRMED`) in the same transaction,
/// which records the decision before the provider is asked to refund; a rerun
/// finds `REFUNDED` and goes straight to the refund. An order still
/// `AWAITING_PAYMENT` after its `expires_at` is left alone (`Postponed`): only
/// the expiration worker expires an order, and it returns the unit when it
/// does, so the retry that finds the order `EXPIRED` refunds without moving
/// stock.
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

        // Refundable: already expired or failed; and, when the guard failed on
        // an in-time payment, the order that was still awaiting it
        let refundable: &[OrderState] = if sold_out {
            &[OrderState::AwaitingPayment, OrderState::Expired, OrderState::Failed]
        } else {
            &[OrderState::Expired, OrderState::Failed]
        };
        let refunded = diesel::update(
            orders::table
                .filter(orders::queue_ticket_id.eq(ticket))
                .filter(orders::payment_session_id.eq(session_id))
                .filter(orders::status.eq_any(refundable)),
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
            Some(OrderState::AwaitingPayment) => Settlement::Postponed,
            Some(other) => Settlement::Unexpected(other),
            None => Settlement::Missing,
        })
    })
}

/// The expiration worker's half of the race with `settle_payment`: in one
/// statement, marks up to `limit` unpaid orders past their deadline `EXPIRED`
/// (oldest deadline first, rows another worker holds skipped) and returns them.
/// The caller then returns each order's unit to stock, once.
///
/// ```sql
/// UPDATE orders SET status = 'EXPIRED', updated_at = now() WHERE id IN (
///   SELECT id FROM orders WHERE status = 'AWAITING_PAYMENT' AND expires_at <= now()
///   ORDER BY expires_at LIMIT $1 FOR UPDATE SKIP LOCKED) RETURNING *
/// ```
///
/// An order is returned by exactly one call: after it, its status is no longer
/// `AWAITING_PAYMENT`, so several workers at once never share an order.
pub fn expire_batch(conn: &mut PgConnection, limit: i64) -> QueryResult<Vec<Order>> {
    // Raw SQL: the DSL cannot update a table from a subselect over the same table
    diesel::sql_query(
        "UPDATE orders SET status = 'EXPIRED', updated_at = now() \
         WHERE id IN (SELECT id FROM orders WHERE status = 'AWAITING_PAYMENT' AND expires_at <= now() \
                      ORDER BY expires_at LIMIT $1 FOR UPDATE SKIP LOCKED) \
         RETURNING *",
    )
    .bind::<diesel::sql_types::BigInt, _>(limit)
    .get_results(conn)
}

/// `EXPIRED` orders last changed between `since` and `before`, in id order
/// after `after_id`: the sweep re-checks that the status reached the read
/// cache and the payment session was stopped (a worker may have died after
/// the order update). It never returns stock.
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
