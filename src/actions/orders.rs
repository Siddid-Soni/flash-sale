// Orders are created when payment starts (never at reserve time: holds live
// only in Redis) and end CONFIRMED, REFUNDED or EXPIRED.

use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel::QueryResult;
use uuid::Uuid;

use crate::models::{NewOrder, Order, OrderStatus};
use crate::schema::{inventory, orders, products};

pub fn find_order(conn: &mut PgConnection, order_id: Uuid) -> QueryResult<Order> {
    orders::table.find(order_id).select(Order::as_select()).first(conn)
}

/// The user's orders, newest first
pub fn list_orders(conn: &mut PgConnection, user_id: i32, limit: i64) -> QueryResult<Vec<Order>> {
    orders::table
        .filter(orders::user_id.eq(user_id))
        .order((orders::created_at.desc(), orders::id.desc()))
        .limit(limit)
        .select(Order::as_select())
        .load(conn)
}

/// Records a hold whose payment is starting: a `PAYMENT_PENDING` order (id =
/// the reservation id) priced from the product, in one transaction.
/// Idempotent: returns the existing order if there is one. `NotFound` if the
/// product does not exist.
pub fn create_payment_pending(
    conn: &mut PgConnection,
    reservation_id: Uuid,
    user_id: i32,
    product_id: i32,
    quantity: i32,
    expires_at: DateTime<Utc>,
    idempotency_key: &str,
) -> QueryResult<Order> {
    conn.transaction(|conn| {
        if let Some(order) = orders::table.find(reservation_id).select(Order::as_select()).first(conn).optional()? {
            return Ok(order);
        }
        let unit_price_cents: i32 = products::table.find(product_id).select(products::price_cents).first(conn)?;

        diesel::insert_into(orders::table)
            .values(&NewOrder {
                id: reservation_id,
                user_id,
                product_id,
                quantity,
                unit_price_cents,
                total_cents: unit_price_cents.saturating_mul(quantity),
                status: OrderStatus::PaymentPending,
                payment_idempotency_key: idempotency_key,
                expires_at,
            })
            .returning(Order::as_returning())
            .get_result(conn)
    })
}

pub fn set_payment_intent(conn: &mut PgConnection, order_id: Uuid, intent_id: &str) -> QueryResult<()> {
    diesel::update(orders::table.find(order_id)).set(orders::payment_intent_id.eq(intent_id)).execute(conn)?;
    Ok(())
}

/// Sells the order's units and confirms it, inside the caller's transaction.
/// The update only takes units still in stock: if Redis and Postgres ever
/// disagree and the stock is short, nothing changes and this returns false
/// (the caller refunds).
pub fn confirm_order(conn: &mut PgConnection, order_id: Uuid, product_id: i32, quantity: i32) -> QueryResult<bool> {
    // A savepoint, so a short stock undoes only this
    let sold = conn.transaction(|conn| {
        let updated = diesel::update(
            inventory::table.filter(inventory::product_id.eq(product_id)).filter(inventory::stock.ge(quantity)),
        )
        .set((inventory::stock.eq(inventory::stock - quantity), inventory::version.eq(inventory::version + 1)))
        .execute(conn)?;
        if updated != 1 {
            return Err(diesel::result::Error::RollbackTransaction);
        }
        Ok(())
    });
    match sold {
        Ok(()) => {
            set_status(conn, order_id, OrderStatus::Confirmed)?;
            Ok(true)
        }
        Err(diesel::result::Error::RollbackTransaction) => Ok(false),
        Err(e) => Err(e),
    }
}

pub fn set_status(conn: &mut PgConnection, order_id: Uuid, status: OrderStatus) -> QueryResult<()> {
    diesel::update(orders::table.find(order_id)).set(orders::status.eq(status)).execute(conn)?;
    Ok(())
}

/// Marks orders whose hold expired during payment `EXPIRED`. Holds that
/// never started payment have no order, so ids without one change nothing.
pub fn expire_payment_pending(conn: &mut PgConnection, ids: &[Uuid]) -> QueryResult<usize> {
    diesel::update(orders::table.filter(orders::id.eq_any(ids)).filter(orders::status.eq(OrderStatus::PaymentPending)))
        .set(orders::status.eq(OrderStatus::Expired))
        .execute(conn)
}

/// Safety net for a cleanup worker that crashed between releasing a hold and
/// marking its order: expires `PAYMENT_PENDING` orders whose hold ended
/// before `before`. If a payment did claim one late, the payment worker still
/// confirms it (it only skips `CONFIRMED` and `REFUNDED` orders).
pub fn expire_stale(conn: &mut PgConnection, before: DateTime<Utc>) -> QueryResult<usize> {
    diesel::update(
        orders::table.filter(orders::status.eq(OrderStatus::PaymentPending)).filter(orders::expires_at.lt(before)),
    )
    .set(orders::status.eq(OrderStatus::Expired))
    .execute(conn)
}

/// Units in confirmed orders of a product, per user (for the rebuild)
pub fn paid_units_by_user(conn: &mut PgConnection, product_id: i32) -> QueryResult<Vec<(i32, i64)>> {
    let rows: Vec<(i32, Option<i64>)> = orders::table
        .filter(orders::product_id.eq(product_id))
        .filter(orders::status.eq(OrderStatus::Confirmed))
        .group_by(orders::user_id)
        .select((orders::user_id, diesel::dsl::sum(orders::quantity)))
        .load(conn)?;
    Ok(rows.into_iter().map(|(user, units)| (user, units.unwrap_or(0))).collect())
}

/// Which of these orders are already `CONFIRMED` (for the rebuild: a hold
/// whose payment committed but is not yet finalized is still in Redis, and
/// its units are already counted as sold)
pub fn confirmed_among(conn: &mut PgConnection, ids: &[Uuid]) -> QueryResult<Vec<Uuid>> {
    orders::table
        .filter(orders::id.eq_any(ids))
        .filter(orders::status.eq(OrderStatus::Confirmed))
        .select(orders::id)
        .load(conn)
}
