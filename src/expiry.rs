//! The expiration worker's logic: expire the unpaid
//! orders whose deadline has passed and give each one's unit back to stock.
//! Driven by PostgreSQL alone: Redis keeps no timer and no per-ticket state.
//!
//! Two passes, each a method of `Expirer` that the binary calls on a timer and
//! the tests call directly:
//!
//! - `expire_pass`: one statement expires a batch (`UPDATE orders SET status =
//!   'EXPIRED' ... WHERE status = 'AWAITING_PAYMENT' AND expires_at <= now()
//!   ... FOR UPDATE SKIP LOCKED RETURNING ...`, the mirror image of the
//!   payment's confirmation, see `crate::inbox`). **Only this worker returns
//!   stock**, besides the Order Service's compensation for a failed publish.
//!   For each order the statement returned, once and without retry,
//!   `return_unit.lua` runs: `INCR stock:{sale}`, and `user_live` is deleted if
//!   it still names the ticket. A Redis error is logged with the ticket and the
//!   worker moves on: the unit is lost for the rest of the sale, which is
//!   accepted (`docs/FAILURE_CASES.md`, case 2) because the worker cannot tell,
//!   after a failure, whether the `INCR` ran, and a second one could sell a
//!   unit twice. Then the read-cache status becomes `EXPIRED` and the provider
//!   session is expired; those two are idempotent and retried (a failure keeps
//!   the order in memory for the next pass).
//! - `sweep`: finishes the status and the session of recently `EXPIRED` orders
//!   whose worker died after the update. It never returns stock.
//!
//! Any number of workers may run at once. `SKIP LOCKED` and the status change
//! hand each order to exactly one statement, so each expired unit is returned
//! once; a late payment is decided by the same row lock (`crate::inbox` leaves
//! an order the worker has not expired yet for a retry, so the inbox never
//! moves stock).
//!
//! **The order of the steps matters.** The order is decided first, in
//! PostgreSQL; Redis follows. The session is expired only after the unit is
//! back, never before.

use std::sync::Mutex;

use chrono::{DateTime, Utc};
use diesel::PgConnection;

use crate::{
    actions,
    cache::{self, ReadCache, RedisConn, StatusUpdate},
    config::Config,
    db::DbPool,
    models::{Order, OrderStatus},
    payment::{PaymentError, SharedProvider},
};

#[derive(Debug, thiserror::Error)]
pub enum ExpiryError {
    #[error("database: {0}")]
    Db(#[from] diesel::result::Error),
    #[error("database pool: {0}")]
    Pool(#[from] diesel::r2d2::PoolError),
    #[error("redis: {0}")]
    Redis(#[from] redis::RedisError),
    #[error("{0}")]
    Payment(#[from] PaymentError),
    #[error("blocking task: {0}")]
    Join(String),
}

/// What the expiration worker settings are
#[derive(Debug, Clone)]
pub struct Settings {
    /// Orders expired per pass (and per sweep page)
    pub batch: usize,
    /// An `EXPIRED` order is old enough for the sweep this long after the change
    pub stale_after: chrono::Duration,
    /// How long after becoming old enough the sweep keeps re-checking an
    /// `EXPIRED` order's status and session
    pub expired_recheck: chrono::Duration,
}

impl Settings {
    pub fn from_config(config: &Config) -> Self {
        Self {
            batch: config.expiry_batch.max(1),
            stale_after: chrono::Duration::minutes(10),
            expired_recheck: chrono::Duration::minutes(30),
        }
    }
}

/// What a pass did
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Pass {
    /// Orders this call expired
    pub expired: usize,
    /// Of those, the ones whose unit could not be returned (Redis failed; each
    /// logged with its ticket)
    pub lost: usize,
    /// The batch came back full: another pass is due at once
    pub more: bool,
}

/// What the sweep repaired
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Swept {
    /// `EXPIRED` orders whose status or session step had been missed
    pub repaired: usize,
}

pub struct Expirer {
    pool: DbPool,
    inventory: RedisConn,
    read_cache: ReadCache,
    provider: SharedProvider,
    settings: Settings,
    /// Expired orders whose status or session step failed; finished before the
    /// next pass (their unit is already back)
    unfinished: Mutex<Vec<Order>>,
}

impl Expirer {
    pub fn new(
        pool: DbPool,
        inventory: RedisConn,
        read_cache: ReadCache,
        provider: SharedProvider,
        settings: Settings,
    ) -> Self {
        Self { pool, inventory, read_cache, provider, settings, unfinished: Mutex::new(Vec::new()) }
    }

    async fn db<T, F>(&self, work: F) -> Result<T, ExpiryError>
    where
        T: Send + 'static,
        F: FnOnce(&mut PgConnection) -> Result<T, diesel::result::Error> + Send + 'static,
    {
        let pool = self.pool.clone();
        tokio::task::spawn_blocking(move || {
            let mut conn = pool.get()?;
            Ok(work(&mut conn)?)
        })
        .await
        .map_err(|e| ExpiryError::Join(e.to_string()))?
    }

    // ---- After the update ----

    /// An order is `EXPIRED`: the status, then the payment session. Safe to
    /// repeat. The provider leaves a session that was paid meanwhile as it is
    /// (the inbox refunds it), so this needs no look at the order's state.
    async fn finish_expired(&self, order: &Order) -> Result<(), ExpiryError> {
        let mut update = StatusUpdate::new(OrderStatus::Expired, order.user_id);
        update.sale = Some(order.product_id);
        let mut cache = self.read_cache.clone();
        cache::set_status(&mut cache, &order.queue_ticket_id, &update).await?;
        if let Some(session) = &order.payment_session_id {
            self.provider.expire_session(session).await?;
        }
        Ok(())
    }

    /// Finishes the orders whose follow-up failed earlier. The ones that fail
    /// again stay for the next pass; returns the first error.
    async fn finish_unfinished(&self) -> Result<(), ExpiryError> {
        let waiting = std::mem::take(&mut *self.unfinished.lock().unwrap_or_else(|p| p.into_inner()));
        let mut first_error = None;
        let mut rest = waiting.into_iter();
        while let Some(order) = rest.next() {
            if let Err(e) = self.finish_expired(&order).await {
                // A dead read cache or provider would fail every remaining
                // order the same way: keep them without trying
                let mut unfinished = self.unfinished.lock().unwrap_or_else(|p| p.into_inner());
                unfinished.push(order);
                unfinished.extend(rest);
                first_error = Some(e);
                break;
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    /// Returns the unit of an order this worker just expired: once, without
    /// retry. Returns whether it worked; a failure is logged with the ticket.
    async fn return_unit(&self, order: &Order) -> bool {
        let mut con = self.inventory.clone();
        match cache::return_unit(&mut con, order.product_id, order.user_id, &order.queue_ticket_id, None).await {
            Ok(()) => true,
            Err(e) => {
                log::error!(
                    "Could not return the unit of {} (sale {}, user {}); it is lost for the rest of the sale: {e}",
                    order.queue_ticket_id,
                    order.product_id,
                    order.user_id
                );
                false
            }
        }
    }

    // ---- The passes ----

    /// Expires one batch of due orders and returns their units (see the module
    /// docs). Orders the batch expired are all handled even if a follow-up step
    /// fails, since their units must come back; the first such error is returned
    /// at the end, and those orders are retried by the next pass.
    pub async fn expire_pass(&self) -> Result<Pass, ExpiryError> {
        let carried = self.finish_unfinished().await;

        let limit = self.settings.batch as i64;
        let expired = self.db(move |conn| actions::orders::expire_batch(conn, limit)).await?;
        let mut pass = Pass { expired: expired.len(), lost: 0, more: expired.len() as i64 >= limit };

        let mut failure: Option<ExpiryError> = None;
        for order in expired {
            if !self.return_unit(&order).await {
                pass.lost += 1;
            }
            if failure.is_some() {
                self.unfinished.lock().unwrap_or_else(|p| p.into_inner()).push(order);
                continue;
            }
            if let Err(e) = self.finish_expired(&order).await {
                self.unfinished.lock().unwrap_or_else(|p| p.into_inner()).push(order);
                failure = Some(e);
            }
        }
        match (failure, carried) {
            (Some(e), _) | (None, Err(e)) => Err(e),
            (None, Ok(())) => Ok(pass),
        }
    }

    /// Sweep for the follow-up of `EXPIRED` orders. A worker that died after
    /// the update leaves an `EXPIRED` order, maybe with a stale status and an
    /// open session. Orders changed between `stale_after` and `stale_after +
    /// expired_recheck` ago whose read-cache status is not `EXPIRED` get their
    /// status and session finished. Never returns stock.
    pub async fn sweep(&self, now: DateTime<Utc>) -> Result<Swept, ExpiryError> {
        let before = now - self.settings.stale_after;
        let since = before - self.settings.expired_recheck;
        let limit = self.settings.batch as i64;
        let mut swept = Swept::default();

        let mut after = 0;
        loop {
            let page = self.db(move |conn| actions::orders::recently_expired(conn, since, before, after, limit)).await?;
            for order in &page {
                let mut cache = self.read_cache.clone();
                let cached = cache::read_status(&mut cache, &order.queue_ticket_id).await?;
                if cached.is_none_or(|c| c.status != OrderStatus::Expired) {
                    self.finish_expired(order).await?;
                    swept.repaired += 1;
                }
            }
            match page.last() {
                Some(last) if page.len() as i64 == limit => after = last.id,
                _ => break,
            }
        }
        Ok(swept)
    }
}

// ---- Logging outages ----

/// Remembers which parts of the worker are failing, so an outage is logged
/// once when it starts and once when it ends, not on every retry.
#[derive(Debug, Default)]
pub struct Outages {
    failing: std::collections::HashSet<&'static str>,
}

impl Outages {
    /// Records a failure of `what`. Returns true (and logs an error) only the
    /// first time since it last worked.
    pub fn failed(&mut self, what: &'static str, error: &dyn std::fmt::Display) -> bool {
        let first = self.failing.insert(what);
        if first {
            log::error!("{what} failed, retrying with backoff: {error}");
        } else {
            log::debug!("{what} still failing: {error}");
        }
        first
    }

    /// Records that `what` worked. Returns true (and logs) if it had been failing.
    pub fn worked(&mut self, what: &'static str) -> bool {
        let was_failing = self.failing.remove(what);
        if was_failing {
            log::info!("{what} works again");
        }
        was_failing
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outages_are_logged_once_each() {
        let mut outages = Outages::default();
        assert!(outages.failed("redis", &"down"));
        assert!(!outages.failed("redis", &"still down"));
        assert!(outages.failed("kafka", &"down"));
        assert!(outages.worked("redis"));
        assert!(!outages.worked("redis"));
        assert!(outages.failed("redis", &"down again"));
    }
}
