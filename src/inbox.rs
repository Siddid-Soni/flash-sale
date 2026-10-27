//! The payment inbox loop of the order processor (roadmap steps 8 and 21):
//! turns each provider event stored by the webhook into a confirmed order or a
//! refund.
//!
//! A successful payment is confirmed in phases, and a crash between any two is
//! repaired by running the event again (it stays in the inbox until the last
//! phase is done):
//!
//! 1. **Claim** the hold in Redis (`confirm.lua claim`: `AWAITING_PAYMENT` to
//!    `PAID`, out of `holds:expiry`). The webhook already did this on receipt;
//!    here the call finds its own claim. If the hold was released, or its
//!    deadline passed, the claim is lost: refund (see below).
//! 2. **Commit** in one PostgreSQL transaction: the order `AWAITING_PAYMENT` to
//!    `CONFIRMED` and `UPDATE products SET sold = sold + 1 WHERE id = $1 AND
//!    sold < total_inventory`. If the guard matches no row the transaction
//!    rolls back and the payment is refunded.
//! 3. **Finalize** in Redis (`confirm.lua finalize`: `sold:{sale}` + 1, hold
//!    `CONFIRMED`), then write `status:<ticket>` `CONFIRMED`.
//! 4. **Mark the event processed**, only now.
//!
//! Why a rerun converges: every step is idempotent and each leaves a trace the
//! next run reads. Before phase 2 commits, the order is `AWAITING_PAYMENT`, so
//! the run starts at phase 1 again. After it commits, the order is `CONFIRMED`:
//! the run goes straight to phase 3 (a crash after the commit but before the
//! finalize leaves the hold `PAID` by this session, which finalize counts
//! once; a finalize that already ran finds the hold `CONFIRMED` and does
//! nothing). Counting `sold:{sale}` only in the finalize, which flips the hold
//! out of `PAID` in the same step, and `products.sold` only in the confirming
//! transaction, which flips the order out of `AWAITING_PAYMENT` in the same
//! step, makes `sold:{sale}` = `products.sold` = number of `CONFIRMED` orders
//! once every event is processed. The status write is forward-only, so
//! repeating it is harmless.
//!
//! A refund (lost claim, or guard failure) goes: refund the provider's
//! payment (a no-op the second time, and only a `PAID` session can be
//! refunded, which a success event guarantees), order `REFUNDED`, then
//! `status` `REFUNDED` (and, after a guard failure, the hold leaves `PAID`).
//! A rerun finds the order `REFUNDED` and repeats only the Redis writes.
//! Stock is not returned on either path: after a lost claim whoever released
//! the hold already returned it, and after a guard failure the unit was
//! never really free.
//!
//! Failure events (the user cancelled) need nothing: the hold runs out by
//! itself. The event is just marked processed.
//!
//! Several processors may run: each locks one inbox row at a time with `FOR
//! UPDATE SKIP LOCKED` for the whole event, on a connection of its own, while
//! the confirming transaction uses another. (If two ever did handle the same
//! event, the order's status guard lets only one confirm it.) An event that
//! fails is recorded (`attempts`, `last_error`), skipped by this processor for
//! a growing delay and retried; the ones behind it carry on.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use diesel::{prelude::*, PgConnection};
use tokio::{runtime::Handle, sync::watch};

use crate::{
    actions::{self, orders::Confirm},
    cache::{self, ClaimOutcome, ReadCache, RedisConn, StatusUpdate},
    db::DbPool,
    models::{Order, OrderStatus, PaymentInboxEntry},
    payment::{EventKind, ProviderEvent, SharedProvider},
    processor::ProcessError,
};

/// How long to wait when the inbox is empty
const IDLE_WAIT: Duration = Duration::from_millis(250);
/// Pause after a failed event before looking at the next one
const ERROR_WAIT: Duration = Duration::from_millis(100);
/// A failing event is skipped by this processor for 1 s, 2 s, 4 s ... up to this
const MAX_EVENT_BACKOFF: Duration = Duration::from_secs(60);

/// What handling one event came to
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Handled {
    /// The order is `CONFIRMED`, counted in PostgreSQL and in Redis
    Confirmed,
    /// The payment was refunded and the order is `REFUNDED`
    Refunded,
    /// A failure or unknown event: nothing to do
    NoAction,
    /// An event this backend cannot act on (a ticket that names no sale); logged
    Ignored,
}

/// Confirms or refunds paid orders from the payment inbox
pub struct InboxWorker {
    pool: DbPool,
    inventory: RedisConn,
    read_cache: ReadCache,
    provider: SharedProvider,
    /// Events that failed here: id -> (failures, skipped until)
    backoff: Mutex<HashMap<i64, (u32, Instant)>>,
}

impl InboxWorker {
    pub fn new(pool: DbPool, inventory: RedisConn, read_cache: ReadCache, provider: SharedProvider) -> Self {
        Self { pool, inventory, read_cache, provider, backoff: Mutex::new(HashMap::new()) }
    }

    async fn db<T, F>(&self, work: F) -> Result<T, ProcessError>
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
        .map_err(|e| ProcessError::Join(e.to_string()))?
    }

    async fn write_status(&self, order: &Order, status: OrderStatus) -> Result<(), ProcessError> {
        let mut update = StatusUpdate::new(status, order.user_id);
        update.sale = Some(order.product_id);
        let mut cache = self.read_cache.clone();
        cache::set_status(&mut cache, &order.queue_ticket_id, &update).await?;
        Ok(())
    }

    /// Phase 1: claims the order's hold for the event's payment
    pub async fn claim(&self, order: &Order, event: &ProviderEvent) -> Result<ClaimOutcome, ProcessError> {
        let payment_id = event.payment_id.as_deref().unwrap_or(&event.session_id);
        let mut con = self.inventory.clone();
        Ok(cache::claim_hold(&mut con, order.product_id, &order.queue_ticket_id, &event.session_id, payment_id).await?)
    }

    /// Phase 2: the confirming transaction
    pub async fn commit(&self, order: &Order) -> Result<Confirm, ProcessError> {
        let (ticket, product) = (order.queue_ticket_id.clone(), order.product_id);
        self.db(move |conn| actions::orders::confirm_order(conn, &ticket, product)).await
    }

    /// Phase 3: counts the sale in Redis and tells the client. Safe to repeat.
    pub async fn finalize(&self, order: &Order) -> Result<(), ProcessError> {
        let session = order.payment_session_id.as_deref().unwrap_or_default();
        let mut con = self.inventory.clone();
        cache::finalize_hold(&mut con, order.product_id, &order.queue_ticket_id, session).await?;
        self.write_status(order, OrderStatus::Confirmed).await
    }

    /// Refunds the payment of `session_id`; a repeat is a no-op
    async fn refund(&self, session_id: &str) -> Result<(), ProcessError> {
        Ok(self.provider.refund(session_id).await?)
    }

    /// The Redis side of a refunded order: the hold leaves `PAID` (only
    /// matters after a guard failure) and the client is told. Safe to repeat.
    async fn finish_refund(&self, order: &Order) -> Result<(), ProcessError> {
        let session = order.payment_session_id.as_deref().unwrap_or_default();
        let mut con = self.inventory.clone();
        cache::close_refunded_hold(&mut con, order.product_id, &order.queue_ticket_id, session).await?;
        self.write_status(order, OrderStatus::Refunded).await
    }

    /// Refunds the payment, marks the order `REFUNDED`, tells Redis
    async fn refund_order(&self, order: &Order, session_id: &str) -> Result<Handled, ProcessError> {
        self.refund(session_id).await?;
        let ticket = order.queue_ticket_id.clone();
        self.db(move |conn| actions::orders::mark_refunded(conn, &ticket)).await?;
        self.finish_refund(order).await?;
        Ok(Handled::Refunded)
    }

    /// Handles one verified event, all phases. Safe to run again after any failure.
    pub async fn handle(&self, event: &ProviderEvent) -> Result<Handled, ProcessError> {
        if event.kind != EventKind::PaymentSucceeded {
            log::info!("Event {} ({}) needs no action; the hold runs out by itself", event.id, event.event_type);
            return Ok(Handled::NoAction);
        }
        let Some(sale) = cache::ticket_sale(&event.ticket) else {
            log::error!("Event {} names the ticket {:?}, which names no sale; ignored", event.id, event.ticket);
            return Ok(Handled::Ignored);
        };
        let ticket = event.ticket.clone();
        let Some(order) = self.db(move |conn| actions::orders::find_by_ticket(conn, &ticket)).await? else {
            // Sessions are made after their order, so this payment is not ours to keep
            log::warn!("Payment for {} has no order; refunding", event.ticket);
            self.refund(&event.session_id).await?;
            return Ok(Handled::Refunded);
        };
        if order.payment_session_id.as_deref() != Some(event.session_id.as_str()) || order.product_id != sale {
            // A payment on a session the order does not own: give that money
            // back and leave the order to its own session
            log::error!("Payment session {} is not the session of order {}; refunding it", event.session_id, order.queue_ticket_id);
            self.refund(&event.session_id).await?;
            return Ok(Handled::Refunded);
        }

        match order.status {
            // The commit happened earlier: only the Redis side may be missing
            OrderStatus::Confirmed => {
                self.finalize(&order).await?;
                Ok(Handled::Confirmed)
            }
            OrderStatus::Refunded => {
                self.finish_refund(&order).await?;
                Ok(Handled::Refunded)
            }
            _ => match self.claim(&order, event).await? {
                ClaimOutcome::Won => self.confirm(order, event).await,
                ClaimOutcome::Gone => {
                    log::info!("The hold of {} ended before payment {} arrived; refunding", order.queue_ticket_id, event.session_id);
                    self.refund_order(&order, &event.session_id).await
                }
            },
        }
    }

    /// Phases 2 and 3 for an order whose claim is won
    async fn confirm(&self, order: Order, event: &ProviderEvent) -> Result<Handled, ProcessError> {
        match self.commit(&order).await? {
            Confirm::Confirmed => {
                log::info!("Order {} is CONFIRMED", order.queue_ticket_id);
                self.finalize(&order).await?;
                Ok(Handled::Confirmed)
            }
            Confirm::SoldOut => {
                log::error!(
                    "Order {}: PostgreSQL has no unit of product {} left although Redis reserved one; refunding",
                    order.queue_ticket_id,
                    order.product_id
                );
                self.refund_order(&order, &event.session_id).await
            }
            Confirm::NotAwaiting => {
                // Another processor got there first: do what its result needs
                let ticket = order.queue_ticket_id.clone();
                let current = self.db(move |conn| actions::orders::find_by_ticket(conn, &ticket)).await?;
                match current {
                    Some(o) if o.status == OrderStatus::Confirmed => {
                        self.finalize(&o).await?;
                        Ok(Handled::Confirmed)
                    }
                    Some(o) if o.status == OrderStatus::Refunded => {
                        self.finish_refund(&o).await?;
                        Ok(Handled::Refunded)
                    }
                    other => Err(ProcessError::Invalid(format!(
                        "order {} won its claim but is {:?}",
                        order.queue_ticket_id,
                        other.map(|o| o.status)
                    ))),
                }
            }
        }
    }

    /// Handles one inbox row: reads its event and acts on it
    async fn process_entry(&self, entry: &PaymentInboxEntry) -> Result<(), ProcessError> {
        match ProviderEvent::from_json(entry.payload.clone()) {
            Some(event) => {
                let handled = self.handle(&event).await?;
                log::debug!("Inbox event {}: {handled:?}", entry.provider_event_id);
            }
            // The webhook parses before it stores, so this is not expected
            None => log::error!("Inbox event {} cannot be read; marked processed", entry.provider_event_id),
        }
        Ok(())
    }

    /// Ids this processor is still backing off from
    fn skipped(&self) -> Vec<i64> {
        let now = Instant::now();
        let mut backoff = self.backoff.lock().unwrap_or_else(|p| p.into_inner());
        backoff.retain(|_, (_, until)| *until > now);
        backoff.keys().copied().collect()
    }

    fn note_failure(&self, id: i64) {
        let mut backoff = self.backoff.lock().unwrap_or_else(|p| p.into_inner());
        let failures = backoff.get(&id).map_or(0, |(n, _)| *n) + 1;
        let wait = Duration::from_secs(1u64 << (failures - 1).min(6)).min(MAX_EVENT_BACKOFF);
        backoff.insert(id, (failures, Instant::now() + wait));
    }

    fn note_success(&self, id: i64) {
        self.backoff.lock().unwrap_or_else(|p| p.into_inner()).remove(&id);
    }

    /// Locks and handles the oldest unprocessed event that no other processor
    /// holds. Returns false if there was none. On failure the event is
    /// recorded (`attempts`, `last_error`) and stays in the inbox.
    pub async fn process_next(self: &Arc<Self>) -> Result<bool, ProcessError> {
        let (me, runtime) = (self.clone(), Handle::current());
        tokio::task::spawn_blocking(move || me.process_next_blocking(&runtime))
            .await
            .map_err(|e| ProcessError::Join(e.to_string()))?
    }

    /// On a blocking thread: the row lock lives in a transaction on this
    /// connection while the async phases run on the runtime
    fn process_next_blocking(&self, runtime: &Handle) -> Result<bool, ProcessError> {
        let mut conn = self.pool.get()?;
        let skip = self.skipped();
        let mut current = None;
        let result = conn.transaction::<_, ProcessError, _>(|conn| {
            let Some(entry) = actions::payments::claim_next(conn, &skip)? else { return Ok(false) };
            current = Some(entry.id);
            runtime.block_on(self.process_entry(&entry))?;
            // Only after the last phase
            actions::payments::mark_processed(conn, entry.id)?;
            Ok(true)
        });
        match (result, current) {
            (Ok(found), id) => {
                if let Some(id) = id {
                    self.note_success(id);
                }
                Ok(found)
            }
            (Err(e), Some(id)) => {
                // Rolled back and unlocked: record why, and look elsewhere for a while
                if let Err(record_error) = actions::payments::record_failure(&mut conn, id, &e.to_string()) {
                    log::error!("Recording the failure of inbox event {id} failed: {record_error}");
                }
                self.note_failure(id);
                Err(e)
            }
            (Err(e), None) => Err(e),
        }
    }

    /// Processes events until `shutdown` is set, finishing the event in hand
    pub async fn run(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) {
        log::info!("Processing the payment inbox");
        while !*shutdown.borrow() {
            let wait = match self.process_next().await {
                Ok(true) => continue,
                Ok(false) => IDLE_WAIT,
                Err(e) => {
                    log::error!("Processing a payment event failed (it will be retried): {e}");
                    ERROR_WAIT
                }
            };
            tokio::select! {
                _ = tokio::time::sleep(wait) => {}
                _ = shutdown.changed() => {}
            }
        }
        log::info!("Payment inbox processing stopped");
    }
}
