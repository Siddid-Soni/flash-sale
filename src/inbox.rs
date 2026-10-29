//! Payment events: the order processor turns each provider event into a
//! confirmed order or a refund, with
//! PostgreSQL alone deciding between a payment and the payment deadline.
//!
//! `InboxWorker::handle_event` is the one place an event is acted on. It runs
//! from two callers, so a payment is confirmed at once and still converges
//! after any failure:
//!
//! - **on receipt**: the webhook endpoint of the order processor
//!   (`crate::api::payments`) stores the event in `payment_inbox`, then calls
//!   `receive`, which handles it and marks the row processed;
//! - **on retry**: the inbox loop (`run`) takes every row not yet processed
//!   and handles it again.
//!
//! **The decision** is one PostgreSQL transaction (`actions::orders::settle_payment`):
//! the order goes `AWAITING_PAYMENT` to `CONFIRMED` only if, at PostgreSQL's
//! own `now()`, its session matches and its deadline is ahead, together with
//! `UPDATE products SET sold = sold + 1 WHERE id = $1 AND sold <
//! total_inventory` (no row: the confirmation rolls back). The expiration
//! worker's `UPDATE ... SET status = 'EXPIRED' ... AND expires_at <= now()`
//! is its mirror image; both take the order's row lock and see each other's
//! result afterwards, on one clock, so exactly one of them wins an order. There
//! is no claim in Redis: the payment path touches no inventory key, only the
//! read-cache status.
//!
//! **The inbox never decides a deadline.** A payment for an order that is
//! still `AWAITING_PAYMENT` after its `expires_at` changes nothing: the event
//! stays unprocessed and the inbox loop retries it, until the expiration worker
//! has expired the order (and returned the unit). The retry then finds the order
//! `EXPIRED` and refunds, moving no stock. Only the worker returns stock.
//!
//! **If the order cannot be confirmed** (it is already `EXPIRED` or `FAILED`,
//! or the in-time payment met a failed guard) the same transaction marks it
//! `REFUNDED`. That is the durable record of the decision. Afterwards, outside
//! any transaction, the provider is asked to refund the session and
//! `status:<ticket>` is set `REFUNDED`; only when both worked is the inbox row
//! marked processed. A guard failure means Redis and PostgreSQL disagree (Redis
//! reserved a unit PostgreSQL does not have) and is logged as an error; the
//! refund returns no stock either.
//!
//! **Why a rerun converges.** Every step is idempotent and the decision
//! leaves a trace the next run reads:
//!
//! - crash before the decision commits: nothing happened, the run starts over;
//! - crash after it commits, before the refund call: the order is `REFUNDED`,
//!   the run goes straight to the refund;
//! - crash after the refund, before the row is marked processed: the provider's
//!   refund is idempotent per session, so the run repeats it harmlessly;
//! - a status write that failed is repeated (a `CONFIRMED` order only needs
//!   that write); the write only moves forward.
//!
//! Two handlers may run the same event at once (the webhook request and the
//! inbox loop); the row lock of the decision lets one decide and the other see
//! the result. Stock is never returned here and no inventory key is touched.
//!
//! Failure events (the user cancelled) need nothing: the order runs out by
//! itself. The event is just marked processed.
//!
//! Several processors may run: each locks one inbox row at a time with `FOR
//! UPDATE SKIP LOCKED` for the whole event, on a connection of its own. An
//! event that fails is recorded (`attempts`, `last_error`), skipped by this
//! processor for a growing delay and retried; the ones behind it carry on.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use diesel::{prelude::*, PgConnection};
use tokio::{runtime::Handle, sync::watch};

use crate::{
    actions::{self, orders::Settlement},
    cache::{self, ReadCache, StatusUpdate},
    db::DbPool,
    models::{NewPaymentInboxEntry, Order, OrderStatus, PaymentInboxEntry},
    payment::{EventKind, ProviderEvent, SharedProvider},
    processor::ProcessError,
};

/// How long to wait when the inbox is empty
const IDLE_WAIT: Duration = Duration::from_millis(250);
/// Pause after a failed event before looking at the next one
const ERROR_WAIT: Duration = Duration::from_millis(100);
/// A failing event is skipped by this processor for 1 s, 2 s, 4 s ... up to this
const MAX_EVENT_BACKOFF: Duration = Duration::from_secs(60);
/// An event for an order past its deadline but not yet expired is skipped for this long
const POSTPONED_WAIT: Duration = Duration::from_secs(1);

/// What handling one event came to
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Handled {
    /// The order is `CONFIRMED` in PostgreSQL and the status says so
    Confirmed,
    /// The payment was refunded at the provider and the order is `REFUNDED`
    Refunded,
    /// The order is past its deadline but the expiration worker has not
    /// expired it yet: nothing was decided and the event must stay in the inbox
    Postponed,
    /// A failure or unknown event: nothing to do
    NoAction,
    /// An event this backend cannot act on (a ticket that names no sale); logged
    Ignored,
}

/// What `receive` did with a webhook event
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Received {
    /// The event had been stored and processed before: nothing to do
    Duplicate,
    /// Stored (now or before) and handled at once; the row is marked processed
    Handled(Handled),
    /// Stored, but not decided: handling failed, or the order is past its
    /// deadline and waits for the expiration worker. The row stays unprocessed
    /// and the inbox loop retries it.
    Deferred(String),
}

/// Confirms or refunds paid orders from provider events
pub struct InboxWorker {
    pool: DbPool,
    read_cache: ReadCache,
    provider: SharedProvider,
    /// Events that failed here: id -> (failures, skipped until)
    backoff: Mutex<HashMap<i64, (u32, Instant)>>,
}

impl InboxWorker {
    pub fn new(pool: DbPool, read_cache: ReadCache, provider: SharedProvider) -> Self {
        Self { pool, read_cache, provider, backoff: Mutex::new(HashMap::new()) }
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

    /// Refunds the payment of `session_id`; a repeat is a no-op
    async fn refund(&self, session_id: &str) -> Result<(), ProcessError> {
        Ok(self.provider.refund(session_id).await?)
    }

    /// After the decision recorded `REFUNDED`: the provider refund, then the
    /// status. Safe to repeat.
    async fn finish_refund(&self, order: &Order, session_id: &str) -> Result<Handled, ProcessError> {
        self.refund(session_id).await?;
        self.write_status(order, OrderStatus::Refunded).await?;
        Ok(Handled::Refunded)
    }

    /// Handles one verified event, all of it. Safe to run again after any
    /// failure and while another caller runs the same event.
    pub async fn handle_event(&self, event: &ProviderEvent) -> Result<Handled, ProcessError> {
        if event.kind != EventKind::PaymentSucceeded {
            log::info!("Event {} ({}) needs no action; the order runs out by itself", event.id, event.event_type);
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

        // The decision: one transaction, one clock
        let (ticket, session_id) = (order.queue_ticket_id.clone(), event.session_id.clone());
        let settlement = self.db(move |conn| actions::orders::settle_payment(conn, &ticket, &session_id, sale)).await?;
        match settlement {
            Settlement::Confirmed => {
                log::info!("Order {} is CONFIRMED", order.queue_ticket_id);
                self.write_status(&order, OrderStatus::Confirmed).await?;
                Ok(Handled::Confirmed)
            }
            // Decided by an earlier run (or the other caller): only the status may be missing
            Settlement::AlreadyConfirmed => {
                self.write_status(&order, OrderStatus::Confirmed).await?;
                Ok(Handled::Confirmed)
            }
            Settlement::Refund => {
                log::info!("Order {} is no longer payable; refunding payment {}", order.queue_ticket_id, event.session_id);
                self.finish_refund(&order, &event.session_id).await
            }
            Settlement::Postponed => {
                log::info!(
                    "Order {} is past its deadline but not expired yet; payment {} waits for the expiration worker",
                    order.queue_ticket_id,
                    event.session_id
                );
                Ok(Handled::Postponed)
            }
            Settlement::SoldOut => {
                log::error!(
                    "Order {}: PostgreSQL has no unit of product {} left although Redis reserved one; refunding",
                    order.queue_ticket_id,
                    order.product_id
                );
                self.finish_refund(&order, &event.session_id).await
            }
            Settlement::AlreadyRefunded => self.finish_refund(&order, &event.session_id).await,
            Settlement::Unexpected(status) => Err(ProcessError::Invalid(format!(
                "order {} has a payment but is {status:?}",
                order.queue_ticket_id
            ))),
            Settlement::Missing => Err(ProcessError::Invalid(format!("order {} vanished", order.queue_ticket_id))),
        }
    }

    /// A verified webhook event arrives: stores it in the inbox, handles it at
    /// once and marks the row processed. An error is returned only if the event
    /// could not be stored (the provider must retry); a handling error after
    /// that is recorded and the inbox loop retries, so the provider need not.
    pub async fn receive(&self, event: &ProviderEvent) -> Result<Received, ProcessError> {
        let stored = event.clone();
        let (id, processed) = self
            .db(move |conn| {
                let entry =
                    NewPaymentInboxEntry { provider_event_id: &stored.id, event_type: &stored.event_type, payload: &stored.payload };
                actions::payments::insert_inbox(conn, &entry)?;
                let row = actions::payments::find_inbox(conn, &stored.id)?;
                Ok((row.id, row.processed_at.is_some()))
            })
            .await?;
        if processed {
            log::debug!("Webhook event {} was already processed", event.id);
            return Ok(Received::Duplicate);
        }
        match self.handle_event(event).await {
            // Nothing decided: the row stays open for the loop
            Ok(Handled::Postponed) => Ok(Received::Deferred("the order is past its deadline and not expired yet".into())),
            Ok(handled) => {
                log::debug!("Webhook event {}: {handled:?}", event.id);
                match self.db(move |conn| actions::payments::mark_processed(conn, id)).await {
                    Ok(()) => Ok(Received::Handled(handled)),
                    // Handled, but the row is still open: the loop repeats the (idempotent) handling
                    Err(e) => Ok(Received::Deferred(e.to_string())),
                }
            }
            Err(e) => {
                log::warn!("Handling webhook event {} failed, the inbox loop will retry: {e}", event.id);
                let reason = e.to_string();
                let recorded = reason.clone();
                if let Err(record_error) = self.db(move |conn| actions::payments::record_failure(conn, id, &recorded)).await {
                    log::error!("Recording the failure of inbox event {id} failed: {record_error}");
                }
                Ok(Received::Deferred(reason))
            }
        }
    }

    /// Handles one inbox row: reads its event and acts on it. Returns whether
    /// the event is done (false: `Postponed`, leave the row unprocessed).
    async fn process_entry(&self, entry: &PaymentInboxEntry) -> Result<bool, ProcessError> {
        match ProviderEvent::from_json(entry.payload.clone()) {
            Some(event) => {
                let handled = self.handle_event(&event).await?;
                log::debug!("Inbox event {}: {handled:?}", entry.provider_event_id);
                Ok(handled != Handled::Postponed)
            }
            // The webhook parses before it stores, so this is not expected
            None => {
                log::error!("Inbox event {} cannot be read; marked processed", entry.provider_event_id);
                Ok(true)
            }
        }
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

    /// The event waits for the expiration worker: skip it for a second, without
    /// counting a failure
    fn note_postponed(&self, id: i64) {
        let mut backoff = self.backoff.lock().unwrap_or_else(|p| p.into_inner());
        backoff.insert(id, (0, Instant::now() + POSTPONED_WAIT));
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
        let mut postponed = false;
        let result = conn.transaction::<_, ProcessError, _>(|conn| {
            let Some(entry) = actions::payments::claim_next(conn, &skip)? else { return Ok(false) };
            current = Some(entry.id);
            if runtime.block_on(self.process_entry(&entry))? {
                // Only after the last phase
                actions::payments::mark_processed(conn, entry.id)?;
            } else {
                postponed = true;
            }
            Ok(true)
        });
        match (result, current) {
            (Ok(found), id) => {
                if let Some(id) = id {
                    if postponed {
                        self.note_postponed(id);
                    } else {
                        self.note_success(id);
                    }
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
