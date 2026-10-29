//! The order processor's logic: turn each checkout
//! event into an order and a payment link.
//!
//! - `Processor::handle` is the per-message work, in this order: (a) the price;
//!   (b) the payment session (the provider's idempotency key is the ticket);
//!   (c) the order, inserted directly as `AWAITING_PAYMENT` with the session,
//!   link and deadline in one INSERT (`ON CONFLICT DO NOTHING`, then read back
//!   by ticket); (d) the link written to the status entry. If the ticket's row
//!   is already `FAILED` (the Order Service could not publish it and recorded
//!   that in PostgreSQL, yet the message arrived) the session is expired and
//!   nothing else is written. If another constraint refuses the order (the user
//!   already has a live one) there is no row: the session is expired and the
//!   ticket is reported `FAILED`. The processor touches no inventory key: Redis
//!   gates admission only, and the unit of an order that is never paid comes
//!   back through the expiration worker (or the Order Service's compensation).
//!   Every step is idempotent, because a message is redelivered after a crash
//!   or a rebalance, so the whole thing can simply run again. `PENDING` is never
//!   stored in PostgreSQL: it exists only in the read cache, while the message
//!   waits in Kafka.
//! - `PartitionProgress` tracks which offsets of one partition are done and
//!   which prefix is complete; only that prefix may be committed.
//! - `run` is the Kafka consumer loop around both (see its docs).

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use diesel::{result::DatabaseErrorKind, PgConnection};
use rdkafka::{
    consumer::{
        stream_consumer::StreamConsumer, BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance,
    },
    error::KafkaError,
    ClientConfig, ClientContext, Message, Offset, TopicPartitionList,
};
use tokio::sync::{mpsc, watch, Semaphore};

use crate::{
    actions,
    cache::{self, ReadCache, StatusUpdate},
    config::Config,
    db::DbPool,
    models::{NewOrder, OrderState, OrderStatus},
    payment::{PaymentError, SharedProvider},
    queue::CheckoutEvent,
};

/// The consumer group the order processors share
pub const GROUP_ID: &str = "order-processors";

/// How a message ended. All of these are final: the message is done and its
/// offset may be committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The order and the payment session exist and the status entry carries
    /// the link (also a redelivery: the status entry was rewritten)
    Linked,
    /// The ticket's order is `FAILED` (its publish was reported as failed): the
    /// session was expired and nothing else was written
    Failed,
    /// The ticket's order is already past `AWAITING_PAYMENT` (a redelivery long
    /// after it was paid or expired): nothing was written
    Skipped,
    /// No order could be made for the message (the user already has a live
    /// order, an unknown product, a malformed ticket)
    Dropped,
}

#[derive(Debug, thiserror::Error)]
pub enum ProcessError {
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
    /// A state that should be impossible; the event stays in the inbox with this as its error
    #[error("{0}")]
    Invalid(String),
}

/// Creates orders and payment links from checkout events
pub struct Processor {
    pool: DbPool,
    /// Waiting for a database connection happens here, not in a blocking thread
    db_slots: Semaphore,
    read_cache: ReadCache,
    provider: SharedProvider,
    payment_window: chrono::Duration,
    /// Price per sale: a product's price does not change during its sale
    prices: Mutex<HashMap<i32, i32>>,
}

impl Processor {
    pub fn new(pool: DbPool, read_cache: ReadCache, provider: SharedProvider, payment_window_secs: u64) -> Self {
        Self {
            db_slots: Semaphore::new(pool.max_size() as usize),
            pool,
            read_cache,
            provider,
            payment_window: chrono::Duration::seconds(payment_window_secs as i64),
            prices: Mutex::new(HashMap::new()),
        }
    }

    /// Runs blocking Diesel work on the blocking pool, holding a connection
    /// only for its duration
    async fn db<T, F>(&self, work: F) -> Result<T, ProcessError>
    where
        T: Send + 'static,
        F: FnOnce(&mut PgConnection) -> Result<T, diesel::result::Error> + Send + 'static,
    {
        let _slot = self.db_slots.acquire().await.expect("the semaphore is never closed");
        let pool = self.pool.clone();
        tokio::task::spawn_blocking(move || {
            let mut conn = pool.get()?;
            Ok(work(&mut conn)?)
        })
        .await
        .map_err(|e| ProcessError::Join(e.to_string()))?
    }

    /// The price of a sale's product in cents; `None` if there is no such product
    async fn price(&self, sale: i32) -> Result<Option<i32>, ProcessError> {
        if let Some(price) = self.prices.lock().unwrap_or_else(|p| p.into_inner()).get(&sale) {
            return Ok(Some(*price));
        }
        let product = self.db(move |conn| actions::products::find_product(conn, sale)).await?;
        if let Some(product) = &product {
            self.prices.lock().unwrap_or_else(|p| p.into_inner()).insert(sale, product.price_cents);
        }
        Ok(product.map(|p| p.price_cents))
    }

    async fn write_status(&self, ticket: &str, update: &StatusUpdate) -> Result<(), ProcessError> {
        let mut cache = self.read_cache.clone();
        cache::set_status(&mut cache, ticket, update).await?;
        Ok(())
    }

    /// Writes the `AWAITING_PAYMENT` entry that carries the payment link
    async fn publish_link(&self, ticket: &str, user: i32, url: &str, expires_at: DateTime<Utc>) -> Result<(), ProcessError> {
        let mut status = StatusUpdate::new(OrderStatus::AwaitingPayment, user);
        status.payment_url = Some(url.to_string());
        status.expires_at = Some(expires_at.to_rfc3339_opts(SecondsFormat::Millis, true));
        self.write_status(ticket, &status).await
    }

    /// Handles one checkout event. Safe to run again for the same event, at
    /// any point after a failure; an `Err` is transient (the caller retries).
    pub async fn handle(&self, event: &CheckoutEvent) -> Result<Outcome, ProcessError> {
        let (sale, ticket, user) = (event.sale, event.ticket.as_str(), event.user);
        if cache::ticket_sale(ticket) != Some(sale) {
            log::error!("Checkout event for {ticket} names sale {sale}; dropped");
            return Ok(Outcome::Dropped);
        }

        // a. The price
        let Some(amount_cents) = self.price(sale).await? else {
            log::error!("{ticket}: no product {sale}; dropped");
            return Ok(Outcome::Dropped);
        };

        // b. The payment session, idempotent per ticket
        //
        // The payment deadline ("now + 5 min") is computed once,
        // in whole milliseconds, and used for the session, the order row and the
        // status entry, so the three agree. A rerun after a crash asks for a
        // new one, but the provider returns the first session and the insert
        // below keeps the first row, so the first deadline stays the one in
        // force (see step c).
        let expires_ms = Utc::now().timestamp_millis() + self.payment_window.num_milliseconds();
        let expires_at = DateTime::from_timestamp_millis(expires_ms).expect("a deadline in range");
        let session = self.provider.create_session(ticket, amount_cents, expires_at, ticket).await?;

        // c. The order, born AWAITING_PAYMENT with its session, link and
        // deadline in one INSERT. A repeated ticket finds its row
        let (idem, session_id, url) = (event.idempotency_key.clone(), session.id.clone(), session.url.clone());
        let ticket_owned = ticket.to_string();
        let inserted = self
            .db(move |conn| {
                let order = NewOrder {
                    user_id: user,
                    product_id: sale,
                    queue_ticket_id: &ticket_owned,
                    idempotency_key: &idem,
                    status: OrderState::AwaitingPayment,
                    amount_cents,
                    payment_session_id: Some(&session_id),
                    checkout_url: Some(&url),
                    expires_at: Some(expires_at),
                };
                actions::orders::insert_awaiting_payment(conn, &order)
            })
            .await;
        let order = match inserted {
            Ok(Some(order)) => order,
            Ok(None) => {
                log::warn!("{ticket}: another constraint refused the order (user {user} has a live order?); failing the ticket");
                return self.drop_ticket(event, &session.id).await;
            }
            // A row that can never be inserted (unknown user...) is not retried forever
            Err(ProcessError::Db(diesel::result::Error::DatabaseError(
                kind @ (DatabaseErrorKind::ForeignKeyViolation
                | DatabaseErrorKind::CheckViolation
                | DatabaseErrorKind::NotNullViolation),
                info,
            ))) => {
                log::error!("{ticket}: order refused ({kind:?}: {}); failing the ticket", info.message());
                return self.drop_ticket(event, &session.id).await;
            }
            Err(e) => return Err(e),
        };

        // d. What PostgreSQL holds for the ticket decides
        match order.status {
            OrderState::AwaitingPayment => {
                // On a redelivery this is the first attempt's row: its session
                // and deadline are the ones in force (PostgreSQL decides on
                // `expires_at`)
                let (Some(url), Some(expires_at)) = (order.checkout_url.as_deref(), order.expires_at) else {
                    log::error!("{ticket}: its order has no link or deadline; dropped");
                    return Ok(Outcome::Dropped);
                };
                self.publish_link(ticket, user, url, expires_at).await?;
                Ok(Outcome::Linked)
            }
            OrderState::Failed => {
                // The Order Service recorded the publish as failed and returned
                // the unit; this message arrived anyway. Stop, writing nothing
                log::info!("{ticket}: its order is FAILED; expiring the payment session");
                self.provider.expire_session(&session.id).await?;
                Ok(Outcome::Failed)
            }
            // Confirmed, expired or refunded: a redelivery after the order moved
            // on. The session is the order's own (paid, or already expired by
            // the worker), so it is left alone
            other => {
                log::info!("{ticket}: its order is already {other:?}; nothing to do");
                Ok(Outcome::Skipped)
            }
        }
    }

    /// No order exists for the ticket and none can: the payment session made
    /// for it is expired and the client is told `FAILED`
    async fn drop_ticket(&self, event: &CheckoutEvent, session_id: &str) -> Result<Outcome, ProcessError> {
        self.provider.expire_session(session_id).await?;
        self.write_status(&event.ticket, &StatusUpdate::new(OrderStatus::Failed, event.user)).await?;
        Ok(Outcome::Dropped)
    }

    /// `handle`, retried with backoff on any error until it succeeds. Returns
    /// `None` only if `shutdown` was set while retrying: the message is then
    /// left undone, so its offset is not committed and it is redelivered.
    pub async fn handle_with_retry(&self, event: &CheckoutEvent, shutdown: &watch::Receiver<bool>) -> Option<Outcome> {
        let mut delay = RETRY_MIN;
        loop {
            match self.handle(event).await {
                Ok(outcome) => return Some(outcome),
                Err(e) => log::warn!("{} failed, retrying in {delay:?}: {e}", event.ticket),
            }
            if *shutdown.borrow() {
                return None;
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(RETRY_MAX);
        }
    }
}

const RETRY_MIN: Duration = Duration::from_millis(100);
const RETRY_MAX: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Slot {
    InFlight,
    Done,
}

/// Which offsets of one partition are in flight or done. Messages finish in
/// any order; only a prefix with no gap and nothing in flight may be
/// committed, or a crash would skip an unfinished message.
#[derive(Debug, Default)]
pub struct PartitionProgress {
    slots: BTreeMap<i64, Slot>,
    in_flight: usize,
}

impl PartitionProgress {
    /// A message was handed to a handler. Offsets need not be consecutive
    /// (a gap is simply not tracked); a repeated offset is left as it is.
    pub fn start(&mut self, offset: i64) {
        if self.slots.insert_if_absent(offset, Slot::InFlight) {
            self.in_flight += 1;
        }
    }

    /// A message was fully handled. Returns the highest offset of the
    /// contiguous done prefix (commit that plus one) if the prefix grew; `None`
    /// for an offset not being tracked (the partition was revoked and tracking
    /// restarted) or one finished behind a message still in flight.
    pub fn finish(&mut self, offset: i64) -> Option<i64> {
        match self.slots.get_mut(&offset)? {
            slot @ Slot::InFlight => {
                *slot = Slot::Done;
                self.in_flight -= 1;
            }
            Slot::Done => return None,
        }

        let mut last = None;
        while let Some((&first, Slot::Done)) = self.slots.first_key_value() {
            self.slots.remove(&first);
            last = Some(first);
        }
        last
    }

    /// Messages being handled
    pub fn in_flight(&self) -> usize {
        self.in_flight
    }

    /// Messages tracked: in flight, or done but behind one that is not
    pub fn tracked(&self) -> usize {
        self.slots.len()
    }
}

trait InsertIfAbsent {
    fn insert_if_absent(&mut self, offset: i64, slot: Slot) -> bool;
}

impl InsertIfAbsent for BTreeMap<i64, Slot> {
    fn insert_if_absent(&mut self, offset: i64, slot: Slot) -> bool {
        match self.entry(offset) {
            std::collections::btree_map::Entry::Vacant(v) => {
                v.insert(slot);
                true
            }
            std::collections::btree_map::Entry::Occupied(_) => false,
        }
    }
}

/// Spaces message starts at least `1 / max_per_sec` apart (`PROCESSOR_MAX_PER_SEC`)
pub struct Throttle {
    interval: Duration,
    next: tokio::sync::Mutex<tokio::time::Instant>,
}

impl Throttle {
    /// `None` for 0, which means unlimited
    pub fn new(max_per_sec: u32) -> Option<Self> {
        (max_per_sec > 0).then(|| Self {
            interval: Duration::from_secs_f64(1.0 / max_per_sec as f64),
            next: tokio::sync::Mutex::new(tokio::time::Instant::now()),
        })
    }

    /// Waits for this caller's slot
    pub async fn wait(&self) {
        let slot = {
            let mut next = self.next.lock().await;
            let slot = (*next).max(tokio::time::Instant::now());
            *next = slot + self.interval;
            slot
        };
        tokio::time::sleep_until(slot).await;
    }
}

// ---- The Kafka consumer loop ----

/// A partition fills up when this many of its messages are tracked per unit of
/// concurrency: a message stuck at the head holds back the commit, and the
/// ones behind it must not pile up without bound.
const WINDOW_FACTOR: usize = 16;
/// How long a shutdown waits for messages in flight
const SHUTDOWN_GRACE: Duration = Duration::from_secs(20);

enum RebalanceEvent {
    Assigned(Vec<i32>),
    Revoked(Vec<i32>),
}

/// Reports partition assignments to the consumer loop
struct RebalanceContext {
    events: mpsc::UnboundedSender<RebalanceEvent>,
}

impl ClientContext for RebalanceContext {}

impl ConsumerContext for RebalanceContext {
    fn post_rebalance(&self, _consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        let partitions = |list: &TopicPartitionList| list.elements().iter().map(|e| e.partition()).collect::<Vec<_>>();
        let event = match rebalance {
            Rebalance::Assign(list) => RebalanceEvent::Assigned(partitions(list)),
            Rebalance::Revoke(list) => RebalanceEvent::Revoked(partitions(list)),
            Rebalance::Error(e) => {
                log::error!("Rebalance failed: {e}");
                return;
            }
        };
        // The loop may be gone (shutting down)
        let _ = self.events.send(event);
    }

    fn commit_callback(&self, result: rdkafka::error::KafkaResult<()>, offsets: &TopicPartitionList) {
        if let Err(e) = result {
            log::warn!("Offset commit failed ({offsets:?}): {e}");
        }
    }
}

/// How a handler task ended
enum Finished {
    Completed { partition: i32, offset: i64 },
    /// Gave up at shutdown, or panicked: not done
    Aborted,
}

/// Sends `Aborted` if the task ends without reporting (a panic)
struct FinishGuard {
    tx: mpsc::UnboundedSender<Finished>,
    sent: bool,
}

impl FinishGuard {
    fn send(mut self, finished: Finished) {
        self.sent = true;
        let _ = self.tx.send(finished);
    }
}

impl Drop for FinishGuard {
    fn drop(&mut self) {
        if !self.sent {
            let _ = self.tx.send(Finished::Aborted);
        }
    }
}

struct PartitionState {
    progress: PartitionProgress,
    paused: bool,
}

/// The consumer loop's state: what is in flight, and what is still to be
/// committed
struct Loop<'a> {
    consumer: &'a StreamConsumer<RebalanceContext>,
    topic: &'a str,
    concurrency: usize,
    partitions: HashMap<i32, PartitionState>,
    /// Handler tasks that have not reported yet
    live: usize,
    /// Next offset to commit per partition, once it advanced
    to_commit: HashMap<i32, i64>,
}

impl Loop<'_> {
    fn tpl(&self, partition: i32) -> TopicPartitionList {
        let mut list = TopicPartitionList::new();
        list.add_partition(self.topic, partition);
        list
    }

    /// Pauses a partition that has enough in flight, resumes it when it has room
    fn update_pause(&mut self, partition: i32) {
        let Some(state) = self.partitions.get(&partition) else { return };
        let full = state.progress.in_flight() >= self.concurrency
            || state.progress.tracked() >= self.concurrency * WINDOW_FACTOR;
        if full == state.paused {
            return;
        }
        let list = self.tpl(partition);
        let result = if full { self.consumer.pause(&list) } else { self.consumer.resume(&list) };
        match result {
            Ok(()) => {
                if let Some(state) = self.partitions.get_mut(&partition) {
                    state.paused = full;
                }
            }
            Err(e) => log::warn!("Cannot {} partition {partition}: {e}", if full { "pause" } else { "resume" }),
        }
    }

    /// Takes in a prefix that advanced up to `offset`: remembers what to commit
    fn advanced(&mut self, partition: i32, offset: i64) {
        self.to_commit.insert(partition, offset + 1);
    }

    /// Commits the offsets of everything that advanced
    fn flush(&mut self, mode: CommitMode) {
        if !self.to_commit.is_empty() {
            let mut list = TopicPartitionList::new();
            for (&partition, &next) in &self.to_commit {
                if let Err(e) = list.add_partition_offset(self.topic, partition, Offset::Offset(next)) {
                    log::warn!("Cannot commit partition {partition}: {e}");
                }
            }
            match self.consumer.commit(&list, mode) {
                Ok(()) => self.to_commit.clear(),
                // Revoked meanwhile, or the broker is away: the next advance commits again
                Err(e) => log::warn!("Commit failed: {e}"),
            }
        }
    }

    /// Takes in a handler's report. Returns whether a prefix advanced.
    fn finished(&mut self, finished: Finished) -> bool {
        self.live = self.live.saturating_sub(1);
        let Finished::Completed { partition, offset } = finished else { return false };
        // No state: the partition was revoked; the message will be redelivered
        let Some(state) = self.partitions.get_mut(&partition) else { return false };
        let advance = state.progress.finish(offset);
        self.update_pause(partition);
        match advance {
            Some(advance) => {
                self.advanced(partition, advance);
                true
            }
            None => false,
        }
    }
}

/// Consumes the checkout topic until `shutdown` is set.
///
/// Messages of a partition are handled concurrently (up to
/// `PROCESSOR_CONCURRENCY` in flight per partition; a full partition is
/// paused, not the whole consumer) and finish in any order. Auto-commit is off:
/// when the contiguous prefix of a partition grows, the offset after it is
/// committed. On revocation the partition's tracking is dropped; its unfinished
/// messages are redelivered to the next owner, which is safe. On shutdown no
/// more messages are fetched, in-flight ones get a bounded time to finish, and
/// the offsets are committed.
pub async fn run(
    config: &Config,
    processor: Arc<Processor>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), KafkaError> {
    let (rebalance_tx, mut rebalance_rx) = mpsc::unbounded_channel();
    let consumer: StreamConsumer<RebalanceContext> = ClientConfig::new()
        .set("bootstrap.servers", &config.kafka_brokers)
        .set("group.id", GROUP_ID)
        .set("client.id", "order-processor")
        // Offsets are committed by hand, once the messages before them are done
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create_with_context(RebalanceContext { events: rebalance_tx })?;
    consumer.subscribe(&[config.checkout_topic.as_str()])?;
    log::info!(
        "Consuming {} as group {GROUP_ID}, up to {} messages in flight per partition",
        config.checkout_topic,
        config.processor_concurrency
    );

    let throttle = Throttle::new(config.processor_max_per_sec).map(Arc::new);
    let (done_tx, mut done_rx) = mpsc::unbounded_channel::<Finished>();
    let mut state = Loop {
        consumer: &consumer,
        topic: &config.checkout_topic,
        concurrency: config.processor_concurrency.max(1),
        partitions: HashMap::new(),
        live: 0,
        to_commit: HashMap::new(),
    };

    while !*shutdown.borrow() {
        tokio::select! {
            biased;
            _ = shutdown.changed() => {}
            Some(event) = rebalance_rx.recv() => match event {
                RebalanceEvent::Assigned(partitions) => {
                    log::info!("Assigned partitions {partitions:?}");
                    // A pause from an earlier assignment must not outlive it
                    let mut list = TopicPartitionList::new();
                    for partition in partitions {
                        list.add_partition(state.topic, partition);
                    }
                    if let Err(e) = consumer.resume(&list) {
                        log::warn!("Cannot resume assigned partitions: {e}");
                    }
                }
                RebalanceEvent::Revoked(partitions) => {
                    log::info!("Revoked partitions {partitions:?}");
                    for partition in partitions {
                        state.partitions.remove(&partition);
                        state.to_commit.remove(&partition);
                    }
                }
            },
            Some(first) = done_rx.recv() => {
                // Take every report that is ready, then flush once
                let mut advanced = state.finished(first);
                while let Ok(next) = done_rx.try_recv() {
                    advanced |= state.finished(next);
                }
                if advanced {
                    state.flush(CommitMode::Async);
                }
            }
            received = consumer.recv() => {
                let message = match received {
                    Ok(message) => message,
                    Err(e) => {
                        log::warn!("Consumer error: {e}");
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        continue;
                    }
                };
                let (partition, offset) = (message.partition(), message.offset());
                let event = message.payload().and_then(|p| serde_json::from_slice::<CheckoutEvent>(p).ok());
                let partition_state = state
                    .partitions
                    .entry(partition)
                    .or_insert_with(|| PartitionState { progress: PartitionProgress::default(), paused: false });
                partition_state.progress.start(offset);
                match event {
                    None => {
                        log::error!("Undecodable message at partition {partition} offset {offset}; skipped");
                        if let Some(advance) = partition_state.progress.finish(offset) {
                            state.advanced(partition, advance);
                            state.flush(CommitMode::Async);
                        }
                    }
                    Some(event) => {
                        state.live += 1;
                        let (processor, throttle, shutdown, guard) = (
                            processor.clone(),
                            throttle.clone(),
                            shutdown.clone(),
                            FinishGuard { tx: done_tx.clone(), sent: false },
                        );
                        tokio::spawn(async move {
                            if let Some(throttle) = &throttle {
                                throttle.wait().await;
                            }
                            let finished = match processor.handle_with_retry(&event, &shutdown).await {
                                Some(outcome) => {
                                    log::debug!("{}: {outcome:?}", event.ticket);
                                    Finished::Completed { partition, offset }
                                }
                                None => Finished::Aborted,
                            };
                            guard.send(finished);
                        });
                    }
                }
                state.update_pause(partition);
            }
        }
    }

    // Shutting down: no more fetching; let what is in flight finish
    log::info!("Shutting down: waiting for {} messages in flight", state.live);
    let deadline = tokio::time::Instant::now() + SHUTDOWN_GRACE;
    while state.live > 0 {
        match tokio::time::timeout_at(deadline, done_rx.recv()).await {
            Ok(Some(finished)) => {
                state.finished(finished);
            }
            _ => break,
        }
    }
    if state.live > 0 {
        log::warn!("{} messages still in flight; they will be redelivered", state.live);
    }
    state.flush(CommitMode::Sync);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_contiguous_prefix_advances() {
        let mut p = PartitionProgress::default();
        for offset in 10..15 {
            p.start(offset);
        }
        assert_eq!(p.in_flight(), 5);

        // 12 and 11 finish first: nothing is committable while 10 is open
        assert_eq!(p.finish(12), None);
        assert_eq!(p.finish(11), None);
        assert_eq!((p.in_flight(), p.tracked()), (3, 5));

        // 10 finishes: the prefix 10..=12 completes
        assert_eq!(p.finish(10), Some(12));
        assert_eq!((p.in_flight(), p.tracked()), (2, 2));

        // 14 is done but 13 is not
        assert_eq!(p.finish(14), None);
        assert_eq!(p.finish(13), Some(14));
        assert_eq!((p.in_flight(), p.tracked()), (0, 0));
    }

    #[test]
    fn gaps_in_offsets_and_repeats_are_tolerated() {
        let mut p = PartitionProgress::default();
        p.start(5);
        p.start(9); // 6..=8 never delivered (e.g. control records)
        p.start(9); // a repeat changes nothing
        assert_eq!(p.in_flight(), 2);
        assert_eq!(p.finish(9), None);
        assert_eq!(p.finish(5), Some(9));
        // Finishing an untracked or already-finished offset does nothing
        assert_eq!(p.finish(5), None);
        assert_eq!(p.finish(77), None);
    }

    #[tokio::test]
    async fn throttle_spaces_starts() {
        assert!(Throttle::new(0).is_none());
        let throttle = Throttle::new(50).unwrap(); // 20 ms apart
        let started = std::time::Instant::now();
        for _ in 0..6 {
            throttle.wait().await;
        }
        assert!(started.elapsed() >= Duration::from_millis(95), "{:?}", started.elapsed());
    }
}
