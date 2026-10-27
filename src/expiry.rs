//! The expiration worker's logic (roadmap steps 22-24): give back the stock of
//! holds that will never be paid or processed, and tidy up the orders.
//!
//! Three passes, each a method of `Expirer` that the binary calls on a timer
//! and the tests call directly:
//!
//! - `unpaid_pass` (step 22): `AWAITING_PAYMENT` holds whose deadline in
//!   `holds:expiry:{sale}` has passed. Release, then the order `EXPIRED`, the
//!   read cache, and last the payment session.
//! - `leaked_pass` (step 23): `PENDING` holds that no message can still reach,
//!   because the processors' progress is past them by more than the producer's
//!   delivery timeout (see `release_cutoff`). Release, then the order
//!   `FAILED` (if the processor made one), the read cache and the session.
//! - `sweep` (the baseline's `expire_stale`): orders a worker left behind when
//!   it crashed between a release and the order update.
//!
//! Any number of workers may run at once. Correctness comes from
//! `release.lua`, which hands each hold to exactly one caller; a worker acts on
//! a hold only when its own call got `released`.
//!
//! **The steps after a release are idempotent and retried.** The unit is back
//! in stock the moment `release.lua` returns, but the order, the status and
//! the session follow in separate systems. If one of them fails, the hold is
//! remembered in memory and finished again before the next pass; if the worker
//! dies instead, the sweep finds the order later (its hold is `EXPIRED` or
//! `FAILED`, or gone).
//!
//! **The order of the steps matters.** The order is set `EXPIRED` only
//! `WHERE status = 'AWAITING_PAYMENT'`, so the `REFUNDED` that the payment
//! inbox writes for a payment that slipped in after the release is never
//! overwritten; and the session is expired only after the release, never
//! before. A payment that lands in between loses its claim (the hold is gone)
//! and the inbox refunds it.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use diesel::PgConnection;
use rdkafka::{
    consumer::{BaseConsumer, Consumer},
    ClientConfig, Offset, TopicPartitionList,
};

use crate::{
    actions,
    cache::{self, HoldState, MaxScore, ReadCache, RedisConn, ReleaseOutcome, ReleasedAs, StatusUpdate},
    config::Config,
    db::DbPool,
    models::{Order, OrderStatus},
    payment::{PaymentError, SharedProvider},
    processor::GROUP_ID,
};

/// How long Kafka metadata, committed offsets and watermarks may take
const KAFKA_TIMEOUT: Duration = Duration::from_secs(5);

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
    #[error("kafka: {0}")]
    Kafka(String),
    #[error("blocking task: {0}")]
    Join(String),
}

/// What the expiration worker settings are
#[derive(Debug, Clone)]
pub struct Settings {
    /// Holds read and released per sale and pass
    pub batch: usize,
    /// The producer's `delivery.timeout.ms`: the margin before a `PENDING`
    /// hold's missing message counts as lost
    pub delivery_timeout_ms: i64,
    /// An order is stale (for the sweep) this long after its deadline / creation
    pub stale_after: chrono::Duration,
    /// Sales that started longer ago than this are no longer watched
    pub sale_lookback: chrono::Duration,
}

impl Settings {
    pub fn from_config(config: &Config) -> Self {
        Self {
            batch: config.expiry_batch.max(1),
            delivery_timeout_ms: config.kafka_delivery_timeout_ms as i64,
            stale_after: chrono::Duration::minutes(10),
            sale_lookback: chrono::Duration::days(3),
        }
    }
}

// ---- Leaked-hold cutoff ----

/// What is known about one partition of the checkout topic, for one sale
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionView {
    /// The order processors' group has messages in this partition it has not
    /// finished (committed offset below the high watermark)
    pub lagging: bool,
    /// `processed_upto:{sale}:{partition}`, if written
    pub processed_upto: Option<i64>,
}

/// The checkout time (epoch ms) before which a `PENDING` hold of a sale is
/// certainly leaked, or `None` if nothing may be released this round.
///
/// For every partition, progress is `processed_upto` if the partition has
/// lag, and `now_ms` if it has none (everything published has been processed,
/// so it is caught up to now, and an idle partition cannot block releases).
/// The cutoff is the lowest progress over all partitions minus the delivery
/// timeout, the longest a publish may take before the producer reports failure
/// (and compensation runs).
///
/// A partition with lag and no `processed_upto` for the sale gives no cutoff:
/// the processors have not reported anything for it, so releasing is not
/// safe. No partitions at all (topic unknown) gives none either.
///
/// `now_ms` is Redis `TIME` (the clock `checkout.lua` stamps holds with), read
/// before the Kafka offsets.
pub fn release_cutoff(partitions: &[PartitionView], now_ms: i64, delivery_timeout_ms: i64) -> Option<i64> {
    let mut lowest: Option<i64> = None;
    for partition in partitions {
        let progress = if partition.lagging { partition.processed_upto? } else { now_ms };
        lowest = Some(lowest.map_or(progress, |l| l.min(progress)));
    }
    lowest.map(|l| l - delivery_timeout_ms)
}

/// Whether each partition of the checkout topic has unfinished messages
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionLag {
    pub partition: i32,
    pub lagging: bool,
}

#[async_trait]
pub trait LagSource: Send + Sync {
    /// The lag of every partition of the topic, as of now
    async fn partitions(&self) -> Result<Vec<PartitionLag>, ExpiryError>;
}

/// Reads the order processors' progress from Kafka: a consumer in the group
/// `order-processors` that never subscribes (it joins nothing and takes no
/// partitions) and only asks the broker for the group's committed offsets and
/// the partitions' watermarks.
pub struct KafkaLag {
    consumer: Arc<BaseConsumer>,
    topic: String,
}

impl KafkaLag {
    pub fn new(config: &Config) -> Result<Self, rdkafka::error::KafkaError> {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &config.kafka_brokers)
            .set("group.id", GROUP_ID)
            .set("client.id", "expiration-worker")
            .set("enable.auto.commit", "false")
            .create()?;
        Ok(Self { consumer: Arc::new(consumer), topic: config.checkout_topic.clone() })
    }

    fn read(&self) -> Result<Vec<PartitionLag>, rdkafka::error::KafkaError> {
        let metadata = self.consumer.fetch_metadata(Some(&self.topic), KAFKA_TIMEOUT)?;
        let mut ids: Vec<i32> = metadata
            .topics()
            .iter()
            .filter(|t| t.name() == self.topic && t.error().is_none())
            .flat_map(|t| t.partitions().iter().map(|p| p.id()))
            .collect();
        ids.sort_unstable();
        if ids.is_empty() {
            return Err(rdkafka::error::KafkaError::MetadataFetch(rdkafka::error::RDKafkaErrorCode::UnknownTopicOrPartition));
        }
        let mut wanted = TopicPartitionList::new();
        for &id in &ids {
            wanted.add_partition(&self.topic, id);
        }
        // The committed offsets first, the watermarks after. A message
        // published between the two reads raises the watermark above the
        // committed offset, so the partition looks lagging: the error can only
        // be towards releasing nothing. In the other order, a message could be
        // consumed and committed between the reads, and a stale high watermark
        // would make an unfinished partition look caught up.
        let committed = self.consumer.committed_offsets(wanted, KAFKA_TIMEOUT)?;
        let mut result = Vec::with_capacity(ids.len());
        for id in ids {
            let (low, high) = self.consumer.fetch_watermarks(&self.topic, id, KAFKA_TIMEOUT)?;
            // No committed offset yet: the group starts at the beginning (auto.offset.reset=earliest)
            let next = match committed.find_partition(&self.topic, id).map(|e| e.offset()) {
                Some(Offset::Offset(offset)) => offset.max(low),
                _ => low,
            };
            result.push(PartitionLag { partition: id, lagging: next < high });
        }
        Ok(result)
    }
}

#[async_trait]
impl LagSource for KafkaLag {
    async fn partitions(&self) -> Result<Vec<PartitionLag>, ExpiryError> {
        let this = Self { consumer: self.consumer.clone(), topic: self.topic.clone() };
        tokio::task::spawn_blocking(move || this.read())
            .await
            .map_err(|e| ExpiryError::Join(e.to_string()))?
            .map_err(|e| ExpiryError::Kafka(e.to_string()))
    }
}

// ---- The passes ----

/// A release this worker made, whose follow-up steps are not all done yet
#[derive(Debug, Clone, PartialEq, Eq)]
struct Released {
    sale: i32,
    ticket: String,
    kind: ReleasedAs,
}

/// What a pass did
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Pass {
    /// Holds this call released
    pub released: usize,
    /// A batch came back full: another pass is due at once
    pub more: bool,
}

impl Pass {
    pub fn merge(self, other: Pass) -> Pass {
        Pass { released: self.released + other.released, more: self.more || other.more }
    }
}

/// What the sweep repaired
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Swept {
    pub expired: usize,
    pub failed: usize,
}

pub struct Expirer {
    pool: DbPool,
    inventory: RedisConn,
    read_cache: ReadCache,
    provider: SharedProvider,
    lag: Arc<dyn LagSource>,
    settings: Settings,
    /// Releases whose follow-up failed; finished before the next pass
    unfinished: Mutex<Vec<Released>>,
}

impl Expirer {
    pub fn new(
        pool: DbPool,
        inventory: RedisConn,
        read_cache: ReadCache,
        provider: SharedProvider,
        lag: Arc<dyn LagSource>,
        settings: Settings,
    ) -> Self {
        Self { pool, inventory, read_cache, provider, lag, settings, unfinished: Mutex::new(Vec::new()) }
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

    /// Redis `TIME` in epoch ms: the clock of `checkout.lua` and `release.lua`
    pub async fn redis_now_ms(&self) -> Result<i64, ExpiryError> {
        let mut con = self.inventory.clone();
        let (secs, micros): (i64, i64) = redis::cmd("TIME").query_async(&mut con).await?;
        Ok(secs * 1000 + micros / 1000)
    }

    /// The sales to watch: those that started within the lookback or are yet to start
    pub async fn load_sales(&self) -> Result<Vec<i32>, ExpiryError> {
        let since = Utc::now() - self.settings.sale_lookback;
        self.db(move |conn| actions::products::sales_starting_since(conn, since)).await
    }

    // ---- After a release ----

    /// The user of a ticket: from its order, else from the hold (kept after a release)
    async fn user_of(&self, sale: i32, ticket: &str, order: Option<&Order>) -> Result<Option<i32>, ExpiryError> {
        if let Some(order) = order {
            return Ok(Some(order.user_id));
        }
        let mut con = self.inventory.clone();
        Ok(cache::read_hold(&mut con, sale, ticket).await?.map(|h| h.user))
    }

    async fn write_status(&self, sale: i32, ticket: &str, user: i32, status: OrderStatus) -> Result<(), ExpiryError> {
        let mut update = StatusUpdate::new(status, user);
        update.sale = Some(sale);
        let mut cache = self.read_cache.clone();
        cache::set_status(&mut cache, ticket, &update).await?;
        Ok(())
    }

    /// An unpaid hold was released: the order `EXPIRED` (only from
    /// `AWAITING_PAYMENT`), the status, and last the payment session. Safe to
    /// repeat. Returns whether this call changed the order.
    pub async fn finish_expired(&self, sale: i32, ticket: &str) -> Result<bool, ExpiryError> {
        let lookup = ticket.to_string();
        let (changed, order) = self
            .db(move |conn| {
                let changed = actions::orders::mark_expired(conn, &lookup)?;
                Ok((changed, actions::orders::find_by_ticket(conn, &lookup)?))
            })
            .await?;
        if let Some(user) = self.user_of(sale, ticket, order.as_ref()).await? {
            self.write_status(sale, ticket, user, OrderStatus::Expired).await?;
        }
        // Only for an order this worker (or an earlier attempt) expired: a
        // REFUNDED order's session is paid, and there is nothing to stop
        if let Some(Order { status: OrderStatus::Expired, payment_session_id: Some(session), .. }) = order {
            self.provider.expire_session(&session).await?;
        }
        Ok(changed)
    }

    /// A leaked hold was released: the order `FAILED` if the processor made
    /// one (from `PENDING` or `AWAITING_PAYMENT`), the status, the session.
    /// Safe to repeat. Returns whether this call changed the order.
    pub async fn finish_failed(&self, sale: i32, ticket: &str) -> Result<bool, ExpiryError> {
        let lookup = ticket.to_string();
        let (changed, order) = self
            .db(move |conn| {
                let changed = actions::orders::mark_failed(conn, &lookup)?;
                Ok((changed, actions::orders::find_by_ticket(conn, &lookup)?))
            })
            .await?;
        if let Some(user) = self.user_of(sale, ticket, order.as_ref()).await? {
            self.write_status(sale, ticket, user, OrderStatus::Failed).await?;
        }
        if let Some(Order { status: OrderStatus::Failed, payment_session_id: Some(session), .. }) = order {
            self.provider.expire_session(&session).await?;
        }
        Ok(changed)
    }

    async fn finish(&self, released: &Released) -> Result<bool, ExpiryError> {
        match released.kind {
            ReleasedAs::Expired => self.finish_expired(released.sale, &released.ticket).await,
            ReleasedAs::Failed => self.finish_failed(released.sale, &released.ticket).await,
        }
    }

    /// Finishes the releases that failed to finish earlier
    async fn finish_unfinished(&self) -> Result<(), ExpiryError> {
        let waiting = std::mem::take(&mut *self.unfinished.lock().unwrap_or_else(|p| p.into_inner()));
        let mut rest = waiting.into_iter();
        while let Some(released) = rest.next() {
            if let Err(e) = self.finish(&released).await {
                let mut unfinished = self.unfinished.lock().unwrap_or_else(|p| p.into_inner());
                unfinished.push(released);
                unfinished.extend(rest);
                return Err(e);
            }
        }
        Ok(())
    }

    /// Releases `ticket` if it is still due, then finishes. On a failure after
    /// the release the hold is remembered and the error returned: the pass stops
    /// there (a dead database would otherwise make every remaining hold wait).
    async fn release_and_finish(
        &self,
        sale: i32,
        ticket: &str,
        expected: HoldState,
        max_score: MaxScore,
        released_as: ReleasedAs,
    ) -> Result<bool, ExpiryError> {
        let mut con = self.inventory.clone();
        match cache::release_hold(&mut con, sale, ticket, expected, max_score, released_as, false).await? {
            ReleaseOutcome::NotReleased => Ok(false),
            ReleaseOutcome::Released => {
                let released = Released { sale, ticket: ticket.to_string(), kind: released_as };
                if let Err(e) = self.finish(&released).await {
                    self.unfinished.lock().unwrap_or_else(|p| p.into_inner()).push(released);
                    return Err(e);
                }
                Ok(true)
            }
        }
    }

    /// Tickets of a sorted set with score in `-inf..=max`, lowest first
    async fn due(&self, key: String, max: i64, limit: usize) -> Result<Vec<String>, ExpiryError> {
        let mut con = self.inventory.clone();
        Ok(redis::cmd("ZRANGEBYSCORE").arg(key).arg("-inf").arg(max).arg("LIMIT").arg(0).arg(limit).query_async(&mut con).await?)
    }

    // ---- The passes ----

    /// Unpaid-hold loop (step 22): one batch per sale of `AWAITING_PAYMENT`
    /// holds whose deadline has passed. The candidates are read by score
    /// (Redis `TIME`); the script re-checks the score and the state, so a hold
    /// paid or released meanwhile is left alone.
    pub async fn unpaid_pass(&self, sales: &[i32]) -> Result<Pass, ExpiryError> {
        self.finish_unfinished().await?;
        let mut pass = Pass::default();
        for &sale in sales {
            let now = self.redis_now_ms().await?;
            let candidates = self.due(cache::holds_expiry_key(sale), now, self.settings.batch).await?;
            pass.more |= candidates.len() >= self.settings.batch;
            for ticket in candidates {
                let released = self
                    .release_and_finish(sale, &ticket, HoldState::AwaitingPayment, MaxScore::Now, ReleasedAs::Expired)
                    .await?;
                pass.released += usize::from(released);
            }
        }
        Ok(pass)
    }

    /// The lag of the topic, and each sale's cutoff
    async fn cutoffs(&self, sales: &[i32]) -> Result<Vec<(i32, i64)>, ExpiryError> {
        // The clock first: a cutoff from an earlier `now` is lower, so the
        // delay between this read and the Kafka reads can only release less
        let now_ms = self.redis_now_ms().await?;
        let lag = self.lag.partitions().await?;
        let mut con = self.inventory.clone();
        let mut cutoffs = Vec::new();
        for &sale in sales {
            let lagging: Vec<i32> = lag.iter().filter(|p| p.lagging).map(|p| p.partition).collect();
            let mut upto = std::collections::HashMap::new();
            if !lagging.is_empty() {
                let keys: Vec<String> = lagging.iter().map(|&p| cache::processed_upto_key(sale, p)).collect();
                let values: Vec<Option<i64>> = redis::cmd("MGET").arg(keys).query_async(&mut con).await?;
                upto = lagging.iter().copied().zip(values).collect();
            }
            let views: Vec<PartitionView> = lag
                .iter()
                .map(|p| PartitionView { lagging: p.lagging, processed_upto: upto.get(&p.partition).copied().flatten() })
                .collect();
            if let Some(cutoff) = release_cutoff(&views, now_ms, self.settings.delivery_timeout_ms) {
                cutoffs.push((sale, cutoff));
            }
        }
        Ok(cutoffs)
    }

    /// Leaked-hold loop (step 23): one batch per sale of `PENDING` holds
    /// checked out before the sale's cutoff (`release_cutoff`). A stalled
    /// processor stops reporting progress, so nothing is released; a backlog
    /// shows as lag with progress behind the holds, so nothing is released
    /// either. Kafka is asked only if some sale has `PENDING` holds at all.
    pub async fn leaked_pass(&self, sales: &[i32]) -> Result<Pass, ExpiryError> {
        self.finish_unfinished().await?;
        let mut con = self.inventory.clone();
        let mut busy = Vec::new();
        for &sale in sales {
            let waiting: i64 = redis::cmd("ZCARD").arg(cache::holds_pending_key(sale)).query_async(&mut con).await?;
            if waiting > 0 {
                busy.push(sale);
            }
        }
        let mut pass = Pass::default();
        if busy.is_empty() {
            return Ok(pass);
        }
        for (sale, cutoff) in self.cutoffs(&busy).await? {
            // Strictly before the cutoff; `release.lua` takes an inclusive bound
            let bound = cutoff - 1;
            let candidates = self.due(cache::holds_pending_key(sale), bound, self.settings.batch).await?;
            pass.more |= candidates.len() >= self.settings.batch;
            for ticket in candidates {
                let released = self
                    .release_and_finish(sale, &ticket, HoldState::Pending, MaxScore::UpTo(bound), ReleasedAs::Failed)
                    .await?;
                pass.released += usize::from(released);
            }
        }
        Ok(pass)
    }

    /// Both loops, one batch per sale each; what the tests (and a simple
    /// caller) use. The binary runs them on their own timers.
    pub async fn round(&self, sales: &[i32]) -> Result<Pass, ExpiryError> {
        let unpaid = self.unpaid_pass(sales).await?;
        Ok(unpaid.merge(self.leaked_pass(sales).await?))
    }

    /// Stale-order sweep. Orders whose worker died between a release and the
    /// order update:
    /// - `AWAITING_PAYMENT` more than `stale_after` past their deadline, whose
    ///   hold is gone or `EXPIRED`, become `EXPIRED`;
    /// - `PENDING` more than `stale_after` old, whose hold is `FAILED` or gone,
    ///   become `FAILED`.
    ///
    /// An order whose hold is still live (or is `PAID`, `CONFIRMED`...) is
    /// never touched. Status and session follow as after a release.
    pub async fn sweep(&self, now: DateTime<Utc>) -> Result<Swept, ExpiryError> {
        let before = now - self.settings.stale_after;
        let limit = self.settings.batch as i64;
        let mut swept = Swept::default();

        let mut after = 0;
        loop {
            let page = self.db(move |conn| actions::orders::stale_awaiting_payment(conn, before, after, limit)).await?;
            for order in &page {
                if self.hold_state(order).await?.is_none_or(|state| state == "EXPIRED")
                    && self.finish_expired(order.product_id, &order.queue_ticket_id).await?
                {
                    swept.expired += 1;
                }
            }
            match page.last() {
                Some(last) if page.len() as i64 == limit => after = last.id,
                _ => break,
            }
        }

        let mut after = 0;
        loop {
            let page = self.db(move |conn| actions::orders::stale_pending(conn, before, after, limit)).await?;
            for order in &page {
                if self.hold_state(order).await?.is_none_or(|state| state == "FAILED")
                    && self.finish_failed(order.product_id, &order.queue_ticket_id).await?
                {
                    swept.failed += 1;
                }
            }
            match page.last() {
                Some(last) if page.len() as i64 == limit => after = last.id,
                _ => break,
            }
        }
        Ok(swept)
    }

    /// The state of an order's hold, `None` if the hold is gone
    async fn hold_state(&self, order: &Order) -> Result<Option<String>, ExpiryError> {
        let mut con = self.inventory.clone();
        Ok(cache::read_hold(&mut con, order.product_id, &order.queue_ticket_id).await?.map(|h| h.state))
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

    const TIMEOUT: i64 = 30_000;

    fn lagging(upto: Option<i64>) -> PartitionView {
        PartitionView { lagging: true, processed_upto: upto }
    }

    fn idle() -> PartitionView {
        PartitionView { lagging: false, processed_upto: None }
    }

    #[test]
    fn the_cutoff_is_the_lowest_progress_minus_the_delivery_timeout() {
        let views = [lagging(Some(900_000)), lagging(Some(500_000)), lagging(Some(700_000))];
        assert_eq!(release_cutoff(&views, 1_000_000, TIMEOUT), Some(500_000 - TIMEOUT));
    }

    #[test]
    fn a_partition_without_lag_counts_as_caught_up_to_now() {
        // An idle partition does not hold the cutoff back...
        let views = [idle(), lagging(Some(900_000))];
        assert_eq!(release_cutoff(&views, 1_000_000, TIMEOUT), Some(900_000 - TIMEOUT));
        // ...and with no lag anywhere the cutoff is now minus the timeout
        assert_eq!(release_cutoff(&[idle(), idle()], 1_000_000, TIMEOUT), Some(1_000_000 - TIMEOUT));
        // A stale processed_upto of an idle partition is ignored
        let stale = PartitionView { lagging: false, processed_upto: Some(1_000) };
        assert_eq!(release_cutoff(&[stale], 1_000_000, TIMEOUT), Some(1_000_000 - TIMEOUT));
    }

    #[test]
    fn a_partition_with_lag_and_no_progress_blocks_every_release() {
        assert_eq!(release_cutoff(&[lagging(None), idle(), lagging(Some(900_000))], 1_000_000, TIMEOUT), None);
        assert_eq!(release_cutoff(&[lagging(None)], 1_000_000, TIMEOUT), None);
    }

    #[test]
    fn no_partitions_means_no_cutoff() {
        assert_eq!(release_cutoff(&[], 1_000_000, TIMEOUT), None);
    }

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
