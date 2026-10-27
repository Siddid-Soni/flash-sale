//! The order processor against real Postgres and Redis (and Kafka for the last
//! test): message handling, retries, and the consumer loop end to end.

mod common;

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration as StdDuration;

use actix_web::http::header::HeaderMap;
use async_trait::async_trait;
use chrono::{DateTime, Duration, SecondsFormat, Utc};
use diesel::prelude::*;
use rdkafka::{
    consumer::{BaseConsumer, Consumer},
    ClientConfig, Offset, TopicPartitionList,
};
use redis::AsyncCommands;
use tokio::sync::watch;

use flash_sale::{
    actions,
    cache::{self, CheckoutOutcome, HoldState, MaxScore, ReleasedAs},
    models::{NewOrder, NewProduct, Order, OrderStatus},
    payment::{FakeProvider, PaymentProvider, PaymentResult, ProviderEvent, Session, SharedProvider},
    processor::{self, Outcome, Processor},
    queue::{ensure_topic, CheckoutEvent, CheckoutQueue, KafkaQueue},
    schema::orders,
};

const WINDOW_SECS: u64 = 300;

struct Fixture {
    env: common::Env,
    fake: Arc<FakeProvider>,
    sale: i32,
}

async fn fixture(stock: i32) -> Fixture {
    let env = common::env().await;
    let product = actions::products::create_product(
        &mut env.pool.get().unwrap(),
        &NewProduct {
            name: "Smartphone",
            price_cents: 59_900,
            total_inventory: stock,
            sale_start_time: Utc::now() - Duration::seconds(60),
        },
    )
    .unwrap();
    cache::init_sale(&mut env.redis.clone(), product.id, stock, product.sale_start_time).await.unwrap();
    // The provider's state, to look at sessions; the processor reaches it over HTTP
    let fake = env.provider.clone();
    Fixture { env, fake, sale: product.id }
}

impl Fixture {
    fn processor_with(&self, provider: SharedProvider) -> Processor {
        Processor::new(self.env.pool.clone(), self.env.redis.clone(), self.env.read_cache.clone(), provider, WINDOW_SECS)
    }

    fn processor(&self) -> Processor {
        self.processor_with(self.env.shared_provider())
    }

    /// A new guest with a `PENDING` hold and the event its checkout would publish
    async fn winner(&self) -> CheckoutEvent {
        let user = actions::users::create_guest(&mut self.env.pool.get().unwrap()).unwrap();
        let ticket = cache::new_ticket(self.sale);
        let outcome =
            cache::checkout(&mut self.env.redis.clone(), self.sale, user.id, &format!("key-{}", user.id), &ticket)
                .await
                .unwrap();
        let CheckoutOutcome::Held { checkout_ms } = outcome else { panic!("{outcome:?}") };
        CheckoutEvent { ticket, sale: self.sale, user: user.id, checkout_ms }
    }

    fn order(&self, ticket: &str) -> Option<Order> {
        actions::orders::find_by_ticket(&mut self.env.pool.get().unwrap(), ticket).unwrap()
    }

    async fn hold_state(&self, ticket: &str) -> String {
        cache::read_hold(&mut self.env.redis.clone(), self.sale, ticket).await.unwrap().unwrap().state
    }

    async fn stock(&self) -> i64 {
        self.env.redis.clone().get::<_, Option<i64>>(cache::stock_key(self.sale)).await.unwrap().unwrap_or(0)
    }

    async fn status(&self, ticket: &str) -> Option<cache::CachedStatus> {
        cache::read_status(&mut self.env.read_cache.clone(), ticket).await.unwrap()
    }

    async fn expiry_score(&self, ticket: &str) -> Option<i64> {
        let score: Option<f64> = self.env.redis.clone().zscore(cache::holds_expiry_key(self.sale), ticket).await.unwrap();
        score.map(|s| s as i64)
    }

    async fn release(&self, ticket: &str) {
        let outcome = cache::release_hold(
            &mut self.env.redis.clone(),
            self.sale,
            ticket,
            HoldState::Pending,
            MaxScore::Any,
            ReleasedAs::Failed,
            false,
        )
        .await
        .unwrap();
        assert_eq!(outcome, cache::ReleaseOutcome::Released);
    }

    async fn cleanup(&self) {
        let mut con = self.env.redis.clone();
        for pattern in [format!("*{{{}}}*", self.sale), format!("status:{}.*", self.sale)] {
            let keys: Vec<String> = con.keys(pattern).await.unwrap();
            if !keys.is_empty() {
                let _: () = con.del(keys).await.unwrap();
            }
        }
    }
}

fn millis(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(SecondsFormat::Millis, true)
}

#[tokio::test]
async fn a_checkout_event_becomes_an_order_with_a_payment_link() {
    let fx = fixture(5).await;
    let event = fx.winner().await;
    let processor = fx.processor();

    assert_eq!(processor.handle(&event).await.unwrap(), Outcome::Linked);

    // The order: AWAITING_PAYMENT with the session, URL and deadline, the price of the product
    let order = fx.order(&event.ticket).unwrap();
    assert_eq!((order.status, order.user_id, order.product_id), (OrderStatus::AwaitingPayment, event.user, fx.sale));
    assert_eq!(order.amount_cents, 59_900);
    assert_eq!(order.idempotency_key, format!("key-{}", event.user), "the key comes from the hold");
    let session_id = order.payment_session_id.clone().unwrap();
    assert_eq!(order.checkout_url.as_deref(), Some(format!("http://pay.test/fake-pay/{session_id}").as_str()));
    let expires_at = order.expires_at.unwrap();
    let window = expires_at - Utc::now();
    assert!(window > Duration::seconds(WINDOW_SECS as i64 - 10) && window <= Duration::seconds(WINDOW_SECS as i64));

    // The hold moved to the expiry set, scored with the same deadline
    assert_eq!(fx.hold_state(&event.ticket).await, "AWAITING_PAYMENT");
    assert_eq!(fx.expiry_score(&event.ticket).await, Some(expires_at.timestamp_millis()));
    let pending: Option<f64> = fx.env.redis.clone().zscore(cache::holds_pending_key(fx.sale), &event.ticket).await.unwrap();
    assert!(pending.is_none());
    assert_eq!(fx.stock().await, 4);

    // The status entry the client polls: the link and the same deadline
    let status = fx.status(&event.ticket).await.unwrap();
    assert_eq!(status.status, OrderStatus::AwaitingPayment);
    assert_eq!(status.user, event.user);
    assert_eq!(status.payment_url, order.checkout_url);
    assert_eq!(status.expires_at, Some(millis(expires_at)));

    // The provider's session is open, for this ticket
    let session = fx.fake.session(&session_id).await.unwrap().unwrap();
    assert_eq!((session.state.as_str(), session.ticket.as_str()), ("OPEN", event.ticket.as_str()));
    assert_eq!(session.expires_at, expires_at);
    fx.cleanup().await;
}

#[tokio::test]
async fn a_redelivered_event_changes_nothing() {
    let fx = fixture(5).await;
    let event = fx.winner().await;
    let processor = fx.processor();
    assert_eq!(processor.handle(&event).await.unwrap(), Outcome::Linked);
    let before = fx.order(&event.ticket).unwrap();
    let score = fx.expiry_score(&event.ticket).await;

    // The crash came before the status write: the redelivery repairs it
    let _: () = fx.env.read_cache.clone().0.del(cache::status_key(&event.ticket)).await.unwrap();
    assert_eq!(processor.handle(&event).await.unwrap(), Outcome::Redelivered);
    assert_eq!(processor.handle(&event).await.unwrap(), Outcome::Redelivered);

    let after = fx.order(&event.ticket).unwrap();
    assert_eq!(
        (after.id, after.status, &after.payment_session_id, &after.checkout_url, after.expires_at),
        (before.id, before.status, &before.payment_session_id, &before.checkout_url, before.expires_at)
    );
    assert_eq!(fx.expiry_score(&event.ticket).await, score);
    assert_eq!(fx.stock().await, 4);
    let status = fx.status(&event.ticket).await.unwrap();
    assert_eq!((status.status, status.payment_url), (OrderStatus::AwaitingPayment, before.checkout_url.clone()));
    assert_eq!(status.expires_at, Some(millis(before.expires_at.unwrap())));
    let one_order: i64 = orders::table.filter(orders::queue_ticket_id.eq(&event.ticket)).count().get_result(&mut fx.env.pool.get().unwrap()).unwrap();
    assert_eq!(one_order, 1);
    fx.cleanup().await;
}

#[tokio::test]
async fn a_released_hold_is_skipped_without_an_order() {
    let fx = fixture(5).await;
    let event = fx.winner().await;
    fx.release(&event.ticket).await;

    assert_eq!(fx.processor().handle(&event).await.unwrap(), Outcome::Skipped);
    assert!(fx.order(&event.ticket).is_none());
    assert!(fx.status(&event.ticket).await.is_none());
    assert!(fx.fake.session_id_for_ticket(&event.ticket).await.unwrap().is_none(), "no payment session was made");
    assert_eq!(fx.stock().await, 5);
    fx.cleanup().await;
}

/// Releases the hold while the processor is creating the session: the window
/// between the order insert and `link_ready.lua`
struct ReleasingProvider {
    inner: SharedProvider,
    fixture_redis: cache::RedisConn,
    sale: i32,
}

#[async_trait]
impl PaymentProvider for ReleasingProvider {
    fn name(&self) -> &'static str {
        "releasing"
    }

    async fn create_session(&self, ticket: &str, amount: i32, at: DateTime<Utc>, key: &str) -> PaymentResult<Session> {
        let session = self.inner.create_session(ticket, amount, at, key).await?;
        let outcome = cache::release_hold(
            &mut self.fixture_redis.clone(),
            self.sale,
            ticket,
            HoldState::Pending,
            MaxScore::Any,
            ReleasedAs::Expired,
            false,
        )
        .await
        .unwrap();
        assert_eq!(outcome, cache::ReleaseOutcome::Released);
        Ok(session)
    }

    async fn expire_session(&self, id: &str) -> PaymentResult<()> {
        self.inner.expire_session(id).await
    }

    async fn refund(&self, id: &str) -> PaymentResult<()> {
        self.inner.refund(id).await
    }

    fn verify_webhook(&self, headers: &HeaderMap, body: &[u8]) -> PaymentResult<ProviderEvent> {
        self.inner.verify_webhook(headers, body)
    }
}

#[tokio::test]
async fn a_hold_released_before_the_link_fails_the_order_and_expires_the_session() {
    let fx = fixture(5).await;
    let event = fx.winner().await;
    let provider: SharedProvider =
        Arc::new(ReleasingProvider { inner: fx.env.shared_provider(), fixture_redis: fx.env.redis.clone(), sale: fx.sale });

    assert_eq!(fx.processor_with(provider).handle(&event).await.unwrap(), Outcome::Failed);

    let order = fx.order(&event.ticket).unwrap();
    assert_eq!(order.status, OrderStatus::Failed);
    let session = fx.fake.session(&order.payment_session_id.unwrap()).await.unwrap().unwrap();
    assert_eq!(session.state, "EXPIRED");
    let status = fx.status(&event.ticket).await.unwrap();
    assert_eq!((status.status, status.payment_url), (OrderStatus::Failed, None));
    // The hold stays released (the releaser returned the unit), never linked
    assert_eq!(fx.hold_state(&event.ticket).await, "EXPIRED");
    assert_eq!(fx.expiry_score(&event.ticket).await, None);
    assert_eq!(fx.stock().await, 5);
    fx.cleanup().await;
}

#[tokio::test]
async fn an_order_another_constraint_refuses_leaves_the_hold_alone() {
    let fx = fixture(5).await;
    let event = fx.winner().await;
    // The user already has a live order from some other ticket
    diesel::insert_into(orders::table)
        .values(&NewOrder {
            user_id: event.user,
            product_id: fx.sale,
            queue_ticket_id: &format!("{}.{}", fx.sale, uuid::Uuid::new_v4()),
            idempotency_key: "older",
            status: OrderStatus::AwaitingPayment,
            amount_cents: 59_900,
        })
        .execute(&mut fx.env.pool.get().unwrap())
        .unwrap();

    assert_eq!(fx.processor().handle(&event).await.unwrap(), Outcome::Dropped);
    assert!(fx.order(&event.ticket).is_none());
    assert_eq!(fx.hold_state(&event.ticket).await, "PENDING", "the leaked-hold loop releases it later");
    assert!(fx.fake.session_id_for_ticket(&event.ticket).await.unwrap().is_none());
    fx.cleanup().await;
}

/// Fails its first `failures` calls, as a provider having a bad moment would
struct FlakyProvider {
    inner: SharedProvider,
    failures: usize,
    calls: AtomicUsize,
}

#[async_trait]
impl PaymentProvider for FlakyProvider {
    fn name(&self) -> &'static str {
        "flaky"
    }

    async fn create_session(&self, ticket: &str, amount: i32, at: DateTime<Utc>, key: &str) -> PaymentResult<Session> {
        if self.calls.fetch_add(1, Ordering::SeqCst) < self.failures {
            return Err(flash_sale::payment::PaymentError("provider unavailable".into()));
        }
        self.inner.create_session(ticket, amount, at, key).await
    }

    async fn expire_session(&self, id: &str) -> PaymentResult<()> {
        self.inner.expire_session(id).await
    }

    async fn refund(&self, id: &str) -> PaymentResult<()> {
        self.inner.refund(id).await
    }

    fn verify_webhook(&self, headers: &HeaderMap, body: &[u8]) -> PaymentResult<ProviderEvent> {
        self.inner.verify_webhook(headers, body)
    }
}

#[tokio::test]
async fn a_transient_error_is_retried_not_skipped() {
    let fx = fixture(5).await;
    let event = fx.winner().await;
    let flaky = Arc::new(FlakyProvider { inner: fx.env.shared_provider(), failures: 2, calls: AtomicUsize::new(0) });
    let processor = fx.processor_with(flaky.clone());

    assert!(processor.handle(&event).await.is_err(), "the error surfaces from a single attempt");
    let (_stop, shutdown) = watch::channel(false);
    let outcome = processor.handle_with_retry(&event, &shutdown).await;
    assert_eq!(outcome, Some(Outcome::Linked));
    assert_eq!(flaky.calls.load(Ordering::SeqCst), 3);
    assert_eq!(fx.order(&event.ticket).unwrap().status, OrderStatus::AwaitingPayment);

    // At shutdown a failing message is given up on (left undone), not retried forever
    let event = fx.winner().await;
    let failing = fx.processor_with(Arc::new(FlakyProvider { inner: fx.env.shared_provider(), failures: usize::MAX, calls: AtomicUsize::new(0) }));
    let (stop, shutdown) = watch::channel(false);
    stop.send(true).unwrap();
    assert_eq!(failing.handle_with_retry(&event, &shutdown).await, None);
    fx.cleanup().await;
}

/// Real Kafka: publish a few events, run the consumer loop, see the orders,
/// the committed offsets and the progress keys
#[tokio::test]
async fn the_consumer_loop_processes_commits_and_reports_progress() {
    let mut fx = fixture(20).await;
    fx.env.config.checkout_topic = format!("test.processor.{}", uuid::Uuid::new_v4().simple());
    fx.env.config.checkout_partitions = 3;
    fx.env.config.processor_concurrency = 1; // fewer than a partition gets, so partitions pause and resume
    let config = fx.env.config.clone();
    ensure_topic(&config).await.unwrap();
    let queue = KafkaQueue::new(&config).unwrap();

    let mut published = Vec::new();
    for _ in 0..9 {
        let event = fx.winner().await;
        let at = queue.publish(&event).await.unwrap();
        published.push((event, at));
    }

    let (stop, shutdown) = watch::channel(false);
    let processor = Arc::new(fx.processor());
    let run = tokio::spawn({
        let (config, inventory, read_cache) = (config.clone(), fx.env.redis.clone(), fx.env.read_cache.clone());
        async move { processor::run(&config, processor, inventory, read_cache, shutdown).await }
    });

    // Per partition: the last message by offset
    let mut last: std::collections::BTreeMap<i32, (i64, i64)> = Default::default();
    for (event, at) in &published {
        let entry = last.entry(at.partition).or_insert((at.offset, event.checkout_ms));
        if at.offset >= entry.0 {
            *entry = (at.offset, event.checkout_ms);
        }
    }

    let deadline = tokio::time::Instant::now() + StdDuration::from_secs(60);
    let mut read_cache = fx.env.read_cache.clone();
    loop {
        let mut all_done = true;
        for (event, _) in &published {
            all_done &= fx.order(&event.ticket).is_some_and(|o| o.status == OrderStatus::AwaitingPayment);
        }
        for (&partition, &(offset, _)) in &last {
            let progress = cache::read_progress(&mut read_cache, &config.checkout_topic, partition).await.unwrap();
            all_done &= progress == Some(offset);
        }
        if all_done {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "the processor did not finish in time");
        tokio::time::sleep(StdDuration::from_millis(200)).await;
    }

    stop.send(true).unwrap();
    tokio::time::timeout(StdDuration::from_secs(30), run).await.expect("shutdown in time").unwrap().unwrap();

    // Every order has its link; status entries carry it
    for (event, _) in &published {
        let status = fx.status(&event.ticket).await.unwrap();
        assert_eq!(status.status, OrderStatus::AwaitingPayment);
        assert!(status.payment_url.unwrap().starts_with("http://pay.test/fake-pay/"));
        assert_eq!(fx.hold_state(&event.ticket).await, "AWAITING_PAYMENT");
    }
    // processed_upto per partition = checkout time of the last message, progress = its offset
    let mut inventory = fx.env.redis.clone();
    for (&partition, &(offset, checkout_ms)) in &last {
        let upto = cache::read_processed_upto(&mut inventory, fx.sale, partition).await.unwrap();
        assert_eq!(upto, Some(checkout_ms), "partition {partition}");
        let progress = cache::read_progress(&mut read_cache, &config.checkout_topic, partition).await.unwrap();
        assert_eq!(progress, Some(offset), "partition {partition}");
    }
    // The committed offsets are the next offsets to read
    let reader: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &config.kafka_brokers)
        .set("group.id", processor::GROUP_ID)
        .create()
        .unwrap();
    let mut wanted = TopicPartitionList::new();
    for &partition in last.keys() {
        wanted.add_partition(&config.checkout_topic, partition);
    }
    let committed = reader.committed_offsets(wanted, StdDuration::from_secs(10)).unwrap();
    for (&partition, &(offset, _)) in &last {
        let got = committed.find_partition(&config.checkout_topic, partition).unwrap().offset();
        assert_eq!(got, Offset::Offset(offset + 1), "partition {partition}");
    }

    fx.cleanup().await;
    let mut con = fx.env.read_cache.0.clone();
    for &partition in last.keys() {
        let _: () = con.del(cache::progress_key(&config.checkout_topic, partition)).await.unwrap();
    }
}
