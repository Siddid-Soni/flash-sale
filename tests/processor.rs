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
    cache::{self, CheckoutOutcome},
    models::{NewOrder, NewProduct, Order, OrderState, OrderStatus},
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
        Processor::new(self.env.pool.clone(), self.env.read_cache.clone(), provider, WINDOW_SECS)
    }

    fn processor(&self) -> Processor {
        self.processor_with(self.env.shared_provider())
    }

    /// A new guest whose checkout reserved a unit, and the event it would publish
    async fn winner(&self) -> CheckoutEvent {
        let user = actions::users::create_guest(&mut self.env.pool.get().unwrap()).unwrap();
        let ticket = cache::new_ticket(self.sale);
        let outcome =
            cache::checkout(&mut self.env.redis.clone(), self.sale, user.id, &format!("key-{}", user.id), &ticket)
                .await
                .unwrap();
        let CheckoutOutcome::Reserved { checkout_ms } = outcome else { panic!("{outcome:?}") };
        CheckoutEvent { ticket, sale: self.sale, user: user.id, checkout_ms, idempotency_key: format!("key-{}", user.id) }
    }

    fn order(&self, ticket: &str) -> Option<Order> {
        actions::orders::find_by_ticket(&mut self.env.pool.get().unwrap(), ticket).unwrap()
    }

    async fn stock(&self) -> i64 {
        self.env.redis.clone().get::<_, Option<i64>>(cache::stock_key(self.sale)).await.unwrap().unwrap_or(0)
    }

    async fn status(&self, ticket: &str) -> Option<cache::CachedStatus> {
        cache::read_status(&mut self.env.read_cache.clone(), ticket).await.unwrap()
    }

    /// A unit-gating key as it stands: every inventory key the processor must leave alone
    async fn inventory_keys(&self) -> Vec<(String, String)> {
        let mut con = self.env.redis.clone();
        let keys: Vec<String> = con.keys(format!("*{{{}}}*", self.sale)).await.unwrap();
        let mut snapshot = Vec::new();
        for key in keys {
            let value: String = redis::cmd("GET").arg(&key).query_async(&mut con).await.unwrap_or_default();
            snapshot.push((key, value));
        }
        snapshot.sort();
        snapshot
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
    let inventory_before = fx.inventory_keys().await;

    assert_eq!(processor.handle(&event).await.unwrap(), Outcome::Linked);

    // The order: AWAITING_PAYMENT with the session, URL and deadline, the price of the product
    let order = fx.order(&event.ticket).unwrap();
    assert_eq!((order.status, order.user_id, order.product_id), (OrderState::AwaitingPayment, event.user, fx.sale));
    assert_eq!(order.amount_cents, 59_900);
    assert_eq!(order.idempotency_key, format!("key-{}", event.user), "the key comes from the event");
    let session_id = order.payment_session_id.clone().unwrap();
    assert_eq!(order.checkout_url.as_deref(), Some(format!("http://pay.test/fake-pay/{session_id}").as_str()));
    let expires_at = order.expires_at.unwrap();
    let window = expires_at - Utc::now();
    assert!(window > Duration::seconds(WINDOW_SECS as i64 - 10) && window <= Duration::seconds(WINDOW_SECS as i64));

    // No inventory key was touched: the stock and the markers are as checkout left them
    assert_eq!(fx.inventory_keys().await, inventory_before);
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

    // The crash came before the status write: the redelivery repairs it
    let _: () = fx.env.read_cache.clone().0.del(cache::status_key(&event.ticket)).await.unwrap();
    assert_eq!(processor.handle(&event).await.unwrap(), Outcome::Linked);
    assert_eq!(processor.handle(&event).await.unwrap(), Outcome::Linked);

    let after = fx.order(&event.ticket).unwrap();
    assert_eq!(
        (after.id, after.status, &after.payment_session_id, &after.checkout_url, after.expires_at),
        (before.id, before.status, &before.payment_session_id, &before.checkout_url, before.expires_at)
    );
    assert_eq!(fx.stock().await, 4);
    let status = fx.status(&event.ticket).await.unwrap();
    assert_eq!((status.status, status.payment_url), (OrderStatus::AwaitingPayment, before.checkout_url.clone()));
    assert_eq!(status.expires_at, Some(millis(before.expires_at.unwrap())));
    let one_order: i64 = orders::table.filter(orders::queue_ticket_id.eq(&event.ticket)).count().get_result(&mut fx.env.pool.get().unwrap()).unwrap();
    assert_eq!(one_order, 1);
    fx.cleanup().await;
}

/// The Order Service could not publish, recorded the ticket as FAILED and
/// returned the unit; the message reached Kafka after all
#[tokio::test]
async fn a_failed_order_stops_the_processor_and_expires_its_session() {
    let fx = fixture(5).await;
    let event = fx.winner().await;
    assert!(matches!(
        actions::orders::insert_failed(&mut fx.env.pool.get().unwrap(), event.user, fx.sale, &event.ticket).unwrap(),
        actions::orders::FailedInsert::Created
    ));
    let inventory_before = fx.inventory_keys().await;

    assert_eq!(fx.processor().handle(&event).await.unwrap(), Outcome::Failed);

    // The row stays FAILED, without a session or link; nothing else was written
    let order = fx.order(&event.ticket).unwrap();
    assert_eq!((order.status, order.payment_session_id, order.checkout_url, order.expires_at), (OrderState::Failed, None, None, None));
    assert!(fx.status(&event.ticket).await.is_none(), "no status entry");
    assert_eq!(fx.inventory_keys().await, inventory_before, "no inventory key");
    // The session made for the message is stopped: nobody can pay it
    let session_id = fx.fake.session_id_for_ticket(&event.ticket).await.unwrap().unwrap();
    assert_eq!(fx.fake.session(&session_id).await.unwrap().unwrap().state, "EXPIRED");
    // A redelivery does the same
    assert_eq!(fx.processor().handle(&event).await.unwrap(), Outcome::Failed);
    fx.cleanup().await;
}

/// A redelivery after the order was paid or expired writes nothing and leaves
/// its (paid) session alone
#[tokio::test]
async fn a_redelivery_after_the_order_moved_on_is_skipped() {
    let fx = fixture(5).await;
    let event = fx.winner().await;
    let processor = fx.processor();
    assert_eq!(processor.handle(&event).await.unwrap(), Outcome::Linked);
    let session_id = fx.order(&event.ticket).unwrap().payment_session_id.unwrap();
    diesel::update(orders::table.filter(orders::queue_ticket_id.eq(&event.ticket)))
        .set(orders::status.eq(OrderState::Confirmed))
        .execute(&mut fx.env.pool.get().unwrap())
        .unwrap();
    let before = fx.status(&event.ticket).await;

    assert_eq!(processor.handle(&event).await.unwrap(), Outcome::Skipped);
    assert_eq!(fx.order(&event.ticket).unwrap().status, OrderState::Confirmed);
    assert_eq!(fx.status(&event.ticket).await, before);
    assert_eq!(fx.fake.session(&session_id).await.unwrap().unwrap().state, "OPEN", "the session is not touched");
    fx.cleanup().await;
}

#[tokio::test]
async fn an_order_another_constraint_refuses_fails_the_ticket_and_expires_the_session() {
    let fx = fixture(5).await;
    let event = fx.winner().await;
    // The user already has a live order from some other ticket
    diesel::insert_into(orders::table)
        .values(&NewOrder {
            user_id: event.user,
            product_id: fx.sale,
            queue_ticket_id: &format!("{}.{}", fx.sale, uuid::Uuid::new_v4()),
            idempotency_key: "older",
            status: OrderState::AwaitingPayment,
            amount_cents: 59_900,
            payment_session_id: None,
            checkout_url: None,
            expires_at: None,
        })
        .execute(&mut fx.env.pool.get().unwrap())
        .unwrap();

    assert_eq!(fx.processor().handle(&event).await.unwrap(), Outcome::Dropped);
    assert!(fx.order(&event.ticket).is_none());
    // The client is told, and the session made for it is stopped: nobody can
    // pay a link that is never shown
    assert_eq!(fx.status(&event.ticket).await.unwrap().status, OrderStatus::Failed);
    let session_id = fx.fake.session_id_for_ticket(&event.ticket).await.unwrap().unwrap();
    assert_eq!(fx.fake.session(&session_id).await.unwrap().unwrap().state, "EXPIRED");
    fx.cleanup().await;
}

/// The processor died after the INSERT but before the status write: the order
/// exists with its first session and deadline. The redelivery keeps that row
/// and writes its link and deadline.
#[tokio::test]
async fn a_redelivery_after_the_insert_publishes_the_orders_link_and_deadline() {
    let fx = fixture(5).await;
    let event = fx.winner().await;
    let first_deadline_ms = Utc::now().timestamp_millis() + 120_000;
    let first_deadline = DateTime::from_timestamp_millis(first_deadline_ms).unwrap();
    let session = fx.env.shared_provider().create_session(&event.ticket, 59_900, first_deadline, &event.ticket).await.unwrap();
    diesel::insert_into(orders::table)
        .values(&NewOrder {
            user_id: event.user,
            product_id: fx.sale,
            queue_ticket_id: &event.ticket,
            idempotency_key: &format!("key-{}", event.user),
            status: OrderState::AwaitingPayment,
            amount_cents: 59_900,
            payment_session_id: Some(&session.id),
            checkout_url: Some(&session.url),
            expires_at: Some(first_deadline),
        })
        .execute(&mut fx.env.pool.get().unwrap())
        .unwrap();
    assert!(fx.status(&event.ticket).await.is_none());

    assert_eq!(fx.processor().handle(&event).await.unwrap(), Outcome::Linked);

    let order = fx.order(&event.ticket).unwrap();
    assert_eq!(
        (order.payment_session_id.as_deref(), order.expires_at),
        (Some(session.id.as_str()), Some(first_deadline)),
        "the row of the first attempt stays"
    );
    let status = fx.status(&event.ticket).await.unwrap();
    assert_eq!((status.status, status.payment_url), (OrderStatus::AwaitingPayment, Some(session.url.clone())));
    assert_eq!(status.expires_at, Some(millis(first_deadline)));
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
    assert_eq!(fx.order(&event.ticket).unwrap().status, OrderState::AwaitingPayment);

    // At shutdown a failing message is given up on (left undone), not retried forever
    let event = fx.winner().await;
    let failing = fx.processor_with(Arc::new(FlakyProvider { inner: fx.env.shared_provider(), failures: usize::MAX, calls: AtomicUsize::new(0) }));
    let (stop, shutdown) = watch::channel(false);
    stop.send(true).unwrap();
    assert_eq!(failing.handle_with_retry(&event, &shutdown).await, None);
    fx.cleanup().await;
}

/// Real Kafka: publish a few events, run the consumer loop, see the orders
/// and the committed offsets
#[tokio::test]
async fn the_consumer_loop_processes_and_commits() {
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
        queue.publish(&event).await.unwrap();
        published.push(event);
    }

    let (stop, shutdown) = watch::channel(false);
    let processor = Arc::new(fx.processor());
    let run = tokio::spawn({
        let config = config.clone();
        async move { processor::run(&config, processor, shutdown).await }
    });

    let deadline = tokio::time::Instant::now() + StdDuration::from_secs(60);
    loop {
        let all_done = published
            .iter()
            .all(|event| fx.order(&event.ticket).is_some_and(|o| o.status == OrderState::AwaitingPayment));
        if all_done {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "the processor did not finish in time");
        tokio::time::sleep(StdDuration::from_millis(200)).await;
    }

    stop.send(true).unwrap();
    tokio::time::timeout(StdDuration::from_secs(30), run).await.expect("shutdown in time").unwrap().unwrap();

    // Every order has its link; status entries carry it
    for event in &published {
        let status = fx.status(&event.ticket).await.unwrap();
        assert_eq!(status.status, OrderStatus::AwaitingPayment);
        assert!(status.payment_url.unwrap().starts_with("http://pay.test/fake-pay/"));
    }
    // The committed offsets are the next offsets to read: each partition's end
    let reader: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &config.kafka_brokers)
        .set("group.id", processor::GROUP_ID)
        .create()
        .unwrap();
    let mut wanted = TopicPartitionList::new();
    for partition in 0..config.checkout_partitions {
        wanted.add_partition(&config.checkout_topic, partition);
    }
    let committed = reader.committed_offsets(wanted, StdDuration::from_secs(10)).unwrap();
    for partition in 0..config.checkout_partitions {
        let (_, end) = reader.fetch_watermarks(&config.checkout_topic, partition, StdDuration::from_secs(10)).unwrap();
        if end == 0 {
            continue; // nothing published there, nothing to commit
        }
        let got = committed.find_partition(&config.checkout_topic, partition).unwrap().offset();
        assert_eq!(got, Offset::Offset(end), "partition {partition}");
    }

    fx.cleanup().await;
}
