//! A sale with real Postgres and Redis, and the helpers the payment tests
//! share: winners, orders waiting for payment, and the events a provider would
//! send for them.
#![allow(dead_code)]

use std::sync::Arc;

use chrono::{Duration, Utc};
use diesel::prelude::*;
use redis::AsyncCommands;

use flash_sale::{
    actions,
    cache::{self, CheckoutOutcome},
    inbox::InboxWorker,
    models::{NewPaymentInboxEntry, NewProduct, Order},
    payment::{
        webhook::{self, CHECKOUT_COMPLETED, CHECKOUT_PAYMENT_FAILED},
        FakeProvider, FakeSession, ProviderEvent, Settle,
    },
    processor::{Outcome, Processor},
    queue::CheckoutEvent,
};

use super::{Env, WEBHOOK_SECRET};

pub const PRICE_CENTS: i32 = 59_900;
pub const WINDOW_SECS: u64 = 300;

pub struct SaleFixture {
    pub env: Env,
    pub sale: i32,
}

/// An order waiting for payment: its hold is `AWAITING_PAYMENT`
pub struct Awaiting {
    pub event: CheckoutEvent,
    pub order: Order,
    pub session: FakeSession,
}

/// An event as the provider sends it: the parsed event and its raw JSON body
pub struct Wire {
    pub event: ProviderEvent,
    pub body: String,
}

impl Wire {
    /// The `Fake-Signature` header for this body, signed now with the test secret
    pub fn signature(&self) -> String {
        webhook::sign(WEBHOOK_SECRET, Utc::now().timestamp(), self.body.as_bytes()).unwrap()
    }
}

impl SaleFixture {
    pub async fn new(stock: i32) -> Self {
        let env = super::env().await;
        let product = actions::products::create_product(
            &mut env.pool.get().unwrap(),
            &NewProduct {
                name: "Smartphone",
                price_cents: PRICE_CENTS,
                total_inventory: stock,
                sale_start_time: Utc::now() - Duration::seconds(60),
            },
        )
        .unwrap();
        cache::init_sale(&mut env.redis.clone(), product.id, stock, product.sale_start_time).await.unwrap();
        Self { sale: product.id, env }
    }

    pub fn fake(&self) -> Arc<FakeProvider> {
        self.env.provider.clone()
    }

    pub fn processor(&self) -> Processor {
        Processor::new(
            self.env.pool.clone(),
            self.env.redis.clone(),
            self.env.read_cache.clone(),
            self.env.shared_provider(),
            WINDOW_SECS,
        )
    }

    pub fn inbox_worker(&self) -> Arc<InboxWorker> {
        Arc::new(InboxWorker::new(self.env.pool.clone(), self.env.read_cache.clone(), self.env.shared_provider()))
    }

    /// A new guest with a `PENDING` hold and the event its checkout would publish
    pub async fn winner(&self) -> CheckoutEvent {
        let user = actions::users::create_guest(&mut self.env.pool.get().unwrap()).unwrap();
        let ticket = cache::new_ticket(self.sale);
        let outcome =
            cache::checkout(&mut self.env.redis.clone(), self.sale, user.id, &format!("key-{}", user.id), &ticket)
                .await
                .unwrap();
        let CheckoutOutcome::Held { checkout_ms } = outcome else { panic!("{outcome:?}") };
        CheckoutEvent { ticket, sale: self.sale, user: user.id, checkout_ms }
    }

    /// A winner whose order and payment session exist and whose hold awaits payment
    pub async fn awaiting(&self) -> Awaiting {
        let event = self.winner().await;
        assert_eq!(self.processor().handle(&event).await.unwrap(), Outcome::Linked);
        let order = self.order(&event.ticket).unwrap();
        let session = self.fake().session(order.payment_session_id.as_deref().unwrap()).await.unwrap().unwrap();
        Awaiting { event, order, session }
    }

    pub fn order(&self, ticket: &str) -> Option<Order> {
        actions::orders::find_by_ticket(&mut self.env.pool.get().unwrap(), ticket).unwrap()
    }

    pub fn product(&self) -> flash_sale::models::Product {
        actions::products::find_product(&mut self.env.pool.get().unwrap(), self.sale).unwrap().unwrap()
    }

    /// Moves the payment deadline of an `AWAITING_PAYMENT` order, in PostgreSQL
    /// (which decides) and in `holds:expiry` (which says when to look), to `at_ms`
    /// (epoch ms)
    pub async fn set_deadline(&self, ticket: &str, at_ms: i64) {
        self.set_pg_deadline(ticket, at_ms);
        self.set_redis_deadline(ticket, at_ms).await;
    }

    /// Only PostgreSQL's deadline of the order
    pub fn set_pg_deadline(&self, ticket: &str, at_ms: i64) {
        use flash_sale::schema::orders;
        let at = chrono::DateTime::from_timestamp_millis(at_ms).unwrap();
        let changed = diesel::update(orders::table.filter(orders::queue_ticket_id.eq(ticket)))
            .set(orders::expires_at.eq(at))
            .execute(&mut self.env.pool.get().unwrap())
            .unwrap();
        assert_eq!(changed, 1, "{ticket} has no order");
    }

    /// Only the hold's score in `holds:expiry`
    pub async fn set_redis_deadline(&self, ticket: &str, at_ms: i64) {
        let changed: i64 = redis::cmd("ZADD")
            .arg(cache::holds_expiry_key(self.sale))
            .arg("XX")
            .arg("CH")
            .arg(at_ms)
            .arg(ticket)
            .query_async(&mut self.env.redis.clone())
            .await
            .unwrap();
        assert_eq!(changed, 1, "{ticket} is not in holds:expiry");
    }

    /// The deadline has passed, for PostgreSQL and for Redis
    pub async fn make_due(&self, ticket: &str) {
        self.set_deadline(ticket, Utc::now().timestamp_millis() - 1_000).await;
    }

    /// Holds waiting in `holds:expiry:{sale}`
    pub async fn expiry_len(&self) -> i64 {
        self.env.redis.clone().zcard(cache::holds_expiry_key(self.sale)).await.unwrap()
    }

    pub async fn hold_state(&self, ticket: &str) -> String {
        cache::read_hold(&mut self.env.redis.clone(), self.sale, ticket).await.unwrap().unwrap().state
    }

    pub async fn counter(&self, key: String) -> i64 {
        self.env.redis.clone().get::<_, Option<i64>>(key).await.unwrap().unwrap_or(0)
    }

    /// `sold:{sale}`
    pub async fn sold_counter(&self) -> i64 {
        self.counter(cache::sold_key(self.sale)).await
    }

    pub async fn stock(&self) -> i64 {
        self.counter(cache::stock_key(self.sale)).await
    }

    pub async fn status(&self, ticket: &str) -> Option<cache::CachedStatus> {
        cache::read_status(&mut self.env.read_cache.clone(), ticket).await.unwrap()
    }

    /// Orders of this sale in the given status
    pub fn count_orders(&self, status: flash_sale::models::OrderStatus) -> i64 {
        use flash_sale::schema::orders;
        orders::table
            .filter(orders::product_id.eq(self.sale))
            .filter(orders::status.eq(status))
            .count()
            .get_result(&mut self.env.pool.get().unwrap())
            .unwrap()
    }

    /// The user pays at the provider: the session turns `PAID` (this also
    /// queues the provider's own webhook, which these tests ignore). Returns
    /// the success event with a unique id.
    pub async fn pay(&self, a: &Awaiting) -> Wire {
        let settled = self.fake().settle(&a.session, Settle::Pay).await.unwrap();
        assert_eq!(settled, flash_sale::payment::Settled::Moved);
        self.wire(a, CHECKOUT_COMPLETED)
    }

    /// The user cancels at the provider
    pub async fn cancel(&self, a: &Awaiting) -> Wire {
        self.fake().settle(&a.session, Settle::Cancel).await.unwrap();
        self.wire(a, CHECKOUT_PAYMENT_FAILED)
    }

    /// An event of `event_type` for the session, without touching the provider
    pub fn wire(&self, a: &Awaiting, event_type: &str) -> Wire {
        let id = format!("evt_test_{}", uuid::Uuid::new_v4().simple());
        let payment_id = (event_type == CHECKOUT_COMPLETED).then(|| flash_sale::payment::fake_payment_id(&a.session.id));
        let body = webhook::event_body(
            &id,
            event_type,
            Utc::now().timestamp(),
            &a.session.id,
            &a.event.ticket,
            a.session.amount_cents,
            payment_id.as_deref(),
        );
        let event = ProviderEvent::from_json(serde_json::from_str(&body).unwrap()).unwrap();
        Wire { event, body }
    }

    /// Stores an event in the inbox, as the webhook endpoint does
    pub fn store(&self, wire: &Wire) {
        let entry = NewPaymentInboxEntry {
            provider_event_id: &wire.event.id,
            event_type: &wire.event.event_type,
            payload: &wire.event.payload,
        };
        assert!(actions::payments::insert_inbox(&mut self.env.pool.get().unwrap(), &entry).unwrap());
    }

    /// The inbox row of an event
    pub fn inbox_row(&self, event_id: &str) -> Option<flash_sale::models::PaymentInboxEntry> {
        use flash_sale::schema::payment_inbox;
        payment_inbox::table
            .filter(payment_inbox::provider_event_id.eq(event_id))
            .select(flash_sale::models::PaymentInboxEntry::as_select())
            .first(&mut self.env.pool.get().unwrap())
            .optional()
            .unwrap()
    }

    pub async fn cleanup(&self) {
        let mut con = self.env.redis.clone();
        for pattern in [format!("*{{{}}}*", self.sale), format!("status:{}.*", self.sale)] {
            let keys: Vec<String> = con.keys(pattern).await.unwrap();
            if !keys.is_empty() {
                let _: () = con.del(keys).await.unwrap();
            }
        }
    }
}

/// Marks every unprocessed inbox row processed, so a test that runs a worker
/// handles only its own events (tests share one database)
pub fn clear_inbox(env: &Env) {
    use flash_sale::schema::payment_inbox;
    diesel::update(payment_inbox::table.filter(payment_inbox::processed_at.is_null()))
        .set(payment_inbox::processed_at.eq(Utc::now()))
        .execute(&mut env.pool.get().unwrap())
        .unwrap();
}

/// Processes the inbox until it is empty; returns how many events it handled
pub async fn drain(worker: &Arc<InboxWorker>) -> usize {
    let mut handled = 0;
    while worker.process_next().await.unwrap() {
        handled += 1;
    }
    handled
}
