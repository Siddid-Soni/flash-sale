//! The whole payment path across the services, each on its own port: the fake
//! provider's payment page and API (a server of this test binary), its webhook
//! dispatcher, the order processor's webhook endpoint (`POST
//! /v1/payments/webhook`, mounted as `order_processor` mounts it), and the
//! expiration worker. The outbox is one queue for the whole Redis database, so
//! the tests take a lock and empty it first.

mod common;

use std::sync::Arc;

use actix_web::{http::StatusCode, web, App, HttpServer};
use async_trait::async_trait;
use chrono::{Duration, Utc};
use diesel::prelude::*;
use redis::AsyncCommands;

use common::sale::{clear_inbox, SaleFixture};
use flash_sale::{
    api,
    cache,
    expiry::{Expirer, ExpiryError, LagSource, PartitionLag, Settings},
    models::OrderStatus,
    payment::{
        dispatcher::{Dispatcher, DispatcherSettings},
        SharedProvider,
    },
    schema::orders,
};

static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct QuietKafka;

#[async_trait]
impl LagSource for QuietKafka {
    async fn partitions(&self) -> Result<Vec<PartitionLag>, ExpiryError> {
        Ok(vec![PartitionLag { partition: 0, lagging: false }])
    }
}

/// The order processor's web server on a free port; returns the webhook URL
fn start_processor_webhook(fx: &SaleFixture) -> String {
    let (provider, inbox) = (web::Data::<SharedProvider>::new(fx.env.shared_provider()), web::Data::from(fx.inbox_worker()));
    let server = HttpServer::new(move || {
        App::new().app_data(provider.clone()).app_data(inbox.clone()).configure(api::payments::configure)
    })
    .workers(1)
    .bind(("127.0.0.1", 0))
    .unwrap();
    let addr = server.addrs()[0];
    actix_web::rt::spawn(server.run());
    format!("http://{addr}/v1/payments/webhook")
}

/// The user pays at the page, the provider's dispatcher delivers the signed
/// event to the processor, which confirms the order at once; the worker later
/// settles the hold and counts the sale once
#[actix_web::test]
async fn paying_at_the_page_ends_with_a_confirmed_order_and_a_counted_sale() {
    let _guard = LOCK.lock().await;
    let fx = SaleFixture::new(3).await;
    let _: () = fx.env.redis.clone().del("fakepay:outbox").await.unwrap();
    clear_inbox(&fx.env);
    let a = fx.awaiting().await;
    let webhook_url = start_processor_webhook(&fx);
    let dispatcher = Dispatcher::new(fx.fake(), webhook_url, DispatcherSettings::default());

    // The user presses Pay on the provider's page
    let link = a.order.checkout_url.clone().unwrap();
    assert!(link.ends_with(&format!("/fake-pay/{}", a.session.id)), "{link}");
    let client = awc::Client::builder().disable_redirects().finish();
    let page = format!("{}/fake-pay/{}", common::provider_url(), a.session.id);
    let resp = client.get(&page).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let resp = client.post(format!("{page}/pay")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::SEE_OTHER);
    assert_eq!(fx.fake().outbox_len().await.unwrap(), 1);

    // The dispatcher delivers; the endpoint answers 200 once the order is decided
    assert_eq!(dispatcher.sweep().await.unwrap(), 1);
    assert_eq!(fx.fake().outbox_len().await.unwrap(), 0, "the endpoint answered 200");
    let ticket = &a.event.ticket;
    assert_eq!(fx.order(ticket).unwrap().status, OrderStatus::Confirmed);
    assert_eq!(fx.status(ticket).await.unwrap().status, OrderStatus::Confirmed);
    assert_eq!(fx.product().sold, 1);
    let unprocessed: i64 = flash_sale::schema::payment_inbox::table
        .filter(flash_sale::schema::payment_inbox::processed_at.is_null())
        .count()
        .get_result(&mut fx.env.pool.get().unwrap())
        .unwrap();
    assert_eq!(unprocessed, 0, "the event was handled on receipt, not left for the inbox loop");
    assert_eq!(fx.hold_state(ticket).await, "AWAITING_PAYMENT", "the worker settles the hold, not the payment path");
    assert_eq!(fx.sold_counter().await, 0, "sold:{{sale}} is counted by the worker");
    assert_eq!(fx.stock().await, 2);

    // The deadline arrives (both clocks); the worker finds the paid order
    let past = Utc::now() - Duration::seconds(1);
    diesel::update(orders::table.filter(orders::queue_ticket_id.eq(ticket)))
        .set(orders::expires_at.eq(past))
        .execute(&mut fx.env.pool.get().unwrap())
        .unwrap();
    let _: i64 = redis::cmd("ZADD")
        .arg(cache::holds_expiry_key(fx.sale))
        .arg("XX")
        .arg("CH")
        .arg(past.timestamp_millis())
        .arg(ticket)
        .query_async(&mut fx.env.redis.clone())
        .await
        .unwrap();
    let settings = Settings {
        batch: 100,
        delivery_timeout_ms: 30_000,
        stale_after: Duration::minutes(10),
        sale_lookback: Duration::days(3),
        expired_recheck: Duration::minutes(30),
    };
    let worker = Expirer::new(
        fx.env.pool.clone(),
        fx.env.redis.clone(),
        fx.env.read_cache.clone(),
        fx.env.shared_provider(),
        Arc::new(QuietKafka),
        settings,
    );
    let pass = worker.unpaid_pass(&[fx.sale]).await.unwrap();
    assert_eq!((pass.confirmed, pass.released), (1, 0));
    assert_eq!(fx.hold_state(ticket).await, "CONFIRMED");
    assert_eq!(fx.sold_counter().await, 1);
    assert_eq!(fx.stock().await, 2, "no stock came back");
    assert_eq!(fx.product().sold, 1);
    let queued: i64 = fx.env.redis.clone().zcard(cache::holds_expiry_key(fx.sale)).await.unwrap();
    assert_eq!(queued, 0);

    // Nothing is counted twice
    assert_eq!(worker.unpaid_pass(&[fx.sale]).await.unwrap().confirmed, 0);
    assert_eq!(fx.sold_counter().await, 1);
    fx.cleanup().await;
}
