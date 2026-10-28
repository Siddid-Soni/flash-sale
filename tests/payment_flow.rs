//! The whole payment path across the services, each on its own port: the fake
//! provider's payment page and API (a server of this test binary), its webhook
//! dispatcher, the order processor's webhook endpoint (`POST
//! /v1/payments/webhook`, mounted as `order_processor` mounts it), and the
//! expiration worker. The outbox is one queue for the whole Redis database, so
//! the tests take a lock and empty it first.

mod common;

use actix_web::{http::StatusCode, web, App, HttpServer};
use chrono::{Duration, Utc};
use diesel::prelude::*;
use redis::AsyncCommands;

use common::sale::{clear_inbox, SaleFixture};
use flash_sale::{
    api,
    models::{OrderState, OrderStatus},
    payment::{
        dispatcher::{Dispatcher, DispatcherSettings},
        SharedProvider,
    },
    schema::orders,
};

static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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
/// event to the processor, which confirms the order at once and counts the
/// sale once; the expiration worker never returns a confirmed order's unit
#[actix_web::test]
async fn paying_at_the_page_ends_with_a_confirmed_order_and_a_counted_sale() {
    let _guard = LOCK.lock().await;
    let _due = common::sale::due_guard().await;
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
    assert_eq!(fx.order(ticket).unwrap().status, OrderState::Confirmed);
    assert_eq!(fx.status(ticket).await.unwrap().status, OrderStatus::Confirmed);
    assert_eq!(fx.product().sold, 1);
    let unprocessed: i64 = flash_sale::schema::payment_inbox::table
        .filter(flash_sale::schema::payment_inbox::processed_at.is_null())
        .count()
        .get_result(&mut fx.env.pool.get().unwrap())
        .unwrap();
    assert_eq!(unprocessed, 0, "the event was handled on receipt, not left for the inbox loop");
    assert_eq!(fx.stock().await, 2);

    // The deadline arrives; the worker leaves the paid order alone
    let past = Utc::now() - Duration::seconds(1);
    diesel::update(orders::table.filter(orders::queue_ticket_id.eq(ticket)))
        .set(orders::expires_at.eq(past))
        .execute(&mut fx.env.pool.get().unwrap())
        .unwrap();
    fx.expire_due().await;
    assert_eq!(fx.order(ticket).unwrap().status, OrderState::Confirmed);
    assert_eq!(fx.stock().await, 2, "no stock came back");
    assert_eq!(fx.user_live(a.event.user).await.as_deref(), Some(ticket.as_str()), "the buyer stays marked");
    assert_eq!(fx.product().sold, 1);
    assert_eq!(fx.status(ticket).await.unwrap().status, OrderStatus::Confirmed);

    // Nothing is touched twice
    fx.expire_due().await;
    assert_eq!((fx.stock().await, fx.product().sold), (2, 1));
    fx.cleanup().await;
}
