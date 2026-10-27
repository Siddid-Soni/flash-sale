//! The fake provider's payment page and webhook dispatcher, against real
//! Postgres and Redis. The outbox (`fakepay:outbox`) is one queue for the
//! whole Redis database, so these tests take a lock and empty it first.
//! (The whole path from the page to a confirmed order is in `payment_flow.rs`.)

mod common;

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

use actix_web::{
    http::{header, header::HeaderMap, StatusCode},
    test, web, App, HttpRequest, HttpResponse, HttpServer,
};
use redis::AsyncCommands;

use common::sale::{clear_inbox, SaleFixture};
use flash_sale::{
    payment::{
        dispatcher::{Dispatcher, DispatcherSettings},
        webhook::{CHECKOUT_COMPLETED, CHECKOUT_PAYMENT_FAILED},
        EventKind, ProviderEvent, Settle,
    },
};

static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn setup(stock: i32) -> (tokio::sync::MutexGuard<'static, ()>, SaleFixture) {
    let guard = LOCK.lock().await;
    let mut fx = SaleFixture::new(stock).await;
    fx.env.config.frontend_url = "http://front.test/".into();
    let _: () = fx.env.redis.clone().del("fakepay:outbox").await.unwrap();
    clear_inbox(&fx.env);
    (guard, fx)
}

async fn body_text(resp: actix_web::dev::ServiceResponse) -> String {
    String::from_utf8(test::read_body(resp).await.to_vec()).unwrap()
}

fn location(resp: &actix_web::dev::ServiceResponse) -> &str {
    resp.headers().get(header::LOCATION).unwrap().to_str().unwrap()
}

#[actix_web::test]
async fn the_page_shows_the_session_and_escapes_what_it_prints() {
    let (_guard, fx) = setup(3).await;
    let a = fx.awaiting().await;
    let app = provider_app!(fx.env);

    let resp = test::call_service(&app, test::TestRequest::get().uri(&format!("/fake-pay/{}", a.session.id)).to_request()).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp.headers().get(header::CONTENT_TYPE).unwrap().to_str().unwrap().starts_with("text/html"));
    let page = body_text(resp).await;
    assert!(page.contains("$599.00"));
    assert!(page.contains(&a.event.ticket));
    assert!(page.contains(&format!("action=\"/fake-pay/{}/pay\"", a.session.id)));
    assert!(page.contains(&format!("action=\"/fake-pay/{}/cancel\"", a.session.id)));

    // A hostile ticket is shown as text, not as markup
    let hostile = "<script>alert(1)</script>\"'&";
    let session = fx.fake().create_session(hostile, 100, chrono::Utc::now() + chrono::Duration::minutes(5), hostile).await.unwrap();
    let resp = test::call_service(&app, test::TestRequest::get().uri(&format!("/fake-pay/{}", session.id)).to_request()).await;
    let page = body_text(resp).await;
    assert!(!page.contains("<script>alert"), "{page}");
    assert!(page.contains("&lt;script&gt;alert(1)&lt;/script&gt;&quot;&#39;&amp;"));

    // An unknown session
    let resp = test::call_service(&app, test::TestRequest::get().uri("/fake-pay/fake_cs_nope").to_request()).await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let resp = test::call_service(&app, test::TestRequest::post().uri("/fake-pay/fake_cs_nope/pay").to_request()).await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    fx.cleanup().await;
}

#[actix_web::test]
async fn paying_redirects_to_the_sale_and_queues_one_event() {
    let (_guard, fx) = setup(3).await;
    let a = fx.awaiting().await;
    let app = provider_app!(fx.env);

    let pay = || test::TestRequest::post().uri(&format!("/fake-pay/{}/pay", a.session.id)).to_request();
    let resp = test::call_service(&app, pay()).await;
    assert_eq!(resp.status(), StatusCode::SEE_OTHER);
    assert_eq!(location(&resp), format!("http://front.test/?sale={}", fx.sale));
    assert_eq!(fx.fake().session(&a.session.id).await.unwrap().unwrap().state, "PAID");
    assert_eq!(fx.fake().outbox_len().await.unwrap(), 1);

    // The page was pressed twice: back to the sale, no second event
    let resp = test::call_service(&app, pay()).await;
    assert_eq!(resp.status(), StatusCode::SEE_OTHER);
    assert_eq!(fx.fake().outbox_len().await.unwrap(), 1);

    // The event is the signed-success kind, for this session and ticket
    let events = fx.fake().claim_due_events(10, Duration::from_secs(30)).await.unwrap();
    assert_eq!(events.len(), 1);
    let event = ProviderEvent::from_json(serde_json::from_str(&events[0].body).unwrap()).unwrap();
    assert_eq!(
        (event.event_type.as_str(), event.session_id.as_str(), event.ticket.as_str(), event.amount_cents),
        (CHECKOUT_COMPLETED, a.session.id.as_str(), a.event.ticket.as_str(), 59_900)
    );
    assert_eq!(event.payment_id.as_deref(), Some(flash_sale::payment::fake_payment_id(&a.session.id).as_str()));

    // A paid session's page says so
    let resp = test::call_service(&app, test::TestRequest::get().uri(&format!("/fake-pay/{}", a.session.id)).to_request()).await;
    assert!(body_text(resp).await.contains("already been paid"));
    fx.cleanup().await;
}

#[actix_web::test]
async fn cancelling_queues_a_failure_event_and_closes_the_session() {
    let (_guard, fx) = setup(3).await;
    let a = fx.awaiting().await;
    let app = provider_app!(fx.env);

    let resp = test::call_service(&app, test::TestRequest::post().uri(&format!("/fake-pay/{}/cancel", a.session.id)).to_request()).await;
    assert_eq!(resp.status(), StatusCode::SEE_OTHER);
    assert_eq!(location(&resp), format!("http://front.test/?sale={}", fx.sale));
    assert_eq!(fx.fake().session(&a.session.id).await.unwrap().unwrap().state, "CANCELED");
    let events = fx.fake().claim_due_events(10, Duration::from_secs(30)).await.unwrap();
    assert_eq!(events.len(), 1);
    let event = ProviderEvent::from_json(serde_json::from_str(&events[0].body).unwrap()).unwrap();
    assert_eq!((event.event_type.as_str(), event.kind, event.payment_id), (CHECKOUT_PAYMENT_FAILED, EventKind::PaymentFailed, None));

    // A cancelled session cannot be paid
    let resp = test::call_service(&app, test::TestRequest::post().uri(&format!("/fake-pay/{}/pay", a.session.id)).to_request()).await;
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    assert!(body_text(resp).await.contains("cancelled"));
    assert_eq!(fx.fake().outbox_len().await.unwrap(), 1, "no second event");
    fx.cleanup().await;
}

#[actix_web::test]
async fn an_expired_session_cannot_be_paid() {
    let (_guard, fx) = setup(3).await;
    let a = fx.awaiting().await;
    fx.fake().expire_session(&a.session.id).await.unwrap();
    let app = provider_app!(fx.env);

    let resp = test::call_service(&app, test::TestRequest::post().uri(&format!("/fake-pay/{}/pay", a.session.id)).to_request()).await;
    assert_eq!(resp.status(), StatusCode::GONE);
    assert!(body_text(resp).await.contains("expired"));
    assert_eq!(fx.fake().session(&a.session.id).await.unwrap().unwrap().state, "EXPIRED");
    assert_eq!(fx.fake().outbox_len().await.unwrap(), 0, "no event");

    let resp = test::call_service(&app, test::TestRequest::get().uri(&format!("/fake-pay/{}", a.session.id)).to_request()).await;
    assert_eq!(resp.status(), StatusCode::GONE);
    let page = body_text(resp).await;
    assert!(page.contains("expired") && !page.contains("/pay\""), "no pay button on an expired page");
    fx.cleanup().await;
}

/// A payment after the hold ran out but before the session was expired is
/// accepted by the provider: it does not know about our hold
#[actix_web::test]
async fn the_provider_accepts_a_payment_the_merchant_will_have_to_refund() {
    let (_guard, fx) = setup(3).await;
    let a = fx.awaiting().await;
    flash_sale::cache::release_hold(
        &mut fx.env.redis.clone(),
        fx.sale,
        &a.event.ticket,
        flash_sale::cache::HoldState::AwaitingPayment,
        flash_sale::cache::MaxScore::Any,
        flash_sale::cache::ReleasedAs::Expired,
        false,
    )
    .await
    .unwrap();
    let app = provider_app!(fx.env);

    let resp = test::call_service(&app, test::TestRequest::post().uri(&format!("/fake-pay/{}/pay", a.session.id)).to_request()).await;
    assert_eq!(resp.status(), StatusCode::SEE_OTHER);
    assert_eq!(fx.fake().outbox_len().await.unwrap(), 1);
    fx.cleanup().await;
}

// ---- The dispatcher ----

/// A webhook receiver that answers 500 to its first `fail_first` requests and
/// records what it got
struct Receiver {
    calls: Mutex<Vec<(HeaderMap, Vec<u8>)>>,
    fail_first: AtomicUsize,
}

impl Receiver {
    fn new(fail_first: usize) -> Arc<Self> {
        Arc::new(Self { calls: Mutex::new(Vec::new()), fail_first: AtomicUsize::new(fail_first) })
    }

    fn count(&self) -> usize {
        self.calls.lock().unwrap().len()
    }
}

async fn receive(req: HttpRequest, body: web::Bytes, receiver: web::Data<Receiver>) -> HttpResponse {
    receiver.calls.lock().unwrap().push((req.headers().clone(), body.to_vec()));
    let failing = receiver.fail_first.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1)).is_ok();
    if failing {
        HttpResponse::InternalServerError().finish()
    } else {
        HttpResponse::Ok().finish()
    }
}

/// Starts the receiver on a free port; returns its URL
fn start_receiver(receiver: Arc<Receiver>) -> String {
    let data = web::Data::from(receiver);
    let server = HttpServer::new(move || App::new().app_data(data.clone()).route("/hook", web::post().to(receive)))
        .workers(1)
        .bind(("127.0.0.1", 0))
        .unwrap();
    let addr = server.addrs()[0];
    actix_web::rt::spawn(server.run());
    format!("http://{addr}/hook")
}

fn quick_settings() -> DispatcherSettings {
    DispatcherSettings {
        base_backoff: Duration::from_millis(100),
        max_backoff: Duration::from_millis(400),
        request_timeout: Duration::from_secs(2),
        ..DispatcherSettings::default()
    }
}

#[actix_web::test]
async fn the_dispatcher_retries_after_a_500_and_removes_the_event_after_a_2xx() {
    let (_guard, fx) = setup(3).await;
    let a = fx.awaiting().await;
    fx.fake().settle(&a.session, Settle::Pay).await.unwrap();
    assert_eq!(fx.fake().outbox_len().await.unwrap(), 1);

    let receiver = Receiver::new(1);
    let url = start_receiver(receiver.clone());
    let dispatcher = Dispatcher::new(fx.fake(), url, quick_settings());

    // First attempt: the receiver answers 500, the event stays
    assert_eq!(dispatcher.sweep().await.unwrap(), 1);
    assert_eq!(receiver.count(), 1);
    assert_eq!(fx.fake().outbox_len().await.unwrap(), 1);
    // Not due yet: it waits out its backoff
    assert_eq!(dispatcher.sweep().await.unwrap(), 0);
    assert_eq!(receiver.count(), 1);

    // Second attempt after the backoff: 200, the event is gone
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(dispatcher.sweep().await.unwrap(), 1);
    assert_eq!(receiver.count(), 2);
    assert_eq!(fx.fake().outbox_len().await.unwrap(), 0);
    assert_eq!(dispatcher.sweep().await.unwrap(), 0);

    // Both attempts carried the same event, each with its own valid signature
    {
        let calls = receiver.calls.lock().unwrap();
        assert_eq!(calls[0].1, calls[1].1);
        for (headers, body) in calls.iter() {
            let event = fx.env.shared_provider().verify_webhook(headers, body).unwrap();
            assert_eq!(event.kind, EventKind::PaymentSucceeded);
            assert_eq!(event.ticket, a.event.ticket);
        }
    }
    fx.cleanup().await;
}

#[actix_web::test]
async fn an_event_that_cannot_be_delivered_for_too_long_is_dropped() {
    let (_guard, fx) = setup(3).await;
    let a = fx.awaiting().await;
    fx.fake().settle(&a.session, Settle::Cancel).await.unwrap();

    let receiver = Receiver::new(usize::MAX);
    let url = start_receiver(receiver.clone());
    let settings = DispatcherSettings { max_age: Duration::ZERO, ..quick_settings() };
    let dispatcher = Dispatcher::new(fx.fake(), url, settings);

    assert_eq!(dispatcher.sweep().await.unwrap(), 1);
    assert_eq!(receiver.count(), 1);
    assert_eq!(fx.fake().outbox_len().await.unwrap(), 0, "given up on");
    fx.cleanup().await;
}

#[actix_web::test]
async fn a_leased_event_is_not_taken_twice_and_returns_when_the_lease_ends() {
    let (_guard, fx) = setup(3).await;
    let a = fx.awaiting().await;
    fx.fake().settle(&a.session, Settle::Pay).await.unwrap();

    let first = fx.fake().claim_due_events(10, Duration::from_millis(300)).await.unwrap();
    assert_eq!(first.len(), 1);
    // Another dispatcher finds it taken
    assert!(fx.fake().claim_due_events(10, Duration::from_millis(300)).await.unwrap().is_empty());
    // The first one died without reporting: it is due again after the lease
    tokio::time::sleep(Duration::from_millis(350)).await;
    let again = fx.fake().claim_due_events(10, Duration::from_secs(30)).await.unwrap();
    assert_eq!(again, first);
    fx.cleanup().await;
}
