//! Step 6.1: the fake provider reports a payment by storing an inbox event.
//! Needs the Postgres at `DATABASE_URL` (`docker compose up -d postgres`).

use std::time::{Duration, Instant};

use diesel::prelude::*;
use uuid::Uuid;

use flash_sale::payment::{parse_event, FakeProvider, PaymentProvider};
use flash_sale::{config::Config, db, models::PaymentInboxEntry, schema::payment_inbox};

#[test]
fn intent_produces_an_inbox_event() {
    let pool = db::init_pool(&Config::from_env());
    let redis = redis::Client::open("redis://127.0.0.1:6379/15").unwrap();
    let provider = FakeProvider::new(pool.clone(), redis, Duration::from_millis(100));
    let reservation_id = Uuid::new_v4();
    let key = Uuid::new_v4().to_string();

    let intent_id = provider.create_intent(reservation_id, 2500, &key).unwrap();
    // Same key, same intent: nothing new is charged or reported
    assert_eq!(provider.create_intent(reservation_id, 2500, &key).unwrap(), intent_id);

    let started = Instant::now();
    let rows = loop {
        let rows: Vec<PaymentInboxEntry> = payment_inbox::table
            .filter(payment_inbox::provider_event_id.eq(format!("evt_{intent_id}")))
            .select(PaymentInboxEntry::as_select())
            .load(&mut pool.get().unwrap())
            .unwrap();
        if !rows.is_empty() || started.elapsed() > Duration::from_secs(5) {
            break rows;
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    // Give the duplicate a moment to try, then check it was ignored
    std::thread::sleep(Duration::from_millis(200));
    let count: i64 = payment_inbox::table
        .filter(payment_inbox::provider_event_id.eq(format!("evt_{intent_id}")))
        .count()
        .get_result(&mut pool.get().unwrap())
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(count, 1);
    let event = parse_event(&rows[0].payload).unwrap();
    assert_eq!((event.intent_id, event.reservation_id), (intent_id, reservation_id));
    assert_eq!(rows[0].event_type, "payment_intent.succeeded");

    // Not a real payment: keep it away from a running payment worker
    diesel::delete(payment_inbox::table.filter(payment_inbox::id.eq(rows[0].id)))
        .execute(&mut pool.get().unwrap())
        .unwrap();
}
