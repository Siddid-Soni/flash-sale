//! The Kafka producer and topic creation. Needs the broker at `KAFKA_BROKERS`
//! (`docker compose up -d kafka`). Uses a topic of its own per run.

use flash_sale::{
    config::Config,
    queue::{ensure_topic, CheckoutEvent, CheckoutQueue, KafkaQueue},
};

#[tokio::test]
async fn publishes_keyed_events_to_an_ensured_topic() {
    let mut config = Config::from_env();
    config.checkout_topic = format!("test.checkout.{}", uuid::Uuid::new_v4().simple());
    config.checkout_partitions = 3;

    // Idempotent: the second call finds the topic
    ensure_topic(&config).await.unwrap();
    ensure_topic(&config).await.unwrap();

    let queue = KafkaQueue::new(&config).unwrap();
    let event = |n: i32| CheckoutEvent { ticket: format!("1.ticket-{n}"), sale: 1, user: n, checkout_ms: 1_000 + n as i64 };

    let first = queue.publish(&event(1)).await.unwrap();
    assert!((0..3).contains(&first.partition));
    assert_eq!(first.offset, 0);

    // The same ticket always lands in the same partition, at the next offset
    let again = queue.publish(&event(1)).await.unwrap();
    assert_eq!(again.partition, first.partition);
    assert_eq!(again.offset, 1);
}
