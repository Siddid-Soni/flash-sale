//! The checkout queue: the Order Service publishes one event per winning
//! checkout to the Kafka topic `orders.checkout`; the order processors consume
//! them. Handlers use the `CheckoutQueue` trait so tests can stub the broker.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    error::RDKafkaErrorCode,
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
    ClientConfig,
};
use serde::{Deserialize, Serialize};

use crate::config::Config;

#[derive(Debug, thiserror::Error)]
#[error("checkout queue: {0}")]
pub struct QueueError(pub String);

pub type QueueResult<T> = Result<T, QueueError>;

/// What the processors need to create an order and its payment session
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckoutEvent {
    /// Queue ticket, `<sale>.<uuid>`; also the Kafka message key
    pub ticket: String,
    pub sale: i32,
    pub user: i32,
    /// When `checkout.lua` created the hold (Redis `TIME`, epoch ms)
    pub checkout_ms: i64,
}

/// Where the broker stored an event. `offset` becomes the queue position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Published {
    pub partition: i32,
    pub offset: i64,
}

#[async_trait]
pub trait CheckoutQueue: Send + Sync {
    /// Appends the event to the topic, keyed by its ticket, and returns once
    /// every in-sync replica has stored it. Fails when the producer gives up
    /// (`delivery.timeout.ms`), after which the message can no longer arrive:
    /// the caller then releases the hold.
    async fn publish(&self, event: &CheckoutEvent) -> QueueResult<Published>;
}

pub type SharedQueue = Arc<dyn CheckoutQueue>;

/// Producer settings shared by `KafkaQueue` and the topic admin client
fn base_config(config: &Config) -> ClientConfig {
    let mut client = ClientConfig::new();
    client.set("bootstrap.servers", &config.kafka_brokers);
    client
}

pub struct KafkaQueue {
    producer: FutureProducer,
    topic: String,
}

impl KafkaQueue {
    /// Creates the producer. Connections to the brokers open lazily, so this
    /// succeeds with Kafka down; `ensure_topic` is the startup reachability check.
    pub fn new(config: &Config) -> QueueResult<Self> {
        let producer = base_config(config)
            .set("client.id", "order-service")
            // Stored by every in-sync replica before a checkout is answered
            .set("acks", "all")
            // A retried send cannot duplicate or reorder a message
            .set("enable.idempotence", "true")
            // How long a publish may take before the producer reports failure
            .set("delivery.timeout.ms", config.kafka_delivery_timeout_ms.to_string())
            // Concurrent checkouts share a produce request; costs at most this
            // much when idle
            .set("linger.ms", "2")
            // A burst of checkouts queues in memory rather than failing
            .set("queue.buffering.max.messages", "500000")
            .create::<FutureProducer>()
            .map_err(|e| QueueError(format!("create producer: {e}")))?;
        Ok(Self {
            producer,
            topic: config.checkout_topic.clone(),
        })
    }
}

#[async_trait]
impl CheckoutQueue for KafkaQueue {
    async fn publish(&self, event: &CheckoutEvent) -> QueueResult<Published> {
        let payload = serde_json::to_vec(event).map_err(|e| QueueError(format!("encode event: {e}")))?;
        let record = FutureRecord::to(&self.topic).key(event.ticket.as_str()).payload(&payload);
        // No waiting for room in the producer's queue: a full queue fails at
        // once (the caller compensates and answers 503). Waiting here would
        // add to `delivery.timeout.ms`, which counts from the enqueue, and the
        // expiration worker's leaked-hold margin is that timeout alone
        match self.producer.send(record, Timeout::After(Duration::ZERO)).await {
            Ok(delivery) => Ok(Published { partition: delivery.partition, offset: delivery.offset }),
            Err((e, _message)) => Err(QueueError(format!("publish to {}: {e}", self.topic))),
        }
    }
}

/// Creates the checkout topic with `CHECKOUT_PARTITIONS` partitions if it does
/// not exist. Idempotent: an existing topic (whatever its partition count) is
/// left as it is. Also the reachability check for Kafka at startup.
pub async fn ensure_topic(config: &Config) -> QueueResult<()> {
    let admin: AdminClient<DefaultClientContext> = base_config(config)
        .set("client.id", "order-service-admin")
        .create()
        .map_err(|e| QueueError(format!("create admin client: {e}")))?;
    let topic = NewTopic::new(&config.checkout_topic, config.checkout_partitions, TopicReplication::Fixed(1));
    let options = AdminOptions::new().request_timeout(Some(Duration::from_secs(15)));
    let results = admin
        .create_topics([&topic], &options)
        .await
        .map_err(|e| QueueError(format!("create topic {}: {e}", config.checkout_topic)))?;
    for result in results {
        match result {
            Ok(name) => log::info!("Created topic {name} with {} partitions", config.checkout_partitions),
            Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
            Err((name, code)) => return Err(QueueError(format!("create topic {name}: {code}"))),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_round_trips_as_json() {
        let event = CheckoutEvent { ticket: "42.6f1c".into(), sale: 42, user: 7, checkout_ms: 1_700_000_000_123 };
        let json = serde_json::to_string(&event).unwrap();
        assert_eq!(json, r#"{"ticket":"42.6f1c","sale":42,"user":7,"checkout_ms":1700000000123}"#);
        assert_eq!(serde_json::from_str::<CheckoutEvent>(&json).unwrap(), event);
    }
}
