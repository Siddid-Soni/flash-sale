//! Admission worker: opens a product's sale by shuffling the lobby into the
//! waiting queue (`lobby.rs`), then reads the queue in order and makes each
//! queued token ACTIVE once there is room for another shopper.
//!
//! Usage: `cargo run --bin admission_worker -- [product_id]`. With a product id
//! it runs that product and exits when its sale closes. Without one it finds
//! the scheduled and on-sale products every few seconds and runs each in its
//! own task, restarting any that fails. Several workers can run either way:
//! Kafka gives each product's partition to one of them, the others stand by.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use kafkit_client::{
    AutoOffsetReset, CommitOffset, ConsumerConfig, ConsumerRebalanceEvent, ConsumerRebalanceListener, ConsumerRecord,
    KafkaConsumer, TopicPartition,
};

use chrono::{DateTime, Utc};
use diesel::{Connection, PgConnection};

use flash_sale::{
    actions,
    cache::{self, AdmitOutcome, RedisConn},
    config::Config,
    models::Product,
    waiting::{topic_name, KafkaQueue, SharedQueue},
};

mod lobby;

// How long to wait before offering a token again while the product has no room
const FULL_RETRY: Duration = Duration::from_millis(200);
// Back-off after a Redis or Kafka error
const ERROR_RETRY: Duration = Duration::from_secs(1);
// How often admission progress is sampled for the estimated wait
const SAMPLE_EVERY: Duration = Duration::from_secs(5);
// How often the worker checks whether the sale has been closed
const CLOSE_CHECK: Duration = Duration::from_secs(1);
// How often the supervisor looks for products to run
const DISCOVER_EVERY: Duration = Duration::from_secs(10);

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Connections shared by every product this process runs
struct Shared {
    config: Config,
    redis: RedisConn,
    queue: SharedQueue,
}

#[tokio::main]
async fn main() {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    let product_id: Option<i32> =
        std::env::args().nth(1).map(|arg| arg.parse().expect("usage: admission_worker [product_id]"));
    let config = Config::from_env();
    let redis = cache::connect(&config).await;
    let queue: SharedQueue = Arc::new(KafkaQueue::connect(&config).await.expect("Kafka should be reachable at KAFKA_BROKERS"));
    let shared = Arc::new(Shared { config, redis, queue });

    match product_id {
        Some(product_id) => {
            let product = with_db(&shared.config, move |conn| Ok(actions::products::find_product(conn, product_id)?))
                .await
                .unwrap_or_else(|e| panic!("load product {product_id}: {e}"));
            if let Err(e) = run_product(shared, product).await {
                log::error!("Product {product_id}: {e}");
                std::process::exit(1);
            }
        }
        None => supervise(shared).await,
    }
}

/// Runs a blocking Postgres operation on its own connection
async fn with_db<T: Send + 'static>(
    config: &Config,
    f: impl FnOnce(&mut PgConnection) -> Result<T, BoxError> + Send + 'static,
) -> Result<T, BoxError> {
    let database_url = config.database_url.clone();
    tokio::task::spawn_blocking(move || f(&mut PgConnection::establish(&database_url)?)).await?
}

/// Keeps one task running per scheduled or on-sale product
async fn supervise(shared: Arc<Shared>) {
    log::info!("Running every scheduled and on-sale product");
    let mut running: HashMap<i32, tokio::task::JoinHandle<()>> = HashMap::new();
    let mut ticker = tokio::time::interval(DISCOVER_EVERY);
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = tokio::signal::ctrl_c() => break,
        }
        // Finished tasks leave: closed products stay out, failed ones start again
        running.retain(|_, task| !task.is_finished());
        let products = match with_db(&shared.config, |conn| Ok(actions::products::live_products(conn)?)).await {
            Ok(products) => products,
            Err(e) => {
                log::error!("Listing products failed: {e}");
                continue;
            }
        };
        for product in products {
            if running.contains_key(&product.id) || !matches!(product.status.as_str(), "SCHEDULED" | "ON_SALE") {
                continue;
            }
            let (shared, id) = (shared.clone(), product.id);
            running.insert(
                id,
                tokio::spawn(async move {
                    if let Err(e) = run_product(shared, product).await {
                        log::error!("Product {id}: {e}; retrying shortly");
                    }
                }),
            );
        }
    }
    log::info!("Shutting down");
    // Each product task stops on Ctrl-C too, leaving its consumer group
    for (_, task) in running {
        let _ = task.await;
    }
}

/// Runs one product until its sale closes (or Ctrl-C)
async fn run_product(shared: Arc<Shared>, product: Product) -> Result<(), BoxError> {
    let product_id = product.id;
    if product.status == "CLOSED" || product.status == "CANCELED" {
        log::info!("Product {product_id} is {}; nothing to admit", product.status);
        return Ok(());
    }
    let config = &shared.config;
    let mut redis = shared.redis.clone();
    // Subscribing needs the topic; product creation normally made it already
    shared.queue.ensure_product(product_id).await?;

    // The topic has one partition, so one worker in the group owns it and any
    // others wait as standbys. A worker that loses it drops the token it holds.
    let owns_partition = Arc::new(AtomicBool::new(false));
    let listener = {
        let owns_partition = owns_partition.clone();
        ConsumerRebalanceListener::new(move |rebalance| {
            let owned = matches!(rebalance, ConsumerRebalanceEvent::Assigned(_));
            // An empty assignment is a no-op for a standby
            if !rebalance.partitions().is_empty() {
                owns_partition.store(owned, Ordering::SeqCst);
            }
            log::info!("Product {product_id}: rebalance {rebalance:?}");
        })
    };

    let consumer = KafkaConsumer::connect(
        ConsumerConfig::new(config.kafka_brokers.as_str(), format!("admission-{product_id}"))
            .with_client_id("admission-worker")
            .with_auto_offset_reset(AutoOffsetReset::Earliest)
            // Committed only after a token is admitted or skipped, never while it waits
            .with_enable_auto_commit(false)
            .with_rebalance_listener(listener),
    )
    .await?;
    consumer.subscribe(vec![topic_name(product_id)]).await?;
    log::info!("Admitting tokens for product {product_id}");

    let sale_ends_at = product.sale_ends_at;
    let helpers = [
        tokio::spawn(lobby::open_sale(
            redis.clone(),
            shared.queue.clone(),
            config.database_url.clone(),
            product,
            owns_partition.clone(),
        )),
        tokio::spawn(sample_admissions(redis.clone(), product_id, owns_partition.clone())),
    ];

    let mut close_redis = redis.clone();
    tokio::select! {
        _ = run(&consumer, &mut redis, config, product_id, &owns_partition) => {
            // Admission saw the flag first; make sure Postgres records the close too
            close_at(&mut close_redis, &config.database_url, product_id, sale_ends_at).await;
            log::info!("Product {product_id}: the sale is closed; stopping")
        }
        _ = close_at(&mut close_redis, &config.database_url, product_id, sale_ends_at) => {
            log::info!("Product {product_id}: the sale is closed; stopping")
        }
        _ = tokio::signal::ctrl_c() => log::info!("Product {product_id}: shutting down"),
    }
    for helper in helpers {
        helper.abort();
    }
    // Leaving the group lets a standby worker take over without waiting for the session timeout
    if let Err(e) = consumer.shutdown().await {
        log::warn!("Product {product_id}: consumer shutdown: {e}");
    }
    Ok(())
}

/// Records admission progress every few seconds so the status endpoint can
/// estimate waits. Only the partition owner samples, so samples stay evenly spaced.
async fn sample_admissions(mut redis: RedisConn, product_id: i32, owns_partition: Arc<AtomicBool>) {
    let mut ticker = tokio::time::interval(SAMPLE_EVERY);
    loop {
        ticker.tick().await;
        if !owns_partition.load(Ordering::SeqCst) {
            continue;
        }
        let now_ms = Utc::now().timestamp_millis();
        if let Err(e) = cache::record_admit_sample(&mut redis, product_id, now_ms).await {
            log::warn!("Recording an admission sample failed: {e}");
        }
    }
}

/// Returns once the sale is closed: at the product's `sale_ends_at`, when this
/// closes it, or earlier through `POST /products/{id}/close`. Also finishes a
/// close that set the Redis flag but failed to update Postgres.
async fn close_at(redis: &mut RedisConn, database_url: &str, product_id: i32, sale_ends_at: DateTime<Utc>) {
    loop {
        let flagged = cache::is_closed(redis, product_id).await.unwrap_or_else(|e| {
            log::warn!("Checking whether the sale is closed failed: {e}");
            false
        });
        if flagged || Utc::now() >= sale_ends_at {
            match close_sale(redis, database_url, product_id).await {
                Ok(()) => return,
                Err(e) => log::error!("Closing the sale failed: {e}"),
            }
        }
        tokio::time::sleep(CLOSE_CHECK).await;
    }
}

/// Redis first, which turns joins away and stops admission at once; then
/// Postgres, the record. Both steps are idempotent, so a failure is retried
/// whole, and the sale stays closed meanwhile.
async fn close_sale(redis: &mut RedisConn, database_url: &str, product_id: i32) -> Result<(), BoxError> {
    cache::close_product(redis, product_id).await?;
    let database_url = database_url.to_owned();
    tokio::task::spawn_blocking(move || -> Result<usize, BoxError> {
        let mut conn = PgConnection::establish(&database_url)?;
        Ok(actions::products::close_sale(&mut conn, product_id)?)
    })
    .await??;
    log::info!("Product {product_id} is CLOSED");
    Ok(())
}

/// What became of a record offered for admission
enum Offered {
    /// Admitted or skipped: commit and move on
    Done,
    /// Another worker owns the partition now; it admits this record
    LostPartition,
    Closed,
}

async fn run(consumer: &KafkaConsumer, redis: &mut RedisConn, config: &Config, product_id: i32, owns_partition: &AtomicBool) {
    loop {
        let records = match consumer.poll_for(Duration::from_secs(1)).await {
            Ok(records) => records,
            Err(e) => {
                log::error!("Poll failed: {e}");
                tokio::time::sleep(ERROR_RETRY).await;
                continue;
            }
        };
        // Committed once per batch rather than per record: a commit is a
        // broker round trip, and records read again after a crash are only
        // skipped by the admit script
        let mut done: Option<CommitOffset> = None;
        for record in records.into_inner() {
            let offered = admit(redis, config, product_id, &record, owns_partition).await;
            if let Offered::Closed = offered {
                return;
            }
            if let Offered::LostPartition = offered {
                // The partition moved to another worker, which resumes from the
                // last committed offset; the rest of this batch is its to admit.
                // If the partition comes back to this worker, read from here
                // again (fails harmlessly while it is not assigned).
                let partition = TopicPartition::new(record.topic.clone(), record.partition);
                let _ = consumer.seek(partition, record.offset).await;
                done = None;
                break;
            }
            done = Some(CommitOffset { topic: record.topic, partition: record.partition, offset: record.offset + 1 });
        }
        if let Some(offset) = done {
            let next = offset.offset;
            if let Err(e) = consumer.commit_offsets(vec![offset]).await {
                log::warn!("Commit of offset {next} failed: {e}");
            }
        }
    }
}

/// Offers the record's token until it is admitted or skipped. While the product
/// has no room the record stays in hand; the client heartbeats on its own task, so
/// the worker keeps its place in the group without polling. Gives up, without
/// admitting, if the worker loses the partition or the sale closes meanwhile.
async fn admit(
    redis: &mut RedisConn,
    config: &Config,
    product_id: i32,
    record: &ConsumerRecord,
    owns_partition: &AtomicBool,
) -> Offered {
    let Some(value) = &record.value else {
        log::warn!("Skipping empty record at offset {}", record.offset);
        return Offered::Done;
    };
    let token_id = String::from_utf8_lossy(value);
    let seq = record.offset + 1;
    let ttl_ms = config.token_ttl_secs * 1000;

    loop {
        if !owns_partition.load(Ordering::SeqCst) {
            log::info!("Lost the partition; leaving token {token_id} (seq {seq}) to the new owner");
            return Offered::LostPartition;
        }
        match cache::admit_token(redis, product_id, &token_id, seq, config.max_active_tokens, ttl_ms).await {
            Ok(AdmitOutcome::Admitted) => {
                log::info!("Admitted token {token_id} (seq {seq})");
                return Offered::Done;
            }
            Ok(AdmitOutcome::Skipped) => {
                log::info!("Skipped token {token_id} (seq {seq}): expired or already admitted");
                return Offered::Done;
            }
            Ok(AdmitOutcome::Full) => tokio::time::sleep(FULL_RETRY).await,
            Ok(AdmitOutcome::Closed) => return Offered::Closed,
            Err(e) => {
                log::error!("Admitting token {token_id} failed: {e}");
                tokio::time::sleep(ERROR_RETRY).await;
            }
        }
    }
}
