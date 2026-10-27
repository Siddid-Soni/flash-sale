//! Opening the sale: at `sale_opens_at` the product goes `ON_SALE` and the lobby
//! (everyone who joined early) is shuffled into the queue, so clicking first
//! before the opening gives no advantage.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use diesel::{Connection, PgConnection};
use tokio::task::JoinSet;

use flash_sale::{
    actions,
    cache::{self, RedisConn},
    models::Product,
    waiting::SharedQueue,
};

use crate::{BoxError, ERROR_RETRY};

// Tokens moved out of the lobby, and published to Kafka, per step
const BATCH: usize = 1000;
// How often a standby checks whether it has become the partition owner
const STANDBY_CHECK: Duration = Duration::from_millis(500);

/// Waits for `sale_opens_at`, then, while this worker owns the product's
/// partition, opens the sale and flushes the lobby into the queue. Resumable:
/// a new owner continues from `lobby_published`. Returns once the lobby is
/// flushed and joins go straight to the queue.
pub async fn open_sale(
    mut redis: RedisConn,
    queue: SharedQueue,
    database_url: String,
    product: Product,
    owns_partition: Arc<AtomicBool>,
) {
    if let Ok(wait) = (product.sale_opens_at - Utc::now()).to_std() {
        log::info!("Sale opens at {}; waiting {}s", product.sale_opens_at, wait.as_secs());
        tokio::time::sleep(wait).await;
    }

    let mut on_sale = false;
    loop {
        // Only the owner flushes, so two workers never publish the same tokens
        if !owns_partition.load(Ordering::SeqCst) {
            tokio::time::sleep(STANDBY_CHECK).await;
            continue;
        }
        if !on_sale {
            match mark_on_sale(&database_url, product.id).await {
                Ok(()) => on_sale = true,
                Err(e) => {
                    log::error!("Marking product {} ON_SALE failed: {e}", product.id);
                    tokio::time::sleep(ERROR_RETRY).await;
                    continue;
                }
            }
        }
        match flush_step(&mut redis, &queue, product.id).await {
            Ok(true) => {
                log::info!("Lobby flushed; joins now go straight to the queue");
                return;
            }
            Ok(false) => {}
            Err(e) => {
                log::error!("Flushing the lobby failed: {e}");
                tokio::time::sleep(ERROR_RETRY).await;
            }
        }
    }
}

async fn mark_on_sale(database_url: &str, product_id: i32) -> Result<(), BoxError> {
    let database_url = database_url.to_owned();
    let updated = tokio::task::spawn_blocking(move || -> Result<usize, BoxError> {
        let mut conn = PgConnection::establish(&database_url)?;
        Ok(actions::products::open_sale(&mut conn, product_id)?)
    })
    .await??;
    if updated == 1 {
        log::info!("Product {product_id} is ON_SALE");
    }
    Ok(())
}

/// Moves one random batch out of the lobby, then publishes everything moved
/// so far. Returns true once the lobby is empty and all of it is published.
async fn flush_step(redis: &mut RedisConn, queue: &SharedQueue, product_id: i32) -> Result<bool, BoxError> {
    if cache::flush_lobby(redis, product_id, BATCH).await? {
        return Ok(true);
    }
    loop {
        let (start, ids) = cache::lobby_unpublished(redis, product_id, BATCH).await?;
        if ids.is_empty() {
            return Ok(false);
        }
        publish_batch(redis, queue, product_id, start, ids).await?;
    }
}

/// Publishes a batch concurrently. Each token's seq comes from its own
/// offset, so it matches the Kafka order even if sends complete out of order.
/// `lobby_published` advances only when the whole batch is in Kafka; after a
/// partial failure the batch is sent again, and the admit script skips the
/// duplicates (the first offset recorded is the one kept).
async fn publish_batch(
    redis: &mut RedisConn,
    queue: &SharedQueue,
    product_id: i32,
    start: u64,
    ids: Vec<String>,
) -> Result<(), BoxError> {
    let count = ids.len() as u64;
    let mut sends = JoinSet::new();
    for id in ids {
        let queue = queue.clone();
        sends.spawn(async move {
            let result = queue.publish(product_id, &id).await;
            (id, result)
        });
    }

    let mut offsets = Vec::with_capacity(count as usize);
    let mut first_error = None;
    while let Some(joined) = sends.join_next().await {
        match joined? {
            (id, Ok(offset)) => offsets.push((id, offset)),
            (_, Err(e)) => first_error = first_error.or(Some(e)),
        }
    }

    let published_upto = first_error.is_none().then_some(start + count);
    cache::record_lobby_published(redis, product_id, &offsets, published_upto).await?;
    match first_error {
        Some(e) => Err(e.into()),
        None => {
            log::info!("Published lobby tokens {}..{}", start + 1, start + count);
            Ok(())
        }
    }
}
