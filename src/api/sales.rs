//! Public sale details. `GET /v1/sales/{id}` is what every client loads first
//! and the CDN caches for a second, so it must stay cheap when a stampede
//! misses the edge: the product comes from a per-replica cache, and only
//! `sold_out` is read from Redis (one `GET`). `GET /v1/sales` lists the latest
//! sales for the home page, edge-cached the same way.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use actix_web::{get, http::header, web, HttpResponse};
use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::{
    actions,
    cache::{self, RedisConn},
    db::DbPool,
    error::ApiError,
    models::Product,
};

/// How long a replica reuses what it read from PostgreSQL
const CACHE_TTL: Duration = Duration::from_secs(1);
/// Entries kept before the map is cleared; unknown ids are cached too, so a
/// scan over random ids cannot grow it without bound
const MAX_ENTRIES: usize = 10_000;

type Slot = Arc<tokio::sync::Mutex<Option<(Instant, Option<Product>)>>>;

/// A tiny in-memory cache of products, one per replica. Concurrent misses for
/// the same sale share one query: the first takes the slot's lock and loads,
/// the rest wait on it and find the entry fresh. An unknown sale (`None`) is
/// cached as well.
#[derive(Default)]
pub struct SaleCache {
    slots: Mutex<HashMap<i32, Slot>>,
}

impl SaleCache {
    fn slot(&self, sale: i32) -> Slot {
        let mut slots = self.slots.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if slots.len() >= MAX_ENTRIES && !slots.contains_key(&sale) {
            slots.clear();
        }
        slots.entry(sale).or_default().clone()
    }

    /// The product, from the cache if it is under a second old, else from `load`.
    /// A failed `load` is not cached.
    pub async fn get_or_load<E>(
        &self,
        sale: i32,
        load: impl std::future::Future<Output = Result<Option<Product>, E>>,
    ) -> Result<Option<Product>, E> {
        let slot = self.slot(sale);
        let mut entry = slot.lock().await;
        if let Some((loaded_at, product)) = entry.as_ref() {
            if loaded_at.elapsed() < CACHE_TTL {
                return Ok(product.clone());
            }
        }
        let product = load.await?;
        *entry = Some((Instant::now(), product.clone()));
        Ok(product)
    }

    /// Drops the entry for a sale, so the next read goes to PostgreSQL
    pub fn forget(&self, sale: i32) {
        self.slots.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).remove(&sale);
    }
}

/// Edge caching for both routes: fresh for a second, then served stale for up
/// to 5 s more while one request refreshes it in the background
const CACHE_CONTROL: &str = "public, max-age=1, stale-while-revalidate=5";

/// Sales shown on the home page
const LIST_LIMIT: i64 = 50;

#[derive(Debug, Serialize)]
struct SaleResponse {
    id: i32,
    name: String,
    price_cents: i32,
    sale_start_time: DateTime<Utc>,
    total_inventory: i32,
    sold_out: bool,
}

/// Sale details for the page: name, price, start time and whether it is sold
/// out. `CACHE_CONTROL` lets the edge absorb the load.
#[get("/sales/{sale_id}")]
async fn get_sale(
    pool: web::Data<DbPool>,
    redis: web::Data<RedisConn>,
    sale_cache: web::Data<SaleCache>,
    path: web::Path<i32>,
) -> Result<HttpResponse, ApiError> {
    let sale = path.into_inner();
    let product = sale_cache
        .get_or_load(sale, async move {
            web::block(move || {
                let mut conn = pool.get()?;
                Ok::<_, ApiError>(actions::products::find_product(&mut conn, sale)?)
            })
            .await?
        })
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("sale {sale} not found")))?;

    let mut con = redis.get_ref().clone();
    let sold_out = cache::sold_out(&mut con, sale).await?;

    Ok(HttpResponse::Ok().insert_header((header::CACHE_CONTROL, CACHE_CONTROL)).json(SaleResponse::new(product, sold_out)))
}

/// The latest sales, newest start first, with the same fields as the details.
/// One query and one `MGET` for the sold-out flags.
#[get("/sales")]
async fn list_sales(pool: web::Data<DbPool>, redis: web::Data<RedisConn>) -> Result<HttpResponse, ApiError> {
    let products = web::block(move || {
        let mut conn = pool.get()?;
        Ok::<_, ApiError>(actions::products::list_sales(&mut conn, LIST_LIMIT)?)
    })
    .await??;

    let ids: Vec<i32> = products.iter().map(|p| p.id).collect();
    let mut con = redis.get_ref().clone();
    let stock = cache::stock_levels(&mut con, &ids).await?;
    let sales: Vec<SaleResponse> = products
        .into_iter()
        .zip(stock)
        .map(|(product, stock)| SaleResponse::new(product, stock.unwrap_or(0) <= 0))
        .collect();

    Ok(HttpResponse::Ok().insert_header((header::CACHE_CONTROL, CACHE_CONTROL)).json(sales))
}

impl SaleResponse {
    fn new(product: Product, sold_out: bool) -> Self {
        SaleResponse {
            id: product.id,
            name: product.name,
            price_cents: product.price_cents,
            sale_start_time: product.sale_start_time,
            total_inventory: product.total_inventory,
            sold_out,
        }
    }
}

// GET /v1/sales, GET /v1/sales/{sale_id}
pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(list_sales).service(get_sale);
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    fn product(id: i32) -> Product {
        let now = Utc::now();
        Product {
            id,
            name: "Phone".into(),
            price_cents: 100,
            total_inventory: 5,
            sold: 0,
            sale_start_time: now,
            created_at: now,
        }
    }

    #[tokio::test]
    async fn concurrent_misses_share_one_load() {
        let cache = Arc::new(SaleCache::default());
        let loads = Arc::new(AtomicUsize::new(0));
        let tasks: Vec<_> = (0..20)
            .map(|_| {
                let (cache, loads) = (cache.clone(), loads.clone());
                tokio::spawn(async move {
                    cache
                        .get_or_load(1, async {
                            loads.fetch_add(1, Ordering::SeqCst);
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            Ok::<_, ()>(Some(product(1)))
                        })
                        .await
                })
            })
            .collect();
        for task in tasks {
            assert_eq!(task.await.unwrap().unwrap().unwrap().id, 1);
        }
        assert_eq!(loads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn entries_expire_and_errors_are_not_cached() {
        let cache = SaleCache::default();
        assert!(cache.get_or_load(2, async { Err::<Option<Product>, _>("db down") }).await.is_err());
        assert!(cache.get_or_load(2, async { Ok::<_, &str>(None) }).await.unwrap().is_none());
        // Cached "not found": a loader that would return a product is not called
        assert!(cache.get_or_load(2, async { Ok::<_, &str>(Some(product(2))) }).await.unwrap().is_none());
        tokio::time::sleep(CACHE_TTL + Duration::from_millis(50)).await;
        assert!(cache.get_or_load(2, async { Ok::<_, &str>(Some(product(2))) }).await.unwrap().is_some());
        cache.forget(2);
        assert!(cache.get_or_load(2, async { Ok::<_, &str>(None) }).await.unwrap().is_none());
    }
}
