//! Public sales, served by the site service. `GET /v1/sales/{id}` is what every
//! client loads first and the CDN caches for a second, so it must stay cheap
//! when a stampede misses the edge: the product comes from a per-process cache,
//! and only `sold_out` is read from Redis (one `GET`). `GET /v1/sales` lists the
//! latest sales still selling for the home page and `GET /v1/sales/search?q=` finds them by
//! name; both are edge-cached for a few seconds.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use actix_web::{get, http::header, web, HttpResponse};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

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

/// Edge caching of the details: fresh for a second, then served stale for up
/// to 5 s more while one request refreshes it in the background
const CACHE_CONTROL: &str = "public, max-age=1, stale-while-revalidate=5";

/// Edge caching of the list and of search: a new sale shows within about 5 s
/// (no purge), and a stale copy is served for 30 s more while one request refreshes it
const LIST_CACHE_CONTROL: &str = "public, max-age=5, stale-while-revalidate=30";

/// Sales in the list and in a search
const LIST_LIMIT: i64 = 50;

/// Longest search text used, in characters
const MAX_QUERY_CHARS: usize = 64;

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

/// The latest sales still selling (not every unit paid for), newest start
/// first, with the same fields as the details.
/// One query and one `MGET` for the sold-out flags.
#[get("/sales")]
async fn list_sales(pool: web::Data<DbPool>, redis: web::Data<RedisConn>) -> Result<HttpResponse, ApiError> {
    let products = web::block(move || {
        let mut conn = pool.get()?;
        Ok::<_, ApiError>(actions::products::list_sales(&mut conn, LIST_LIMIT)?)
    })
    .await??;
    sales_response(products, &redis).await
}

#[derive(Debug, Deserialize)]
struct SearchParams {
    q: Option<String>,
}

/// The text searched for: trimmed, lowercased, without NUL (PostgreSQL refuses
/// it) and cut to 64 characters. Empty means "no search".
fn normalize_query(q: &str) -> String {
    q.trim().to_lowercase().chars().filter(|&c| c != '\0').take(MAX_QUERY_CHARS).collect::<String>().trim().to_string()
}

/// Sales whose name contains `q` (any case), newest start first, in the shape
/// of the list. `%`, `_` and `\` in `q` match themselves. An empty `q` answers
/// as the list does.
#[get("/sales/search")]
async fn search_sales(
    pool: web::Data<DbPool>,
    redis: web::Data<RedisConn>,
    params: web::Query<SearchParams>,
) -> Result<HttpResponse, ApiError> {
    let needle = normalize_query(params.q.as_deref().unwrap_or(""));
    let products = web::block(move || {
        let mut conn = pool.get()?;
        Ok::<_, ApiError>(if needle.is_empty() {
            actions::products::list_sales(&mut conn, LIST_LIMIT)?
        } else {
            actions::products::search_sales(&mut conn, &needle, LIST_LIMIT)?
        })
    })
    .await??;
    sales_response(products, &redis).await
}

/// The answer for a list of products: their sold-out flags from one `MGET`
async fn sales_response(products: Vec<Product>, redis: &RedisConn) -> Result<HttpResponse, ApiError> {
    let ids: Vec<i32> = products.iter().map(|p| p.id).collect();
    let mut con = redis.clone();
    let stock = cache::stock_levels(&mut con, &ids).await?;
    let sales: Vec<SaleResponse> = products
        .into_iter()
        .zip(stock)
        .map(|(product, stock)| SaleResponse::new(product, stock.unwrap_or(0) <= 0))
        .collect();

    Ok(HttpResponse::Ok().insert_header((header::CACHE_CONTROL, LIST_CACHE_CONTROL)).json(sales))
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

// GET /v1/sales, GET /v1/sales/search, GET /v1/sales/{sale_id}
pub fn configure(cfg: &mut web::ServiceConfig) {
    // `search` first: `{sale_id}` would otherwise be tried on "search" (and fail with 404)
    cfg.service(list_sales).service(search_sales).service(get_sale);
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

    #[test]
    fn queries_are_trimmed_lowercased_and_cut() {
        assert_eq!(normalize_query("  PhOnE \t"), "phone");
        assert_eq!(normalize_query("   "), "");
        assert_eq!(normalize_query("a\0b"), "ab");
        assert_eq!(normalize_query(&"Ä".repeat(100)), "ä".repeat(64));
        assert_eq!(normalize_query(&format!("{} z", "a".repeat(63))), "a".repeat(63), "the cut leaves no trailing space");
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
