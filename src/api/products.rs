use actix_web::{get, http::header, post, web, HttpResponse};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    actions,
    cache::{self, RedisConn},
    config::Config,
    db::DbPool,
    error::ApiError,
    models::{NewProduct, Product},
    waiting::SharedQueue,
};

const MAX_STOCK: i32 = 1_000_000;
const SKU_LEN: std::ops::RangeInclusive<usize> = 1..=64;

#[derive(Debug, Deserialize)]
struct CreateProductRequest {
    sku: String,
    name: String,
    #[serde(default)]
    description: String,
    image_url: Option<String>,
    price_cents: i32,
    /// Units for sale
    stock: i32,
    /// Units one user may hold or buy; the configured default when absent
    max_per_user: Option<i32>,
    /// When the waiting room starts admitting; joins before this wait in a lobby
    sale_opens_at: DateTime<Utc>,
    sale_ends_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct CreateProductResponse {
    #[serde(flatten)]
    product: Product,
    stock: i32,
}

#[post("/products")]
async fn create_product(
    pool: web::Data<DbPool>,
    redis: web::Data<RedisConn>,
    queue: web::Data<SharedQueue>,
    config: web::Data<Config>,
    body: web::Json<CreateProductRequest>,
) -> Result<HttpResponse, ApiError> {
    let body = body.into_inner();
    if !SKU_LEN.contains(&body.sku.chars().count())
        || !body.sku.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(ApiError::BadRequest("sku must be 1-64 characters of letters, digits, '_' or '-'".into()));
    }
    if body.name.trim().is_empty() {
        return Err(ApiError::BadRequest("name must not be empty".into()));
    }
    if body.price_cents < 0 {
        return Err(ApiError::BadRequest("price_cents must not be negative".into()));
    }
    if !(1..=MAX_STOCK).contains(&body.stock) {
        return Err(ApiError::BadRequest(format!("stock must be between 1 and {MAX_STOCK}")));
    }
    let max_per_user = match body.max_per_user {
        Some(max) if max < 1 => return Err(ApiError::BadRequest("max_per_user must be at least 1".into())),
        Some(max) => max,
        None => config.default_max_per_user as i32,
    };
    if body.sale_opens_at <= Utc::now() {
        return Err(ApiError::BadRequest("sale_opens_at must be in the future".into()));
    }
    if body.sale_ends_at <= Utc::now() {
        return Err(ApiError::BadRequest("sale_ends_at must be in the future".into()));
    }
    if body.sale_opens_at >= body.sale_ends_at {
        return Err(ApiError::BadRequest("sale_opens_at must be before sale_ends_at".into()));
    }

    let stock = body.stock;
    let product = web::block(move || {
        let mut conn = pool.get()?;
        let new_product = NewProduct {
            sku: &body.sku,
            name: &body.name,
            description: &body.description,
            image_url: body.image_url.as_deref(),
            price_cents: body.price_cents,
            max_per_user,
            sale_opens_at: body.sale_opens_at,
            sale_ends_at: body.sale_ends_at,
        };
        actions::products::create_product(&mut conn, &new_product, stock).map_err(|err| match ApiError::from(err) {
            ApiError::Conflict(_) => ApiError::Conflict(format!("a product with sku '{}' already exists", body.sku)),
            other => other,
        })
    })
    .await??;

    // Only after the commit: a rolled-back product must not get counters or a topic.
    // If this fails, the product exists without counters until the cleanup
    // worker's rebuild finds it missing (the first join creates a missing topic).
    let mut con = redis.get_ref().clone();
    cache::init_product(&mut con, product.id, stock as u32, max_per_user as u32, product.sale_opens_at).await?;
    // Idempotent; the join endpoint calls it too, so a topic missed here is created on first join
    queue.ensure_product(product.id).await?;

    Ok(HttpResponse::Created().json(CreateProductResponse { product, stock }))
}

#[derive(Debug, Deserialize)]
struct ListQuery {
    limit: Option<i64>,
    offset: Option<i64>,
}

#[derive(Debug, Serialize)]
struct ListResponse {
    products: Vec<Product>,
}

/// The catalog, soonest sale first. Product details do not change during a
/// sale, so CDNs and browsers may cache the response for a short while.
#[get("/products")]
async fn list_products(pool: web::Data<DbPool>, query: web::Query<ListQuery>) -> Result<HttpResponse, ApiError> {
    let limit = query.limit.unwrap_or(50).clamp(1, 100);
    let offset = query.offset.unwrap_or(0).max(0);

    let products = web::block(move || {
        let mut conn = pool.get()?;
        Ok::<_, ApiError>(actions::products::list_products(&mut conn, limit, offset)?)
    })
    .await??;

    Ok(HttpResponse::Ok()
        .insert_header((header::CACHE_CONTROL, "public, max-age=30"))
        .json(ListResponse { products }))
}

#[get("/products/{id}")]
async fn get_product(pool: web::Data<DbPool>, path: web::Path<i32>) -> Result<HttpResponse, ApiError> {
    let product_id = path.into_inner();

    let product = web::block(move || {
        let mut conn = pool.get()?;
        actions::products::find_product(&mut conn, product_id).map_err(|err| match err {
            diesel::result::Error::NotFound => ApiError::NotFound(format!("product {product_id} not found")),
            other => other.into(),
        })
    })
    .await??;

    Ok(HttpResponse::Ok().insert_header((header::CACHE_CONTROL, "public, max-age=30")).json(product))
}

#[derive(Debug, Serialize)]
struct StockResponse {
    product_id: i32,
    /// Units neither held nor sold
    available: i64,
    sold: i64,
    total: i64,
    closed: bool,
}

/// Units left, for the product page. Redis only (never Postgres), and cacheable
/// for a second, so the crowd refreshing it is absorbed by CDNs and proxies.
#[get("/products/{id}/stock")]
async fn get_stock(redis: web::Data<RedisConn>, path: web::Path<i32>) -> Result<HttpResponse, ApiError> {
    let product_id = path.into_inner();

    let mut con = redis.get_ref().clone();
    let stock = cache::stock_snapshot(&mut con, product_id)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("product {product_id} not found")))?;

    Ok(HttpResponse::Ok().insert_header((header::CACHE_CONTROL, "public, max-age=1")).json(StockResponse {
        product_id,
        available: stock.available,
        sold: stock.sold,
        total: stock.total,
        closed: stock.closed,
    }))
}

#[derive(Debug, Serialize)]
struct CloseResponse {
    product_id: i32,
    status: &'static str,
}

/// Ends the sale early (the admission worker otherwise closes it at
/// `sale_ends_at`). Final and idempotent: joins then return `410`, waiting
/// tokens report CLOSED, and the worker stops. The product check is in Redis,
/// like joins.
#[post("/products/{id}/close")]
async fn close_product(
    pool: web::Data<DbPool>,
    redis: web::Data<RedisConn>,
    path: web::Path<i32>,
) -> Result<HttpResponse, ApiError> {
    let product_id = path.into_inner();

    // Redis first: the flag turns joins away and stops admission at once. If
    // Postgres then fails, the sale is still closed; a retry, or the admission
    // worker when it sees the flag, records it. Postgres is updated even when
    // the flag was already set, so a retry does exactly that.
    let mut con = redis.get_ref().clone();
    if !cache::close_existing_product(&mut con, product_id).await? {
        return Err(ApiError::NotFound(format!("product {product_id} not found")));
    }
    // Leaves a canceled product canceled
    web::block(move || {
        let mut conn = pool.get()?;
        Ok::<_, ApiError>(actions::products::close_sale(&mut conn, product_id)?)
    })
    .await??;

    Ok(HttpResponse::Ok().json(CloseResponse { product_id, status: "CLOSED" }))
}

// POST /products, GET /products, GET /products/{id}, GET /products/{id}/stock,
// POST /products/{id}/close
pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(create_product)
        .service(list_products)
        .service(get_product)
        .service(get_stock)
        .service(close_product);
}
