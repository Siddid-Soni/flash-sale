//! Operator routes under `/v1/admin`, guarded by `AdminAuth`.

use actix_web::{post, web, HttpResponse};
use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::{
    actions,
    api::sales::SaleCache,
    auth::AdminAuth,
    cache::{self, RedisConn},
    db::DbPool,
    error::ApiError,
    models::NewProduct,
};

const NAME_LEN: std::ops::RangeInclusive<usize> = 1..=255;
const MAX_INVENTORY: i32 = 10_000_000;

#[derive(Debug, Deserialize)]
struct NewSale {
    name: String,
    price_cents: i32,
    total_inventory: i32,
    sale_start_time: DateTime<Utc>,
}

fn validate(body: &NewSale) -> Result<(), ApiError> {
    if !NAME_LEN.contains(&body.name.trim().chars().count()) {
        return Err(ApiError::BadRequest("name must be 1-255 characters".into()));
    }
    if body.price_cents <= 0 {
        return Err(ApiError::BadRequest("price_cents must be positive".into()));
    }
    if !(1..=MAX_INVENTORY).contains(&body.total_inventory) {
        return Err(ApiError::BadRequest(format!("total_inventory must be between 1 and {MAX_INVENTORY}")));
    }
    Ok(())
}

/// Creates a sale: the product row (`sold = 0`), then its keys in Redis
/// (`stock:{id}`, `sale_start:{id}`) in one atomic pipeline. Redis does not
/// count sales; `products.sold` is the record.
/// PostgreSQL goes first so the sale id exists before Redis refers to it; if
/// Redis then fails the answer is `500` and the row stays without stock (the
/// sale reads as sold out), which the operator can ignore and recreate.
#[post("/sales")]
async fn create_sale(
    _admin: AdminAuth,
    pool: web::Data<DbPool>,
    redis: web::Data<RedisConn>,
    sale_cache: web::Data<SaleCache>,
    body: web::Json<NewSale>,
) -> Result<HttpResponse, ApiError> {
    let body = body.into_inner();
    validate(&body)?;

    let product = web::block(move || {
        let mut conn = pool.get()?;
        let new_product = NewProduct {
            name: body.name.trim(),
            price_cents: body.price_cents,
            total_inventory: body.total_inventory,
            sale_start_time: body.sale_start_time,
        };
        Ok::<_, ApiError>(actions::products::create_product(&mut conn, &new_product)?)
    })
    .await??;

    let mut con = redis.get_ref().clone();
    cache::init_sale(&mut con, product.id, product.total_inventory, product.sale_start_time).await?;
    // This replica may have cached "not found" for the id a moment ago
    sale_cache.forget(product.id);

    log::info!("Created sale {} ({} units)", product.id, product.total_inventory);
    Ok(HttpResponse::Created().json(product))
}

// POST /v1/admin/sales
pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::scope("/admin").service(create_sale));
}
