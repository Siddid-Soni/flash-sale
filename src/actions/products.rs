use diesel::prelude::*;
use diesel::QueryResult;

use crate::models::{NewInventory, NewProduct, Product};
use crate::schema::{inventory, products};

/// Inserts the product and its inventory row (`stock = stock_total = stock`),
/// in one transaction.
pub fn create_product(conn: &mut PgConnection, new_product: &NewProduct, stock: i32) -> QueryResult<Product> {
    conn.transaction(|conn| {
        let product = diesel::insert_into(products::table)
            .values(new_product)
            .returning(Product::as_returning())
            .get_result(conn)?;
        diesel::insert_into(inventory::table)
            .values(&NewInventory { product_id: product.id, stock_total: stock, stock })
            .execute(conn)?;
        Ok(product)
    })
}

pub fn find_product(conn: &mut PgConnection, product_id: i32) -> QueryResult<Product> {
    products::table.find(product_id).select(Product::as_select()).first(conn)
}

/// Products that are not canceled, soonest sale first
pub fn list_products(conn: &mut PgConnection, limit: i64, offset: i64) -> QueryResult<Vec<Product>> {
    products::table
        .filter(products::status.ne("CANCELED"))
        .order((products::sale_opens_at.asc(), products::id.asc()))
        .limit(limit)
        .offset(offset)
        .select(Product::as_select())
        .load(conn)
}

/// Moves a `SCHEDULED` product to `ON_SALE`. Idempotent: returns 0 if it was
/// already on sale (or closed or canceled, which stay as they are).
pub fn open_sale(conn: &mut PgConnection, product_id: i32) -> QueryResult<usize> {
    diesel::update(products::table.find(product_id).filter(products::status.eq("SCHEDULED")))
        .set(products::status.eq("ON_SALE"))
        .execute(conn)
}

/// Closes the sale for good. Idempotent: returns 0 if it was already closed
/// or the product was canceled.
pub fn close_sale(conn: &mut PgConnection, product_id: i32) -> QueryResult<usize> {
    diesel::update(products::table.find(product_id).filter(products::status.eq_any(["SCHEDULED", "ON_SALE"])))
        .set(products::status.eq("CLOSED"))
        .execute(conn)
}

/// Products whose Redis state may be needed: not canceled, and not long over
pub fn live_products(conn: &mut PgConnection) -> QueryResult<Vec<Product>> {
    products::table
        .filter(products::status.ne("CANCELED"))
        .filter(products::sale_ends_at.gt(chrono::Utc::now() - chrono::Duration::days(1)))
        .select(Product::as_select())
        .load(conn)
}

/// Total units, how many are sold (total minus the stock Postgres still
/// counts), and the per-user limit (for the rebuild)
pub fn inventory_state(conn: &mut PgConnection, product_id: i32) -> QueryResult<(i32, i64, i32)> {
    let (stock_total, stock, max_per_user): (i32, i32, i32) = inventory::table
        .inner_join(products::table)
        .filter(inventory::product_id.eq(product_id))
        .select((inventory::stock_total, inventory::stock, products::max_per_user))
        .first(conn)?;
    Ok((stock_total, (stock_total - stock) as i64, max_per_user))
}
