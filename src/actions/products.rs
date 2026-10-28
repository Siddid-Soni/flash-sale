use chrono::{DateTime, Utc};
use diesel::prelude::*;

use crate::models::{NewProduct, Product};
use crate::schema::products;

/// Inserts the product (`sold = 0`). Its id is the sale id.
pub fn create_product(conn: &mut PgConnection, new_product: &NewProduct) -> QueryResult<Product> {
    diesel::insert_into(products::table)
        .values(new_product)
        .returning(Product::as_returning())
        .get_result(conn)
}

pub fn find_product(conn: &mut PgConnection, product_id: i32) -> QueryResult<Option<Product>> {
    products::table.find(product_id).select(Product::as_select()).first(conn).optional()
}

/// The latest `limit` sales, by start time (newest first), for the home page
pub fn list_sales(conn: &mut PgConnection, limit: i64) -> QueryResult<Vec<Product>> {
    products::table
        .select(Product::as_select())
        .order((products::sale_start_time.desc(), products::id.desc()))
        .limit(limit)
        .load(conn)
}

/// Ids of the sales the expiration worker watches: those that start at or
/// after `since` (running now, finished recently, or still to come)
pub fn sales_starting_since(conn: &mut PgConnection, since: DateTime<Utc>) -> QueryResult<Vec<i32>> {
    products::table.filter(products::sale_start_time.ge(since)).select(products::id).order(products::id.asc()).load(conn)
}
