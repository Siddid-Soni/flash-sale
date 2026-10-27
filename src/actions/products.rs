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
