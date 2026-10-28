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

/// `%`, `_` and `\` made literal for `LIKE`/`ILIKE` (the default escape character)
fn escape_like(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for c in text.chars() {
        if matches!(c, '%' | '_' | '\\') {
            escaped.push('\\');
        }
        escaped.push(c);
    }
    escaped
}

/// Up to `limit` sales whose name contains `needle`, any case, newest start
/// first. `needle` is plain text: its `%`, `_` and `\` match themselves.
pub fn search_sales(conn: &mut PgConnection, needle: &str, limit: i64) -> QueryResult<Vec<Product>> {
    products::table
        .filter(products::name.ilike(format!("%{}%", escape_like(needle))).escape('\\'))
        .select(Product::as_select())
        .order((products::sale_start_time.desc(), products::id.desc()))
        .limit(limit)
        .load(conn)
}

#[cfg(test)]
mod tests {
    use super::escape_like;

    #[test]
    fn wildcards_are_escaped() {
        assert_eq!(escape_like("100%_off\\"), "100\\%\\_off\\\\");
        assert_eq!(escape_like("phone"), "phone");
    }
}
