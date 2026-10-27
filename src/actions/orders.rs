use diesel::prelude::*;

use crate::models::Order;
use crate::schema::orders;

/// The order for a queue ticket, if the processor has created it. The last
/// fallback of the status endpoint, after the read cache and the hold.
pub fn find_by_ticket(conn: &mut PgConnection, ticket: &str) -> QueryResult<Option<Order>> {
    orders::table
        .filter(orders::queue_ticket_id.eq(ticket))
        .select(Order::as_select())
        .first(conn)
        .optional()
}
