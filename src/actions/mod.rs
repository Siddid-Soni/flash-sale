//! Database operations, one file per area. Each function takes `&mut PgConnection`
//! and returns `QueryResult`, so it works both from a handler (inside `web::block`)
//! and from a worker. Functions that change several tables open their own
//! transaction with `conn.transaction(...)`.

pub mod payments;
pub mod products;
pub mod users;
