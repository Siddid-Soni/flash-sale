use diesel::prelude::*;

use crate::models::User;
use crate::schema::users;

/// A guest has no username or email: it exists to own a session token
pub fn create_guest(conn: &mut PgConnection) -> QueryResult<User> {
    diesel::insert_into(users::table)
        .default_values()
        .returning(User::as_returning())
        .get_result(conn)
}
