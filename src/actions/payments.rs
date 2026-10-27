//! The payment inbox: provider events are stored first, handled at once by the
//! webhook endpoint of the order processor, and handled again by the inbox
//! loop until they are marked processed. The loop claims unprocessed rows with
//! `FOR UPDATE SKIP LOCKED` so several workers can share the work.

use chrono::Utc;
use diesel::prelude::*;
use diesel::QueryResult;

use crate::models::{NewPaymentInboxEntry, PaymentInboxEntry};
use crate::schema::payment_inbox;

/// Stores a provider event. Returns false if it was already stored (providers
/// deliver events more than once).
pub fn insert_inbox(conn: &mut PgConnection, entry: &NewPaymentInboxEntry) -> QueryResult<bool> {
    let inserted = diesel::insert_into(payment_inbox::table)
        .values(entry)
        .on_conflict(payment_inbox::provider_event_id)
        .do_nothing()
        .execute(conn)?;
    Ok(inserted == 1)
}

/// The stored row of a provider event (it must exist: call after `insert_inbox`)
pub fn find_inbox(conn: &mut PgConnection, provider_event_id: &str) -> QueryResult<PaymentInboxEntry> {
    payment_inbox::table
        .filter(payment_inbox::provider_event_id.eq(provider_event_id))
        .select(PaymentInboxEntry::as_select())
        .first(conn)
}

/// Locks the oldest unprocessed event no other worker holds, for the rest of
/// the caller's transaction. `None` when there is nothing to do. `skip` lists
/// events to pass over (the caller's own backoff for events that keep
/// failing), so one that fails always does not hold up the ones behind it.
pub fn claim_next(conn: &mut PgConnection, skip: &[i64]) -> QueryResult<Option<PaymentInboxEntry>> {
    payment_inbox::table
        .filter(payment_inbox::processed_at.is_null())
        .filter(payment_inbox::id.ne_all(skip))
        .order(payment_inbox::id.asc())
        .for_update()
        .skip_locked()
        .select(PaymentInboxEntry::as_select())
        .first(conn)
        .optional()
}

pub fn mark_processed(conn: &mut PgConnection, id: i64) -> QueryResult<()> {
    diesel::update(payment_inbox::table.find(id))
        .set((payment_inbox::processed_at.eq(Utc::now()), payment_inbox::attempts.eq(payment_inbox::attempts + 1)))
        .execute(conn)?;
    Ok(())
}

/// Records a failed attempt; the event stays unprocessed and is retried.
pub fn record_failure(conn: &mut PgConnection, id: i64, error: &str) -> QueryResult<()> {
    diesel::update(payment_inbox::table.find(id))
        .set((payment_inbox::attempts.eq(payment_inbox::attempts + 1), payment_inbox::last_error.eq(error)))
        .execute(conn)?;
    Ok(())
}
