//! The fake provider's payment page, `/fake-pay/{session_id}` (roadmap step
//! 19), served by the `fake_payment_provider` binary. It stands in for the
//! page Stripe hosts: the user sees the amount and pays or cancels with a
//! plain HTML form, and the browser is sent back to the sale
//! (`{FRONTEND_URL}/?sale=<id>`, where the frontend resumes). The gateway
//! proxies `/fake-pay/` to the provider.
//!
//! The page reads and writes sessions only through `FakeProvider`. It does
//! not call the backend: pressing a button settles the session and queues a
//! webhook event, and the provider's dispatcher delivers that separately
//! (`crate::payment::dispatcher`). The provider does not know our deadline,
//! so paying an `OPEN` session always works, even after the deadline; the
//! backend then refunds. Paying an `EXPIRED` session is refused.

use actix_web::{get, http::header, post, web, HttpResponse};
use chrono::{DateTime, Utc};

use super::{FakeProvider, FakeSession, Settle, Settled};
use crate::{cache, config::Config, error::ApiError};

/// Escapes text for HTML element content and quoted attributes
fn escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            other => out.push(other),
        }
    }
    out
}

fn dollars(cents: i64) -> String {
    format!("${}.{:02}", cents / 100, cents % 100)
}

fn utc(at: DateTime<Utc>) -> String {
    at.format("%Y-%m-%d %H:%M:%S UTC").to_string()
}

/// Wraps a page body in a small self-contained document (no scripts, no
/// external requests)
fn page(title: &str, body: &str) -> String {
    format!(
        "<!doctype html>\n<html lang=\"en\"><head><meta charset=\"utf-8\">\
<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\
<title>{title}</title>\
<style>\
body{{font-family:system-ui,sans-serif;background:#f4f5f7;color:#1c2330;margin:0;padding:24px 16px}}\
main{{max-width:26rem;margin:8vh auto 0;background:#fff;border-radius:12px;padding:24px;box-shadow:0 1px 6px rgba(0,0,0,.12)}}\
h1{{font-size:1.25rem;margin:0 0 4px}}\
.note{{color:#667085;font-size:.85rem;margin:0 0 16px}}\
dl{{display:grid;grid-template-columns:auto 1fr;gap:6px 16px;margin:0 0 20px;font-size:.95rem}}\
dt{{color:#667085}}dd{{margin:0;overflow-wrap:anywhere}}\
form{{margin:0 0 8px}}\
button{{width:100%;padding:12px;font-size:1rem;border-radius:8px;border:1px solid #1c2330;background:#1c2330;color:#fff;cursor:pointer}}\
button.secondary{{background:#fff;color:#1c2330}}\
a{{color:#1c2330}}\
</style></head><body><main>\
<h1>Fake Pay</h1><p class=\"note\">A stand-in payment provider for development. No money moves.</p>\
{body}</main></body></html>",
        title = escape(title),
    )
}

fn html(status: actix_web::http::StatusCode, title: &str, body: &str) -> HttpResponse {
    HttpResponse::build(status)
        .insert_header((header::CONTENT_TYPE, "text/html; charset=utf-8"))
        .insert_header((header::CACHE_CONTROL, "no-store"))
        .insert_header((header::REFERRER_POLICY, "no-referrer"))
        .insert_header((header::CONTENT_SECURITY_POLICY, "default-src 'none'; style-src 'unsafe-inline'"))
        .body(page(title, body))
}

/// Where the browser goes back to: the sale the ticket belongs to
fn return_url(config: &Config, ticket: &str) -> String {
    let base = config.frontend_url.trim_end_matches('/');
    match cache::ticket_sale(ticket) {
        Some(sale) => format!("{base}/?sale={sale}"),
        None => format!("{base}/"),
    }
}

fn back_link(config: &Config, ticket: &str) -> String {
    format!("<p><a href=\"{}\">Back to the sale</a></p>", escape(&return_url(config, ticket)))
}

fn not_found() -> HttpResponse {
    html(actix_web::http::StatusCode::NOT_FOUND, "Not found", "<p>This payment session does not exist.</p>")
}

/// The page for a session that is no longer open
fn closed(config: &Config, session: &FakeSession, state: &str) -> HttpResponse {
    use actix_web::http::StatusCode;
    let (status, title, message) = match state {
        "PAID" => (StatusCode::CONFLICT, "Paid", "This session has already been paid."),
        "EXPIRED" => (StatusCode::GONE, "Expired", "This payment link has expired. Payment is no longer possible."),
        "CANCELED" => (StatusCode::CONFLICT, "Cancelled", "This payment was cancelled."),
        "REFUNDED" => (StatusCode::CONFLICT, "Refunded", "This payment has been refunded."),
        _ => (StatusCode::CONFLICT, "Unavailable", "This session cannot be used."),
    };
    html(status, title, &format!("<p>{}</p>{}", escape(message), back_link(config, &session.ticket)))
}

fn redirect(location: &str) -> HttpResponse {
    HttpResponse::SeeOther().insert_header((header::LOCATION, location)).insert_header((header::CACHE_CONTROL, "no-store")).finish()
}

/// The payment page: amount, ticket, deadline, Pay and Cancel. `404` for an
/// unknown session; a page saying so for one that is not open.
#[get("/fake-pay/{session_id}")]
async fn show(
    path: web::Path<String>,
    provider: web::Data<FakeProvider>,
    config: web::Data<Config>,
) -> Result<HttpResponse, ApiError> {
    let session_id = path.into_inner();
    let Some(session) = provider.session(&session_id).await? else { return Ok(not_found()) };
    if session.state != "OPEN" {
        let state = session.state.clone();
        return Ok(closed(&config, &session, &state));
    }
    let id = escape(&session.id);
    let amount = escape(&dollars(session.amount_cents));
    let body = format!(
        "<dl><dt>Ticket</dt><dd>{ticket}</dd><dt>Amount</dt><dd>{amount}</dd><dt>Pay before</dt><dd>{expires}</dd></dl>\
<form method=\"post\" action=\"/fake-pay/{id}/pay\"><button type=\"submit\">Pay {amount}</button></form>\
<form method=\"post\" action=\"/fake-pay/{id}/cancel\"><button type=\"submit\" class=\"secondary\">Cancel</button></form>",
        ticket = escape(&session.ticket),
        expires = escape(&utc(session.expires_at)),
    );
    Ok(html(actix_web::http::StatusCode::OK, "Pay", &body))
}

/// Settles the session as `action` asks and answers: `303` back to the sale
/// when it is settled (or already was the same way), the matching page when
/// the session refuses it.
async fn settle(
    session_id: String,
    action: Settle,
    provider: web::Data<FakeProvider>,
    config: &Config,
) -> Result<HttpResponse, ApiError> {
    let Some(session) = provider.session(&session_id).await? else { return Ok(not_found()) };
    match provider.settle(&session, action).await? {
        Settled::Moved | Settled::Already => Ok(redirect(&return_url(config, &session.ticket))),
        Settled::Missing => Ok(not_found()),
        Settled::Refused(state) => Ok(closed(config, &session, &state)),
    }
}

/// Pay: `OPEN` to `PAID`, queues `checkout.session.completed`, `303` to `/?sale=<id>`
#[post("/fake-pay/{session_id}/pay")]
async fn pay(
    path: web::Path<String>,
    provider: web::Data<FakeProvider>,
    config: web::Data<Config>,
) -> Result<HttpResponse, ApiError> {
    settle(path.into_inner(), Settle::Pay, provider, &config).await
}

/// Cancel: `OPEN` to `CANCELED`, queues `checkout.session.payment_failed`, `303` to `/?sale=<id>`
#[post("/fake-pay/{session_id}/cancel")]
async fn cancel(
    path: web::Path<String>,
    provider: web::Data<FakeProvider>,
    config: web::Data<Config>,
) -> Result<HttpResponse, ApiError> {
    settle(path.into_inner(), Settle::Cancel, provider, &config).await
}

// GET /fake-pay/{id}, POST /fake-pay/{id}/pay, POST /fake-pay/{id}/cancel
pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(show).service(pay).service(cancel);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_is_escaped() {
        assert_eq!(escape("<script>\"x\" & 'y'</script>"), "&lt;script&gt;&quot;x&quot; &amp; &#39;y&#39;&lt;/script&gt;");
        assert_eq!(escape("plain"), "plain");
    }

    #[test]
    fn amounts_are_dollars_and_cents() {
        assert_eq!(dollars(59_900), "$599.00");
        assert_eq!(dollars(5), "$0.05");
    }
}
