//! The fake provider's webhook delivery: a background task that takes due
//! events from the outbox and POSTs them, signed, to the merchant's webhook
//! URL, like a real provider's delivery system. The payment page only queues
//! an event; nothing waits for its delivery.
//!
//! - Several Order Service replicas may each run a dispatcher: an event is
//!   taken with a lease (`FakeProvider::claim_due_events`), so one delivers it
//!   at a time. A dispatcher that dies mid-delivery leaves the event to be
//!   taken again when the lease ends, so delivery is at-least-once; the
//!   receiver stores each event id once.
//! - A `2xx` answer removes the event. Any other answer, or a network error,
//!   schedules the next attempt with exponential backoff (1 s, 2 s, 4 s ...
//!   up to an hour). An event still undelivered after 24 hours is dropped with
//!   an error log.
//! - Every attempt is signed afresh, so a retry hours later carries a current
//!   timestamp and passes the receiver's replay check.
//!
//! The client is `awc` without TLS: the target is the Order Service on the
//! same network, plain HTTP.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;

use super::{webhook::SIGNATURE_HEADER, FakeProvider, OutboxEvent, PaymentError, PaymentResult};

/// Delivery timings; `Default` is what `order_service` runs with
#[derive(Debug, Clone)]
pub struct DispatcherSettings {
    /// How often an idle dispatcher looks for due events
    pub poll_interval: Duration,
    /// How long a taken event is reserved for the taker; longer than
    /// `batch * request_timeout` so a batch is finished within its lease
    pub lease: Duration,
    /// Events taken per sweep
    pub batch: usize,
    /// Per-request timeout
    pub request_timeout: Duration,
    /// Delay before the second attempt; it doubles from there
    pub base_backoff: Duration,
    /// Longest delay between attempts
    pub max_backoff: Duration,
    /// An event older than this is dropped
    pub max_age: Duration,
}

impl Default for DispatcherSettings {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(250),
            lease: Duration::from_secs(60),
            batch: 10,
            request_timeout: Duration::from_secs(5),
            base_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(60 * 60),
            max_age: Duration::from_secs(24 * 60 * 60),
        }
    }
}

impl DispatcherSettings {
    /// The wait after the `attempts`-th failed attempt (1-based): `base`,
    /// `2 * base`, `4 * base` ... capped at `max_backoff`
    pub fn backoff(&self, attempts: u32) -> Duration {
        let doublings = attempts.saturating_sub(1).min(32);
        self.base_backoff.saturating_mul(1u32.checked_shl(doublings).unwrap_or(u32::MAX)).min(self.max_backoff)
    }
}

/// Delivers outbox events to `url`
pub struct Dispatcher {
    provider: Arc<FakeProvider>,
    url: String,
    client: awc::Client,
    settings: DispatcherSettings,
}

impl Dispatcher {
    pub fn new(provider: Arc<FakeProvider>, url: String, settings: DispatcherSettings) -> Self {
        // A redirect answer is a failure to retry, not a hop to follow
        let client = awc::Client::builder().timeout(settings.request_timeout).disable_redirects().finish();
        Self { provider, url, client, settings }
    }

    /// One attempt at one event, signed now: `Ok` if the receiver answered `2xx`
    async fn post(&self, event: &OutboxEvent) -> Result<(), String> {
        let signature = self.provider.sign_delivery(event.body.as_bytes()).map_err(|e| e.to_string())?;
        let response = self
            .client
            .post(&self.url)
            .insert_header(("Content-Type", "application/json"))
            .insert_header((SIGNATURE_HEADER, signature))
            .send_body(event.body.clone())
            .await
            .map_err(|e| format!("request failed: {e}"))?;
        match response.status() {
            status if status.is_success() => Ok(()),
            status => Err(format!("answered {status}")),
        }
    }

    /// Takes the events that are due and delivers each once. Returns how many
    /// it took (0: nothing was due).
    pub async fn sweep(&self) -> PaymentResult<usize> {
        let events = self.provider.claim_due_events(self.settings.batch, self.settings.lease).await?;
        let taken = events.len();
        for event in events {
            let age = Utc::now().timestamp_millis() - event.created_ms;
            match self.post(&event).await {
                Ok(()) => {
                    log::debug!("Fake provider delivered {}", event.id);
                    self.provider.event_done(&event.id).await?;
                }
                Err(reason) if age >= self.settings.max_age.as_millis() as i64 => {
                    log::error!(
                        "Fake provider gave up on event {} after {} attempts over {} h: {reason}",
                        event.id,
                        event.attempts + 1,
                        age / 3_600_000
                    );
                    self.provider.event_done(&event.id).await?;
                }
                Err(reason) => {
                    let attempts = event.attempts + 1;
                    let wait = self.settings.backoff(attempts);
                    log::warn!("Fake provider could not deliver {} (attempt {attempts}): {reason}; retrying in {wait:?}", event.id);
                    let next = Utc::now().timestamp_millis() + wait.as_millis() as i64;
                    self.provider.event_failed(&event.id, attempts, next).await?;
                }
            }
        }
        Ok(taken)
    }

    /// Delivers forever: sweeps, and sleeps when nothing was due. Run it with
    /// `actix_web::rt::spawn` (the client is not `Send`).
    pub async fn run(self) {
        log::info!("Fake provider delivers webhooks to {}", self.url);
        loop {
            match self.sweep().await {
                Ok(0) => actix_web::rt::time::sleep(self.settings.poll_interval).await,
                Ok(_) => {}
                Err(PaymentError(e)) => {
                    log::warn!("Fake provider outbox: {e}");
                    actix_web::rt::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_and_is_capped() {
        let s = DispatcherSettings::default();
        let secs = |n| s.backoff(n).as_secs();
        assert_eq!((secs(1), secs(2), secs(3), secs(4)), (1, 2, 4, 8));
        assert_eq!(secs(12), 2048);
        assert_eq!(secs(13), 3600);
        assert_eq!(secs(1000), 3600);
        assert_eq!(secs(0), 1);
    }
}
