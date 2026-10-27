//! The backend's side of the provider's HTTP API: `HttpProvider` implements
//! `PaymentProvider` by calling the provider at `PAYMENT_PROVIDER_URL`, as the
//! processor and the worker would call Stripe.
//!
//! The requests are made with `awc`, whose futures are not `Send`, while the
//! callers run on tokio's multi-thread runtime. So the client lives on a
//! thread of its own, inside an actix runtime, and the calls reach it over a
//! channel; each is answered through a oneshot. The thread ends when the last
//! `HttpProvider` is dropped. Requests run concurrently on that thread.
//!
//! The API (see `payment::provider_api`): `POST /v1/checkout/sessions` with an
//! `Idempotency-Key` header, `POST /v1/checkout/sessions/{id}/expire`,
//! `POST /v1/refunds`. Any answer but `2xx` is an error, which the callers retry.

use std::rc::Rc;
use std::time::Duration;

use actix_web::http::header::HeaderMap;
use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot};

use super::{
    webhook::{self, ProviderEvent},
    PaymentError, PaymentProvider, PaymentResult, Session,
};

/// How long one request to the provider may take
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// A request for the client thread, with the channel for its answer
struct Call {
    path: String,
    idempotency_key: Option<String>,
    body: String,
    reply: oneshot::Sender<Result<(u16, Vec<u8>), String>>,
}

/// The provider's HTTP API as a `PaymentProvider`
pub struct HttpProvider {
    calls: mpsc::UnboundedSender<Call>,
    /// Verifies the webhooks the provider sends (`PAYMENT_WEBHOOK_SECRET`)
    webhook_secret: String,
}

impl HttpProvider {
    /// A client for the provider at `base_url`, e.g. `http://fake-payment-provider:8090`
    pub fn new(base_url: String, webhook_secret: String) -> Self {
        let (calls, incoming) = mpsc::unbounded_channel();
        let base_url = base_url.trim_end_matches('/').to_string();
        std::thread::Builder::new()
            .name("payment-http".into())
            .spawn(move || actix_web::rt::System::new().block_on(serve(base_url, incoming)))
            .expect("a thread for the payment provider client");
        Self { calls, webhook_secret }
    }

    /// POSTs `body` to `path` and returns the JSON answer of a `2xx`
    async fn post(&self, path: String, idempotency_key: Option<&str>, body: Value) -> PaymentResult<Value> {
        let (reply, answer) = oneshot::channel();
        let call = Call { path, idempotency_key: idempotency_key.map(str::to_owned), body: body.to_string(), reply };
        self.calls.send(call).map_err(|_| PaymentError("the provider client stopped".into()))?;
        let (status, bytes) = answer
            .await
            .map_err(|_| PaymentError("the provider client stopped".into()))?
            .map_err(PaymentError)?;
        if !(200..300).contains(&status) {
            let detail = serde_json::from_slice::<Value>(&bytes)
                .ok()
                .and_then(|v| v.get("message").and_then(Value::as_str).map(str::to_owned))
                .unwrap_or_else(|| String::from_utf8_lossy(&bytes).chars().take(200).collect());
            return Err(PaymentError(format!("answered {status}: {detail}")));
        }
        serde_json::from_slice(&bytes).map_err(|e| PaymentError(format!("answer is not JSON: {e}")))
    }
}

/// The client thread: takes calls and runs each as a task of its own
async fn serve(base_url: String, mut incoming: mpsc::UnboundedReceiver<Call>) {
    // A redirect answer is a failure to retry, not a hop to follow
    let client = Rc::new(awc::Client::builder().timeout(REQUEST_TIMEOUT).disable_redirects().finish());
    while let Some(call) = incoming.recv().await {
        let (client, url) = (client.clone(), format!("{base_url}{}", call.path));
        actix_web::rt::spawn(async move {
            let mut request = client.post(url).insert_header(("Content-Type", "application/json"));
            if let Some(key) = &call.idempotency_key {
                request = request.insert_header(("Idempotency-Key", key.as_str()));
            }
            let result = match request.send_body(call.body.clone()).await {
                Ok(mut response) => {
                    let status = response.status().as_u16();
                    response.body().await.map(|bytes| (status, bytes.to_vec())).map_err(|e| format!("reading the answer: {e}"))
                }
                Err(e) => Err(format!("request failed: {e}")),
            };
            let _ = call.reply.send(result);
        });
    }
}

fn field(answer: &Value, name: &str) -> PaymentResult<String> {
    answer
        .get(name)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| PaymentError(format!("the answer has no {name}")))
}

#[async_trait]
impl PaymentProvider for HttpProvider {
    fn name(&self) -> &'static str {
        "fake"
    }

    async fn create_session(
        &self,
        ticket: &str,
        amount_cents: i32,
        expires_at: DateTime<Utc>,
        idempotency_key: &str,
    ) -> PaymentResult<Session> {
        let body = json!({
            "ticket": ticket,
            "amount_cents": amount_cents,
            "expires_at": expires_at.to_rfc3339_opts(SecondsFormat::Millis, true),
        });
        let answer = self.post("/v1/checkout/sessions".into(), Some(idempotency_key), body).await?;
        Ok(Session { id: field(&answer, "id")?, url: field(&answer, "url")? })
    }

    async fn expire_session(&self, session_id: &str) -> PaymentResult<()> {
        self.post(format!("/v1/checkout/sessions/{session_id}/expire"), None, json!({})).await.map(drop)
    }

    async fn refund(&self, session_id: &str) -> PaymentResult<()> {
        self.post("/v1/refunds".into(), None, json!({ "session_id": session_id })).await.map(drop)
    }

    fn verify_webhook(&self, headers: &HeaderMap, body: &[u8]) -> PaymentResult<ProviderEvent> {
        let header = headers
            .get(webhook::SIGNATURE_HEADER)
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| PaymentError(format!("no {} header", webhook::SIGNATURE_HEADER)))?;
        webhook::verify_signature(&self.webhook_secret, header, body, Utc::now().timestamp())?;
        let payload = serde_json::from_slice(body).map_err(|e| PaymentError(format!("event is not JSON: {e}")))?;
        ProviderEvent::from_json(payload).ok_or_else(|| PaymentError("event is missing required fields".into()))
    }
}
