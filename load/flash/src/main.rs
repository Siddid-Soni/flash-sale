//! Simulated buyers for the Stage 9 load tests (`load/flash.sh`, `load/backlog.sh`).
//!
//! Every user is one task that behaves like the browser client:
//!
//! 1. arrives at a uniformly random moment within `ARRIVAL_SECS` after the sale
//!    opens (the opening time is read from `GET /v1/sales/{id}`);
//! 2. optionally looks at the sale details, then `POST /v1/sales/{id}/checkout`
//!    with one `Idempotency-Key` reused on every retry. `202`/`200` give a
//!    ticket; `409`/`410` end the user's story; `429` waits `Retry-After`;
//!    `503`, other 5xx and network errors back off with jitter and retry with
//!    the same key (counted);
//! 3. polls `GET /v1/orders/status/{ticket}` every `POLL_MS` until
//!    `AWAITING_PAYMENT` (the time from checkout to link is recorded);
//! 4. with probability `PAY_RATIO` pays by POSTing the fake-pay form at the
//!    provider's own address, outside the gateway (the redirect is not
//!    followed) after a think time, else abandons; a share
//!    (`LATE_RATIO`) of the payers pays only after the link has expired, and
//!    another (`RACE_RATIO`) pays 150 ms before it, racing the expiration worker;
//! 5. keeps polling until a final state (`CONFIRMED`, `EXPIRED`, `FAILED`,
//!    `REFUNDED`) or `DEADLINE_SECS`; a payer who sees `EXPIRED` polls on for
//!    `REFUND_GRACE`, since a payment at the deadline is refunded after the expiry.
//!
//! Load must reach the system through the gateway on port 8000 with a distinct
//! `X-Forwarded-For` per simulated user (the gateway trusts it only when started
//! with `GATEWAY_TRUST_XFF=1`) and a browser-like `User-Agent`.
//!
//! Environment (required: `SALE_ID`, `USERS_FILE`, `JWT_SECRET`):
//!   BASE_URL (http://localhost:8000), PROVIDER_URL (http://localhost:8090: where the
//!   payment page is; the path of the `payment_url` the user was given is used on it),
//!   USERS_LIMIT (all lines of the file),
//!   ARRIVAL_SECS (10), PAY_RATIO (0.6), LATE_RATIO (0.05), RACE_RATIO (0.05), POLL_MS (2000),
//!   PAY_THINK_MIN_MS (500), PAY_THINK_MAX_MS (6000), DEADLINE_SECS (180),
//!   SESSION_SAMPLE (0: this many users get their token from `POST /v1/session`
//!   instead of signing it locally), VIEW_SALE (1: look at the sale before
//!   checkout), REPORT_JSON (path of the JSON summary; default stdout only),
//!   MARGIN_MS (1500: how far from the expiry a payment is "surely in time" or
//!   "surely late").
//!
//! Prints a table and a JSON summary; the exit code is 0 unless the run could
//! not start (the scripts judge the numbers).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde::Serialize;
use serde_json::{json, Value};
use tokio::time::sleep_until;

/// How long a payer keeps polling after seeing EXPIRED (the frontend's `REFUND_GRACE_MS`)
const REFUND_GRACE: Duration = Duration::from_secs(60);

const BROWSER_UA: &str =
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36";

#[derive(Clone, Copy, PartialEq, Eq, Debug, PartialOrd, Ord)]
enum Ep {
    Session,
    Sale,
    Checkout,
    Status,
    Pay,
}

const ENDPOINTS: [(Ep, &str); 5] = [
    (Ep::Session, "POST /v1/session"),
    (Ep::Sale, "GET /v1/sales/{id}"),
    (Ep::Checkout, "POST /v1/sales/{id}/checkout"),
    (Ep::Status, "GET /v1/orders/status/{ticket}"),
    (Ep::Pay, "POST /fake-pay/{session}/pay"),
];

struct Sample {
    ep: Ep,
    /// 0 = network error or timeout
    status: u16,
    micros: u64,
    /// Seconds since the generator started
    at_sec: u32,
}

struct Reply {
    status: u16,
    retry_after: Option<f64>,
    body: Vec<u8>,
}

struct Ctx {
    client: reqwest::Client,
    base: String,
    /// The payment provider's address (it is not behind the gateway)
    provider: String,
    sale: i32,
    secret: String,
    started: Instant,
    deadline: Instant,
    arrival: Duration,
    pay_ratio: f64,
    late_ratio: f64,
    race_ratio: f64,
    poll: Duration,
    think: (u64, u64),
    session_sample: usize,
    view_sale: bool,
    margin: i64,
    done: AtomicUsize,
    retries_5xx: AtomicU64,
    retries_network: AtomicU64,
    waits_429: AtomicU64,
}

/// What one user's requests recorded. Each user task owns its own, so recording a
/// response takes no lock; `main` merges them once every user has finished.
#[derive(Default)]
struct Log {
    samples: Vec<Sample>,
    /// Why requests failed at the network level, with counts
    errors: BTreeMap<String, usize>,
}

/// What happened to one user
#[derive(Default, Serialize)]
struct UserRec {
    #[serde(skip)]
    log: Log,
    outcome: &'static str,
    /// Milliseconds from the first checkout attempt to the first poll that showed the link
    link_ms: Option<u64>,
    /// `expires_at` minus the moment the link was seen (ms): at most the payment window
    window_left_ms: Option<i64>,
    /// "in_time", "ambiguous" or "late", for a payment that the provider accepted
    pay_class: Option<&'static str>,
    pay_refused: bool,
    final_status: Option<String>,
}

fn xff(index: usize) -> String {
    let n = index as u32 + 1;
    format!("10.{}.{}.{}", (n >> 16) & 255, (n >> 8) & 255, n & 255)
}

fn sign(user: i64, secret: &str) -> String {
    let now = Utc::now().timestamp();
    let claims = json!({ "sub": user.to_string(), "iat": now, "exp": now + 6 * 3600 });
    encode(&Header::new(Algorithm::HS256), &claims, &EncodingKey::from_secret(secret.as_bytes())).expect("sign a token")
}

impl Ctx {
    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base, path)
    }

    /// Sends the request (a clone per attempt is the caller's business), records
    /// status and latency in the user's `log`, and returns the reply. Status 0 =
    /// network failure.
    async fn call(&self, log: &mut Log, ep: Ep, request: reqwest::RequestBuilder) -> Reply {
        let sent = Instant::now();
        let result = async {
            let response = request.send().await?;
            let status = response.status().as_u16();
            let retry_after = response
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<f64>().ok());
            let body = response.bytes().await?.to_vec();
            Ok::<_, reqwest::Error>(Reply { status, retry_after, body })
        }
        .await;
        let reply = match result {
            Ok(reply) => reply,
            Err(e) => {
                // The innermost cause says what happened (reset, refused, timed out, ...)
                let mut cause: &dyn std::error::Error = &e;
                while let Some(next) = cause.source() {
                    cause = next;
                }
                let kind = format!("{}{}", if e.is_timeout() { "timeout: " } else if e.is_connect() { "connect: " } else { "" }, cause);
                *log.errors.entry(kind).or_default() += 1;
                Reply { status: 0, retry_after: None, body: Vec::new() }
            }
        };
        log.samples.push(Sample {
            ep,
            status: reply.status,
            micros: sent.elapsed().as_micros() as u64,
            at_sec: self.started.elapsed().as_secs() as u32,
        });
        reply
    }

    fn get(&self, path: &str, ip: &str, bearer: Option<&str>) -> reqwest::RequestBuilder {
        let mut r = self.client.get(self.url(path)).header("X-Forwarded-For", ip);
        if let Some(b) = bearer {
            r = r.header("Authorization", b);
        }
        r
    }
}

async fn sleep_ms(ms: u64) {
    tokio::time::sleep(Duration::from_millis(ms)).await;
}

/// Exponential backoff with jitter: about `base * 2^attempt`, capped at 4 s
fn backoff_ms(attempt: u32) -> u64 {
    let ceiling = (250u64 << attempt.min(6)).min(4000);
    fastrand::u64(ceiling / 2..=ceiling)
}

async fn user(ctx: Arc<Ctx>, index: usize, user_id: i64, open_at: Instant) -> UserRec {
    let mut rec = user_inner(&ctx, index, user_id, open_at).await;
    if rec.outcome.is_empty() {
        rec.outcome = "error";
    }
    ctx.done.fetch_add(1, Ordering::Relaxed);
    rec
}

async fn user_inner(ctx: &Ctx, index: usize, user_id: i64, open_at: Instant) -> UserRec {
    let mut rec = UserRec::default();
    let ip = xff(index);

    // Token: signed locally, or (for a sample of users) a real guest session
    let bearer = if index < ctx.session_sample {
        sleep_ms(fastrand::u64(0..=3000)).await;
        let reply = ctx.call(&mut rec.log, Ep::Session, ctx.client.post(ctx.url("/v1/session")).header("X-Forwarded-For", &ip)).await;
        let body: Value = serde_json::from_slice(&reply.body).unwrap_or_default();
        match body["token"].as_str() {
            Some(token) if reply.status == 201 => format!("Bearer {token}"),
            _ => {
                rec.outcome = "error";
                return rec;
            }
        }
    } else {
        format!("Bearer {}", sign(user_id, &ctx.secret))
    };

    // Arrival: uniform within ARRIVAL_SECS after the sale opens
    let arrival = open_at + Duration::from_millis(fastrand::u64(0..=ctx.arrival.as_millis() as u64));
    sleep_until(arrival.into()).await;

    if ctx.view_sale {
        ctx.call(&mut rec.log, Ep::Sale, ctx.get(&format!("/v1/sales/{}", ctx.sale), &ip, None)).await;
    }

    // Checkout: one key for every attempt
    let key = uuid::Uuid::new_v4().to_string();
    let first_checkout = Instant::now();
    let ticket = {
        let mut attempt = 0u32;
        loop {
            if Instant::now() > ctx.deadline {
                rec.outcome = "timeout";
                return rec;
            }
            let request = ctx
                .client
                .post(ctx.url(&format!("/v1/sales/{}/checkout", ctx.sale)))
                .header("X-Forwarded-For", &ip)
                .header("Authorization", &bearer)
                .header("Idempotency-Key", &key)
                .header("Content-Length", "0");
            let reply = ctx.call(&mut rec.log, Ep::Checkout, request).await;
            match reply.status {
                200 | 202 => {
                    let body: Value = serde_json::from_slice(&reply.body).unwrap_or_default();
                    match body["queue_ticket_id"].as_str() {
                        Some(t) => break t.to_owned(),
                        None => {
                            rec.outcome = "error";
                            return rec;
                        }
                    }
                }
                409 => {
                    rec.outcome = "conflict";
                    return rec;
                }
                410 => {
                    rec.outcome = "sold_out";
                    return rec;
                }
                429 => {
                    ctx.waits_429.fetch_add(1, Ordering::Relaxed);
                    let wait = reply.retry_after.unwrap_or(1.0);
                    sleep_ms((wait * 1000.0) as u64 + fastrand::u64(0..=250)).await;
                }
                // Sale not open yet by the server's clock: try again shortly
                403 => sleep_ms(100 + fastrand::u64(0..=100)).await,
                0 => {
                    ctx.retries_network.fetch_add(1, Ordering::Relaxed);
                    sleep_ms(backoff_ms(attempt)).await;
                    attempt += 1;
                }
                s if s >= 500 => {
                    ctx.retries_5xx.fetch_add(1, Ordering::Relaxed);
                    let wait = reply.retry_after.map(|r| (r * 1000.0) as u64).unwrap_or(0);
                    sleep_ms(backoff_ms(attempt).max(wait)).await;
                    attempt += 1;
                }
                _ => {
                    rec.outcome = "error";
                    return rec;
                }
            }
        }
    };

    // Poll until a final state; pay when the plan says so
    let status_path = format!("/v1/orders/status/{ticket}");
    let poll = ctx.poll.as_millis() as u64;
    let mut next_poll = Instant::now() + Duration::from_millis(fastrand::u64(0..=poll));
    let mut pay_at: Option<(Instant, String)> = None;
    let mut expires: Option<DateTime<Utc>> = None;
    let mut paid = false;
    let mut expired_since: Option<Instant> = None;
    let mut last_status = String::new();
    loop {
        let wake = match &pay_at {
            Some((at, _)) if !paid => (*at).min(next_poll),
            _ => next_poll,
        };
        if wake > ctx.deadline {
            rec.outcome = "timeout";
            rec.final_status = Some(last_status);
            return rec;
        }
        sleep_until(wake.into()).await;

        if let Some((at, url)) = &pay_at {
            if !paid && Instant::now() >= *at {
                paid = true;
                let sent_utc = Utc::now();
                let reply = pay(ctx, &mut rec.log, url).await;
                let received_utc = Utc::now();
                if reply.status == 303 {
                    let e = expires.expect("expiry known when paying");
                    let margin = chrono::Duration::milliseconds(ctx.margin);
                    rec.pay_class = Some(if received_utc + margin < e {
                        "in_time"
                    } else if sent_utc > e + margin {
                        "late"
                    } else {
                        "ambiguous"
                    });
                } else {
                    rec.pay_refused = true;
                }
                continue;
            }
        }
        if Instant::now() < next_poll {
            continue;
        }
        next_poll = Instant::now() + Duration::from_millis(poll * 9 / 10 + fastrand::u64(0..=poll / 5));

        let reply = ctx.call(&mut rec.log, Ep::Status, ctx.get(&status_path, &ip, Some(&bearer))).await;
        match reply.status {
            200 => {}
            429 => {
                ctx.waits_429.fetch_add(1, Ordering::Relaxed);
                next_poll = Instant::now() + Duration::from_millis((reply.retry_after.unwrap_or(1.0) * 1000.0) as u64 + fastrand::u64(0..=250));
                continue;
            }
            _ => continue, // network errors and 5xx are counted in the samples; poll again
        }
        let body: Value = serde_json::from_slice(&reply.body).unwrap_or_default();
        let status = body["status"].as_str().unwrap_or("").to_owned();
        match status.as_str() {
            "AWAITING_PAYMENT" if rec.link_ms.is_none() => {
                rec.link_ms = Some(first_checkout.elapsed().as_millis() as u64);
                let e: Option<DateTime<Utc>> =
                    body["expires_at"].as_str().and_then(|s| DateTime::parse_from_rfc3339(s).ok()).map(Into::into);
                let url = body["payment_url"].as_str().unwrap_or("").to_owned();
                if let (Some(e), false) = (e, url.is_empty()) {
                    expires = Some(e);
                    rec.window_left_ms = Some((e - Utc::now()).num_milliseconds());
                    if fastrand::f64() < ctx.pay_ratio {
                        let late = fastrand::f64() < ctx.late_ratio;
                        let race = !late && fastrand::f64() < ctx.race_ratio;
                        let until_expiry = (e - Utc::now()).num_milliseconds();
                        let when = if late {
                            // After the link has expired (the provider usually refuses it)
                            until_expiry + 1000
                        } else if race {
                            // Just before the expiry: the provider accepts the payment, but its
                            // webhook arrives about when the expiration worker expires the order, so either side
                            // may win (the loser is refunded)
                            until_expiry - 150
                        } else {
                            // A think time, but never closer to the expiry than 4 s
                            let think = fastrand::u64(ctx.think.0..=ctx.think.1.max(ctx.think.0)) as i64;
                            think.min((until_expiry - 4000).max(0))
                        };
                        pay_at = Some((Instant::now() + Duration::from_millis(when.max(0) as u64), url));
                    }
                } else {
                    rec.outcome = "error";
                    return rec;
                }
            }
            // Like the frontend: a user whose payment the provider accepted keeps
            // polling a while after EXPIRED, because a payment made at the deadline
            // is refunded only once the order has expired
            "EXPIRED" if rec.pay_class.is_some() && expired_since.get_or_insert_with(Instant::now).elapsed() < REFUND_GRACE => {}
            "CONFIRMED" | "EXPIRED" | "FAILED" | "REFUNDED" => {
                rec.outcome = match status.as_str() {
                    "CONFIRMED" => "confirmed",
                    "EXPIRED" => "expired",
                    "FAILED" => "failed",
                    _ => "refunded",
                };
                rec.final_status = Some(status);
                return rec;
            }
            _ => {}
        }
        last_status = status;
    }
}

/// POSTs the fake provider's pay form; the redirect is not followed
async fn pay(ctx: &Ctx, log: &mut Log, payment_url: &str) -> Reply {
    // The URL names the provider's published address; `PROVIDER_URL` replaces the host when the
    // generator reaches the provider some other way
    let path = payment_url.find("/fake-pay/").map(|i| &payment_url[i..]).unwrap_or(payment_url);
    let mut attempt = 0;
    loop {
        let request = ctx
            .client
            .post(format!("{}{path}/pay", ctx.provider))
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header("Content-Length", "0");
        let reply = ctx.call(log, Ep::Pay, request).await;
        let retryable = reply.status == 0 || reply.status >= 500 || reply.status == 429;
        if !retryable || attempt >= 5 {
            return reply;
        }
        if reply.status == 429 {
            ctx.waits_429.fetch_add(1, Ordering::Relaxed);
        } else if reply.status == 0 {
            ctx.retries_network.fetch_add(1, Ordering::Relaxed);
        } else {
            ctx.retries_5xx.fetch_add(1, Ordering::Relaxed);
        }
        sleep_ms(backoff_ms(attempt).max((reply.retry_after.unwrap_or(0.0) * 1000.0) as u64)).await;
        attempt += 1;
    }
}

fn percentile(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    sorted[((sorted.len() as f64 - 1.0) * p).round() as usize] as f64 / 1000.0
}

fn raise_fd_limit() {
    unsafe {
        let mut lim = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) == 0 {
            lim.rlim_cur = lim.rlim_max;
            libc::setrlimit(libc::RLIMIT_NOFILE, &lim);
        }
    }
}

fn stats_ms(values: &mut [u64]) -> Value {
    values.sort_unstable();
    json!({
        "count": values.len(),
        "p50": percentile(values, 0.50) * 1000.0,
        "p95": percentile(values, 0.95) * 1000.0,
        "p99": percentile(values, 0.99) * 1000.0,
        "max": values.last().copied().unwrap_or(0),
    })
}

#[tokio::main]
async fn main() {
    let var = |key: &str| std::env::var(key).unwrap_or_else(|_| panic!("{key} must be set"));
    let opt = |key: &str, default: &str| std::env::var(key).unwrap_or_else(|_| default.to_owned());
    raise_fd_limit();

    let sale: i32 = var("SALE_ID").parse().expect("SALE_ID");
    let mut users: Vec<i64> =
        std::fs::read_to_string(var("USERS_FILE")).expect("read USERS_FILE").lines().filter_map(|l| l.trim().parse().ok()).collect();
    if let Ok(limit) = std::env::var("USERS_LIMIT") {
        users.truncate(limit.parse().expect("USERS_LIMIT"));
    }
    let base = opt("BASE_URL", "http://localhost:8000").trim_end_matches('/').to_owned();
    let arrival_secs: f64 = opt("ARRIVAL_SECS", "10").parse().unwrap();
    let deadline_secs: u64 = opt("DEADLINE_SECS", "180").parse().unwrap();
    let started = Instant::now();

    let client = reqwest::Client::builder()
        .user_agent(BROWSER_UA)
        .pool_max_idle_per_host(users.len().clamp(64, 20_000))
        // Dropped before the gateway closes them (`keepalive_timeout 30s`), so a reused
        // connection is never already closed
        .pool_idle_timeout(Duration::from_secs(25))
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .tcp_nodelay(true)
        .build()
        .unwrap();

    // The opening time comes from the server, so the arrival window starts with the sale
    let details: Value = client
        .get(format!("{base}/v1/sales/{sale}"))
        .header("X-Forwarded-For", "10.255.255.254")
        .send()
        .await
        .expect("GET the sale")
        .json()
        .await
        .expect("sale details");
    let start: DateTime<Utc> = details["sale_start_time"].as_str().and_then(|s| s.parse().ok()).expect("sale_start_time");
    let until_open = (start - Utc::now()).to_std().unwrap_or(Duration::ZERO);
    let open_at = Instant::now() + until_open;

    let ctx = Arc::new(Ctx {
        client,
        base,
        provider: opt("PROVIDER_URL", "http://localhost:8090").trim_end_matches('/').to_owned(),
        sale,
        secret: var("JWT_SECRET"),
        started,
        deadline: open_at + Duration::from_secs(deadline_secs),
        arrival: Duration::from_secs_f64(arrival_secs),
        pay_ratio: opt("PAY_RATIO", "0.6").parse().unwrap(),
        late_ratio: opt("LATE_RATIO", "0.05").parse().unwrap(),
        race_ratio: opt("RACE_RATIO", "0.05").parse().unwrap(),
        poll: Duration::from_millis(opt("POLL_MS", "2000").parse().unwrap()),
        think: (opt("PAY_THINK_MIN_MS", "500").parse().unwrap(), opt("PAY_THINK_MAX_MS", "6000").parse().unwrap()),
        session_sample: opt("SESSION_SAMPLE", "0").parse().unwrap(),
        view_sale: opt("VIEW_SALE", "1") != "0",
        margin: opt("MARGIN_MS", "1500").parse().unwrap(),
        done: AtomicUsize::new(0),
        retries_5xx: AtomicU64::new(0),
        retries_network: AtomicU64::new(0),
        waits_429: AtomicU64::new(0),
    });
    println!(
        "{} users, sale {} opens in {:.1}s, arrival over {}s, pay ratio {}, poll {} ms",
        users.len(),
        sale,
        until_open.as_secs_f64(),
        arrival_secs,
        ctx.pay_ratio,
        ctx.poll.as_millis()
    );

    let total = users.len();
    let reporter = {
        let ctx = ctx.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                eprintln!("  t+{:>4.0}s  {}/{} users finished", ctx.started.elapsed().as_secs_f64(), ctx.done.load(Ordering::Relaxed), total);
            }
        })
    };
    let tasks: Vec<_> =
        users.iter().enumerate().map(|(index, &id)| tokio::spawn(user(ctx.clone(), index, id, open_at))).collect();
    let mut records = Vec::with_capacity(total);
    for task in tasks {
        records.push(task.await.unwrap_or(UserRec { outcome: "panic", ..Default::default() }));
    }
    reporter.abort();
    let elapsed = started.elapsed();

    // ---- summary ---------------------------------------------------------
    let mut outcomes: BTreeMap<&str, usize> = BTreeMap::new();
    let mut pay_matrix: BTreeMap<String, usize> = BTreeMap::new();
    let (mut in_time, mut ambiguous, mut late, mut refused) = (0, 0, 0, 0);
    let (mut in_time_not_confirmed, mut late_not_refunded, mut boundary_unresolved) = (0, 0, 0);
    let mut links = Vec::new();
    let mut window_left = Vec::new();
    for r in &records {
        *outcomes.entry(r.outcome).or_default() += 1;
        if let Some(ms) = r.link_ms {
            links.push(ms);
        }
        if let Some(w) = r.window_left_ms {
            window_left.push(w);
        }
        if r.pay_refused {
            refused += 1;
        }
        if let Some(class) = r.pay_class {
            *pay_matrix.entry(format!("{class}:{}", r.final_status.clone().unwrap_or_else(|| "none".into()))).or_default() += 1;
            match class {
                "in_time" => {
                    in_time += 1;
                    in_time_not_confirmed += (r.outcome != "confirmed") as usize;
                }
                "late" => {
                    late += 1;
                    late_not_refunded += (r.outcome != "refunded") as usize;
                }
                _ => {
                    ambiguous += 1;
                    boundary_unresolved += (r.outcome != "confirmed" && r.outcome != "refunded") as usize;
                }
            }
        }
    }

    // Every user's log, merged now that no task is writing
    let mut samples = Vec::with_capacity(records.iter().map(|r| r.log.samples.len()).sum());
    let mut error_kinds: BTreeMap<String, usize> = BTreeMap::new();
    for r in &mut records {
        samples.append(&mut r.log.samples);
        for (kind, n) in std::mem::take(&mut r.log.errors) {
            *error_kinds.entry(kind).or_default() += n;
        }
    }
    let mut endpoints = serde_json::Map::new();
    let (mut total429, mut total5xx, mut net) = (0usize, 0usize, 0usize);
    println!("\nFinished in {:.1}s\n\nOutcomes:", elapsed.as_secs_f64());
    for (outcome, count) in &outcomes {
        println!("  {outcome}: {count}");
    }
    println!("\n| endpoint | requests | peak req/s | p50 ms | p95 ms | p99 ms | max ms | responses |");
    println!("|---|---|---|---|---|---|---|---|");
    for (ep, name) in ENDPOINTS {
        let mine: Vec<&Sample> = samples.iter().filter(|s| s.ep == ep).collect();
        if mine.is_empty() {
            continue;
        }
        let mut lat: Vec<u64> = mine.iter().map(|s| s.micros).collect();
        lat.sort_unstable();
        let mut codes: BTreeMap<u16, usize> = BTreeMap::new();
        let mut per_sec: BTreeMap<u32, usize> = BTreeMap::new();
        for s in &mine {
            *codes.entry(s.status).or_default() += 1;
            *per_sec.entry(s.at_sec).or_default() += 1;
        }
        let peak = per_sec.values().copied().max().unwrap_or(0);
        total429 += codes.get(&429).copied().unwrap_or(0);
        total5xx += codes.iter().filter(|(c, _)| **c >= 500).map(|(_, n)| n).sum::<usize>();
        net += codes.get(&0).copied().unwrap_or(0);
        let shown = codes.iter().map(|(c, n)| format!("{}: {n}", if *c == 0 { "net-err".into() } else { c.to_string() })).collect::<Vec<_>>().join(", ");
        println!(
            "| {name} | {} | {peak} | {:.1} | {:.1} | {:.1} | {:.1} | {shown} |",
            mine.len(),
            percentile(&lat, 0.50),
            percentile(&lat, 0.95),
            percentile(&lat, 0.99),
            percentile(&lat, 1.0),
        );
        endpoints.insert(
            name.to_owned(),
            json!({
                "requests": mine.len(), "peak_rps": peak, "p50_ms": percentile(&lat, 0.50), "p95_ms": percentile(&lat, 0.95),
                "p99_ms": percentile(&lat, 0.99), "max_ms": percentile(&lat, 1.0),
                "statuses": codes.iter().map(|(c, n)| (c.to_string(), *n)).collect::<BTreeMap<_, _>>(),
            }),
        );
    }
    let link_stats = stats_ms(&mut links);
    let min_window_left = window_left.iter().min().copied();
    println!(
        "\nTurned away by rate limits (429): {total429}; server errors (5xx): {total5xx}; network failures: {net}\n\
         Retries: {} after 5xx, {} after network errors; {} waits on 429",
        ctx.retries_5xx.load(Ordering::Relaxed),
        ctx.retries_network.load(Ordering::Relaxed),
        ctx.waits_429.load(Ordering::Relaxed),
    );
    for (cause, n) in &error_kinds {
        println!("  network failure: {cause}: {n}");
    }
    println!(
        "Checkout to payment link: {} users, p50 {:.0} ms, p95 {:.0} ms, max {} ms; least time left to pay when seen: {} ms",
        link_stats["count"],
        link_stats["p50"].as_f64().unwrap_or(0.0),
        link_stats["p95"].as_f64().unwrap_or(0.0),
        link_stats["max"],
        min_window_left.map(|m| m.to_string()).unwrap_or_else(|| "-".into()),
    );
    println!(
        "Payments accepted by the provider: {} (surely in time {in_time}, at the boundary {ambiguous}, after expiry {late}); \
         refused by the provider: {refused}",
        in_time + ambiguous + late
    );
    println!("In-time payments not CONFIRMED: {in_time_not_confirmed}; late payments not REFUNDED: {late_not_refunded}");

    let summary = json!({
        "users": total,
        "sale": sale,
        "duration_secs": elapsed.as_secs_f64(),
        "outcomes": outcomes,
        "endpoints": endpoints,
        "rate_limited_429": total429,
        "server_errors_5xx": total5xx,
        "network_failures": net,
        "network_failure_causes": &error_kinds,
        "retries_after_5xx": ctx.retries_5xx.load(Ordering::Relaxed),
        "retries_after_network_error": ctx.retries_network.load(Ordering::Relaxed),
        "waits_on_429": ctx.waits_429.load(Ordering::Relaxed),
        "checkout_to_link_ms": link_stats,
        "min_window_left_ms": min_window_left,
        "payments": {
            "accepted": in_time + ambiguous + late,
            "in_time": in_time,
            "boundary": ambiguous,
            "late": late,
            "refused_by_provider": refused,
            "in_time_not_confirmed": in_time_not_confirmed,
            "late_not_refunded": late_not_refunded,
            "boundary_neither_confirmed_nor_refunded": boundary_unresolved,
            "by_class_and_final_status": pay_matrix,
        },
    });
    if let Ok(path) = std::env::var("REPORT_JSON") {
        std::fs::write(&path, serde_json::to_string_pretty(&summary).unwrap()).expect("write REPORT_JSON");
        println!("JSON summary written to {path}");
    }
}
