//! The same flash sale (BUYERS buyers racing for UNITS units, all at once) run
//! against five ways of decrementing stock, side by side:
//!
//! 1. `naive`        `SELECT stock`, then `UPDATE ... SET stock = <value read> - 1` in a
//!                   separate statement. Two buyers read the same value and both write
//!                   it minus one: a lost update, so more buyers are told "yes" than
//!                   there are units (overselling).
//! 2. `for_update`   `BEGIN; SELECT ... FOR UPDATE; UPDATE; COMMIT`. Correct, but every
//!                   buyer queues on one row lock: latency grows with the queue, and a
//!                   `lock_timeout` turns the tail of the queue into errors.
//! 3. `optimistic`   a `version` column: read stock and version, `UPDATE ... WHERE
//!                   version = <read>`; if no row matched, read again. Correct, but under
//!                   contention most attempts lose and retry (the retry storm).
//! 4. `conditional`  `UPDATE ... SET stock = stock - 1 WHERE id = $1 AND stock > 0`: one
//!                   statement, the database decides atomically. Correct.
//! 5. `redis_lua`    a Lua script in Redis: decrement if positive. Correct, no row lock,
//!                   no PostgreSQL.
//!
//! Each strategy gets its own table in the schema `oversell_demo` (created and dropped
//! here) or its own key in Redis database 13 under the prefix `oversell_demo:`. The
//! application's `products` table and Redis keys are never touched.
//!
//! "Latency" is what a buyer experiences: from asking for a connection (the PostgreSQL
//! strategies share a pool of PG_POOL connections, as an application would) to the answer.
//!
//! Environment: BUYERS (5000), UNITS (500), PG_POOL (50), LOCK_TIMEOUT_MS (0 = none; the
//! `for_update` strategy then reports buyers that gave up), THINK_US (0: pause between the
//! read and the write of `naive`/`optimistic`, for application work), MAX_RETRIES (200:
//! optimistic attempts before a buyer gives up), DATABASE_URL, REDIS_URL, ONLY (comma list
//! of strategy names). Prints a Markdown table.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use deadpool_postgres::{Config, Pool, PoolConfig, Runtime};
use tokio::sync::Barrier;
use tokio_postgres::error::SqlState;
use tokio_postgres::NoTls;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Answer {
    /// The buyer was told it got a unit
    Yes,
    /// The buyer was told it is sold out
    SoldOut,
    /// The buyer gave up or got an error (lock timeout, too many retries)
    Failed,
}

struct Settings {
    buyers: usize,
    units: i32,
    think: Duration,
    max_retries: u64,
    lock_timeout_ms: u64,
}

struct Report {
    name: &'static str,
    yes: usize,
    sold_out: usize,
    failed: usize,
    /// Units the data store says are gone (`units - stock`)
    taken: i64,
    elapsed: Duration,
    latencies: Vec<Duration>,
    note: String,
}

/// Runs `buyers` tasks that all start together; `buy(i)` is one buyer's whole attempt
async fn race<F, Fut>(settings: &Settings, buy: F) -> (Vec<(Answer, Duration)>, Duration)
where
    F: Fn(usize) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Answer> + Send + 'static,
{
    let buy = Arc::new(buy);
    let gate = Arc::new(Barrier::new(settings.buyers + 1));
    let tasks: Vec<_> = (0..settings.buyers)
        .map(|i| {
            let (buy, gate) = (buy.clone(), gate.clone());
            tokio::spawn(async move {
                gate.wait().await;
                let started = Instant::now();
                let answer = buy(i).await;
                (answer, started.elapsed())
            })
        })
        .collect();
    gate.wait().await;
    let started = Instant::now();
    let mut results = Vec::with_capacity(tasks.len());
    for task in tasks {
        results.push(task.await.expect("buyer task"));
    }
    (results, started.elapsed())
}

fn summarize(name: &'static str, results: Vec<(Answer, Duration)>, elapsed: Duration, taken: i64, note: String) -> Report {
    let count = |a: Answer| results.iter().filter(|(x, _)| *x == a).count();
    let mut latencies: Vec<Duration> = results.iter().map(|(_, d)| *d).collect();
    latencies.sort_unstable();
    Report { name, yes: count(Answer::Yes), sold_out: count(Answer::SoldOut), failed: count(Answer::Failed), taken, elapsed, latencies, note }
}

fn ms(sorted: &[Duration], p: f64) -> f64 {
    sorted[((sorted.len() as f64 - 1.0) * p).round() as usize].as_secs_f64() * 1000.0
}

async fn stock_of(pool: &Pool, table: &str) -> i64 {
    let c = pool.get().await.expect("connection");
    c.query_one(&format!("SELECT stock FROM oversell_demo.{table} WHERE id = 1"), &[]).await.expect("read stock").get::<_, i32>(0) as i64
}

// ---- the strategies --------------------------------------------------------

async fn naive(pool: &Pool, s: &Settings) -> Report {
    let (p, think) = (pool.clone(), s.think);
    let (results, elapsed) = race(s, move |_| {
        let p = p.clone();
        async move {
            let c = p.get().await.expect("connection");
            let stock: i32 = c.query_one("SELECT stock FROM oversell_demo.naive WHERE id = 1", &[]).await.expect("select").get(0);
            if stock <= 0 {
                return Answer::SoldOut;
            }
            if !think.is_zero() {
                tokio::time::sleep(think).await;
            }
            // The value is computed here, from what was read: the bug
            c.execute("UPDATE oversell_demo.naive SET stock = $1 WHERE id = 1", &[&(stock - 1)]).await.expect("update");
            Answer::Yes
        }
    })
    .await;
    let taken = s.units as i64 - stock_of(pool, "naive").await;
    summarize("naive SELECT then UPDATE", results, elapsed, taken, String::new())
}

async fn for_update(pool: &Pool, s: &Settings) -> Report {
    let p = pool.clone();
    let lock_timeout_ms = s.lock_timeout_ms;
    let timeouts = Arc::new(AtomicU64::new(0));
    // Time spent inside `SELECT ... FOR UPDATE` (waiting for the row lock), in microseconds
    let waited = Arc::new((AtomicU64::new(0), AtomicU64::new(0)));
    let (t, w) = (timeouts.clone(), waited.clone());
    let (results, elapsed) = race(s, move |_| {
        let (p, t, w) = (p.clone(), t.clone(), w.clone());
        async move {
            let mut c = p.get().await.expect("connection");
            let tx = c.transaction().await.expect("begin");
            if lock_timeout_ms > 0 {
                tx.batch_execute(&format!("SET LOCAL lock_timeout = '{lock_timeout_ms}ms'")).await.expect("set lock_timeout");
            }
            let locking = Instant::now();
            let locked = tx.query_one("SELECT stock FROM oversell_demo.for_update WHERE id = 1 FOR UPDATE", &[]).await;
            let waited_us = locking.elapsed().as_micros() as u64;
            w.0.fetch_add(waited_us, Ordering::Relaxed);
            w.1.fetch_max(waited_us, Ordering::Relaxed);
            let row = match locked {
                Ok(row) => row,
                Err(e) if e.code() == Some(&SqlState::LOCK_NOT_AVAILABLE) => {
                    t.fetch_add(1, Ordering::Relaxed);
                    return Answer::Failed; // dropping the transaction rolls it back
                }
                Err(e) => panic!("select for update: {e}"),
            };
            let stock: i32 = row.get(0);
            if stock <= 0 {
                tx.commit().await.expect("commit");
                return Answer::SoldOut;
            }
            tx.execute("UPDATE oversell_demo.for_update SET stock = stock - 1 WHERE id = 1", &[]).await.expect("update");
            tx.commit().await.expect("commit");
            Answer::Yes
        }
    })
    .await;
    let taken = s.units as i64 - stock_of(pool, "for_update").await;
    let mean_wait = waited.0.load(Ordering::Relaxed) as f64 / s.buyers as f64 / 1000.0;
    let max_wait = waited.1.load(Ordering::Relaxed) as f64 / 1000.0;
    let note = if lock_timeout_ms > 0 {
        format!("lock_timeout {lock_timeout_ms} ms: {} lock timeouts; row lock wait mean {mean_wait:.1} ms, max {max_wait:.0} ms", timeouts.load(Ordering::Relaxed))
    } else {
        format!("no lock_timeout; row lock wait mean {mean_wait:.1} ms, max {max_wait:.0} ms")
    };
    summarize("SELECT ... FOR UPDATE", results, elapsed, taken, note)
}

async fn optimistic(pool: &Pool, s: &Settings) -> Report {
    let (p, think, max_retries) = (pool.clone(), s.think, s.max_retries);
    let retries = Arc::new(AtomicU64::new(0));
    let r = retries.clone();
    let (results, elapsed) = race(s, move |_| {
        let (p, r) = (p.clone(), r.clone());
        async move {
            let c = p.get().await.expect("connection");
            for attempt in 0..=max_retries {
                let row = c.query_one("SELECT stock, version FROM oversell_demo.optimistic WHERE id = 1", &[]).await.expect("select");
                let (stock, version): (i32, i32) = (row.get(0), row.get(1));
                if stock <= 0 {
                    return Answer::SoldOut;
                }
                if !think.is_zero() {
                    tokio::time::sleep(think).await;
                }
                let n = c
                    .execute(
                        "UPDATE oversell_demo.optimistic SET stock = stock - 1, version = version + 1 WHERE id = 1 AND version = $1",
                        &[&version],
                    )
                    .await
                    .expect("update");
                if n == 1 {
                    return Answer::Yes;
                }
                if attempt < max_retries {
                    r.fetch_add(1, Ordering::Relaxed);
                }
            }
            Answer::Failed
        }
    })
    .await;
    let taken = s.units as i64 - stock_of(pool, "optimistic").await;
    let total = retries.load(Ordering::Relaxed);
    let note = format!("{total} retries ({:.1} per buyer), up to {} per buyer", total as f64 / s.buyers as f64, s.max_retries);
    summarize("optimistic version column", results, elapsed, taken, note)
}

async fn conditional(pool: &Pool, s: &Settings) -> Report {
    let p = pool.clone();
    let (results, elapsed) = race(s, move |_| {
        let p = p.clone();
        async move {
            let c = p.get().await.expect("connection");
            let n = c.execute("UPDATE oversell_demo.conditional SET stock = stock - 1 WHERE id = 1 AND stock > 0", &[]).await.expect("update");
            if n == 1 { Answer::Yes } else { Answer::SoldOut }
        }
    })
    .await;
    let taken = s.units as i64 - stock_of(pool, "conditional").await;
    summarize("UPDATE ... WHERE stock > 0", results, elapsed, taken, String::new())
}

const DECREMENT_IF_POSITIVE: &str = "local s = tonumber(redis.call('GET', KEYS[1])) \
    if s and s > 0 then redis.call('DECR', KEYS[1]) return 1 end return 0";

async fn redis_lua(redis_url: &str, s: &Settings) -> Report {
    let key = "oversell_demo:stock";
    let client = redis::Client::open(redis_url).expect("redis url");
    // A few multiplexed connections, as the Order Service uses
    let mut cons = Vec::new();
    for _ in 0..8 {
        cons.push(redis::aio::ConnectionManager::new(client.clone()).await.expect("redis connection"));
    }
    let mut setup = cons[0].clone();
    redis::cmd("SET").arg(key).arg(s.units).query_async::<()>(&mut setup).await.expect("set stock");
    let script = Arc::new(redis::Script::new(DECREMENT_IF_POSITIVE));
    // Load the script once so buyers only send its hash
    script.prepare_invoke().load_async(&mut setup).await.expect("load script");
    let cons = Arc::new(cons);
    let (results, elapsed) = race(s, {
        let cons = cons.clone();
        move |i| {
            let (cons, script) = (cons.clone(), script.clone());
            async move {
                let mut con = cons[i % cons.len()].clone();
                let granted: i32 = script.key(key).invoke_async(&mut con).await.expect("script");
                if granted == 1 { Answer::Yes } else { Answer::SoldOut }
            }
        }
    })
    .await;
    let left: i64 = redis::cmd("GET").arg(key).query_async(&mut setup).await.expect("get stock");
    redis::cmd("DEL").arg(key).query_async::<()>(&mut setup).await.expect("cleanup");
    summarize("Redis Lua script", results, elapsed, s.units as i64 - left, "no PostgreSQL".into())
}

fn env<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

#[tokio::main]
async fn main() {
    let settings = Settings {
        buyers: env("BUYERS", 5000),
        units: env("UNITS", 500),
        think: Duration::from_micros(env("THINK_US", 0)),
        max_retries: env("MAX_RETRIES", 200),
        lock_timeout_ms: env("LOCK_TIMEOUT_MS", 0),
    };
    let database_url = env("DATABASE_URL", "postgresql://actix:actix@127.0.0.1:5432/flash_sale".to_string());
    let redis_url = env("REDIS_URL", "redis://127.0.0.1:6379/13".to_string());
    let only: Vec<String> = env("ONLY", String::new()).split(',').filter(|s| !s.is_empty()).map(str::to_owned).collect();
    let wanted = |name: &str| only.is_empty() || only.iter().any(|o| o == name);

    let mut cfg = Config::new();
    cfg.url = Some(database_url);
    cfg.pool = Some(PoolConfig::new(env("PG_POOL", 50)));
    let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls).expect("pool");

    // Own schema, own tables; the application's tables are not touched
    {
        let c = pool.get().await.expect("connection");
        c.batch_execute("DROP SCHEMA IF EXISTS oversell_demo CASCADE; CREATE SCHEMA oversell_demo").await.expect("schema");
        for table in ["naive", "for_update", "optimistic", "conditional"] {
            c.batch_execute(&format!(
                "CREATE TABLE oversell_demo.{table} (id INT PRIMARY KEY, stock INT NOT NULL, version INT NOT NULL DEFAULT 0);
                 INSERT INTO oversell_demo.{table} (id, stock) VALUES (1, {})",
                settings.units
            ))
            .await
            .expect("table");
        }
    }

    let mut reports = Vec::new();
    // Run in a fixed order, one strategy at a time so they do not compete
    if wanted("naive") {
        reports.push(naive(&pool, &settings).await);
    }
    if wanted("for_update") {
        reports.push(for_update(&pool, &settings).await);
    }
    if wanted("optimistic") {
        reports.push(optimistic(&pool, &settings).await);
    }
    if wanted("conditional") {
        reports.push(conditional(&pool, &settings).await);
    }
    if wanted("redis_lua") {
        reports.push(redis_lua(&redis_url, &settings).await);
    }

    pool.get().await.expect("connection").batch_execute("DROP SCHEMA oversell_demo CASCADE").await.expect("drop schema");

    println!(
        "{} buyers race for {} units (PostgreSQL pool of {} connections; think time {} us)\n",
        settings.buyers,
        settings.units,
        env("PG_POOL", 50usize),
        settings.think.as_micros()
    );
    println!("| strategy | told yes | sold out | gave up | units taken in the store | oversold | seconds | buyers/s | p50 ms | p95 ms | p99 ms | notes |");
    println!("|---|---|---|---|---|---|---|---|---|---|---|---|");
    for r in &reports {
        let oversold = r.yes as i64 - settings.units as i64;
        println!(
            "| {} | {} | {} | {} | {} | {} | {:.2} | {:.0} | {:.1} | {:.1} | {:.1} | {} |",
            r.name,
            r.yes,
            r.sold_out,
            r.failed,
            r.taken,
            if oversold > 0 { format!("**{oversold} OVERSOLD**") } else { "no".into() },
            r.elapsed.as_secs_f64(),
            settings.buyers as f64 / r.elapsed.as_secs_f64(),
            ms(&r.latencies, 0.50),
            ms(&r.latencies, 0.95),
            ms(&r.latencies, 0.99),
            r.note
        );
    }
    println!();
    for r in &reports {
        // Told yes more often than the store recorded: the lost updates themselves
        if r.yes as i64 != r.taken {
            println!("{}: {} buyers were told yes but the store lost {} of their decrements", r.name, r.yes, r.yes as i64 - r.taken);
        }
    }
}
