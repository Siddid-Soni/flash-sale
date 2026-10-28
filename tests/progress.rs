//! Processor progress (`set_max.lua`, `cache::record_progress`) against a real
//! Redis, database 15 (`TEST_REDIS_URL`).

use redis::AsyncCommands;

use flash_sale::cache::{self, RedisConn};

async fn connect() -> RedisConn {
    dotenvy::dotenv().ok();
    let url = std::env::var("TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/15".into());
    let client = redis::Client::open(url).unwrap();
    redis::aio::ConnectionManager::new(client).await.expect("Redis should be running for these tests")
}

#[tokio::test]
async fn raise_to_never_lowers() {
    let mut con = connect().await;
    let key = format!("test:set_max:{}", uuid::Uuid::new_v4());
    assert!(cache::raise_to(&mut con, &key, 100).await.unwrap());
    assert!(!cache::raise_to(&mut con, &key, 90).await.unwrap());
    assert!(!cache::raise_to(&mut con, &key, 100).await.unwrap());
    assert!(cache::raise_to(&mut con, &key, 101).await.unwrap());
    assert_eq!(con.get::<_, i64>(&key).await.unwrap(), 101);
    let _: () = con.del(&key).await.unwrap();
}

/// The queue-position progress never goes backwards
#[tokio::test]
async fn recorded_progress_only_rises() {
    let con = connect().await;
    let mut read_cache = cache::ReadCache(con);
    let topic = format!("test.progress.{}", uuid::Uuid::new_v4().simple());

    cache::record_progress(&mut read_cache, &topic, 2, 10).await.unwrap();
    cache::record_progress(&mut read_cache, &topic, 2, 12).await.unwrap();
    assert_eq!(cache::read_progress(&mut read_cache, &topic, 2).await.unwrap(), Some(12));
    // A stale write (e.g. a previous owner of the partition) changes nothing
    cache::record_progress(&mut read_cache, &topic, 2, 8).await.unwrap();
    assert_eq!(cache::read_progress(&mut read_cache, &topic, 2).await.unwrap(), Some(12));
    assert_eq!(cache::read_progress(&mut read_cache, &topic, 1).await.unwrap(), None);

    let _: () = read_cache.0.del(cache::progress_key(&topic, 2)).await.unwrap();
}
