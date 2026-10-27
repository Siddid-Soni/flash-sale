use diesel::{r2d2, PgConnection};
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};

use crate::config::Config;

pub type DbPool = r2d2::Pool<r2d2::ConnectionManager<PgConnection>>;

// Compiled into the binary, so a container needs no diesel CLI
const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

/// Applies any migrations not yet run. The API calls this at startup.
pub fn run_migrations(pool: &DbPool) {
    let mut conn = pool.get().expect("a database connection for migrations");
    let applied = conn.run_pending_migrations(MIGRATIONS).expect("migrations should apply");
    for version in applied {
        log::info!("Applied migration {version}");
    }
}

pub fn init_pool(config: &Config) -> DbPool {
    let manager = r2d2::ConnectionManager::<PgConnection>::new(&config.database_url);
    let pool = r2d2::Pool::builder()
        .max_size(config.db_pool_size)
        .build(manager)
        .expect("DATABASE_URL should point to a reachable PostgreSQL database");
    log::info!("Database pool size: {}", config.db_pool_size);
    pool
}
