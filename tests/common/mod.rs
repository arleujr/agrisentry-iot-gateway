// tests/common/mod.rs

use agrisentry_iot_gateway::db::DbClient;
use sqlx::postgres::PgPoolOptions;

/// Shared fixture mimicking Python's conftest.py behavior.
/// Initializes a lazy database connection pool for isolated integration tests.
pub async fn setup_test_context() -> DbClient {
    println!("[TEST SETUP] Initializing lazy testing client database context...");

    // connect_lazy instantiates the pool structurally without triggering a real network handshake,
    // making it perfect for cloud environments like GitHub Codespaces during CI/CD execution.
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://localhost/agrisentry_test")
        .expect("Failed to create lazy test database pool infrastructure");

    DbClient::new(pool)
}
