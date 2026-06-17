use actix_web::{web, App, HttpServer};
use sqlx::postgres::PgPoolOptions;
use std::env;
use tokio::signal;
use tokio::sync::watch;

mod mqtt;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Initialize standard logging subscribers
    tracing_subscriber::fmt::init();
    tracing::info!("🚀 Starting AgriSentry Enterprise IoT Gateway...");

    let database_url = env::var("DATABASE_URL")
        .expect("CRITICAL: DATABASE_URL environment variable must be set");

    // Establish the high-performance connection pool
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&database_url)
        .await
        .expect("CRITICAL: Failed to establish PostgreSQL connection pool");

    // Initialize the Graceful Shutdown state transmission channel
    // false = running, true = shutting down broadcast
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Initialize MQTT configuration variables from environment with safe local defaults
    let mqtt_host = env::var("MQTT_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let mqtt_port: u16 = env::var("MQTT_PORT")
        .unwrap_or_else(|_| "1883".to_string())
        .parse()
        .expect("MQTT_PORT must be a valid u16 integer");

    // Spawn the MQTT Background Worker task concurrently
    let mqtt_pool = pool.clone();
    let mqtt_handle = tokio::spawn(async move {
        // Passing the 4 required parameters to match the fixed src/mqtt.rs signature
        mqtt::start_mqtt_worker(mqtt_pool, &mqtt_host, mqtt_port, shutdown_rx).await;
    });

    // Create a clone specifically for the server to avoid ownership errors
    let pool_for_server = pool.clone();

    // Build and prepare the Actix-Web Server engine instance
    let server = HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(pool_for_server.clone()))
            // Define production REST API modules here (e.g., .service(ingest_telemetry))
    })
    .bind(("0.0.0.0", 8080))?
    .run();

    // Extract the server runtime handle before running to control its lifecycle down the road
    let server_handle = server.handle();

    // Concurrent OS termination signaling listeners setup
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install SIGINT handler");
    };

    #[cfg(unix)]
    let sigterm = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let sigterm = std::future::pending::<()>();

    // Race the active server future against the termination signals listener matrix
    tokio::select! {
        _ = server => {
            tracing::warn!("HTTP Server workflow terminated unexpectedly on its own.");
        }
        _ = ctrl_c => {
            tracing::warn!("⛔ Received SIGINT (Ctrl+C). Initiating Graceful Shutdown...");
        }
        _ = sigterm => {
            tracing::warn!("🐳 Received SIGTERM (Docker/K8s). Initiating Graceful Shutdown...");
        }
    }

    // PHASE 1: Broadcast execution termination to the streaming MQTT background workers
    tracing::info!("Phase 1: Broadcasting shutdown token to background workers...");
    let _ = shutdown_tx.send(true);

    // PHASE 2: Shut down the server listeners gracefully
    tracing::info!("Phase 2: Draining in-flight HTTP streams and stopping Actix-Web engine...");
    server_handle.stop(true).await;

    // PHASE 3: Wait for the background Tokio task loops to yield and finish clean connection drops
    tracing::info!("Phase 3: Awaiting MQTT worker thread resource cleanup...");
    if let Err(e) = mqtt_handle.await {
        tracing::error!("MQTT Task experienced unhandled panic during close routine: {:?}", e);
    }

    // PHASE 4: Close the core database connection pool explicitly
    tracing::info!("Phase 4: Flashing caches and closing PostgreSQL connection pool safely...");
    pool.close().await;

    tracing::info!("🎉 Graceful Shutdown sequence finalized successfully. Process exiting.");
    Ok(())
}