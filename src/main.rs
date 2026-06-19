use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use sqlx::postgres::PgPoolOptions;

use std::env;
use std::time::Duration;
use tokio::signal;
use tokio::sync::watch;

mod models;
mod engine;
use crate::engine::mqtt;
mod error;
mod api;
mod db;

async fn health_check() -> impl Responder {
    HttpResponse::Ok().json(serde_json::json!({
        "status": "healthy",
        "service": "agrisentry-iot-gateway"
    }))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt::init();
    tracing::info!("🚀 Starting AgriSentry Enterprise IoT Gateway...");

    let database_url = env::var("DATABASE_URL")
        .expect("CRITICAL: DATABASE_URL environment variable must be set");

    // Establish high-performance PostgreSQL connection pool
    let pool = {
        let mut attempts = 0;
        let max_attempts = 10;
        let mut established_pool = None;

        let mut connect_options: sqlx::postgres::PgConnectOptions = database_url
            .parse()
            .expect("CRITICAL: Failed to parse DATABASE_URL configuration matrix");

        connect_options = connect_options.statement_cache_capacity(0);

        while attempts < max_attempts {
            tracing::info!("Connecting to database (Attempt {}/{})...", attempts + 1, max_attempts);

            match PgPoolOptions::new()
                .max_connections(20)
                .acquire_timeout(Duration::from_secs(10))
                .connect_with(connect_options.clone())
                .await
            {
                Ok(p) => {
                    established_pool = Some(p);
                    break;
                }
                Err(e) => {
                    attempts += 1;
                    tracing::error!("Database connection failed: {}. Retrying in 5 seconds...", e);
                    if attempts >= max_attempts {
                        break;
                    }
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }

        established_pool.expect("CRITICAL: Failed to establish PostgreSQL connection pool after maximum retry ceiling")
    };

    // Create DbClient and inject into Actix
    let db_client = web::Data::new(crate::db::DbClient::new(pool.clone()));

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let mqtt_host = env::var("MQTT_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let mqtt_port: u16 = env::var("MQTT_PORT")
        .unwrap_or_else(|_| "1883".to_string())
        .parse()
        .expect("MQTT_PORT must be a valid u16 integer");

    // Spawn MQTT worker
    let mqtt_pool = pool.clone();
    let mqtt_handle = tokio::spawn(async move {
        mqtt::start_mqtt_worker(mqtt_pool, &mqtt_host, mqtt_port, shutdown_rx).await;
    });

    // 🧠 Spawn Analysis Worker (Enterprise version integrated with Python AI API)
    let analysis_pool = pool.clone();
    let mut analysis_shutdown_rx = shutdown_tx.subscribe();

    // Pull AI API URL from environment variable
    let ai_api_url = env::var("AI_API_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:8000/v1/analyze".to_string());

    let analysis_handle = tokio::spawn(async move {
        tracing::info!("🧠 AI & Rule Analysis Background Worker started in production mode.");
        let db_client = crate::db::DbClient::new(analysis_pool);

        // Initialize reusable HTTP client
        let http_client = reqwest::Client::new();

        loop {
            tokio::select! {
                // Monitor shutdown signal
                res = analysis_shutdown_rx.changed() => {
                    if res.is_ok() && *analysis_shutdown_rx.borrow() {
                        tracing::warn!("Analysis Worker caught termination sequence. Exiting safely...");
                        break;
                    }
                }
                // Execute every 5 seconds
                _ = tokio::time::sleep(Duration::from_secs(5)) => {
                    match db_client.fetch_pending_readings(100).await {
                        Ok(readings) if !readings.is_empty() => {
                            tracing::info!("Pulling batch of {} PENDING rows to send to AI...", readings.len());

                            // Map DB rows to JSON payload expected by FastAPI
                            let telemetry_readings: Vec<serde_json::Value> = readings
                                .iter()
                                .map(|(id, value, created_at)| {
                                    serde_json::json!({
                                        "id": id,
                                        "value": value,
                                        "created_at": created_at
                                    })
                                })
                                .collect();

                            let payload = serde_json::json!({ "readings": telemetry_readings });

                            // Dispatch batch via HTTP POST to Python microservice
                            match http_client.post(&ai_api_url).json(&payload).send().await {
                                Ok(response) => {
                                    if response.status().is_success() {
                                        if let Ok(ai_response) = response.json::<serde_json::Value>().await {
                                            if let Some(results) = ai_response.get("results").and_then(|r| r.as_array()) {
                                                for result in results {
                                                    if let (Some(id_str), Some(created_at_str), Some(status), Some(note)) = (
                                                        result.get("id").and_then(|v| v.as_str()),
                                                        result.get("created_at").and_then(|v| v.as_str()),
                                                        result.get("status").and_then(|v| v.as_str()),
                                                        result.get("note").and_then(|v| v.as_str()),
                                                    ) {
                                                        if let (Ok(id), Ok(created_at)) = (
                                                            uuid::Uuid::parse_str(id_str),
                                                            chrono::DateTime::parse_from_rfc3339(created_at_str)
                                                        ) {
                                                            let created_at_utc = chrono::DateTime::<chrono::Utc>::from(created_at);

                                                            if let Err(err) = db_client.update_reading_status(id, created_at_utc, status, note).await {
                                                                tracing::error!("Failed to update DB for ID {:?}: {:?}", id, err);
                                                            }
                                                        }
                                                    }
                                                }
                                                tracing::info!("🚀 Batch of {} telemetry readings processed and classified successfully by AI.", results.len());
                                            }
                                        }
                                    } else {
                                        tracing::error!("FastAPI service returned error status: {:?}", response.status());
                                    }
                                }
                                Err(e) => {
                                    tracing::error!("Critical network failure communicating with AI microservice: {:?}", e);
                                }
                            }
                        }
                        Ok(_) => {} // No pending data, skip cycle
                        Err(e) => tracing::error!("Error in Analysis Worker pipeline: {:?}", e),
                    }
                }
            }
        }
        tracing::info!("🏁 Analysis Worker fully terminated.");
    });

    // Configure HTTP server
    let server = HttpServer::new(move || {
        App::new()
            .app_data(db_client.clone())
            .service(crate::api::ingest_telemetry)
            .route("/", web::get().to(health_check))
            .route("/health", web::get().to(health_check))
    })
    .bind(("0.0.0.0", 8080))?
    .run();

    let server_handle = server.handle();

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

    tracing::info!("Phase 1: Broadcasting shutdown token to background workers...");
    let _ = shutdown_tx.send(true);

    tracing::info!("Phase 2: Draining in-flight HTTP streams and stopping Actix-Web engine...");
    server_handle.stop(true).await;

    tracing::info!("Phase 3: Awaiting Background Workers resource cleanup...");
    if let Err(e) = mqtt_handle.await {
        tracing::error!("MQTT Task experienced unhandled panic during close routine: {:?}", e);
    }
    if let Err(e) = analysis_handle.await {
        tracing::error!("Analysis Task experienced unhandled panic during close routine: {:?}", e);
    }

    tracing::info!("Phase 4: Flashing caches and closing PostgreSQL connection pool safely...");
    pool.close().await;

    tracing::info!("🎉 Graceful Shutdown sequence finalized successfully. Process exiting.");
    Ok(())
}
