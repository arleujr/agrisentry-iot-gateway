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

    // Estabelece o pool de conexões de alta performance
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

    // Cria o DbClient e injeta no Actix
    let db_client = web::Data::new(crate::db::DbClient::new(pool.clone()));

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let mqtt_host = env::var("MQTT_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let mqtt_port: u16 = env::var("MQTT_PORT")
        .unwrap_or_else(|_| "1883".to_string())
        .parse()
        .expect("MQTT_PORT must be a valid u16 integer");

    let mqtt_pool = pool.clone();
    let mqtt_handle = tokio::spawn(async move {
        mqtt::start_mqtt_worker(mqtt_pool, &mqtt_host, mqtt_port, shutdown_rx).await;
    });

    let server = HttpServer::new(move || {
        App::new()
            .app_data(db_client.clone()) // injeta o banco
            .service(crate::api::ingest_telemetry) // registra rota do api.rs
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

    tracing::info!("Phase 3: Awaiting MQTT worker thread resource cleanup...");
    if let Err(e) = mqtt_handle.await {
        tracing::error!("MQTT Task experienced unhandled panic during close routine: {:?}", e);
    }

    tracing::info!("Phase 4: Flashing caches and closing PostgreSQL connection pool safely...");
    pool.close().await;

    tracing::info!("🎉 Graceful Shutdown sequence finalized successfully. Process exiting.");
    Ok(())
}
