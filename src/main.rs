use actix_web::{web, App, HttpServer};
use dotenvy::dotenv;
use std::env;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

mod db;
mod models;
mod mqtt;
mod api;
mod error; // Assuming GatewayError resides here based on your db.rs

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // 1. Bootstrapping Environment & High-Performance Logging
    dotenv().ok();
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO) // Set to DEBUG in development to see raw SQL
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to initialize tracing subscriber");

    info!("🚀 Booting AgriSentry Multi-Protocol Gateway...");

    // 2. Initialize the Unified Database Core
    let database_url = env::var("DATABASE_URL")
        .expect("CRITICAL: DATABASE_URL environment variable is missing");
    let db_client = db::DbClient::new(&database_url)
        .await
        .expect("CRITICAL: Failed to establish database connection pool");

    // 3. Spawn the MQTT Background Worker (Tokio async thread)
    let mqtt_db_client = db_client.clone();
    let mqtt_host = env::var("MQTT_HOST").unwrap_or_else(|_| "localhost".to_string());
    let mqtt_port = env::var("MQTT_PORT")
        .unwrap_or_else(|_| "1883".to_string())
        .parse::<u16>()
        .unwrap_or(1883);
    
    tokio::spawn(async move {
        mqtt::start_mqtt_worker(mqtt_db_client, &mqtt_host, mqtt_port).await;
    });

    // 4. Mount the Database state and Ignite the HTTP Server on the Main Thread
    let server_port = env::var("HTTP_PORT").unwrap_or_else(|_| "8080".to_string());
    let server_host = env::var("HTTP_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let actix_db_data = web::Data::new(db_client);

    info!("🌐 Actix-Web HTTP Server active and listening on {}:{}", server_host, server_port);
    
    HttpServer::new(move || {
        App::new()
            .app_data(actix_db_data.clone())
            .service(api::ingest_telemetry)
    })
    .bind(format!("{}:{}", server_host, server_port))?
    .run()
    .await
}