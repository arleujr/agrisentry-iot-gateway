use actix_cors::Cors; // CORS Middleware to allow the Vue frontend to connect
use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use sqlx::postgres::PgPoolOptions;
use sqlx::Row; // Required for dynamic query field extraction (.get)

use std::env;
use std::time::Duration;
use tokio::signal;
use tokio::sync::watch;

// Bring the unified library modules into the binary scope
use agrisentry_iot_gateway::{api, db, engine, models};
use engine::mqtt;

/// Helper function to safely map Python's API string responses to Rust's strong types
fn status_from_str(status: &str) -> Option<models::DataQualityStatus> {
    match status {
        "PENDING" => Some(models::DataQualityStatus::Pending),
        "VALID" => Some(models::DataQualityStatus::Valid),
        "ANOMALY_NOISE" => Some(models::DataQualityStatus::AnomalyNoise),
        "ANOMALY_CRITICAL" => Some(models::DataQualityStatus::AnomalyCritical),
        _ => None,
    }
}

/// Active health check endpoint for cluster orchestration and monitoring liveness probes
async fn health_check() -> impl Responder {
    HttpResponse::Ok().json(serde_json::json!({
        "status": "healthy",
        "service": "agrisentry-iot-gateway"
    }))
}

/// Real-time metrics aggregator query exposing telemetry and data quality KPI metrics for dashboards
async fn get_dashboard_metrics(db_client: web::Data<db::DbClient>) -> impl Responder {
    // Switched to dynamic sqlx::query to avoid compile-time environment requirements during cargo test
    match sqlx::query(
        r#"
        SELECT 
            status::text AS status,
            COUNT(*) as count
        FROM "sensor_readings"
        WHERE created_at > NOW() - INTERVAL '24 hours'
        GROUP BY status;
        "#,
    )
    .fetch_all(&db_client.pool)
    .await
    {
        Ok(records) => {
            let metrics: Vec<serde_json::Value> = records
                .into_iter()
                .map(|rec| {
                    let status_str: String = rec.get("status");
                    let count: i64 = rec.get("count");
                    serde_json::json!({
                        "status": status_str,
                        "count": count
                    })
                })
                .collect();
            HttpResponse::Ok().json(serde_json::json!({ "timeframe": "24h", "metrics": metrics }))
        }
        Err(e) => {
            tracing::error!("Database analytics aggregation failed: {:?}", e);
            HttpResponse::InternalServerError()
                .json(serde_json::json!({ "error": "Analytics execution failure" }))
        }
    }
}

/// Retrieve latest system logs for the Vue Dashboard
async fn get_system_logs(db_client: web::Data<db::DbClient>) -> impl Responder {
    // Switched to dynamic sqlx::query to allow isolated integration tests execution safely
    match sqlx::query(
        r#"SELECT component, message, level, created_at FROM "system_events" ORDER BY id DESC LIMIT 25"#
    )
    .fetch_all(&db_client.pool)
    .await {
        Ok(rows) => {
            let logs: Vec<serde_json::Value> = rows.into_iter().map(|r| {
                let component: String = r.get("component");
                let message: String = r.get("message");
                let level: String = r.get("level");
                let created_at: chrono::DateTime<chrono::Utc> = r.get("created_at");
                serde_json::json!({
                    "component": component,
                    "message": message,
                    "level": level,
                    "created_at": created_at
                })
            }).collect();
            HttpResponse::Ok().json(logs)
        }
        Err(e) => {
            tracing::error!("Failed to fetch system logs: {:?}", e);
            HttpResponse::InternalServerError().finish()
        }
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Initialize enterprise structural tracing subscriber framework
    tracing_subscriber::fmt::init();
    tracing::info!("🚀 Starting AgriSentry Enterprise IoT Gateway engine...");

    let database_url =
        env::var("DATABASE_URL").expect("CRITICAL: DATABASE_URL environment variable must be set");

    // Establish high-performance, resilient PostgreSQL/TimescaleDB connection pool
    let pool = {
        let mut attempts = 0;
        let max_attempts = 10;
        let mut established_pool = None;

        let mut connect_options: sqlx::postgres::PgConnectOptions = database_url
            .parse()
            .expect("CRITICAL: Failed to parse DATABASE_URL configuration matrix");

        // Disable client-side statement caching to maximize TimescaleDB distributed compatibility
        connect_options = connect_options.statement_cache_capacity(0);

        while attempts < max_attempts {
            tracing::info!(
                "Connecting to database repository (Attempt {}/{})...",
                attempts + 1,
                max_attempts
            );

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
                    tracing::error!(
                        "Database connectivity handshake failed: {}. Retrying in 5 seconds...",
                        e
                    );
                    if attempts >= max_attempts {
                        break;
                    }
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }

        established_pool.expect(
            "CRITICAL: Failed to establish PostgreSQL connection pool after maximum retry ceiling",
        )
    };

    // Instantiate thread-safe database client and inject into Actix ecosystem shared state
    let db_client = web::Data::new(db::DbClient::new(pool.clone()));

    // Broadcast channel to safely orchestrate graceful shutdown sequences across background tasks
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let mqtt_host = env::var("MQTT_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let mqtt_port: u16 = env::var("MQTT_PORT")
        .unwrap_or_else(|_| "8883".to_string()) // Updated default to 8883 for secure connections
        .parse()
        .expect("MQTT_PORT must be a valid u16 integer");

    // Spawn async worker dedicated to low-latency MQTT message streaming ingestion
    let mqtt_pool = pool.clone();
    let mqtt_handle = tokio::spawn(async move {
        mqtt::start_mqtt_worker(mqtt_pool, &mqtt_host, mqtt_port, shutdown_rx).await;
    });

    // 🧠 Spawn Analysis Worker (Enterprise pipeline integrated with FastAPI AI microservice)
    let analysis_pool = pool.clone();
    let mut analysis_shutdown_rx = shutdown_tx.subscribe();

    let ai_api_url =
        env::var("AI_API_URL").unwrap_or_else(|_| "http://127.0.0.1:8000/v1/analyze".to_string());

    let analysis_handle = tokio::spawn(async move {
        tracing::info!("🧠 AI & Rule Analysis Background Worker started in production mode.");
        let db_worker_client = db::DbClient::new(analysis_pool);
        let http_client = reqwest::Client::new();

        loop {
            tokio::select! {
                res = analysis_shutdown_rx.changed() => {
                    if res.is_ok() && *analysis_shutdown_rx.borrow() {
                        tracing::warn!("Analysis Background Worker caught termination sequence. Exiting lifecycle cleanly...");
                        break;
                    }
                }
                _ = tokio::time::sleep(Duration::from_secs(5)) => {
                    match db_worker_client.fetch_pending_readings(100).await {
                        Ok(readings) if !readings.is_empty() => {
                            let batch_size = readings.len();
                            tracing::info!("Extracting batch of {} PENDING telemetry records for AI analytics evaluation...", batch_size);

                            // Log extraction phase to persistent storage
                            let extract_msg = format!("Extracted batch of {} PENDING records for AI evaluation.", batch_size);
                            if let Err(log_err) = db_worker_client.insert_system_log("RUST_CORE", "INFO", &extract_msg).await {
                                tracing::error!("Failed to write extraction log to db: {:?}", log_err);
                            }

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

                            match http_client.post(&ai_api_url).json(&payload).send().await {
                                Ok(response) => {
                                    if response.status().is_success() {
                                        if let Ok(ai_response) = response.json::<serde_json::Value>().await {
                                            if let Some(results) = ai_response.get("results").and_then(|r| r.as_array()) {
                                                for result in results {
                                                    if let (Some(id_str), Some(created_at_str), Some(status_str), Some(note)) = (
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

                                                            if let Some(status_enum) = status_from_str(status_str) {
                                                                if let Err(err) = db_worker_client.update_reading_status(id, created_at_utc, status_enum, note).await {
                                                                    tracing::error!("Failed to persist AI classification update for ID {:?}: {:?}", id, err);
                                                                }
                                                            } else {
                                                                tracing::error!("Received unmappable data quality payload status tag from AI microservice: {}", status_str);
                                                            }
                                                        }
                                                    }
                                                }
                                                tracing::info!("🚀 Batch of {} telemetry readings analyzed, classified, and committed successfully by AI runtime.", results.len());

                                                // Log successful analysis phase to persistent storage
                                                let success_msg = format!("Batch of {} telemetry readings classified by AI runtime.", results.len());
                                                if let Err(log_err) = db_worker_client.insert_system_log("AI_ENGINE", "INFO", &success_msg).await {
                                                    tracing::error!("Failed to write runtime success log to db: {:?}", log_err);
                                                }
                                            }
                                        }
                                    } else {
                                        tracing::error!("FastAPI inference microservice returned unhandled error status code: {:?}", response.status());
                                    }
                                }
                                Err(e) => {
                                    tracing::error!("Critical network transport failure communicating with AI microservice: {:?}", e);

                                    // Log critical transport failure phase to persistent storage
                                    if let Err(log_err) = db_worker_client.insert_system_log("RUST_CORE", "CRITICAL", "Network transport failure communicating with AI microservice.").await {
                                        tracing::error!("Failed to write runtime crash log to db: {:?}", log_err);
                                    }
                                }
                            }
                        }
                        Ok(_) => {}
                        Err(e) => tracing::error!("Error intercepted inside the Analysis Worker execution pipeline: {:?}", e),
                    }
                }
            }
        }
        tracing::info!("🏁 Analysis Background Worker fully decoupled and terminated.");
    });

    // =====================================================================
    // 🛡️ HIGH-PERFORMANCE IN-MEMORY BUFFER (PRODUCER-CONSUMER PATTERN)
    // =====================================================================
    tracing::info!("📦 Initializing MPSC in-memory buffer for high-throughput telemetry stream...");
    
    // Initialize an asynchronous channel with a capacity of 1000 queued readings
    // This acts as a shock-absorber during burst telemetry transmission, preventing database connection exhaustion.
    let (telemetry_tx, mut telemetry_rx) = tokio::sync::mpsc::channel::<models::SensorPayload>(1000);
    
    // Isolate a database client instance specifically for the background consumer thread
    let consumer_db_client = db::DbClient::new(pool.clone());
    
    // Spawn the Consumer (Background Worker) to process the queue sequentially
    tokio::spawn(async move {
        tracing::info!("👷 MPSC Consumer worker successfully deployed and running in background...");
        
        // Perpetually await incoming payloads from the channel and execute database inserts
        while let Some(payload) = telemetry_rx.recv().await {
            if let Err(e) = consumer_db_client.insert_reading(&payload).await {
                tracing::error!("MPSC Consumer failed to persist reading to database repository: {:?}", e);
            }
        }
    });

    // Wrap the Producer (TX) side to inject into the Actix-Web shared application state
    let tx_data = web::Data::new(telemetry_tx);

    let port: u16 = env::var("PORT")
        .unwrap_or_else(|_| "8080".to_string())
        .parse()
        .expect("PORT must be a valid u16 integer");

    // Configure and bind runtime HTTP Rest Server instance
    let server = HttpServer::new(move || {
        // Global CORS configuration allowing all connections (Suitable for development/dashboards)
        let cors = Cors::default()
            .allow_any_origin()
            .allow_any_method()
            .allow_any_header()
            .max_age(3600);

        App::new()
            .wrap(cors)
            .app_data(db_client.clone())
            .app_data(tx_data.clone()) // Inject the MPSC Transmitter into the application scope
            // 🌐 API V1 Scope - All Dashboard and Telemetry routes live here
            .service(
                web::scope("/api/v1")
                    .service(api::ingest_telemetry)
                    .route("/dashboard/metrics", web::get().to(get_dashboard_metrics))
                    .route("/dashboard/logs", web::get().to(get_system_logs))
                    // Fixed: Registered via .service() because get_live_sensor_nodes uses an Actix attribute macro inside api.rs
                    .service(api::get_live_sensor_nodes),
            )
            // Global monitoring routes
            .route("/", web::get().to(health_check))
            .route("/health", web::get().to(health_check))
    })
    .bind(("0.0.0.0", port))?
    .run();

    let server_handle = server.handle();

    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install SIGINT OS signal handler listener");
    };

    #[cfg(unix)]
    let sigterm = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM OS signal handler listener")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let sigterm = std::future::pending::<()>();

    tokio::select! {
        _ = server => {
            tracing::warn!("HTTP Runtime Server execution context stopped unexpectedly on its own loop execution.");
        }
        _ = ctrl_c => {
            tracing::warn!("⛔ Received SIGINT (Ctrl+C). Triggering orchestrated Graceful Shutdown protocol...");
        }
        _ = sigterm => {
            tracing::warn!("🐳 Received SIGTERM (Orchestration Engine). Triggering orchestrated Graceful Shutdown protocol...");
        }
    }

    tracing::info!(
        "Phase 1: Broadcasting shutdown interruption token to asynchronous core workers..."
    );
    let _ = shutdown_tx.send(true);

    tracing::info!(
        "Phase 2: Draining active in-flight networking sockets and halting Actix engine..."
    );
    server_handle.stop(true).await;

    tracing::info!("Phase 3: Synchronizing and awaiting structural background tasks hardware resources release...");
    if let Err(e) = mqtt_handle.await {
        tracing::error!(
            "MQTT Processing loop crashed during exit execution sequence: {:?}",
            e
        );
    }
    if let Err(e) = analysis_handle.await {
        tracing::error!(
            "Analysis Processing loop crashed during exit execution sequence: {:?}",
            e
        );
    }

    tracing::info!(
        "Phase 4: Flashing memory caches and disconnecting PostgreSQL master pool safely..."
    );
    pool.close().await;

    tracing::info!("🎉 Graceful Shutdown routing finalized cleanly. AgriSentry core microservice safely closed.");
    Ok(())
}
