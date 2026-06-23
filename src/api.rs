use actix_web::{get, post, web, HttpResponse, Responder};
use tracing::{error, info};
use crate::db::DbClient;
use crate::models::{SensorPayload, SensorNodeMetrics};

/// REST Endpoint for infrastructure and orchestration health checks.
#[get("/health")]
pub async fn health_check() -> impl Responder {
    HttpResponse::Ok().json(serde_json::json!({
        "status": "healthy",
        "service": "agrisentry-iot-gateway",
        "timestamp": chrono::Utc::now().to_rfc3339()
    }))
}

/// HTTP Endpoint for REST telemetry ingestion.
/// Ideal for testing, integrations, or edge devices without MQTT capabilities.
#[post("/telemetry")]
pub async fn ingest_telemetry(
    db: web::Data<DbClient>,
    payload: web::Json<SensorPayload>,
) -> impl Responder {
    info!("Received HTTP telemetry from device: {}", payload.device_id);
    
    // Unwraps the JSON payload and sends it to our DB core
    match db.insert_reading(&payload.into_inner()).await {
        Ok(_) => {
            HttpResponse::Ok().json(serde_json::json!({
                "status": "success", 
                "message": "Telemetry queued as PENDING for AI analysis"
            }))
        },
        Err(e) => {
            error!("Failed to persist HTTP telemetry: {:?}", e);
            HttpResponse::InternalServerError().json(serde_json::json!({
                "status": "error", 
                "message": "Internal database error"
            }))
        }
    }
}

/// High-Performance Descriptive Statistics Aggregation Engine.
/// Refactored with strict Enterprise LEFT JOIN architectures to prevent UI starvation.
#[get("/nodes")]
pub async fn get_live_sensor_nodes(
    db_client: web::Data<DbClient>,
) -> impl Responder {
    
    // Professional LEFT JOIN Query ensuring inventory nodes persist even with empty telemetry states
    // Added explicit type casts to string text for seamless enum parsing into strong Rust types
    let query = r#"
        WITH telemetry_stats AS (
            SELECT
                sensor_id,
                MIN(value) as min_threshold,
                MAX(value) as max_threshold,
                AVG(value) as arithmetic_mean
            FROM sensor_readings
            WHERE created_at >= NOW() - INTERVAL '24 hours'
            GROUP BY sensor_id
        ),
        latest_readings AS (
            SELECT DISTINCT ON (sensor_id)
                sensor_id,
                value,
                status::text as status_str,
                created_at
            FROM sensor_readings
            ORDER BY sensor_id, created_at DESC
        )
        SELECT 
            s.hardware_id,
            s.name as sensor_name,
            s.type as sensor_type,
            lr.value as latest_reading,
            s.unit as unit_of_measurement,
            ts.min_threshold,
            ts.max_threshold,
            ts.arithmetic_mean,
            COALESCE(lr.status_str, 'PENDING') as operational_status,
            lr.created_at as last_telemetry_timestamp
        FROM sensors s
        LEFT JOIN telemetry_stats ts ON s.id = ts.sensor_id
        LEFT JOIN latest_readings lr ON s.id = lr.sensor_id
        ORDER BY s.name ASC;
    "#;

    // Maps rows efficiently to SensorNodeMetrics safely handling potential database NULL markers
    match sqlx::query_as::<_, SensorNodeMetrics>(query)
        .fetch_all(&db_client.pool)
        .await
    {
        Ok(metrics) => HttpResponse::Ok().json(metrics),
        Err(e) => {
            error!("Database descriptive statistics processing matrix failure: {:?}", e);
            HttpResponse::InternalServerError().json(serde_json::json!({
                "error": "Telemetry aggregation pipeline execution failure",
                "details": format!("{:?}", e)
            }))
        }
    }
}

/// Unified routing configuration to plug seamlessly into main.rs Actix entrypoint
pub fn config_services(cfg: &mut web::ServiceConfig) {
    cfg.service(health_check);
    cfg.service(ingest_telemetry);
    cfg.service(get_live_sensor_nodes);
}

// =========================================================================
// INTEGRATION ROUTE TESTING SUITE
// =========================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{test, App};

    #[actix_web::test]
    async fn test_health_check_endpoint_returns_200_ok() {
        // Arrange: Initialize the test server mapping our decoupled routing setup
        let app = test::init_service(
            App::new().configure(config_services)
        ).await;

        // Act: Fire a structured mock GET request targeting the health gateway
        let req = test::TestRequest::get().uri("/health").to_request();
        let resp = test::call_service(&app, req).await;

        // Assert: Ensure ecosystem robustness by validating the status code
        assert!(resp.status().is_success(), "The infrastructure health route is failing");
        
        // Assert: Parse body payload to secure JSON contract preservation
        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["status"], "healthy");
        assert_eq!(body["service"], "agrisentry-iot-gateway");
    }
}