use crate::db::DbClient;
use crate::models::{SensorNodeMetrics, SensorPayload};
use actix_web::{get, post, web, HttpResponse, Responder};
use tokio::sync::mpsc::Sender; // Essential async channel
use tracing::{error, info};

/// Health check endpoint
#[get("/health")]
pub async fn health_check() -> impl Responder {
    HttpResponse::Ok().json(serde_json::json!({
        "status": "healthy",
        "service": "agrisentry-iot-gateway",
        "timestamp": chrono::Utc::now().to_rfc3339()
    }))
}

/// HTTP telemetry ingestion (for devices without MQTT)
#[post("/telemetry")]
pub async fn ingest_telemetry(
    tx: web::Data<Sender<SensorPayload>>, // Injected channel sender
    payload: web::Json<SensorPayload>,
) -> impl Responder {
    info!("Received HTTP telemetry from device: {}", payload.device_id);

    // Push telemetry into async buffer (RAM)
    match tx.send(payload.into_inner()).await {
        Ok(_) => HttpResponse::Accepted().json(serde_json::json!({
            "status": "accepted",
            "message": "Telemetry queued for background processing"
        })),
        Err(e) => {
            error!("Failed to enqueue telemetry: {:?}", e);
            HttpResponse::ServiceUnavailable().json(serde_json::json!({
                "status": "error",
                "message": "Ingestion buffer capacity reached"
            }))
        }
    }
}

/// Aggregated sensor node metrics
#[get("/nodes")]
pub async fn get_live_sensor_nodes(db_client: web::Data<DbClient>) -> impl Responder {
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

    match sqlx::query_as::<_, SensorNodeMetrics>(query)
        .fetch_all(&db_client.pool)
        .await
    {
        Ok(metrics) => HttpResponse::Ok().json(metrics),
        Err(e) => {
            error!("Failed to aggregate telemetry: {:?}", e);
            HttpResponse::InternalServerError().json(serde_json::json!({
                "error": "Telemetry aggregation failure",
                "details": format!("{:?}", e)
            }))
        }
    }
}

/// Service configuration
pub fn config_services(cfg: &mut web::ServiceConfig) {
    cfg.service(health_check);
    cfg.service(ingest_telemetry);
    cfg.service(get_live_sensor_nodes);
}

// =========================================================================
// Integration tests
// =========================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{test, App};

    #[actix_web::test]
    async fn test_health_check_endpoint_returns_200_ok() {
        let app = test::init_service(App::new().configure(config_services)).await;
        let req = test::TestRequest::get().uri("/health").to_request();
        let resp = test::call_service(&app, req).await;

        assert!(resp.status().is_success(), "Health route failed");
        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["status"], "healthy");
        assert_eq!(body["service"], "agrisentry-iot-gateway");
    }
}
