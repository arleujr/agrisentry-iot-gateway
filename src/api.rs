use actix_web::{post, web, HttpResponse, Responder};
use tracing::{error, info};
use crate::db::DbClient;
use crate::models::{SensorPayload, SensorNodeMetrics};

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
/// Parses 24-hour moving window statistics directly from TimescaleDB/PostgreSQL subqueries.
pub async fn get_live_sensor_nodes(
    db_client: web::Data<DbClient>,
) -> impl Responder {
    
    // Optimized Common Table Expressions (CTE) separating analytic aggregates from chronological point logs
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
            lr.status_str as operational_status,
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
            error!("Database descriptive statistics processing matrix failure: {:?}", e);
            HttpResponse::InternalServerError().json(serde_json::json!({
                "error": "Telemetry aggregation pipeline execution failure"
            }))
        }
    }
}
