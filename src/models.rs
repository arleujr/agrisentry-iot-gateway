use axum::{extract::Extension, http::StatusCode, Json};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool, Type};

// =========================================================================
// DATA MODELS & DTOS
// =========================================================================

/// Represents a simple telemetry data point (used in some contexts)
#[derive(Debug, Deserialize)]
pub struct TelemetryData {
    pub value: f64,
}

/// Represents a rule retrieved from the database
#[derive(Debug, Deserialize)]
pub struct RuleFromDb {
    pub trigger_condition: String,
    pub trigger_value: f64,
    pub action_type: String,
}

/// Represents a sensor payload received via HTTP ingestion
#[derive(Debug, Deserialize)]
pub struct SensorPayload {
    pub device_id: String,
    pub sensor_type: String,
    pub reading_value: f64,
    pub timestamp: DateTime<Utc>,
    pub metadata_hash: Option<String>,
}

/// Enum mapping to PostgreSQL custom type `dataqualitystatus`
/// Used to track the quality status of telemetry readings.
/// Fixed to follow Rust's PascalCase naming conventions while persisting as SCREAMING_SNAKE_CASE.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Type)]
#[sqlx(type_name = "dataqualitystatus", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DataQualityStatus {
    Pending,
    Valid,
    AnomalyNoise,
    AnomalyCritical,
}

/// Represents the JSON payload sent by sensors via MQTT
/// This is deserialized directly from the MQTT message body
#[derive(Debug, Deserialize)]
pub struct MqttPayload {
    pub value: f64,
    pub timestamp: DateTime<Utc>,
}

/// Comprehensive tracking model mapping real-time operational database states to UI metrics components
#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct SensorNodeMetrics {
    #[sqlx(rename = "hardware_id")]
    pub sensor_id: String,
    #[sqlx(rename = "sensor_name")]
    pub name: String,
    #[sqlx(rename = "sensor_type")]
    pub r#type: String, // Maps to sensor classifications like 'humidity', 'temperature', etc.
    pub latest_reading: Option<f64>,
    pub unit_of_measurement: String,
    pub min_threshold: Option<f64>,
    pub max_threshold: Option<f64>,
    pub arithmetic_mean: Option<f64>,
    pub operational_status: String, // Evaluation state string format representing "VALID" or "ANOMALY" flags
    pub last_telemetry_timestamp: Option<DateTime<Utc>>,
}

// =========================================================================
// API ROUTE HANDLERS
// =========================================================================

/// Axum API Endpoint handler parsing distributed field node states straight from Postgres SQL query calculations
pub async fn get_live_sensor_nodes(
    Extension(pool): Extension<PgPool>,
) -> Result<Json<Vec<SensorNodeMetrics>>, (StatusCode, String)> {
    
    // High-performance SQL Query using Common Table Expressions (CTE) to isolate historical window aggregates from latest snapshot lookups
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
                status,
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
            lr.status as operational_status,
            lr.created_at as last_telemetry_timestamp
        FROM sensors s
        LEFT JOIN telemetry_stats ts ON s.id = ts.sensor_id
        LEFT JOIN latest_readings lr ON s.id = lr.sensor_id
        ORDER BY s.name ASC;
    "#;

    let metrics = sqlx::query_as::<_, SensorNodeMetrics>(query)
        .fetch_all(&pool)
        .await
        .map_err(|e| {
            tracing::error!("Database aggregation telemetry failure: {:?}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, "Internal Data Engine Error".to_string())
        })?;

    Ok(Json(metrics))
}
