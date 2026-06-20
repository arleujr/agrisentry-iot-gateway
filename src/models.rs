use axum::{extract::Extension, http::StatusCode, Json};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool, Type};

// =========================================================================
// DATA MODELS & DTOS (INDUSTRIAL ENTERPRISE STANDARD)
// =========================================================================

/// Represents a simple telemetry data point used in minimal state evaluations
#[derive(Debug, Deserialize)]
pub struct TelemetryData {
    pub value: f64,
}

/// Represents an operational rule threshold checklist retrieved from the persistence layer
#[derive(Debug, Deserialize)]
pub struct RuleFromDb {
    pub trigger_condition: String,
    pub trigger_value: f64,
    pub action_type: String,
}

/// Represents an edge sensor payload ingested via the HTTP REST telemetry API pipeline
#[derive(Debug, Deserialize)]
pub struct SensorPayload {
    pub device_id: String,
    pub sensor_type: String,
    pub reading_value: f64,
    pub timestamp: DateTime<Utc>,
    pub metadata_hash: Option<String>,
}

/// Native PostgreSQL Enum mapping explicitly to `dataqualitystatus` custom type.
/// Governs the pipeline classification states computed by the Rust Core and AI Models.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Type, Serialize, Deserialize)]
#[sqlx(type_name = "dataqualitystatus", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DataQualityStatus {
    Pending,
    Valid,
    AnomalyNoise,
    AnomalyCritical,
}

/// Represents the raw payload structure transmitted by field hardware microcontrollers via MQTT brokers
#[derive(Debug, Deserialize)]
pub struct MqttPayload {
    pub value: f64,
    pub timestamp: DateTime<Utc>,
}

/// High-throughput statistics reporting structure that maps historical aggregates directly to UI metrics components
#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct SensorNodeMetrics {
    #[sqlx(rename = "hardware_id")]
    pub sensor_id: String,
    #[sqlx(rename = "sensor_name")]
    pub name: String,
    #[sqlx(rename = "sensor_type")]
    pub r#type: String, // Architectural categorization: 'humidity', 'temperature', etc.
    pub latest_reading: Option<f64>,
    pub unit_of_measurement: String,
    pub min_threshold: Option<f64>,
    pub max_threshold: Option<f64>,
    pub arithmetic_mean: Option<f64>,
    pub operational_status: Option<String>, // Direct state string representation matching current data node status
    pub last_telemetry_timestamp: Option<DateTime<Utc>>,
}

// =========================================================================
// API ROUTE HANDLERS
// =========================================================================

/// High-Performance Descriptive Statistics Aggregation Engine.
/// Generates 24-hour moving statistical metrics window aggregates (MIN, MAX, AVG)
/// combined with atomic-level instant snapshots of active sensor hardware nodes.
pub async fn get_live_sensor_nodes(
    Extension(pool): Extension<PgPool>,
) -> Result<Json<Vec<SensorNodeMetrics>>, (StatusCode, String)> {
    
    // Optimized Common Table Expressions (CTE) separating batch time-series rollups from target point queries
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

    let metrics = sqlx::query_as::<_, SensorNodeMetrics>(query)
        .fetch_all(&pool)
        .await
        .map_err(|e| {
            tracing::error!("Database descriptive statistics query execution failure: {:?}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, "Internal Data Engine Error".to_string())
        })?;

    Ok(Json(metrics))
}
