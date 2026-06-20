use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Type};

// =========================================================================
// DATA MODELS & DTOS (STRUCTURAL PERSISTENCE LAYER)
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
    /// Wrapped in Option to prevent 500 ColumnDecode UnexpectedNullError during empty LEFT JOIN states
    #[sqlx(rename = "sensor_type")]
    pub r#type: Option<String>,
    pub latest_reading: Option<f64>,
    /// Wrapped in Option to prevent 500 ColumnDecode UnexpectedNullError during empty LEFT JOIN states
    pub unit_of_measurement: Option<String>,
    pub min_threshold: Option<f64>,
    pub max_threshold: Option<f64>,
    pub arithmetic_mean: Option<f64>,
    pub operational_status: Option<String>,
    pub last_telemetry_timestamp: Option<DateTime<Utc>>,
}
