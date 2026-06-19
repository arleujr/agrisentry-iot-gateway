use chrono::{DateTime, Utc};
use serde::Deserialize;
use sqlx::Type;

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
/// Used to track the quality status of telemetry readings
#[derive(Debug, Clone, Copy, PartialEq, Eq, Type)]
#[sqlx(type_name = "dataqualitystatus", rename_all = "UPPERCASE")]
pub enum DataQualityStatus {
    PENDING,
    VALID,
    ANOMALY_NOISE,
    ANOMALY_CRITICAL,
}

/// Represents the JSON payload sent by sensors via MQTT
/// This is deserialized directly from the MQTT message body
#[derive(Debug, Deserialize)]
pub struct MqttPayload {
    pub value: f64,
    pub timestamp: DateTime<Utc>,
}
