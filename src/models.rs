use chrono::{DateTime, Utc};
use serde::Deserialize;

// Estruturas que estavam faltando
#[derive(Debug, Deserialize)]
pub struct TelemetryData {
    pub value: f64,
}

#[derive(Debug, Deserialize)]
pub struct RuleFromDb {
    pub trigger_condition: String,
    pub trigger_value: f64,
    pub action_type: String,
}

#[derive(Debug, Deserialize)]
pub struct SensorPayload {
    pub device_id: String,
    pub sensor_type: String,
    pub reading_value: f64,
    pub timestamp: DateTime<Utc>,
    pub metadata_hash: Option<String>,
}

#[derive(Debug, sqlx::Type)]
#[sqlx(type_name = "DataQualityStatus", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DataQualityStatus {
    Pending,
    Valid,
    AnomalyNoise,
    AnomalyCritical,
}