use chrono::{DateTime, Utc};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct SensorPayload {
    pub device_id: String,
    pub sensor_type: String,
    pub reading_value: f64,
    pub timestamp: DateTime<Utc>,
    pub metadata_hash: Option<String>,
}

// O ENUM que representa o status de qualidade do dado no banco (Antiredundância)
#[derive(Debug, sqlx::Type)]
#[sqlx(type_name = "DataQualityStatus", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DataQualityStatus {
    Pending,
    Valid,
    AnomalyNoise,
    AnomalyCritical,
}