use sqlx::PgPool;
use crate::models::{SensorPayload, DataQualityStatus};
use crate::error::GatewayError;
use chrono::{DateTime, Utc};

#[derive(Clone)]
pub struct DbClient {
    pub pool: PgPool,
}

impl DbClient {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Inserts a telemetry reading into TimescaleDB via HTTP as PENDING
    /// Returns the number of affected rows
    pub async fn insert_reading(&self, payload: &SensorPayload) -> Result<u64, GatewayError> {
        let result = sqlx::query(
            r#"
            INSERT INTO "sensor_readings" (id, value, sensor_id, status, created_at)
            SELECT gen_random_uuid(), $1, s.id, $2::dataqualitystatus, $3
            FROM "sensors" s
            WHERE s.hardware_id = $4
            "#
        )
        .bind(payload.reading_value)
        .bind(DataQualityStatus::Pending)
        .bind(payload.timestamp)
        .bind(&payload.device_id) // device_id in JSON maps to hardware_id
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }

    /// Inserts a telemetry reading into TimescaleDB from MQTT
    /// Uses the sensor-provided timestamp instead of NOW()
    /// Returns the number of affected rows
    pub async fn insert_mqtt_reading(
        &self, 
        device_id: &str, 
        value: f64, 
        timestamp: DateTime<Utc>
    ) -> Result<u64, GatewayError> {
        let result = sqlx::query(
            r#"
            INSERT INTO "sensor_readings" (id, value, sensor_id, status, created_at)
            SELECT gen_random_uuid(), $1, s.id, 'PENDING'::dataqualitystatus, $3
            FROM "sensors" s
            WHERE s.hardware_id = $2
            "#
        )
        .bind(value)
        .bind(device_id)
        .bind(timestamp) // <-- Now stores the real sensor timestamp
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }
}
