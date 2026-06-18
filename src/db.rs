use sqlx::PgPool;
use crate::models::{SensorPayload, DataQualityStatus};
use crate::error::GatewayError;

#[derive(Clone)]
pub struct DbClient {
    pub pool: PgPool,
}

impl DbClient {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Inserts a telemetry reading into TimescaleDB as PENDING (Usado pelo HTTP)
    pub async fn insert_reading(&self, payload: &SensorPayload) -> Result<(), GatewayError> {
        sqlx::query(
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
        .bind(&payload.device_id) // device_id no JSON mapeia para hardware_id
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Inserts a telemetry reading into TimescaleDB from MQTT (AQUI ESTAVA O ERRO DO LOG)
    pub async fn insert_mqtt_reading(&self, device_id: &str, value: f64) -> Result<(), GatewayError> {
        sqlx::query(
            r#"
            INSERT INTO "sensor_readings" (id, value, sensor_id, status, created_at)
            SELECT gen_random_uuid(), $1, s.id, 'PENDING'::dataqualitystatus, NOW()
            FROM "sensors" s
            WHERE s.hardware_id = $2
            "#
        )
        .bind(value)
        .bind(device_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}