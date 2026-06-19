use sqlx::PgPool;
use crate::models::{SensorPayload, DataQualityStatus};
use crate::error::GatewayError;
use chrono::{DateTime, Utc};
use uuid::Uuid;

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

    /// Fetches records with status PENDING for processing
    /// Returns a vector of (id, value, created_at)
    pub async fn fetch_pending_readings(
        &self, 
        limit: i64
    ) -> Result<Vec<(Uuid, f64, DateTime<Utc>)>, GatewayError> {
        let rows = sqlx::query!(
            r#"
            SELECT id, value, created_at 
            FROM "sensor_readings" 
            WHERE status = 'PENDING'::dataqualitystatus
            LIMIT $1
            "#,
            limit
        )
        .fetch_all(&self.pool)
        .await?;

        let result = rows.into_iter().map(|r| (r.id, r.value, r.created_at)).collect();
        Ok(result)
    }

    /// Updates the status of a record after AI or Rules Engine analysis
    pub async fn update_reading_status(
        &self, 
        id: Uuid, 
        created_at: DateTime<Utc>, 
        status: &str, 
        note: &str
    ) -> Result<(), GatewayError> {
        sqlx::query!(
            r#"
            UPDATE "sensor_readings"
            SET status = $1::dataqualitystatus, ai_analysis_note = $2
            WHERE id = $3 AND created_at = $4
            "#,
            status,
            note,
            id,
            created_at
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
