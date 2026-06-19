use sqlx::{PgPool, Postgres, query};
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

    /// Inserts a telemetry reading into TimescaleDB via HTTP as Pending
    pub async fn insert_reading(&self, payload: &SensorPayload) -> Result<u64, GatewayError> {
        let result = sqlx::query(
            r#"
            INSERT INTO "sensor_readings" (id, value, sensor_id, status, created_at)
            SELECT gen_random_uuid(), $1, s.id, $2, $3
            FROM "sensors" s
            WHERE s.hardware_id = $4
            "#,
        )
        .bind(payload.reading_value)
        .bind(DataQualityStatus::Pending)
        .bind(payload.timestamp)
        .bind(&payload.device_id)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }

    /// Inserts a telemetry reading into TimescaleDB from MQTT
    pub async fn insert_mqtt_reading(
        &self, 
        device_id: &str, 
        value: f64, 
        timestamp: DateTime<Utc>
    ) -> Result<u64, GatewayError> {
        let result = sqlx::query(
            r#"
            INSERT INTO "sensor_readings" (id, value, sensor_id, status, created_at)
            SELECT gen_random_uuid(), $1, s.id, $2, $3
            FROM "sensors" s
            WHERE s.hardware_id = $4
            "#,
        )
        .bind(value)
        .bind(DataQualityStatus::Pending)
        .bind(timestamp)
        .bind(device_id)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }

    /// Fetches records with status Pending for processing
    pub async fn fetch_pending_readings(
        &self, 
        limit: i64
    ) -> Result<Vec<(Uuid, f64, DateTime<Utc>)>, GatewayError> {
        // Usamos query_as mapeando explicitamente a tupla para evitar atritos com a macro estrita
        let rows = sqlx::query_as::<Postgres, (Uuid, f64, DateTime<Utc>)>(
            r#"
            SELECT id, value, created_at 
            FROM "sensor_readings" 
            WHERE status = $1
            LIMIT $2
            "#,
        )
        .bind(DataQualityStatus::Pending)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    /// Updates the status of a record after AI or Rules Engine analysis
    pub async fn update_reading_status(
        &self, 
        id: Uuid, 
        created_at: DateTime<Utc>, 
        status: DataQualityStatus, 
        note: &str
    ) -> Result<(), GatewayError> {
        sqlx::query(
            r#"
            UPDATE "sensor_readings"
            SET status = $1, ai_analysis_note = $2
            WHERE id = $3 AND created_at = $4
            "#,
        )
        .bind(status)
        .bind(note)
        .bind(id)
        .bind(created_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
