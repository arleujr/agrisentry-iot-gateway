// db.rs
use crate::contracts::mqtt_v1::{TelemetryEnvelopeV1, TelemetryQuality};
use crate::error::GatewayError;
use crate::models::{DataQualityStatus, SensorPayload};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{PgPool, Postgres};
use uuid::Uuid;

const INSERT_TELEMETRY_EVENT_SQL: &str = r#"
    INSERT INTO telemetry_events (
        event_id,
        device_id,
        sequence,
        observed_at,
        firmware_version,
        received_at,
        raw_payload
    )
    VALUES ($1, $2, $3, $4, $5, NOW(), $6::jsonb)
    ON CONFLICT (event_id) DO NOTHING
"#;

const INSERT_TELEMETRY_READING_SQL: &str = r#"
    INSERT INTO telemetry_readings (
        event_id,
        channel,
        raw_value,
        value,
        unit,
        quality,
        calibration_id
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7)
"#;

/// Result of trying to persist one MQTT v1 telemetry envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TelemetryPersistenceOutcome {
    /// A new event and all its readings were committed.
    Inserted { readings: usize },
    /// The event had already been stored with the same `event_id`.
    Duplicate,
}

/// Database client wrapper for PostgreSQL/TimescaleDB connection pool.
#[derive(Clone)]
pub struct DbClient {
    pub pool: PgPool,
}

impl DbClient {
    /// Creates a new database client instance.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Persists one MQTT v1 envelope and all its readings atomically.
    ///
    /// Idempotency is enforced by the `telemetry_events.event_id` primary key.
    /// A device sequence may only be used once, which exposes firmware replay
    /// bugs where a new event id is generated for an already-used sequence.
    pub async fn persist_telemetry_v1(
        &self,
        envelope: &TelemetryEnvelopeV1,
        raw_payload: &Value,
    ) -> Result<TelemetryPersistenceOutcome, GatewayError> {
        let sequence = sequence_for_database(envelope.sequence)?;
        let mut transaction = self.pool.begin().await?;

        let event_result = sqlx::query(INSERT_TELEMETRY_EVENT_SQL)
            .bind(envelope.event_id)
            .bind(&envelope.device_id)
            .bind(sequence)
            .bind(envelope.observed_at)
            .bind(&envelope.firmware_version)
            .bind(raw_payload.to_string())
            .execute(&mut *transaction)
            .await?;

        if event_result.rows_affected() == 0 {
            transaction.rollback().await?;
            return Ok(TelemetryPersistenceOutcome::Duplicate);
        }

        for reading in &envelope.readings {
            sqlx::query(INSERT_TELEMETRY_READING_SQL)
                .bind(envelope.event_id)
                .bind(reading.channel.as_str())
                .bind(reading.raw_value)
                .bind(reading.value)
                .bind(reading.unit.as_str())
                .bind(telemetry_quality_as_str(reading.quality))
                .bind(reading.calibration_id.as_deref())
                .execute(&mut *transaction)
                .await?;
        }

        transaction.commit().await?;

        Ok(TelemetryPersistenceOutcome::Inserted {
            readings: envelope.readings.len(),
        })
    }

    /// Inserts a structured system log event into database storage for UI terminal observability.
    pub async fn insert_system_log(
        &self,
        component: &str,
        level: &str,
        message: &str,
    ) -> Result<(), GatewayError> {
        sqlx::query(
            r#"
            INSERT INTO "system_events" (component, level, message, created_at)
            VALUES ($1, $2, $3, NOW())
            "#,
        )
        .bind(component)
        .bind(level)
        .bind(message)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Inserts a telemetry reading into TimescaleDB via HTTP as Pending.
    /// - Generates a UUID for the record
    /// - Resolves sensor_id from hardware_id
    /// - Uses payload.timestamp to preserve exact device time
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

    /// Inserts a telemetry reading into TimescaleDB from the legacy MQTT contract.
    pub async fn insert_mqtt_reading(
        &self,
        device_id: &str,
        value: f64,
        timestamp: DateTime<Utc>,
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

    /// Fetches records with status Pending for processing.
    pub async fn fetch_pending_readings(
        &self,
        limit: i64,
    ) -> Result<Vec<(Uuid, f64, DateTime<Utc>)>, GatewayError> {
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

    /// Updates the status of a record after AI or Rules Engine analysis.
    pub async fn update_reading_status(
        &self,
        id: Uuid,
        created_at: DateTime<Utc>,
        status: DataQualityStatus,
        note: &str,
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

fn sequence_for_database(sequence: u64) -> Result<i64, GatewayError> {
    i64::try_from(sequence).map_err(|_| {
        GatewayError::ValidationError(
            "telemetry sequence exceeds the PostgreSQL BIGINT range".to_string(),
        )
    })
}

const fn telemetry_quality_as_str(quality: TelemetryQuality) -> &'static str {
    match quality {
        TelemetryQuality::Valid => "valid",
        TelemetryQuality::Estimated => "estimated",
        TelemetryQuality::Unstable => "unstable",
        TelemetryQuality::OutOfRange => "out_of_range",
        TelemetryQuality::SensorError => "sensor_error",
        TelemetryQuality::NotCalibrated => "not_calibrated",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mqtt_v1_event_insert_is_idempotent_by_event_id() {
        assert!(INSERT_TELEMETRY_EVENT_SQL.contains("ON CONFLICT (event_id) DO NOTHING"));
    }

    #[test]
    fn rejects_a_sequence_that_does_not_fit_postgresql_bigint() {
        let error = sequence_for_database(u64::MAX)
            .expect_err("u64::MAX must not fit in a signed PostgreSQL BIGINT");

        assert!(matches!(error, GatewayError::ValidationError(_)));
    }

    #[test]
    fn maps_contract_quality_to_database_values() {
        assert_eq!(telemetry_quality_as_str(TelemetryQuality::Valid), "valid");
        assert_eq!(
            telemetry_quality_as_str(TelemetryQuality::NotCalibrated),
            "not_calibrated"
        );
    }
}
