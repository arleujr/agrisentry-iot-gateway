use sqlx::{postgres::PgPoolOptions, PgPool};
use tracing::{debug, info, warn, error};
use std::time::Duration;
use crate::models::SensorPayload;
use crate::error::GatewayError;

#[derive(Clone)]
pub struct DbClient {
    pool: PgPool,
}

impl DbClient {
    pub async fn new(database_url: &str) -> Result<Self, GatewayError> {
        // Pool configured for high concurrency with strict connection timeout
        let pool = PgPoolOptions::new()
            .max_connections(50)
            .acquire_timeout(Duration::from_secs(3))
            .connect(database_url)
            .await?;
        
        info!("Successfully established PostgreSQL/TimescaleDB connection pool.");
        Ok(Self { pool })
    }

    pub async fn insert_reading(&self, payload: &SensorPayload) -> Result<(), GatewayError> {
        let unique_hardware_id = format!("{}_{}", payload.device_id, payload.sensor_type);
        
        // Resilience Matrix: 3 Retry attempts with Exponential Backoff
        let mut attempts = 0;
        let mut delay = Duration::from_millis(200);
        let max_attempts = 3;

        loop {
            // Step 1: High-Performance Direct Hypertable Insertion
            // Optimistically assumes the sensor is already registered to avoid double query footprint
            let result = sqlx::query(
                r#"
                INSERT INTO "sensor_readings" (id, value, sensor_id, status, created_at)
                SELECT gen_random_uuid(), $1, s.id, 'PENDING'::"DataQualityStatus", NOW()
                FROM "sensors" s
                WHERE s.hardware_id = $2
                "#
            )
            .bind(payload.reading_value)
            .bind(&unique_hardware_id)
            .execute(&self.pool)
            .await;

            match result {
                Ok(res) if res.rows_affected() > 0 => {
                    debug!("Telemetry raw event persisted as PENDING: {} -> {:.2}", payload.sensor_type, payload.reading_value);
                    return Ok(());
                }
                // Step 2: Cache-Miss Recovery (Sensor not registered yet)
                Ok(_) => {
                    warn!("Sensor entry missing for hardware_id: [{}]. Executing dynamic provision UPSERT...", unique_hardware_id);
                    
                    let provision_result = sqlx::query(
                        r#"
                        INSERT INTO "sensors" (id, hardware_id, name, created_at)
                        VALUES (gen_random_uuid(), $1, $2, NOW())
                        ON CONFLICT (hardware_id) DO NOTHING
                        "#
                    )
                    .bind(&unique_hardware_id)
                    .bind(&payload.sensor_type)
                    .execute(&self.pool)
                    .await;

                    if let Err(e) = provision_result {
                        error!("Fatal error during fallback hardware sensor provisioning: {:?}", e);
                        return Err(GatewayError::from(e));
                    }
                    
                    // After successful dynamic configuration setup, fallthrough to retry insertion
                }
                // Step 3: Network Jitter / Database Lock Recovery
                Err(err) => {
                    attempts += 1;
                    if attempts >= max_attempts {
                        error!("CRITICAL: Ingestion failed after {} retries. Discarding packet. Error: {:?}", max_attempts, err);
                        return Err(GatewayError::from(err));
                    }

                    warn!("Database query friction detected. Retrying insertion task in {:?} (Attempt {}/{})", delay, attempts, max_attempts);
                    tokio::time::sleep(delay).await;
                    delay *= 2; // Double the sleep duration (Exponential Backoff)
                }
            }
        }
    }
}
