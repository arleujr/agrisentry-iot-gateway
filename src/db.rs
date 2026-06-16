use sqlx::{postgres::PgPoolOptions, PgPool};
use tracing::{debug, info};
use crate::models::SensorPayload;
use crate::error::GatewayError;

#[derive(Clone)]
pub struct DbClient {
    pool: PgPool,
}

impl DbClient {
    pub async fn new(database_url: &str) -> Result<Self, GatewayError> {
        // Pool configurado para alta concorrência (até 50 conexões simultâneas)
        let pool = PgPoolOptions::new()
            .max_connections(50)
            .connect(database_url)
            .await?;
        
        info!("Conexão com PostgreSQL/TimescaleDB estabelecida com sucesso.");
        Ok(Self { pool })
    }

    pub async fn insert_reading(&self, payload: &SensorPayload) -> Result<(), GatewayError> {
        // 1. Resolve o ID relacional do Sensor
        let unique_hardware_id = format!("{}_{}", payload.device_id, payload.sensor_type);
        
        // UPSERT: Insere o sensor ou apenas atualiza se já existir, retornando o UUID
        let sensor_id: uuid::Uuid = sqlx::query_scalar(
            r#"
            INSERT INTO sensors (hardware_id, name)
            VALUES ($1, $2)
            ON CONFLICT (hardware_id) DO UPDATE SET name = EXCLUDED.name
            RETURNING id
            "#
        )
        .bind(&unique_hardware_id)
        .bind(&payload.sensor_type)
        .fetch_one(&self.pool)
        .await?;

        // 2. Insere a leitura na Hypertable (O status 'PENDING' é gerado pelo DEFAULT do banco)
        sqlx::query(
            r#"
            INSERT INTO sensor_readings (value, sensor_id, created_at)
            VALUES ($1, $2, $3)
            "#
        )
        .bind(payload.reading_value)
        .bind(sensor_id)
        .bind(payload.timestamp)
        .execute(&self.pool)
        .await?;

        debug!("Dado persistido: {} -> {:.2}", payload.sensor_type, payload.reading_value);
        Ok(())
    }
}