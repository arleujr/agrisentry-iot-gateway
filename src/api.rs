use actix_web::{post, web, HttpResponse, Responder};
use tracing::{error, info};
use crate::db::DbClient;
use crate::models::SensorPayload;

/// HTTP Endpoint for REST telemetry ingestion.
/// Ideal for testing, integrations, or edge devices without MQTT capabilities.
#[post("/telemetry")] // 👇 AQUI ESTÁ A MUDANÇA (Tirei o /api)
pub async fn ingest_telemetry(
    db: web::Data<DbClient>,
    payload: web::Json<SensorPayload>,
) -> impl Responder {
    info!("Received HTTP telemetry from device: {}", payload.device_id);
    
    // Unwraps the JSON payload and sends it to our DB core
    match db.insert_reading(&payload.into_inner()).await {
        Ok(_) => {
            HttpResponse::Ok().json(serde_json::json!({
                "status": "success", 
                "message": "Telemetry queued as PENDING for AI analysis"
            }))
        },
        Err(e) => {
            error!("Failed to persist HTTP telemetry: {:?}", e);
            HttpResponse::InternalServerError().json(serde_json::json!({
                "status": "error", 
                "message": "Internal database error"
            }))
        }
    }
}
