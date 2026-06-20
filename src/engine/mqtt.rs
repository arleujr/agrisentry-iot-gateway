use rumqttc::{AsyncClient, MqttOptions, QoS, Event, Incoming, Transport, TlsConfiguration};
use sqlx::PgPool;
use std::time::Duration;
use tokio::sync::watch;
use crate::db::DbClient;
use crate::models::MqttPayload; // Structured MQTT payload model

/// Initializes and runs the background MQTT worker task.
/// This worker handles subscription, message ingestion, and graceful shutdown.
pub async fn start_mqtt_worker(
    pool: PgPool, 
    broker_host: &str, 
    broker_port: u16, 
    mut shutdown_rx: watch::Receiver<bool>
) {
    // Configure buffer size from environment variable (default = 100)
    let buffer_size: usize = std::env::var("MQTT_BUFFER_SIZE")
        .unwrap_or_else(|_| "100".to_string())
        .parse()
        .unwrap_or(100);

    // Configure MQTT client options
    let mut mqttoptions = MqttOptions::new("agrisentry_gateway_core", broker_host, broker_port);
    mqttoptions.set_keep_alive(Duration::from_secs(10));

    // 🚨 Critical step: Enable native TLS transport for secure communication
    mqttoptions.set_transport(Transport::tls(TlsConfiguration::Native));

    // Create asynchronous MQTT client and event loop
    let (client, mut eventloop) = AsyncClient::new(mqttoptions, buffer_size);

    tracing::info!("MQTT Worker attempting connection to broker {}:{}", broker_host, broker_port);
    
    // Subscribe to the gateway topic hierarchy
    if let Err(e) = client.subscribe("agrisentry/gateway/#", QoS::AtLeastOnce).await {
        tracing::error!("CRITICAL: Failed to subscribe to core gateway routing hierarchy: {:?}", e);
        return;
    }
    tracing::info!("MQTT Client successfully subscribed to 'agrisentry/gateway/#'");

    // Main worker loop: handles shutdown signals and incoming MQTT events
    loop {
        tokio::select! {
            // Shutdown signal received
            res = shutdown_rx.changed() => {
                if res.is_ok() && *shutdown_rx.borrow() {
                    tracing::warn!("MQTT Worker caught termination sequence signal. Executing clean broker disconnect...");
                    if let Err(err) = client.disconnect().await {
                        tracing::error!("Non-fatal telemetry error during broker teardown: {:?}", err);
                    }
                    break;
                }
            }

            // Process MQTT events
            notification = eventloop.poll() => {
                match notification {
                    Ok(Event::Incoming(Incoming::Publish(p))) => {
                        let topic_parts: Vec<&str> = p.topic.split('/').collect();
                        
                        if topic_parts.len() >= 4 {
                            let mac_address = topic_parts[2].to_string();
                            let sensor_type = topic_parts[3].to_string();
                            
                            if let Ok(payload_str) = String::from_utf8(p.payload.to_vec()) {
                                // ✅ Deserialize structured JSON payload (value + timestamp)
                                if let Ok(mqtt_data) = serde_json::from_str::<MqttPayload>(&payload_str) {
                                    let db_client = DbClient::new(pool.clone());
                                    let mac_address_clone = mac_address.clone();
                                    let sensor_type_clone = sensor_type.clone();
                                    
                                    tokio::spawn(async move {
                                        // Insert reading into TimescaleDB
                                        match db_client.insert_mqtt_reading(&mac_address_clone, mqtt_data.value, mqtt_data.timestamp).await {
                                            Ok(rows) if rows > 0 => {
                                                tracing::info!(
                                                    "[INGESTION SUCCESS] Logged reading for Sensor [{} - {}]: {:.2}",
                                                    mac_address_clone,
                                                    sensor_type_clone,
                                                    mqtt_data.value
                                                );
                                            }
                                            Ok(_) => {
                                                tracing::warn!(
                                                    "[INGESTION REJECTED] Valid data but hardware registration missing for MAC: {}",
                                                    mac_address_clone
                                                );
                                            }
                                            Err(e) => {
                                                tracing::error!(
                                                    "CRITICAL: TimescaleDB insertion failure: {:?}",
                                                    e
                                                );
                                            }
                                        }
                                    });
                                } else {
                                    tracing::warn!(
                                        "Discarded packet payload: Invalid JSON format. Expected {{'value': f64, 'timestamp': string}} - Received: '{}'",
                                        payload_str
                                    );
                                }
                            }
                        }
                    }
                    Ok(_) => {} 
                    Err(e) => {
                        tracing::error!("MQTT connection error: {:?}. Retrying after delay...", e);
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        }
    }

    tracing::info!("🏁 MQTT Worker thread terminated. Resources flushed.");
}
