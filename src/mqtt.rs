use rumqttc::{AsyncClient, MqttOptions, QoS, Event, Incoming};
use std::time::Duration;
use tracing::{info, error, debug};
use chrono::Utc;
use crate::db::DbClient;
use crate::models::SensorPayload;

/// Background worker that listens to the Mosquitto broker and funnels data to the DB.
pub async fn start_mqtt_worker(db: DbClient, broker_host: &str, broker_port: u16) {
    // Unique ID for the Gateway client, no credentials required for local Docker Mosquitto
    let mut mqttoptions = MqttOptions::new("agrisentry_gateway_core", broker_host, broker_port);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    
    if let Err(e) = client.subscribe("agrisentry/gateway/#", QoS::AtLeastOnce).await {
        error!("Fatal: Failed to subscribe to MQTT topics: {}", e);
        return;
    }
    
    info!("MQTT Worker connected. Listening on 'agrisentry/gateway/#'");

    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Incoming::Publish(p))) => {
                // Topic standard: agrisentry/gateway/{MAC_ADDRESS}/{SENSOR_TYPE}
                let topic_parts: Vec<&str> = p.topic.split('/').collect();
                
                if topic_parts.len() >= 4 {
                    let mac_address = topic_parts[2].to_string();
                    let sensor_type = topic_parts[3].to_string();
                    
                    if let Ok(payload_str) = String::from_utf8(p.payload.to_vec()) {
                        // Assuming the payload is a raw string number (e.g., "25.5")
                        if let Ok(value) = payload_str.parse::<f64>() {
                            let payload = SensorPayload {
                                device_id: mac_address,
                                sensor_type,
                                reading_value: value,
                                timestamp: Utc::now(),
                                metadata_hash: None,
                            };
                            
                            // Send to the unified DB layer
                            if let Err(e) = db.insert_reading(&payload).await {
                                error!("DB Insert Error from MQTT worker: {:?}", e);
                            }
                        } else {
                            debug!("Ignored payload: Could not parse '{}' as f64", payload_str);
                        }
                    }
                }
            }
            Ok(_) => {} // Silently ignore other MQTT events (PINGRESP, PUBACK, etc.)
            Err(e) => {
                error!("MQTT Connection dropped: {:?}. Attempting reconnect in 5s...", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}