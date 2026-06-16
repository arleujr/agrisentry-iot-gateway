use rumqttc::{AsyncClient, MqttOptions, QoS, Event, Incoming};
use sqlx::PgPool;
use std::time::Duration;
use tokio::sync::watch;
use chrono::Utc;

/// Initializes the background MQTT worker task.
/// Multiplexes between parsing streaming physical hardware data into the TimescaleDB 
/// as PENDING rows and listening for the system termination Watch signal matrix.
pub async fn start_mqtt_worker(
    pool: PgPool, 
    broker_host: &str, 
    broker_port: u16, 
    mut shutdown_rx: watch::Receiver<bool>
) {
    // Dynamic Buffer Sizing defaults to 100 to mitigate Backpressure during heavy telemetry spikes
    let buffer_size: usize = std::env::var("MQTT_BUFFER_SIZE")
        .unwrap_or_else(|_| "100".to_string())
        .parse()
        .unwrap_or(100);

    let mut mqttoptions = MqttOptions::new("agrisentry_gateway_core", broker_host, broker_port);
    mqttoptions.set_keep_alive(Duration::from_secs(10));

    // Spin up the async client with the expanded memory queue boundary
    let (client, mut eventloop) = AsyncClient::new(mqttoptions, buffer_size);

    tracing::info!("MQTT Worker attempting connection to broker {}:{}", broker_host, broker_port);
    
    // Core routing subscription matching our cyber-physical systems architecture
    if let Err(e) = client.subscribe("agrisentry/gateway/#", QoS::AtLeastOnce).await {
        tracing::error!("CRITICAL: Failed to subscribe to core gateway routing hierarchy: {:?}", e);
        return;
    }
    tracing::info!("MQTT Client successfully subscribed to 'agrisentry/gateway/#'");

    loop {
        tokio::select! {
            // Branch 1: Graceful Shutdown System Intercept
            res = shutdown_rx.changed() => {
                if res.is_ok() && *shutdown_rx.borrow() {
                    tracing::warn!("MQTT Worker caught termination sequence signal. Executing clean broker disconnect drop...");
                    
                    // Inform Mosquitto explicitly to teardown session metadata allocation
                    if let Err(err) = client.disconnect().await {
                        tracing::error!("Non-fatal telemetry error during Mosquitto server teardown: {:?}", err);
                    }
                    break;
                }
            }

            // Branch 2: Telemetry Processing Stream Pipeline
            notification = eventloop.poll() => {
                match notification {
                    Ok(Event::Incoming(Incoming::Publish(p))) => {
                        // Dynamic routing slice extraction: agrisentry/gateway/{MAC_ADDRESS}/{SENSOR_TYPE}
                        let topic_parts: Vec<&str> = p.topic.split('/').collect();
                        
                        if topic_parts.len() >= 4 {
                            let mac_address = topic_parts[2].to_string();
                            let sensor_type = topic_parts[3].to_string();
                            
                            if let Ok(payload_str) = String::from_utf8(p.payload.to_vec()) {
                                if let Ok(reading_value) = payload_str.parse::<f64>() {
                                    let db_pool = pool.clone();
                                    
                                    // Spawn an independent lightweight micro-task per ingestion record 
                                    // to eliminate PostgreSQL blocking constraints over the MQTT thread
                                    tokio::spawn(async move {
                                        let result = sqlx::query(
                                            r#"
                                            INSERT INTO "sensor_readings" (id, value, sensor_id, status, created_at)
                                            SELECT gen_random_uuid(), $1, s.id, 'PENDING'::"DataQualityStatus", NOW()
                                            FROM "sensors" s
                                            WHERE s.hardware_id = $2
                                            "#
                                        )
                                        .bind(reading_value)
                                        .bind(&mac_address)
                                        .execute(&db_pool)
                                        .await;

                                        match result {
                                            Ok(res) if res.rows_affected() > 0 => {
                                                tracing::debug!("[INGESTION] Logged PENDING reading for Sensor [{}]: {:.2}", sensor_type, reading_value);
                                            }
                                            Ok(_) => {
                                                tracing::warn!("[INGESTION REJECTED] Valid data but hardware registration entry missing for MAC: {}", mac_address);
                                            }
                                            Err(e) => {
                                                tracing::error!("CRITICAL: TimescaleDB internal insertion statement panic: {:?}", e);
                                            }
                                        }
                                    });
                                } else {
                                    tracing::debug!("Discarded packet payload: Could not translate stream segment '{}' into float precision matrix.", payload_str);
                                }
                            }
                        }
                    }
                    Ok(_) => {} // Skip noise protocol handshakes (PINGRESP, PUBACK, etc)
                    Err(e) => {
                        tracing::error!("MQTT Network connection barrier encountered: {:?}. Throttling reconnection pipeline...", e);
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        }
    }

    tracing::info!("🏁 MQTT Worker thread safely and completely terminated. Resources flushed.");
}
