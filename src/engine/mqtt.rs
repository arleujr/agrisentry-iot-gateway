use rumqttc::{AsyncClient, MqttOptions, QoS, Event, Incoming};
use sqlx::PgPool;
use std::time::Duration;
use tokio::sync::watch;

/// Initializes the background MQTT worker task.
pub async fn start_mqtt_worker(
    pool: PgPool, 
    broker_host: &str, 
    broker_port: u16, 
    mut shutdown_rx: watch::Receiver<bool>
) {
    let buffer_size: usize = std::env::var("MQTT_BUFFER_SIZE")
        .unwrap_or_else(|_| "100".to_string())
        .parse()
        .unwrap_or(100);

    let mut mqttoptions = MqttOptions::new("agrisentry_gateway_core", broker_host, broker_port);
    mqttoptions.set_keep_alive(Duration::from_secs(10));

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, buffer_size);

    tracing::info!("MQTT Worker attempting connection to broker {}:{}", broker_host, broker_port);
    
    if let Err(e) = client.subscribe("agrisentry/gateway/#", QoS::AtLeastOnce).await {
        tracing::error!("CRITICAL: Failed to subscribe to core gateway routing hierarchy: {:?}", e);
        return;
    }
    tracing::info!("MQTT Client successfully subscribed to 'agrisentry/gateway/#'");

    loop {
        tokio::select! {
            res = shutdown_rx.changed() => {
                if res.is_ok() && *shutdown_rx.borrow() {
                    tracing::warn!("MQTT Worker caught termination sequence signal. Executing clean broker disconnect drop...");
                    if let Err(err) = client.disconnect().await {
                        tracing::error!("Non-fatal telemetry error during Mosquitto server teardown: {:?}", err);
                    }
                    break;
                }
            }

            notification = eventloop.poll() => {
                match notification {
                    Ok(Event::Incoming(Incoming::Publish(p))) => {
                        let topic_parts: Vec<&str> = p.topic.split('/').collect();
                        
                        if topic_parts.len() >= 4 {
                            let mac_address = topic_parts[2].to_string();
                            let sensor_type = topic_parts[3].to_string();
                            
                            if let Ok(payload_str) = String::from_utf8(p.payload.to_vec()) {
                                if let Ok(reading_value) = payload_str.parse::<f64>() {
                                    let db_pool = pool.clone();
                                    
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
                                                tracing::info!("[INGESTION SUCCESS] Logged PENDING reading for Sensor [{} - {}]: {:.2}", mac_address, sensor_type, reading_value);
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
                                    tracing::warn!("Discarded packet payload: Could not translate stream segment '{}' into float precision matrix.", payload_str);
                                }
                            }
                        }
                    }
                    Ok(_) => {} 
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