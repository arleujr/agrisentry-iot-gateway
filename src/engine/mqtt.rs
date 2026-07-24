use crate::contracts::mqtt_v1::{parse_telemetry_message, TELEMETRY_TOPIC_FILTER};
use crate::db::DbClient;
use crate::models::MqttPayload;
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS, TlsConfiguration, Transport};
use sqlx::PgPool;
use std::time::Duration;
use tokio::sync::watch;

const LEGACY_TOPIC_FILTER: &str = "agrisentry/gateway/#";

/// Initializes and runs the background MQTT worker task.
///
/// MQTT v1 messages are validated against the shared AgriSentry contract.
/// The legacy topic remains temporarily available while the firmware migrates.
pub async fn start_mqtt_worker(
    pool: PgPool,
    broker_host: &str,
    broker_port: u16,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let buffer_size = environment_usize("MQTT_BUFFER_SIZE", 100);
    let legacy_enabled = environment_bool("MQTT_ENABLE_LEGACY", true);
    let tls_enabled = environment_bool("MQTT_TLS", broker_port == 8883);

    let mut mqtt_options = MqttOptions::new("agrisentry_gateway_core", broker_host, broker_port);
    mqtt_options.set_keep_alive(Duration::from_secs(10));

    let mqtt_user = std::env::var("MQTT_USER").unwrap_or_default();
    let mqtt_pass = std::env::var("MQTT_PASS").unwrap_or_default();

    if !mqtt_user.is_empty() {
        mqtt_options.set_credentials(mqtt_user, mqtt_pass);
    }

    if tls_enabled {
        mqtt_options.set_transport(Transport::Tls(TlsConfiguration::Native));
    }

    let (client, mut event_loop) = AsyncClient::new(mqtt_options, buffer_size);

    tracing::info!(
        broker_host,
        broker_port,
        tls_enabled,
        "MQTT worker connecting"
    );

    if let Err(error) = client
        .subscribe(TELEMETRY_TOPIC_FILTER, QoS::AtLeastOnce)
        .await
    {
        tracing::error!(?error, "failed to subscribe to MQTT v1 telemetry topic");
        return;
    }

    tracing::info!(
        topic = TELEMETRY_TOPIC_FILTER,
        "subscribed to MQTT v1 telemetry"
    );

    if legacy_enabled {
        if let Err(error) = client
            .subscribe(LEGACY_TOPIC_FILTER, QoS::AtLeastOnce)
            .await
        {
            tracing::error!(?error, "failed to subscribe to legacy MQTT topic");
            return;
        }

        tracing::warn!(
            topic = LEGACY_TOPIC_FILTER,
            "legacy MQTT ingestion is enabled during the migration period"
        );
    }

    loop {
        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_ok() && *shutdown_rx.borrow() {
                    tracing::info!("MQTT worker received shutdown signal");

                    if let Err(error) = client.disconnect().await {
                        tracing::warn!(?error, "MQTT disconnect returned a non-fatal error");
                    }

                    break;
                }
            }

            notification = event_loop.poll() => {
                match notification {
                    Ok(Event::Incoming(Incoming::Publish(publish))) => {
                        if publish.topic.starts_with("agrisentry/v1/") {
                            handle_v1_telemetry(&publish.topic, publish.payload.as_ref());
                        } else if legacy_enabled {
                            handle_legacy_telemetry(
                                pool.clone(),
                                &publish.topic,
                                publish.payload.as_ref(),
                            );
                        }
                    }
                    Ok(_) => {}
                    Err(error) => {
                        tracing::error!(?error, "MQTT connection error; retrying");
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        }
    }

    tracing::info!("MQTT worker terminated");
}

fn handle_v1_telemetry(topic: &str, payload: &[u8]) {
    match parse_telemetry_message(topic, payload) {
        Ok(envelope) => {
            tracing::info!(
                event_id = %envelope.event_id,
                device_id = %envelope.device_id,
                sequence = envelope.sequence,
                observed_at = %envelope.observed_at,
                reading_count = envelope.readings.len(),
                "MQTT v1 telemetry accepted by the contract layer"
            );
        }
        Err(error) => {
            tracing::warn!(
                topic,
                error = %error,
                "MQTT v1 telemetry rejected"
            );
        }
    }
}

fn handle_legacy_telemetry(pool: PgPool, topic: &str, payload: &[u8]) {
    let parts: Vec<&str> = topic.split('/').collect();
    if parts.len() != 4 || parts[0] != "agrisentry" || parts[1] != "gateway" {
        tracing::warn!(topic, "legacy MQTT message used an unsupported topic");
        return;
    }

    let device_id = parts[2].to_string();
    let sensor_type = parts[3].to_string();

    let mqtt_data = match serde_json::from_slice::<MqttPayload>(payload) {
        Ok(data) => data,
        Err(error) => {
            tracing::warn!(topic, ?error, "legacy MQTT payload was rejected");
            return;
        }
    };

    tokio::spawn(async move {
        let db_client = DbClient::new(pool);

        match db_client
            .insert_mqtt_reading(&device_id, mqtt_data.value, mqtt_data.timestamp)
            .await
        {
            Ok(rows) if rows > 0 => {
                tracing::info!(
                    device_id,
                    sensor_type,
                    value = mqtt_data.value,
                    "legacy MQTT reading persisted"
                );
            }
            Ok(_) => {
                tracing::warn!(
                    device_id,
                    "legacy MQTT reading references an unregistered device"
                );
            }
            Err(error) => {
                tracing::error!(?error, device_id, "legacy MQTT persistence failed");
            }
        }
    });
}

fn environment_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .and_then(|value| match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
        .unwrap_or(default)
}

fn environment_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}
