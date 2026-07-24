use agrisentry_iot_gateway::contracts::mqtt_v1::{
    parse_telemetry_message, TelemetryChannel, TelemetryContractError,
};

const TOPIC: &str = "agrisentry/v1/devices/hydro-lab-node-01/telemetry";

fn valid_payload() -> String {
    r#"
    {
      "protocol_version": "1.0",
      "event_id": "7d30ef8f-7835-4f38-a687-e530195891ad",
      "device_id": "hydro-lab-node-01",
      "sequence": 1,
      "observed_at": "2026-07-24T16:00:00Z",
      "firmware_version": "2.0.0-alpha.1",
      "readings": [
        {
          "channel": "air_temperature",
          "raw_value": 27.4,
          "value": 27.4,
          "unit": "celsius",
          "quality": "valid",
          "calibration_id": null
        },
        {
          "channel": "air_relative_humidity",
          "raw_value": 68.2,
          "value": 68.2,
          "unit": "percent",
          "quality": "valid",
          "calibration_id": null
        },
        {
          "channel": "solution_temperature",
          "raw_value": 2048,
          "value": 23.1,
          "unit": "celsius",
          "quality": "estimated",
          "calibration_id": "cal-ntc-001"
        }
      ]
    }
    "#
    .to_string()
}

#[test]
fn accepts_a_valid_mqtt_v1_telemetry_message() {
    let payload = valid_payload();
    let envelope = parse_telemetry_message(TOPIC, payload.as_bytes())
        .expect("valid contract example must be accepted");

    assert_eq!(envelope.device_id, "hydro-lab-node-01");
    assert_eq!(envelope.sequence, 1);
    assert_eq!(envelope.readings.len(), 3);
    assert_eq!(
        envelope.readings[0].channel,
        TelemetryChannel::AirTemperature
    );
}

#[test]
fn rejects_when_topic_and_payload_device_ids_differ() {
    let payload = valid_payload().replace(
        "\"device_id\": \"hydro-lab-node-01\"",
        "\"device_id\": \"another-device\"",
    );

    let error = parse_telemetry_message(TOPIC, payload.as_bytes())
        .expect_err("mismatched device ids must be rejected");

    assert!(matches!(
        error,
        TelemetryContractError::DeviceIdMismatch { .. }
    ));
}

#[test]
fn rejects_an_incompatible_channel_unit() {
    let payload = valid_payload().replace("\"unit\": \"celsius\"", "\"unit\": \"percent\"");

    let error = parse_telemetry_message(TOPIC, payload.as_bytes())
        .expect_err("temperature using percent must be rejected");

    assert!(matches!(error, TelemetryContractError::UnitMismatch { .. }));
}

#[test]
fn rejects_unknown_payload_properties() {
    let payload = valid_payload().replace(
        "\"sequence\": 1,",
        "\"sequence\": 1, \"unexpected_field\": true,",
    );

    let error = parse_telemetry_message(TOPIC, payload.as_bytes())
        .expect_err("unknown fields must be rejected");

    assert!(matches!(error, TelemetryContractError::InvalidJson(_)));
}
