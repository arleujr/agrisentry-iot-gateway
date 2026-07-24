const MIGRATION: &str = include_str!("../migrations/20260724000000_create_mqtt_v1_telemetry.sql");

#[test]
fn migration_enforces_event_idempotency() {
    assert!(MIGRATION.contains("event_id UUID PRIMARY KEY"));
    assert!(MIGRATION.contains("UNIQUE (device_id, sequence)"));
}

#[test]
fn migration_keeps_readings_owned_by_their_event() {
    assert!(MIGRATION.contains("REFERENCES telemetry_events(event_id)"));
    assert!(MIGRATION.contains("ON DELETE CASCADE"));
    assert!(MIGRATION.contains("PRIMARY KEY (event_id, channel)"));
}

#[test]
fn migration_supports_every_mqtt_v1_sensor_channel() {
    for channel in [
        "air_temperature",
        "air_relative_humidity",
        "solution_temperature",
        "solution_ph",
        "solution_tds",
        "reservoir_level",
        "relative_light",
    ] {
        assert!(
            MIGRATION.contains(channel),
            "missing database constraint for channel {channel}"
        );
    }
}
