-- MQTT v1 telemetry persistence.
--
-- One envelope is stored in telemetry_events and its sensor channels are
-- stored in telemetry_readings. The foreign key and transaction used by the
-- gateway guarantee that an event is never persisted partially.

CREATE TABLE telemetry_events (
    event_id UUID PRIMARY KEY,
    device_id VARCHAR(64) NOT NULL,
    sequence BIGINT NOT NULL CHECK (sequence >= 0),
    observed_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    firmware_version VARCHAR(100) NOT NULL,
    raw_payload JSONB NOT NULL,
    UNIQUE (device_id, sequence)
);

CREATE TABLE telemetry_readings (
    event_id UUID NOT NULL
        REFERENCES telemetry_events(event_id)
        ON DELETE CASCADE,
    channel VARCHAR(64) NOT NULL CHECK (
        channel IN (
            'air_temperature',
            'air_relative_humidity',
            'solution_temperature',
            'solution_ph',
            'solution_tds',
            'reservoir_level',
            'relative_light'
        )
    ),
    raw_value DOUBLE PRECISION NOT NULL,
    value DOUBLE PRECISION,
    unit VARCHAR(24) NOT NULL CHECK (
        unit IN ('celsius', 'percent', 'ph', 'ppm')
    ),
    quality VARCHAR(32) NOT NULL CHECK (
        quality IN (
            'valid',
            'estimated',
            'unstable',
            'out_of_range',
            'sensor_error',
            'not_calibrated'
        )
    ),
    calibration_id VARCHAR(100),
    PRIMARY KEY (event_id, channel)
);

CREATE INDEX idx_telemetry_events_device_observed_at
    ON telemetry_events (device_id, observed_at DESC);

CREATE INDEX idx_telemetry_events_received_at
    ON telemetry_events (received_at DESC);

CREATE INDEX idx_telemetry_readings_channel
    ON telemetry_readings (channel);
