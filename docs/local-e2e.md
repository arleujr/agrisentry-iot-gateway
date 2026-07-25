# Local MQTT → Gateway → PostgreSQL proof

This local environment proves the first complete AgriSentry v2 ingestion path
without requiring the ESP32.

## Components

- TimescaleDB/PostgreSQL on `127.0.0.1:5432`
- Mosquitto on `127.0.0.1:1883`
- Rust gateway running directly with Cargo
- PowerShell publisher acting as the temporary device

Both container ports are bound only to loopback. Anonymous MQTT is enabled only
for this isolated development environment.

## Run

### Terminal 1 — infrastructure

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\local\start-infra.ps1
```

### Terminal 2 — gateway

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\local\run-gateway.ps1
```

Keep this terminal open.

### Terminal 3 — end-to-end proof

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\local\test-e2e.ps1
```

The test generates a unique event, publishes the same MQTT message twice and
asserts that PostgreSQL stores:

- exactly one telemetry event;
- exactly seven sensor readings.

## Stop

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\local\stop-infra.ps1
```

## Delete local test data

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\local\reset-infra.ps1
```

## Expected gateway evidence

The gateway should log one successful persistence and one ignored duplicate for
the same `event_id`.
