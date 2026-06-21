# AgriSentry IoT Ingestion Gateway

![Rust](https://img.shields.io/badge/rust-1.75%2B-orange.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15+-336791.svg)
![Actix-Web](https://img.shields.io/badge/Actix--Web-HTTP-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green)

High-performance, multi-protocol asynchronous ingestion gateway engineered in Rust. This microservice acts as the critical entry point for high-throughput cyber-physical agricultural telemetry streams, funneling massive telemetry events concurrently into a PostgreSQL instance. All data points are persisted with an initial validation status to enable downstream processing and analysis.

## System Architecture

The gateway isolates network ingestion protocol loops from persistence constraints, utilizing a unified database client pool to process incoming packets asynchronously:

```mermaid
graph LR
    A[Edge/Simulator] -->|MQTT| B(Rumqttc Worker)
    A -->|HTTP REST| C(Actix-Web Server)
    B --> D{Unified SQLx Pool}
    C --> D
    D -->|Optimistic Insert| E[(PostgreSQL)]

```

* **REST Layer:** Handled via an Actix-Web HTTP server routing stateful payload requests non-blockingly.
* **Telemetry Streaming Layer:** Powered by an asynchronous Rumqttc event loop multiplexing edge hardware data.
* **Persistence Layer:** Unified SQLx client pools dispatching operations straight to a PostgreSQL relational database.

## Key Engineering Decisions

* **Multi-Protocol Ingestion:** Native capability to simultaneously ingest structural payload events via HTTP REST endpoints and lightweight streaming slices via MQTT brokers.
* **Single-Query Optimistic Ingestion:** Instead of executing double-query footprints (INSERT ON CONFLICT for sensors followed by reading inserts), the database client optimistically targets the database. Fallback configuration sequences only execute if a physical sensor mismatch occurs, cutting database load by 50%.
* **Thread-Safe Graceful Shutdown:** Leverages cross-thread watch broadcast state signaling. Upon receiving OS termination signals (SIGINT/SIGTERM), the gateway drains active HTTP server streams, informs the MQTT broker to drop connections safely, flushes memory buffers, and closes connection pools without data loss or orphan sockets.
* **Asynchronous AI Pipeline Workers:** Integrated background worker loops that poll pending data streams, offloading intensive structural data classifications to external FastAPI AI microservices asynchronously without introducing blocking delays into critical ingestion paths.
* **Exponential Backoff Retry Matrix:** Database statement errors caused by temporary network latency or locking conditions trigger an automated retry loop with incremental delay multipliers, preventing application panic and safeguarding edge telemetry data.

## Contract Protocol Specification

The gateway parses incoming raw network packets by converting them into structured database payloads.

### 🌐 HTTP REST API

Devices can push complete telemetry states to the `/api/v1/telemetry` pipeline using the standard `SensorPayload` structure:

```json
{
  "device_id": "ESP32-TEST-001",
  "sensor_type": "TEMPERATURE",
  "reading_value": 24.5,
  "timestamp": "2026-06-20T23:16:00Z",
  "metadata_hash": null
}

```

### 📡 MQTT Streaming

* **Target Topic Structure:** `agrisentry/gateway/{MAC_ADDRESS}/{SENSOR_TYPE}`
* **Supported Sensors:** TEMPERATURE, HUMIDITY, SOIL_MOISTURE, LUMINOSITY
* **Payload Format:** JSON structured packets matching the `MqttPayload` layout:

```json
{
  "value": 24.50,
  "timestamp": "2026-06-20T23:16:00Z"
}

```

---

## Quick Start

### 1. Configuration Matrix

Create a local environment file by copying the example template:

```bash
cp .env.example .env

```

**`.env` reference (Local & Cloud Configurations):**

```env
# Persistence Setup
DATABASE_URL=postgres://agrisentry_admin:admin_secure_password123@localhost:5432/agrisentry_db

# Microservices Pipelines
AI_API_URL=[https://agrisentry-core.onrender.com/v1/analyze](https://agrisentry-core.onrender.com/v1/analyze)

# MQTT Broker Configuration (Secure TLS Default)
MQTT_HOST=127.0.0.1
MQTT_PORT=8883
MQTT_USER=your_mqtt_user
MQTT_PASS=your_mqtt_password
MQTT_BUFFER_SIZE=100

# Runtime HTTP Server Setup
PORT=8080
RUST_LOG=debug

```

### 2. Execution

To compile and launch the gateway server locally:

```bash
cargo run

```

---

## 📄 License
Distributed under the MIT License.

