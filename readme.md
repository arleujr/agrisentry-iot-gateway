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
* **Thread-Safe Graceful Shutdown:** Leverages cross-thread watch broadcast state signaling. Upon receiving OS termination signals (SIGINT/SIGTERM), the gateway drains active HTTP server streams, informs the Mosquitto broker to drop connections safely, flushes memory buffers, and closes connection pools without data loss or orphan sockets.
* **Exponential Backoff Retry Matrix:** Database statement errors caused by temporary network latency or locking conditions trigger an automated retry loop with incremental delay multipliers, preventing application panic and safeguarding edge telemetry data.
* **Dynamic Buffer Sizing:** Parameterized network event channels to mitigate internal backpressure constraints under heavy loading stress.

## Contract Protocol Specification

The gateway parses incoming raw network packets by converting them into structured database payloads. Devices must route their streaming telemetry following this exact pattern:

* **Target Topic Structure:** `agrisentry/gateway/{MAC_ADDRESS}/{SENSOR_TYPE}`
* **Supported Sensors:** TEMPERATURE, HUMIDITY, SOIL_MOISTURE, LUMINOSITY
* **Payload Format:** Compressed numeric string values (e.g., "24.50") transmitted inside text protocol wrappers without structural JSON overhead.

## Quick Start

### 1. Configuration Matrix

Create a local environment file by copying the example template:

```bash
cp .env.example .env
```

**`.env` reference:**

```env
DATABASE_URL=postgres://agrisentry_admin:admin_secure_password123@localhost:5432/agrisentry_db
MQTT_HOST=127.0.0.1
MQTT_PORT=1883
MQTT_BUFFER_SIZE=100
HTTP_HOST=0.0.0.0
HTTP_PORT=8080
```

### 2. Execution

To compile and launch the gateway server locally:

```bash
cargo run
```

## License

Distributed under the MIT License.
