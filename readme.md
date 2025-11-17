# AgriSentry IoT Gateway & AgroCore Engine

**Author:** [Arleu Júnior](https://github.com/arleujr)

[](https://www.google.com/search?q=https://github.com/arleujr/agrisentry-iot-gateway/blob/main/LICENSE)
[](https://www.rust-lang.org/)
[](https://www.google.com/search?q=https://github.com/arleujr/agrisentry-iot-gateway/actions)

A high-performance, resilient, and secure hybrid IoT gateway for the AgriSentry platform, built in Rust. This service acts as the central nervous system, handling real-time device communication via MQTT and providing an intelligent API via HTTP.

-----

## Core Philosophy

This project is engineered to solve a common problem in IoT: the gap between simple, unreliable hobbyist projects and overly complex, expensive enterprise platforms.

The goal is to provide a single, efficient binary that is:

1.  **Performant:** Built in Rust on top of Actix-web and SQLx for asynchronous, non-blocking I/O capable of handling thousands of concurrent device connections.
2.  **Resilient:** The MQTT client features an automatic reconnection loop, and the `agrisentry-device` firmware is designed with an offline cache to ensure zero data loss during network failures.
3.  **Intelligent:** This is not just a data pipeline. It embeds the **AgroCore** engine, a rule validator and simulator, allowing for complex automation logic to be tested and deployed safely.
4.  **Flexible:** It can be run as a 24/7 server or as a standalone **CLI tool** for offline development, testing, and simulation.

## System Architecture

This gateway serves as the bridge between the physical hardware (`agrisentry-device`) and the management interface (`agrisentry-dashboard`).

```
[AgriSentry Dashboard] --- (HTTP API) ---> [Gateway (Actix)] ---> [AgroCore Engine]
 (React/Node.js)                               (Rust)                  (Rust)
                                               ^                       |
                                               | (MQTT)                v
[ESP32 Devices] <------- (MQTT) ------- [Mosquitto Broker] <-----> [Gateway (Rumqttc)]
 (MicroPython)           (Secure)            (Docker)                (Rust)
```

## Features

  - **Hybrid Server:** Runs a multi-threaded Actix-web HTTP server and a separate, resilient `rumqttc` MQTT client in parallel within a single Tokio runtime.
  - **Secure MQTT Broker:** Configured to run a hardened Mosquitto broker in Docker, disabling anonymous access and requiring username/password authentication for all clients.
  - **Real-time Rule Engine (`AgroCore`):**
      - **Validation:** Intercepts rule configuration requests from devices and validates them against the `validator.rs` module before processing.
      - **Simulation:** Exposes a `POST /simulate` endpoint, allowing the React dashboard to send "what-if" scenarios to the Rust backend for instant validation.
  - **Standalone CLI:** The entire binary can be run as a `clap`-based CLI tool to test the simulation engine locally from your terminal, completely independent of the server.

## Usage

This project has two primary modes of operation.

### 1\. As a Server (HTTP + MQTT)

This is the default mode. It starts the full gateway for production or development use.
**Remember to add your MQTT password** to the `.set_credentials()` call in `src/main.rs`.

```sh
# Start the server
cargo run
```

**Output:**

```
Server mode detected: Starting HTTP server and MQTT client...
Successfully connected to the database!
Starting HTTP server at http://localhost:8080
Connecting to MQTT broker with client ID: agrisentry-gateway-1763345...
MQTT Client is running and listening...
```

**Testing the Simulation API (Example):**

```sh
# Send a test rule and data to the simulation endpoint
curl -X POST http://localhost:8080/simulate \
     -H "Content-Type: application/json" \
     -d '{
           "rule": {
             "trigger_condition": "LESS_THAN",
             "trigger_value": 30.0,
             "action_type": "TURN_ON",
             "action_actuator_id": "11111111-1111-1111-1111-111111111111"
           },
           "data": {
             "value": 25.0,
             "sensor_type": "SOIL_MOISTURE"
           }
         }'
```

**API Response:**

```json
{
  "simulation_result": "TurnOn"
}
```

### 2\. As a CLI Tool

This mode runs the simulation engine directly from your terminal for local testing.

```sh
# Run the 'simulate' subcommand with local JSON files
cargo run -- simulate --rule-file rule.json --data-file data.json
```

**Output:**

```
--- AgroCore CLI Simulator ---
Simulating with Rule: RuleFromDb { trigger_condition: "LESS_THAN", ... }
And Data: TelemetryData { value: 25.0, ... }
[SIMULATOR] Simulating rule with sensor value: 25
[SIMULATOR] Condition met: true
[SIMULATOR] Result: TurnOn

--- Simulation Result ---
TurnOn
-------------------------
```

## Installation & Setup

### 1\. Prerequisites

  - Rust & Cargo (`cargo --version`)
  - Docker & Docker Compose (`docker --version`)
  - A running PostgreSQL instance (or use the one in `docker-compose.yml`)

### 2\. Environment Setup

1.  **Configure `.env`:**
    Create a `.env` file and add the connection string for your database.

    ```sh
    DATABASE_URL="postgres://admin:password123@localhost:5432/agrisentry"
    ```

2.  **Configure Mosquitto (Security):**

      * Run the following command to create a new authorized user (e.g., `agrisentry_user`):
        ```sh
        docker-compose exec mosquitto mosquitto_passwd -c /mosquitto/config/pwfile agrisentry_user
        ```
      * You will be prompted to enter a new password.
      * Ensure your `mosquitto/config/mosquitto.conf` file has `allow_anonymous false` and `password_file /mosquitto/config/pwfile`.

### 3\. Run the Environment

This starts the required services (PostgreSQL and Mosquitto) in Docker.

```sh
docker-compose up -d
```

### 4\. Prepare the Database

Run the schema SQL script to create all tables, types, and functions. This script is idempotent and can be run multiple times.

```powershell
# (Use this command on PowerShell)
Get-Content ./schema.sql | docker-compose exec -i -T postgres psql -U admin -d agrisentry
```

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.