use actix_cors::Cors;
use actix_web::{web, App, HttpServer, Responder, HttpResponse, post};
use clap::Parser;
use dotenvy::dotenv;
use sqlx::{PgPool, FromRow};
use std::env;
use serde::{Deserialize, Serialize};
use rumqttc::{MqttOptions, AsyncClient, QoS, Event, Packet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::task;

// Load our custom modules
mod engine;
mod cli;

// --- Data Structures ---

#[derive(Deserialize, Debug)]
struct TelemetryData {
    value: f64,
    sensor_type: String,
}

#[derive(FromRow, Debug, Deserialize, Serialize, Clone)]
pub struct RuleFromDb {
    pub trigger_condition: String,
    pub trigger_value: f64,
    pub action_type: String,
    pub action_actuator_id: uuid::Uuid,
}

#[derive(Serialize, Debug)]
struct RuleForDevice {
    condition: String,
    threshold: f64,
    action: String,
    actuator_id: String,
}

#[derive(Deserialize, Debug)]
struct SimulateRequest {
    rule: RuleFromDb,
    data: TelemetryData,
}

// State to be shared with all HTTP handlers
#[allow(dead_code)]
struct AppState {
    db_pool: PgPool,
}

// --- HTTP API Handler ---

#[post("/simulate")]
async fn simulate_post(req_body: web::Json<SimulateRequest>) -> impl Responder {
    println!("\n--- [API /simulate] ---");
    let result = engine::simulator::simulate_rule(&req_body.rule, &req_body.data);
    let result_str = format!("{:?}", result);
    HttpResponse::Ok().json(serde_json::json!({ "simulation_result": result_str }))
}

// --- Application Entry Point ---

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Parse command-line arguments
    let cli_args = cli::Cli::parse();

    // Decide whether to run as a CLI tool or as a server
    match cli_args.command {
        Some(cli::Commands::Simulate { rule_file, data_file }) => {
            println!("CLI mode detected: Running simulation...");
            if let Err(e) = cli::run(&rule_file, &data_file) {
                eprintln!("CLI Error: {}", e);
                std::process::exit(1);
            }
        }
        None => {
            println!("Server mode detected: Starting HTTP server and MQTT client...");
            run_server().await?;
        }
    }

    Ok(())
}

// --- Server & MQTT Client Logic ---

async fn run_server() -> std::io::Result<()> {
    dotenv().ok();

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pool = PgPool::connect(&database_url).await.expect("Failed to connect to database");
    println!("Successfully connected to the database!");

    // Clone the pool to be moved into the async MQTT task
    let pool_clone_mqtt = pool.clone();
    // Spawn the MQTT client as a separate, parallel background task
    task::spawn(async move {
        run_mqtt_client(pool_clone_mqtt).await;
    });

    println!("Starting HTTP server at http://localhost:8080");

    // Create the shared state for the HTTP server
    let app_state = web::Data::new(AppState {
        db_pool: pool.clone(),
    });

    // Configure and run the Actix HTTP server
    HttpServer::new(move || {
        // Configure CORS to allow requests from our React frontend
        let cors = Cors::default()
              .allow_any_origin()
              .allow_any_method()
              .allow_any_header();

        App::new()
            .wrap(cors)
            .app_data(app_state.clone())
            .service(simulate_post) // Register the /simulate route
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}

/// Runs the resilient MQTT client with an auto-reconnect loop.
async fn run_mqtt_client(pool: PgPool) {
    // This is the outer reconnection loop.
    // If the connection is lost, it will wait 5s and restart from here.
    loop {
        // Generate a unique client ID to prevent broker conflicts
        let client_id = format!("agrisentry-gateway-{}", SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis());
        println!("Connecting to MQTT broker with client ID: {}", client_id);

        let mut mqttoptions = MqttOptions::new(client_id, "localhost", 1883);
        mqttoptions.set_keep_alive(Duration::from_secs(5)); // Send pings every 5s

        // Set security credentials
        mqttoptions.set_credentials("agrisentry_user", "Pessego30");

        let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10); // 10 = event buffer size

        // Attempt to subscribe to topics
        if let Err(e) = client.subscribe("agrisentry/devices/+/telemetry", QoS::AtLeastOnce).await {
            println!("[ERROR] Failed to subscribe to telemetry: {}. Retrying...", e);
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue; // Jump to the start of the reconnection loop
        }
        if let Err(e) = client.subscribe("agrisentry/devices/+/config/get", QoS::AtLeastOnce).await {
            println!("[ERROR] Failed to subscribe to config: {}. Retrying...", e);
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue; // Jump to the start of the reconnection loop
        }

        let client_clone = Arc::new(client);
        println!("MQTT Client is running and listening...");

        // This is the inner event loop, which runs while the connection is healthy.
        while let Ok(event) = eventloop.poll().await {
            if let Event::Incoming(Packet::Publish(p)) = event {
                // Clone client and pool for the async handler task
                let topic_handler_client = Arc::clone(&client_clone);
                let topic_handler_pool = pool.clone();
                
                // Spawn a new task to handle the message without blocking the event loop
                task::spawn(async move {
                    if p.topic.ends_with("/telemetry") {
                        handle_telemetry(&topic_handler_pool, &p.topic, &p.payload).await;
                    } else if p.topic.ends_with("/config/get") {
                        handle_config_request(&topic_handler_pool, &topic_handler_client, &p.topic).await;
                    }
                });
            }
        }

        // If the 'while let' loop breaks, poll() returned an Err.
        // This signifies the connection was lost.
        println!("[ERROR] MQTT Connection lost. Reconnecting in 5 seconds...");
        tokio::time::sleep(Duration::from_secs(5)).await;
        // The outer 'loop' will now restart, creating a fresh connection.
    }
}


// --- MQTT Message Handlers ---

async fn handle_telemetry(pool: &PgPool, topic: &str, payload: &[u8]) {
    println!("\n--- [TELEMETRY] ---");
    let data: TelemetryData = match serde_json::from_slice(payload) {
        Ok(d) => d,
        Err(e) => {
            println!("[ERROR] Failed to deserialize telemetry: {}", e);
            return;
        }
    };
    
    let mac_address = match topic.split('/').nth(2) {
        Some(mac) => mac,
        None => return,
    };

    let result = sqlx::query(
        "INSERT INTO sensor_readings (value, sensor_id) SELECT $1, s.id FROM sensors s WHERE s.name = $2"
    )
    .bind(data.value)
    .bind(mac_address)
    .execute(pool)
    .await;

    match result {
        Ok(res) if res.rows_affected() > 0 => println!("[SUCCESS] Telemetry stored."),
        // This warning is crucial: it means the data is valid but no device is registered with this MAC.
        Ok(_) => println!("[WARNING] Telemetry received, but no matching sensor found for MAC: {}", mac_address),
        Err(e) => println!("[ERROR] Database query failed while storing telemetry: {}", e),
    }
    println!("--- [TELEMETRY PROCESSED] ---");
}

async fn handle_config_request(pool: &PgPool, client: &Arc<AsyncClient>, topic: &str) {
    println!("\n--- [CONFIG REQUEST] ---");
    println!("[1] Received config request on topic: {}", topic);

    let mac_address = match topic.split('/').nth(2) {
        Some(mac) => mac,
        None => {
            println!("[ERROR] Could not extract MAC from topic.");
            return;
        }
    };

    println!("[2] Fetching active rule for MAC: {}", mac_address);

    let rule_result = sqlx::query_as::<_, RuleFromDb>(
        r#"
        SELECT r.trigger_condition, r.trigger_value, r.action_type, r.action_actuator_id
        FROM rules r
        JOIN sensors s ON r.trigger_sensor_id = s.id
        WHERE s.name = $1
        AND r.is_active = true   -- Ensures we only get the active rule
        ORDER BY r.version DESC  -- Gets the newest version if multiple are somehow active
        LIMIT 1
        "#
    )
    .bind(mac_address)
    .fetch_optional(pool) // Use fetch_optional to gracefully handle no-rule-found
    .await;

    let response_topic = format!("agrisentry/devices/{}/config/set", mac_address);

    match rule_result {
        Ok(Some(rule_from_db)) => {
            // Found a rule, now validate it
            match engine::validator::validate_rule(&rule_from_db) {
                Ok(_) => {
                    let rule_for_device = RuleForDevice {
                        condition: rule_from_db.trigger_condition.clone(),
                        threshold: rule_from_db.trigger_value,
                        action: rule_from_db.action_type.clone(),
                        actuator_id: rule_from_db.action_actuator_id.to_string(), // Convert UUID to String
                    };
                    
                    let payload = serde_json::to_string(&rule_for_device).unwrap();
                    println!("[3] Active rule is valid. Sending config to topic: {}", response_topic);
                    client.publish(&response_topic, QoS::AtLeastOnce, false, payload).await.unwrap();
                }
                Err(e) => {
                    // The rule in the DB is invalid, send empty config
                    println!("[3Warning] Rule is invalid: {}. Sending empty config.", e);
                    client.publish(&response_topic, QoS::AtLeastOnce, false, "{}").await.unwrap();
                }
            }
        }
        Ok(None) => {
            // This is not an error; the device simply has no rules assigned
            println!("[3] No active rule found for this MAC. Sending empty config.");
            client.publish(&response_topic, QoS::AtLeastOnce, false, "{}").await.unwrap();
        }
        Err(e) => {
            println!("[ERROR] Failed to fetch rule from database: {}", e);
        }
    }
    println!("--- [CONFIG REQUEST PROCESSED] ---");
}