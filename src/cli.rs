// src/cli.rs

use clap::{Parser, Subcommand};
use crate::engine::simulator;
use crate::{RuleFromDb, TelemetryData};

/// Defines the main CLI structure.
/// This acts as the entry point for parsing command-line arguments.
#[derive(Parser, Debug)]
#[command(author, version, about = "AgroCore: IoT Gateway & Simulation Engine")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,
}

/// Defines the available subcommands for the CLI.
#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Simulates a single rule execution using local JSON files.
    Simulate {
        /// Path to the JSON file containing the rule definition.
        #[arg(short, long)]
        rule_file: String,

        /// Path to the JSON file containing the sensor data for simulation.
        #[arg(short, long)]
        data_file: String,
    },
    // Future subcommands like 'validate', 'deploy', etc. can be added here.
}

/// The main execution function for the `simulate` subcommand.
pub fn run(rule_file: &str, data_file: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- AgroCore CLI Simulator ---");

    // 1. Read and parse the rule file
    let rule_content = std::fs::read_to_string(rule_file)?;
    let rule: RuleFromDb = serde_json::from_str(&rule_content)?;

    // 2. Read and parse the data file
    let data_content = std::fs::read_to_string(data_file)?;
    let data: TelemetryData = serde_json::from_str(&data_content)?;

    println!("Simulating with Rule: {:?}", rule);
    println!("And Data: {:?}", data);

    // 3. Run the simulation using the core engine
    let result = simulator::simulate_rule(&rule, &data);

    println!("\n--- Simulation Result ---");
    println!("{:?}", result);
    println!("-------------------------\n");

    Ok(())
}