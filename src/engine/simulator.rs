// src/engine/simulator.rs

use crate::models::{RuleFromDb, TelemetryData};

/// Represents the potential outcome of a rule simulation.
/// Using an enum is safer and more professional than raw strings.
#[derive(Debug, PartialEq)]
pub enum ActionResult {
    TurnOn,
    TurnOff,
    DoNothing,
}

/// The main simulation function.
/// It evaluates a given rule against a set of sensor data and returns an action.
pub fn simulate_rule(rule: &RuleFromDb, data: &TelemetryData) -> ActionResult {
    println!(
        "[SIMULATOR] Simulating rule with sensor value: {}",
        data.value
    );

    let mut should_activate = false;

    // Evaluate the rule's trigger condition
    match rule.trigger_condition.as_str() {
        "LESS_THAN" if data.value < rule.trigger_value => {
            should_activate = true;
        }
        "GREATER_THAN" if data.value > rule.trigger_value => {
            should_activate = true;
        }
        "EQUAL_TO" if data.value == rule.trigger_value => {
            should_activate = true;
        }
        // If the condition is not met or not recognized, do nothing.
        _ => {}
    }

    println!("[SIMULATOR] Condition met: {}", should_activate);

    // Determine the final action based on the condition result
    if should_activate {
        match rule.action_type.as_str() {
            "TURN_ON" => {
                println!("[SIMULATOR] Result: TurnOn");
                ActionResult::TurnOn
            }
            "TURN_OFF" => {
                println!("[SIMULATOR] Result: TurnOff");
                ActionResult::TurnOff
            }
            _ => ActionResult::DoNothing,
        }
    } else {
        println!("[SIMULATOR] Result: DoNothing");
        ActionResult::DoNothing
    }
}
