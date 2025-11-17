use crate::RuleFromDb;

/// Checks if a rule fetched from the database is valid before sending it to a device.
/// Returns Ok(()) on success or an error message string on failure.
pub fn validate_rule(rule: &RuleFromDb) -> Result<(), String> {
    println!("[VALIDATOR] Running validation for rule...");

    // 1. Validate the trigger condition
    match rule.trigger_condition.as_str() {
        "LESS_THAN" | "GREATER_THAN" | "EQUAL_TO" => {
            // This condition is valid, proceed.
            println!("[VALIDATOR] Condition '{}' is valid.", rule.trigger_condition);
        }
        _ => {
            // This is an unknown or unsupported condition.
            let error_message = format!("[VALIDATOR] Error: Invalid trigger condition '{}'.", rule.trigger_condition);
            println!("{}", error_message);
            return Err(error_message);
        }
    }

    // 2. Validate the action type
    match rule.action_type.as_str() {
        "TURN_ON" | "TURN_OFF" => {
            // This action is valid.
            println!("[VALIDATOR] Action '{}' is valid.", rule.action_type);
        }
        _ => {
            let error_message = format!("[VALIDATOR] Error: Invalid action type '{}'.", rule.action_type);
            println!("{}", error_message);
            return Err(error_message);
        }
    }
    
    // Add more validation checks here in the future.
    // Example:
    // if rule.trigger_value < 0.0 {
    //     return Err("Trigger value cannot be negative.".to_string());
    // }

    Ok(())
}