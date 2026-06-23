use crate::models::{RuleFromDb, SensorPayload};

/// Validates and parses the incoming raw JSON string into a SensorPayload.
/// Ensures graceful error handling instead of panicking.
pub fn parse_and_validate_telemetry(raw_json: &str) -> Result<SensorPayload, String> {
    serde_json::from_str::<SensorPayload>(raw_json)
        .map_err(|e| format!("Failed to parse sensor payload: {}", e))
}

/// Checks if a rule fetched from the database is valid before sending it to a device.
/// Returns Ok(()) on success or an error message string on failure.
pub fn validate_rule(rule: &RuleFromDb) -> Result<(), String> {
    println!("[VALIDATOR] Running validation for rule...");

    // 1. Validate the trigger condition
    match rule.trigger_condition.as_str() {
        "LESS_THAN" | "GREATER_THAN" | "EQUAL_TO" => {
            println!(
                "[VALIDATOR] Condition '{}' is valid.",
                rule.trigger_condition
            );
        }
        _ => {
            let error_message = format!(
                "[VALIDATOR] Error: Invalid trigger condition '{}'.",
                rule.trigger_condition
            );
            println!("{}", error_message);
            return Err(error_message);
        }
    }

    // 2. Validate the action type
    match rule.action_type.as_str() {
        "TURN_ON" | "TURN_OFF" => {
            println!("[VALIDATOR] Action '{}' is valid.", rule.action_type);
        }
        _ => {
            let error_message = format!(
                "[VALIDATOR] Error: Invalid action type '{}'.",
                rule.action_type
            );
            println!("{}", error_message);
            return Err(error_message);
        }
    }

    // Future enhancement safety check example:
    if rule.trigger_value.is_nan() {
        return Err("[VALIDATOR] Error: Trigger value cannot be NaN.".to_string());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // TELEMETRY INGESTION TESTS
    // =========================================================================

    #[test]
    fn test_parse_perfect_json_payload_success() {
        let perfect_json = r#"{
            "device_id": "urn:agrisentry:sensor:soil:001",
            "sensor_type": "moisture",
            "reading_value": 42.58,
            "timestamp": "2026-06-23T14:44:00Z",
            "metadata_hash": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        }"#;

        let result = parse_and_validate_telemetry(perfect_json);
        assert!(result.is_ok());
    }

    // =========================================================================
    // DB RULE VALIDATION TESTS (YOUR FUNCTION)
    // =========================================================================

    #[test]
    fn test_validate_rule_with_valid_parameters_success() {
        let valid_rule = RuleFromDb {
            trigger_condition: "GREATER_THAN".to_string(),
            trigger_value: 35.5,
            action_type: "TURN_ON".to_string(),
        };

        let result = validate_rule(&valid_rule);
        assert!(result.is_ok(), "Valid rules must return Ok(())");
    }

    #[test]
    fn test_validate_rule_with_invalid_condition_fails_gracefully() {
        let bad_rule = RuleFromDb {
            trigger_condition: "UNKNOWN_COMPARE_OPERATOR".to_string(),
            trigger_value: 10.0,
            action_type: "TURN_OFF".to_string(),
        };

        let result = validate_rule(&bad_rule);
        assert!(
            result.is_err(),
            "Invalid trigger conditions must return an Err"
        );
        assert!(result.unwrap_err().contains("Invalid trigger condition"));
    }

    #[test]
    fn test_validate_rule_with_invalid_action_fails_gracefully() {
        let bad_rule = RuleFromDb {
            trigger_condition: "LESS_THAN".to_string(),
            trigger_value: 12.0,
            action_type: "EXPLODE_DEVICE".to_string(), // Invalid action
        };

        let result = validate_rule(&bad_rule);
        assert!(result.is_err(), "Invalid action types must return an Err");
        assert!(result.unwrap_err().contains("Invalid action type"));
    }
}
