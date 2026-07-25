const COMPOSE: &str = include_str!("../compose.local.yaml");
const MOSQUITTO: &str = include_str!("../infra/mosquitto.conf");
const E2E_SCRIPT: &str = include_str!("../scripts/local/test-e2e.ps1");
const RUN_GATEWAY_SCRIPT: &str = include_str!("../scripts/local/run-gateway.ps1");

#[test]
fn local_services_are_bound_to_loopback() {
    assert!(COMPOSE.contains("127.0.0.1:5432:5432"));
    assert!(COMPOSE.contains("127.0.0.1:1883:1883"));
}

#[test]
fn local_database_image_is_pinned() {
    assert!(COMPOSE.contains("postgres:16-alpine"));
}

#[test]
fn anonymous_mqtt_is_explicitly_local_only() {
    assert!(MOSQUITTO.contains("allow_anonymous true"));
    assert!(MOSQUITTO.contains("Local development broker only"));
    assert!(COMPOSE.contains("127.0.0.1:1883:1883"));
}

#[test]
fn e2e_test_proves_duplicate_suppression_and_complete_batch() {
    assert!(E2E_SCRIPT.contains("Messages published: 2"));
    assert!(E2E_SCRIPT.contains("expected 1 event"));
    assert!(E2E_SCRIPT.contains("expected 7 readings"));
}

#[test]
fn local_gateway_disables_the_legacy_analysis_worker() {
    assert!(RUN_GATEWAY_SCRIPT.contains(r#"$env:ANALYSIS_WORKER_ENABLED = "false""#));
}
