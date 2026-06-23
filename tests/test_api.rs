use actix_web::{test, App};
use agrisentry_iot_gateway::api::config_services;

// Import the shared fixture module
mod common;

#[actix_web::test]
async fn test_integration_health_gateway_returns_200() {
    // Arrange: Setup shared database/app context using our fixture
    let _test_db = common::setup_test_context().await;
    
    let app = test::init_service(
        App::new().configure(config_services)
    ).await;

    // Act: Fire a real HTTP Request against the app instance
    let req = test::TestRequest::get().uri("/health").to_request();
    let resp = test::call_service(&app, req).await;

    // Assert: Verify contract preservation and HTTP standards
    assert!(resp.status().is_success(), "Integration test failed: /health is unreachable.");
}