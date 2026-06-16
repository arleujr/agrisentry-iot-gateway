use thiserror::Error;

#[derive(Error, Debug)]
pub enum GatewayError {
    #[error("Falha na comunicação com o banco de dados: {0}")]
    DatabaseError(#[from] sqlx::Error),

    #[error("Falha no parse do payload JSON (Possível dado corrompido): {0}")]
    PayloadParseError(#[from] serde_json::Error),

    #[error("Erro interno do broker MQTT: {0}")]
    MqttError(String),

    #[error("Erro de validação de regra de negócio: {0}")]
    ValidationError(String),
}