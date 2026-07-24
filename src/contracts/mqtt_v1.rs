use std::collections::HashSet;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

pub const TELEMETRY_TOPIC_FILTER: &str = "agrisentry/v1/devices/+/telemetry";
pub const PROTOCOL_VERSION: &str = "1.0";

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TelemetryChannel {
    AirTemperature,
    AirRelativeHumidity,
    SolutionTemperature,
    SolutionPh,
    SolutionTds,
    ReservoirLevel,
    RelativeLight,
}

impl TelemetryChannel {
    pub const fn expected_unit(self) -> TelemetryUnit {
        match self {
            Self::AirTemperature | Self::SolutionTemperature => TelemetryUnit::Celsius,
            Self::AirRelativeHumidity | Self::ReservoirLevel | Self::RelativeLight => {
                TelemetryUnit::Percent
            }
            Self::SolutionPh => TelemetryUnit::Ph,
            Self::SolutionTds => TelemetryUnit::Ppm,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AirTemperature => "air_temperature",
            Self::AirRelativeHumidity => "air_relative_humidity",
            Self::SolutionTemperature => "solution_temperature",
            Self::SolutionPh => "solution_ph",
            Self::SolutionTds => "solution_tds",
            Self::ReservoirLevel => "reservoir_level",
            Self::RelativeLight => "relative_light",
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TelemetryUnit {
    Celsius,
    Percent,
    Ph,
    Ppm,
}

impl TelemetryUnit {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Celsius => "celsius",
            Self::Percent => "percent",
            Self::Ph => "ph",
            Self::Ppm => "ppm",
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TelemetryQuality {
    Valid,
    Estimated,
    Unstable,
    OutOfRange,
    SensorError,
    NotCalibrated,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TelemetryReadingV1 {
    pub channel: TelemetryChannel,
    pub raw_value: f64,
    pub value: Option<f64>,
    pub unit: TelemetryUnit,
    pub quality: TelemetryQuality,
    #[serde(default)]
    pub calibration_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TelemetryEnvelopeV1 {
    pub protocol_version: String,
    pub event_id: Uuid,
    pub device_id: String,
    pub sequence: u64,
    pub observed_at: DateTime<Utc>,
    pub firmware_version: String,
    pub readings: Vec<TelemetryReadingV1>,
}

#[derive(Debug, Error)]
pub enum TelemetryContractError {
    #[error("invalid telemetry topic: {0}")]
    InvalidTopic(String),

    #[error("invalid telemetry JSON: {0}")]
    InvalidJson(#[from] serde_json::Error),

    #[error("unsupported protocol version '{0}'")]
    UnsupportedProtocolVersion(String),

    #[error("invalid device id '{0}'")]
    InvalidDeviceId(String),

    #[error("topic device id '{topic_device_id}' does not match payload device id '{payload_device_id}'")]
    DeviceIdMismatch {
        topic_device_id: String,
        payload_device_id: String,
    },

    #[error("firmware version must use a semantic version such as 2.0.0-alpha.1")]
    InvalidFirmwareVersion,

    #[error("telemetry must contain between 1 and 32 readings")]
    InvalidReadingCount,

    #[error("duplicated telemetry channel '{0}'")]
    DuplicateChannel(String),

    #[error("channel '{channel}' requires unit '{expected}', received '{received}'")]
    UnitMismatch {
        channel: String,
        expected: String,
        received: String,
    },

    #[error("channel '{channel}' contains a non-finite numeric value")]
    NonFiniteValue { channel: String },

    #[error("calibration id cannot be empty when provided")]
    EmptyCalibrationId,
}

pub fn parse_telemetry_message(
    topic: &str,
    payload: &[u8],
) -> Result<TelemetryEnvelopeV1, TelemetryContractError> {
    let topic_device_id = telemetry_topic_device_id(topic)?;
    let envelope: TelemetryEnvelopeV1 = serde_json::from_slice(payload)?;

    validate_telemetry_envelope(topic_device_id, &envelope)?;

    Ok(envelope)
}

pub fn telemetry_topic_device_id(topic: &str) -> Result<&str, TelemetryContractError> {
    let mut parts = topic.split('/');
    let root = parts.next();
    let version = parts.next();
    let resource = parts.next();
    let device_id = parts.next();
    let message_type = parts.next();
    let has_extra_part = parts.next().is_some();

    match (
        root,
        version,
        resource,
        device_id,
        message_type,
        has_extra_part,
    ) {
        (
            Some("agrisentry"),
            Some("v1"),
            Some("devices"),
            Some(device_id),
            Some("telemetry"),
            false,
        ) if !device_id.is_empty() => Ok(device_id),
        _ => Err(TelemetryContractError::InvalidTopic(topic.to_string())),
    }
}

pub fn validate_telemetry_envelope(
    topic_device_id: &str,
    envelope: &TelemetryEnvelopeV1,
) -> Result<(), TelemetryContractError> {
    if envelope.protocol_version != PROTOCOL_VERSION {
        return Err(TelemetryContractError::UnsupportedProtocolVersion(
            envelope.protocol_version.clone(),
        ));
    }

    if !is_valid_device_id(topic_device_id) {
        return Err(TelemetryContractError::InvalidDeviceId(
            topic_device_id.to_string(),
        ));
    }

    if !is_valid_device_id(&envelope.device_id) {
        return Err(TelemetryContractError::InvalidDeviceId(
            envelope.device_id.clone(),
        ));
    }

    if topic_device_id != envelope.device_id {
        return Err(TelemetryContractError::DeviceIdMismatch {
            topic_device_id: topic_device_id.to_string(),
            payload_device_id: envelope.device_id.clone(),
        });
    }

    if !is_semantic_version(&envelope.firmware_version) {
        return Err(TelemetryContractError::InvalidFirmwareVersion);
    }

    if envelope.readings.is_empty() || envelope.readings.len() > 32 {
        return Err(TelemetryContractError::InvalidReadingCount);
    }

    let mut channels = HashSet::with_capacity(envelope.readings.len());

    for reading in &envelope.readings {
        if !channels.insert(reading.channel) {
            return Err(TelemetryContractError::DuplicateChannel(
                reading.channel.as_str().to_string(),
            ));
        }

        let expected_unit = reading.channel.expected_unit();
        if reading.unit != expected_unit {
            return Err(TelemetryContractError::UnitMismatch {
                channel: reading.channel.as_str().to_string(),
                expected: expected_unit.as_str().to_string(),
                received: reading.unit.as_str().to_string(),
            });
        }

        if !reading.raw_value.is_finite() || reading.value.is_some_and(|value| !value.is_finite()) {
            return Err(TelemetryContractError::NonFiniteValue {
                channel: reading.channel.as_str().to_string(),
            });
        }

        if reading
            .calibration_id
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            return Err(TelemetryContractError::EmptyCalibrationId);
        }
    }

    Ok(())
}

fn is_valid_device_id(device_id: &str) -> bool {
    let length = device_id.len();
    if !(1..=64).contains(&length) || device_id.starts_with('-') || device_id.ends_with('-') {
        return false;
    }

    device_id
        .bytes()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

fn is_semantic_version(version: &str) -> bool {
    let core = version
        .split_once('+')
        .map_or(version, |(without_build, _)| without_build);
    let core = core
        .split_once('-')
        .map_or(core, |(without_prerelease, _)| without_prerelease);
    let mut parts = core.split('.');

    let major = parts.next();
    let minor = parts.next();
    let patch = parts.next();

    parts.next().is_none()
        && [major, minor, patch].into_iter().all(|part| {
            part.is_some_and(|value| {
                !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
            })
        })
}
