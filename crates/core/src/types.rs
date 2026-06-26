//! Shared types.

use crate::log::types::{SequenceNumber, SharedMessageEnvelope};
use base64::Engine;
use bytes::Bytes;
use fast32::base32;
use std::fmt::Display;
use std::ops::RangeInclusive;
use std::str::FromStr;

/// Stable identity for a daemon within a workspace.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DaemonWriterId(pub Bytes);

impl DaemonWriterId {
    pub fn encode_b64(&self) -> String {
        base64::engine::general_purpose::STANDARD.encode(self.0.as_ref())
    }
}

pub fn crockford_base32_lower(bytes: &[u8]) -> String {
    base32::CROCKFORD_LOWER.encode(bytes)
}

pub fn short_crockford_base32_lower(bytes: &[u8]) -> String {
    let encoded = crockford_base32_lower(bytes);
    encoded[..6.min(encoded.len())].to_string()
}

pub fn short_crockford_base32_lower_from_b64(value: &str) -> String {
    match base64::engine::general_purpose::STANDARD.decode(value) {
        Ok(bytes) => short_crockford_base32_lower(&bytes),
        Err(_) => value.chars().take(8).collect(),
    }
}

/// ID for a message within the durable operation outbox.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OutboxId(u64);

impl OutboxId {
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn get(self) -> u64 {
        self.0
    }

    pub fn checked_next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

/// Stable workspace id.
#[derive(Debug, Clone)]
pub struct WorkspaceId(pub String);

impl Display for WorkspaceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl WorkspaceId {
    pub fn generate() -> Self {
        let workspace_id_bytes = rand::random::<[u8; 20]>();
        let workspace_str = crockford_base32_lower(workspace_id_bytes.as_ref());
        assert_eq!(workspace_str.len(), 32);
        Self(workspace_str)
    }
}

impl FromStr for WorkspaceId {
    type Err = eyre::Report;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != 32 {
            eyre::bail!("workspace id must be 32 characters");
        }

        if !value
            .bytes()
            .all(|byte: u8| byte.is_ascii_digit() || byte.is_ascii_alphabetic())
        {
            eyre::bail!("workspace id must be alphanumeric");
        }

        Ok(Self(value.to_ascii_lowercase()))
    }
}

pub struct SharedMessageBatch {
    pub sequence_range: RangeInclusive<SequenceNumber>,
    pub messages: Vec<SharedMessageEnvelope>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_crockford_base32_decodes_b64_ids() {
        let bytes = [7u8; 16];
        let b64 = base64::engine::general_purpose::STANDARD.encode(bytes);

        assert_eq!(
            short_crockford_base32_lower_from_b64(&b64),
            &crockford_base32_lower(&bytes)[..6]
        );
    }

    #[test]
    fn short_crockford_base32_falls_back_for_non_b64_values() {
        assert_eq!(
            short_crockford_base32_lower_from_b64("not actually base64"),
            "not actu"
        );
    }
}

impl TryFrom<Vec<SharedMessageEnvelope>> for SharedMessageBatch {
    type Error = eyre::Error;

    fn try_from(value: Vec<SharedMessageEnvelope>) -> Result<Self, Self::Error> {
        if value.is_empty() {
            return Err(eyre::eyre!("empty batch"));
        }
        let first = value.first().expect("non-empty batch").sequence_number;
        let last = value.last().expect("non-empty batch").sequence_number;
        if first > last {
            return Err(eyre::eyre!("sequence numbers out of order"));
        }
        let sequence_range = first..=last;

        let mut running = first;
        let iter = value.iter().skip(1);
        for message in iter {
            if message.sequence_number != running + 1 {
                return Err(eyre::eyre!("sequence numbers not contiguous"));
            }
            running = message.sequence_number;
        }
        Ok(Self {
            sequence_range,
            messages: value,
        })
    }
}
