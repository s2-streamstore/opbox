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
        let workspace_str = base32::CROCKFORD_LOWER.encode(workspace_id_bytes.as_ref());
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
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'z' | b'A'..=b'Z'))
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
