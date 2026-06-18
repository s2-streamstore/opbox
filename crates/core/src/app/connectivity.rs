use serde::{Deserialize, Serialize};
use std::time::Duration;
use time::OffsetDateTime;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectivityRole {
    Reader,
    Writer,
}

impl ConnectivityRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Reader => "reader",
            Self::Writer => "writer",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectivityOverallState {
    Online,
    Reconnecting,
    Offline,
    Degraded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LinkState {
    Online,
    Reconnecting,
    Offline,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinkStatus {
    pub state: LinkState,
    pub last_error: Option<String>,
    pub retry_at_ns: Option<i128>,
}

impl LinkStatus {
    pub fn online() -> Self {
        Self {
            state: LinkState::Online,
            last_error: None,
            retry_at_ns: None,
        }
    }

    pub fn reconnecting(last_error: Option<String>) -> Self {
        Self {
            state: LinkState::Reconnecting,
            last_error,
            retry_at_ns: None,
        }
    }

    pub fn offline(last_error: String, retry_at: OffsetDateTime) -> Self {
        Self {
            state: LinkState::Offline,
            last_error: Some(last_error),
            retry_at_ns: Some(retry_at.unix_timestamp_nanos()),
        }
    }

    pub fn retry_after(&self, now: OffsetDateTime) -> Option<Duration> {
        let retry_at_ns = self.retry_at_ns?;
        let now_ns = now.unix_timestamp_nanos();
        let remaining_ns = retry_at_ns.saturating_sub(now_ns).max(0);
        Some(Duration::from_nanos(
            u64::try_from(remaining_ns).unwrap_or(u64::MAX),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectivitySnapshot {
    pub overall: ConnectivityOverallState,
    pub reader: LinkStatus,
    pub writer: LinkStatus,
    pub updated_at_ns: i128,
}

impl ConnectivitySnapshot {
    pub fn starting() -> Self {
        Self::from_links(
            LinkStatus::reconnecting(None),
            LinkStatus::reconnecting(None),
        )
    }

    pub fn from_links(reader: LinkStatus, writer: LinkStatus) -> Self {
        let overall = match (reader.state, writer.state) {
            (LinkState::Online, LinkState::Online) => ConnectivityOverallState::Online,
            (LinkState::Reconnecting, _) | (_, LinkState::Reconnecting) => {
                ConnectivityOverallState::Reconnecting
            }
            (LinkState::Offline, LinkState::Offline) => ConnectivityOverallState::Offline,
            (LinkState::Online, LinkState::Offline) | (LinkState::Offline, LinkState::Online) => {
                ConnectivityOverallState::Degraded
            }
        };

        Self {
            overall,
            reader,
            writer,
            updated_at_ns: OffsetDateTime::now_utc().unix_timestamp_nanos(),
        }
    }
}
