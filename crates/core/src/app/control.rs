use crate::app::connectivity::ConnectivitySnapshot;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, strum::IntoStaticStr)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum EnginePhaseStatus {
    Idle,
    AwaitingNextWork,
    Scanning,
    PlanningImport,
    Importing,
    Projecting,
}

impl std::fmt::Display for EnginePhaseStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let text: &'static str = (*self).into();
        f.write_str(text)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StreamRetentionSummary {
    Age { seconds: u64 },
    Unspecified,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DaemonWarning {
    OpsStreamRetentionNotInfinite { retention: StreamRetentionSummary },
    BasinDefaultStreamRetentionNotInfinite { retention: StreamRetentionSummary },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonStatus {
    pub workspace_id: String,
    pub basin: String,
    pub root: String,
    pub pid: u32,
    pub stable_cursor_end: u64,
    pub daemon_writer_id_b64: String,
    pub started_at_ns: i64,
    pub engine_phase: EnginePhaseStatus,
    pub connectivity: ConnectivitySnapshot,
    pub warnings: Vec<DaemonWarning>,
}
