use crate::log::types::SequenceNumber;
use crate::types::{DaemonWriterId, OutboxId, WorkspaceId};
use eyre::eyre;
use s2_sdk::types::BasinName;
use std::ops::RangeTo;

#[derive(Debug, Clone)]
pub struct Row {
    pub workspace_id: WorkspaceId,
    pub s2_basin: BasinName,
    pub daemon_writer_id: DaemonWriterId,
    /// The portion of the shared log that has been applied to the semantic state.
    ///  - (..0) means we have never applied the log
    ///  - (..1) means we have applied only a single message at seqNum=0
    pub stable_cursor: RangeTo<SequenceNumber>,
    pub next_outbox_id: OutboxId,
}

impl Row {
    pub fn from_sql_row(row: &turso::Row) -> eyre::Result<Self> {
        let workspace_id = WorkspaceId(row.get::<String>(0)?);
        let s2_basin_raw = row.get::<String>(1)?;
        let daemon_writer_id = DaemonWriterId(row.get::<Vec<u8>>(2)?.into());
        let stable_cursor_raw = row.get::<i64>(3)?;
        let next_outbox_id_raw = row.get::<i64>(4)?;

        if workspace_id.0.is_empty() {
            return Err(eyre!("daemon_state.workspace_id missing"));
        }
        if s2_basin_raw.is_empty() {
            return Err(eyre!("daemon_state.s2_basin missing"));
        }
        if daemon_writer_id.0.is_empty() {
            return Err(eyre!("daemon_state.writer_id missing"));
        }

        let stable_cursor_end = u64::try_from(stable_cursor_raw)
            .map_err(|_| eyre!("daemon_state.stable_cursor negative: {stable_cursor_raw}"))?;
        if next_outbox_id_raw < 0 {
            return Err(eyre!(
                "daemon_state.next_outbox_id negative: {next_outbox_id_raw}"
            ));
        }
        let next_outbox_id = OutboxId::new(next_outbox_id_raw as u64);

        let s2_basin = s2_basin_raw
            .parse()
            .map_err(|err| eyre!("invalid daemon_state.s2_basin {s2_basin_raw:?}: {err}"))?;

        Ok(Self {
            workspace_id,
            s2_basin,
            daemon_writer_id,
            stable_cursor: ..stable_cursor_end,
            next_outbox_id,
        })
    }
}
