use crate::log::types::SequenceNumber;
use crate::types::{DaemonWriterId, OutboxId, WorkspaceId};
use eyre::eyre;
use s2_sdk::types::{AccountEndpoint, BasinEndpoint, BasinName, EncryptionKey, S2Endpoints};
use std::ops::RangeTo;

#[derive(Debug, Clone)]
pub struct Row {
    pub workspace_id: WorkspaceId,
    pub s2_basin: BasinName,
    pub s2_account_endpoint: Option<String>,
    pub s2_basin_endpoint: Option<String>,
    pub daemon_writer_id: DaemonWriterId,
    /// The portion of the shared log that has been applied to the semantic state.
    ///  - (..0) means we have never applied the log
    ///  - (..1) means we have applied only a single message at seqNum=0
    pub stable_cursor: RangeTo<SequenceNumber>,
    pub next_outbox_id: OutboxId,
    pub encryption_key: Option<EncryptionKey>,
}

impl Row {
    pub fn from_sql_row(row: &turso::Row) -> eyre::Result<Self> {
        let workspace_id = WorkspaceId(row.get::<String>(0)?);
        let s2_basin_raw = row.get::<String>(1)?;
        let daemon_writer_id = DaemonWriterId(row.get::<Vec<u8>>(2)?.into());
        let stable_cursor_raw = row.get::<i64>(3)?;
        let next_outbox_id_raw = row.get::<i64>(4)?;
        let s2_account_endpoint =
            optional_endpoint(row.get::<Option<String>>(5)?, "s2_account_endpoint")?;
        let s2_basin_endpoint =
            optional_endpoint(row.get::<Option<String>>(6)?, "s2_basin_endpoint")?;
        let encryption_key_raw = row.get::<Option<String>>(7)?;

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
        validate_endpoint_pair(&s2_account_endpoint, &s2_basin_endpoint)?;

        let encryption_key = encryption_key_raw
            .map(|s| s.parse::<EncryptionKey>())
            .transpose()
            .map_err(|err| eyre!("invalid daemon_state.encryption_key: {err}"))?;

        Ok(Self {
            workspace_id,
            s2_basin,
            s2_account_endpoint,
            s2_basin_endpoint,
            daemon_writer_id,
            stable_cursor: ..stable_cursor_end,
            next_outbox_id,
            encryption_key,
        })
    }
}

fn optional_endpoint(raw: Option<String>, column: &str) -> eyre::Result<Option<String>> {
    let Some(value) = raw else {
        return Ok(None);
    };
    if value.is_empty() {
        return Err(eyre!("daemon_state.{column} is empty"));
    }
    Ok(Some(value))
}

fn validate_endpoint_pair(
    account_endpoint: &Option<String>,
    basin_endpoint: &Option<String>,
) -> eyre::Result<()> {
    match (account_endpoint, basin_endpoint) {
        (Some(account_endpoint), Some(basin_endpoint)) => {
            let account_endpoint = AccountEndpoint::new(account_endpoint)
                .map_err(|err| eyre!("invalid daemon_state.s2_account_endpoint: {err}"))?;
            let basin_endpoint = BasinEndpoint::new(basin_endpoint)
                .map_err(|err| eyre!("invalid daemon_state.s2_basin_endpoint: {err}"))?;
            S2Endpoints::new(account_endpoint, basin_endpoint)
                .map_err(|err| eyre!("invalid daemon_state S2 endpoint pair: {err}"))?;
        }
        (Some(_), None) => {
            return Err(eyre!(
                "daemon_state.s2_account_endpoint is set but s2_basin_endpoint is missing"
            ));
        }
        (None, Some(_)) => {
            return Err(eyre!(
                "daemon_state.s2_basin_endpoint is set but s2_account_endpoint is missing"
            ));
        }
        (None, None) => {}
    }
    Ok(())
}
