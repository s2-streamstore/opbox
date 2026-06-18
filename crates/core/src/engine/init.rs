use crate::fs::client::{FsClient, FsClientResponse};
use crate::fs::types::ScanScope;
use crate::log::types::{LogWriterRequest, LogWriterResponse};
use crate::semantic::client::{SemanticClient, SemanticClientResponse};
use crate::semantic::types::{ImportActionResult, NextWork};
use crate::types::OutboxId;
use eyre::eyre;
use tokio::sync::mpsc;
use tracing::debug;

const INIT_OUTBOX_READ_BATCH_SIZE: u64 = 1024;

pub struct InitClients {
    pub fs: FsClient,
    pub semantic: SemanticClient,
    pub log_writer: mpsc::UnboundedSender<LogWriterRequest>,
}

pub struct InitEvents {
    pub log_writer: mpsc::UnboundedReceiver<LogWriterResponse>,
}

pub struct InitConfig {
    pub clients: InitClients,
    pub events: InitEvents,
}

#[derive(Debug, Default)]
pub struct InitResult {
    pub imported_actions: usize,
    pub appended_outbox_messages: usize,
}

pub async fn run(config: InitConfig) -> eyre::Result<InitResult> {
    let InitConfig { clients, events } = config;
    let InitClients {
        mut fs,
        mut semantic,
        log_writer,
    } = clients;
    let InitEvents {
        log_writer: mut log_writer_rx,
    } = events;

    fs.scan(ScanScope::Full)?;
    let scan = await_scan(&mut fs).await?;

    semantic.apply_init_scan(scan)?;
    let next_work = await_apply_init_scan(&mut semantic).await?;

    let imported_actions = match next_work {
        NextWork::None => 0,
        NextWork::Project(_) => {
            eyre::bail!("init scan returned projection work before import completed");
        }
        NextWork::Import(started) => {
            let mut imported_actions = 0;
            for action in started.plan.actions {
                debug!(?action, "applying import action");
                fs.apply_import_action(action)?;
                let result = await_import_action(&mut fs).await?;
                debug!(?result, "import action completed");
                semantic.commit_import_action(result)?;
                imported_actions += 1;
            }

            // Semantic owns epoch accounting. This is the barrier that must
            // verify all action commits for the epoch were durably accepted.
            semantic.commit_import_epoch(started.epoch)?;
            await_commit_import_epoch(&mut semantic).await?;

            imported_actions
        }
    };

    let appended_outbox_messages =
        drain_outbox(&mut semantic, &log_writer, &mut log_writer_rx).await?;

    Ok(InitResult {
        imported_actions,
        appended_outbox_messages,
    })
}

async fn await_scan(fs: &mut FsClient) -> eyre::Result<crate::fs::types::ScanResult> {
    match fs
        .next()
        .await
        .ok_or_else(|| eyre!("fs actor stopped while init waited for scan"))?
    {
        FsClientResponse::Scan { result } => result,
        _ => Err(eyre!("unexpected fs response while init waited for scan")),
    }
}

async fn await_import_action(fs: &mut FsClient) -> eyre::Result<ImportActionResult> {
    match fs
        .next()
        .await
        .ok_or_else(|| eyre!("fs actor stopped while init waited for import action"))?
    {
        FsClientResponse::ApplyImportAction { result } => Ok(result),
        _ => Err(eyre!(
            "unexpected fs response while init waited for import action"
        )),
    }
}

async fn await_apply_init_scan(semantic: &mut SemanticClient) -> eyre::Result<NextWork> {
    match semantic
        .next()
        .await
        .ok_or_else(|| eyre!("semantic actor stopped while init waited for apply_init_scan"))?
    {
        SemanticClientResponse::ApplyInitScan(result) => result,
        _ => Err(eyre!(
            "unexpected semantic response while init waited for apply_init_scan"
        )),
    }
}

async fn await_commit_import_epoch(semantic: &mut SemanticClient) -> eyre::Result<()> {
    match semantic
        .next()
        .await
        .ok_or_else(|| eyre!("semantic actor stopped while init waited for commit_import_epoch"))?
    {
        SemanticClientResponse::CommitImportEpoch(result) => result,
        _ => Err(eyre!(
            "unexpected semantic response while init waited for commit_import_epoch"
        )),
    }
}

async fn await_read_outbox(
    semantic: &mut SemanticClient,
) -> eyre::Result<Vec<(OutboxId, crate::crdt::types::SharedMessage)>> {
    match semantic
        .next()
        .await
        .ok_or_else(|| eyre!("semantic actor stopped while init waited for read_outbox"))?
    {
        SemanticClientResponse::ReadOutbox(result) => result,
        _ => Err(eyre!(
            "unexpected semantic response while init waited for read_outbox"
        )),
    }
}

async fn drain_outbox(
    semantic: &mut SemanticClient,
    log_writer: &mpsc::UnboundedSender<LogWriterRequest>,
    log_writer_rx: &mut mpsc::UnboundedReceiver<LogWriterResponse>,
) -> eyre::Result<usize> {
    let mut appended = 0;

    loop {
        semantic.read_outbox(INIT_OUTBOX_READ_BATCH_SIZE)?;
        let messages = await_read_outbox(semantic).await?;
        let Some(last_outbox_id) = messages.last().map(|(outbox_id, _)| *outbox_id) else {
            return Ok(appended);
        };

        for (outbox_id, shared_message) in messages {
            log_writer.send(LogWriterRequest::Append {
                outbox_id,
                shared_message,
            })?;
            appended += 1;
        }

        let durable_through = await_durable_through(log_writer_rx, last_outbox_id).await?;
        semantic.trim_outbox(..=durable_through)?;
        debug!(?durable_through, "init outbox batch durable");
    }
}

async fn await_durable_through(
    log_writer_rx: &mut mpsc::UnboundedReceiver<LogWriterResponse>,
    target: OutboxId,
) -> eyre::Result<OutboxId> {
    loop {
        match log_writer_rx
            .recv()
            .await
            .ok_or_else(|| eyre!("log writer stopped while init waited for durable append"))?
        {
            LogWriterResponse::Connected => {}
            LogWriterResponse::Disconnected { reason } => {
                eyre::bail!("log writer disconnected during init: {reason}");
            }
            LogWriterResponse::Ping => {}
            LogWriterResponse::Durable { outbox_range } if outbox_range.end >= target => {
                return Ok(outbox_range.end);
            }
            LogWriterResponse::Durable { .. } => {}
        }
    }
}
