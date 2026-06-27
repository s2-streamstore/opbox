use clap::Parser;
use opbox_core::app::db::{open_database, semantic_pool};
use opbox_core::app::ipc::{self, ControlServerConfig};
use opbox_core::app::runtime::{AppRuntime, AppRuntimeConfig, RunMode};
use opbox_core::app::s2::{
    S2ConnectionConfig, ensure_workspace_stream_exists, report_is_s2_connectivity,
    s2_basin_from_config, workspace_stream_retention_warning,
};
use opbox_core::app::user_config::{UserConfig, load_user_config, load_user_config_from_path};
use opbox_core::app::workspace::{
    DaemonLock, canonicalize_existing_dir, load_configured_daemon_state, remove_pid,
    workspace_config_path, write_pid,
};
use opbox_core::engine::actor::EngineStatusConfig;
use opbox_core::fs::fio::local::LocalFileIO;
use opbox_core::notify::nio::LocalNotifyIO;
use opbox_core::semantic::service::SemanticService;
use opbox_core::semantic::table::daemon_state;
use std::path::{Path, PathBuf};
use std::time::Duration;
use time::OffsetDateTime;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long)]
    root: PathBuf,
}

fn load_workspace_config(sync_root: &Path) -> eyre::Result<UserConfig> {
    load_user_config_from_path(&workspace_config_path(sync_root))
}

fn init_tracing(
    sync_root: &Path,
    workspace_config: &UserConfig,
    user_config: &UserConfig,
) -> eyre::Result<()> {
    let filter = if let Some(log_level) = workspace_config.daemon_log_level.as_deref() {
        tracing_subscriber::EnvFilter::try_new(log_level).map_err(|err| {
            eyre::eyre!(
                "invalid daemon-log-level in {}: {err}",
                workspace_config_path(sync_root).display()
            )
        })?
    } else if let Some(log_level) = user_config.daemon_log_level.as_deref() {
        tracing_subscriber::EnvFilter::try_new(log_level)
            .map_err(|err| eyre::eyre!("invalid user daemon-log-level: {err}"))?
    } else {
        tracing_subscriber::EnvFilter::new("info")
    };
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .try_init();
    Ok(())
}

fn apply_workspace_basin(
    workspace_config: &UserConfig,
    daemon_row: &mut daemon_state::Row,
) -> eyre::Result<()> {
    let Some(basin) = workspace_config.basin.as_deref() else {
        return Ok(());
    };
    daemon_row.s2_basin = basin
        .parse()
        .map_err(|err| eyre::eyre!("invalid basin in workspace config {basin:?}: {err}"))?;
    Ok(())
}

fn main() -> eyre::Result<()> {
    let args = Args::parse();
    let sync_root = canonicalize_existing_dir(&args.root)?;
    let user_config = load_user_config()?;
    let workspace_config = load_workspace_config(&sync_root)?;
    init_tracing(&sync_root, &workspace_config, &user_config)?;

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run(sync_root, user_config, workspace_config))
}

async fn run(
    sync_root: PathBuf,
    user_config: UserConfig,
    workspace_config: UserConfig,
) -> eyre::Result<()> {
    let _lock = DaemonLock::acquire(&sync_root)?;
    write_pid(&sync_root)?;
    let _pid_guard = PidGuard {
        sync_root: sync_root.clone(),
    };

    let (db_path, mut daemon_row) = load_configured_daemon_state(&sync_root).await?;
    apply_workspace_basin(&workspace_config, &mut daemon_row)?;
    let s2_connection = S2ConnectionConfig::from_workspace_or_user_config(
        &workspace_config,
        daemon_row.s2_account_endpoint.as_deref(),
        daemon_row.s2_basin_endpoint.as_deref(),
        &user_config,
    )?;
    let s2_basin = s2_basin_from_config(daemon_row.s2_basin.clone(), &s2_connection).await?;
    let mut status_warnings = Vec::new();
    match ensure_workspace_stream_exists(&s2_basin, &daemon_row.workspace_id).await {
        Ok(()) => {
            match workspace_stream_retention_warning(&s2_basin, &daemon_row.workspace_id).await {
                Ok(Some(warning)) => {
                    warn!(
                        ?warning,
                        "workspace ops stream retention is not infinite; future clones may fail after records expire"
                    );
                    status_warnings.push(warning);
                }
                Ok(None) => {}
                Err(error) if report_is_s2_connectivity(&error) => {
                    warn!(
                        ?error,
                        "could not verify workspace ops stream retention because S2 is unavailable"
                    );
                }
                Err(error) => {
                    warn!(?error, "could not verify workspace ops stream retention");
                }
            }
        }
        Err(err) if report_is_s2_connectivity(&err) => {
            tracing::warn!(
                ?err,
                "could not verify workspace stream at startup; daemon will start offline"
            );
        }
        Err(err) => return Err(err),
    }

    let db = open_database(&db_path).await?;
    let pool = semantic_pool(db).await?;
    let semantic_service = SemanticService::new(pool);
    let notify_io = Some(LocalNotifyIO::new(&sync_root, Duration::from_millis(50))?);
    let (spy_tx, _) = broadcast::channel(1024);
    let started_at = OffsetDateTime::now_utc();

    let token = CancellationToken::new();
    let mut actors = AppRuntime::new(AppRuntimeConfig {
        mode: RunMode::Sync,
        file_io: LocalFileIO::new(&sync_root),
        notify_io,
        semantic_service,
        daemon_row: daemon_row.clone(),
        s2_basin,
        clone_log_read_stop: None,
        engine_status: Some(EngineStatusConfig {
            sync_root: sync_root.clone(),
            workspace_id: daemon_row.workspace_id.clone(),
            daemon_writer_id: daemon_row.daemon_writer_id.clone(),
            stable_cursor: daemon_row.stable_cursor.clone(),
            started_at,
            warnings: status_warnings,
        }),
        spy_tx: Some(spy_tx.clone()),
    })
    .spawn(token.clone());
    let engine_tx = actors
        .engine_command_tx()
        .expect("sync runtime exposes engine command mailbox");

    let control_config = ControlServerConfig {
        sync_root: sync_root.clone(),
        daemon_state: daemon_row.clone(),
        engine_tx,
    };
    let mut control_task = tokio::spawn({
        let token = token.clone();
        async move { ipc::serve_control(control_config, token).await }
    });

    info!(root = %sync_root.display(), "opbox daemon started");

    let mut shutdown_error = tokio::select! {
        ctrl_c = tokio::signal::ctrl_c() => {
            ctrl_c?;
            info!("ctrl-c received");
            None
        }
        actor_error = actors.wait_for_actor_stop() => actor_error,
        control_result = &mut control_task => {
            match control_result {
                Ok(Ok(())) => None,
                Ok(Err(err)) => {
                    error!(?err, "control server failed");
                    Some(err)
                }
                Err(err) => {
                    error!(?err, "control server task failed");
                    Some(err.into())
                }
            }
        }
    };

    token.cancel();
    if !control_task.is_finished() {
        control_task.abort();
    }
    if let Some(error) = actors.shutdown(token).await
        && shutdown_error.is_none()
    {
        shutdown_error = Some(error);
    }

    info!("opbox daemon exiting");

    match shutdown_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

struct PidGuard {
    sync_root: PathBuf,
}

impl Drop for PidGuard {
    fn drop(&mut self) {
        remove_pid(&self.sync_root);
    }
}
