use clap::Parser;
use opbox_core::app::db::{open_database, semantic_pool};
use opbox_core::app::ipc::{self, ControlServerConfig};
use opbox_core::app::runtime::{AppRuntime, AppRuntimeConfig, RunMode};
use opbox_core::app::s2::{
    S2ConnectionConfig, ensure_workspace_stream_exists, s2_basin_from_config,
};
use opbox_core::app::user_config::{UserConfig, load_user_config};
use opbox_core::app::workspace::{
    DaemonLock, canonicalize_existing_dir, load_configured_daemon_state, load_workspace_env,
    remove_pid, write_pid,
};
use opbox_core::fs::fio::local::LocalFileIO;
use opbox_core::notify::nio::LocalNotifyIO;
use opbox_core::semantic::service::SemanticService;
use std::path::PathBuf;
use std::time::Duration;
use time::OffsetDateTime;
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long)]
    root: PathBuf,
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .try_init();
}

fn main() -> eyre::Result<()> {
    let args = Args::parse();
    let sync_root = canonicalize_existing_dir(&args.root)?;
    // Load .opbox/env while still single-threaded (set_var is unsafe under
    // concurrent env reads) and before tracing init, so RUST_LOG from the
    // file takes effect too.
    let env_applied = load_workspace_env(&sync_root)?;
    init_tracing();
    if !env_applied.is_empty() {
        info!(vars = ?env_applied, "loaded workspace env from .opbox/env");
    }
    let user_config = load_user_config()?;

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run(sync_root, user_config))
}

async fn run(sync_root: PathBuf, user_config: UserConfig) -> eyre::Result<()> {
    let _lock = DaemonLock::acquire(&sync_root)?;
    write_pid(&sync_root)?;
    let _pid_guard = PidGuard {
        sync_root: sync_root.clone(),
    };

    let (db_path, daemon_row) = load_configured_daemon_state(&sync_root).await?;
    let s2_connection = S2ConnectionConfig::from_env_workspace_or_user_config(
        daemon_row.s2_account_endpoint.as_deref(),
        daemon_row.s2_basin_endpoint.as_deref(),
        &user_config,
    )?;
    let s2_basin = s2_basin_from_config(daemon_row.s2_basin.clone(), &s2_connection).await?;
    ensure_workspace_stream_exists(&s2_basin, &daemon_row.workspace_id).await?;

    let db = open_database(&db_path).await?;
    let pool = semantic_pool(db).await?;
    let semantic_service = SemanticService::new(pool);
    let notify_io = Some(LocalNotifyIO::new(&sync_root, Duration::from_millis(50))?);
    let (spy_tx, _) = broadcast::channel(1024);

    let token = CancellationToken::new();
    let mut actors = AppRuntime::new(AppRuntimeConfig {
        mode: RunMode::Sync,
        file_io: LocalFileIO::new(&sync_root),
        notify_io,
        semantic_service,
        daemon_row: daemon_row.clone(),
        s2_basin,
        spy_tx: Some(spy_tx.clone()),
    })
    .spawn(token.clone());

    let control_config = ControlServerConfig {
        sync_root: sync_root.clone(),
        db_path: db_path.clone(),
        daemon_state: daemon_row.clone(),
        started_at: OffsetDateTime::now_utc(),
        spy_tx,
    };
    let (stop_tx, mut stop_rx) = mpsc::unbounded_channel();
    let mut control_task = tokio::spawn({
        let token = token.clone();
        async move { ipc::serve_control(control_config, token, stop_tx).await }
    });

    info!(root = %sync_root.display(), "opbox daemon started");

    let mut shutdown_error = tokio::select! {
        ctrl_c = tokio::signal::ctrl_c() => {
            ctrl_c?;
            info!("ctrl-c received");
            None
        }
        actor_error = actors.wait_for_actor_stop() => actor_error,
        Some(()) = stop_rx.recv() => {
            info!("stop requested");
            None
        }
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
