use clap::{Parser, Subcommand};
use eyre::eyre;
use opbox::app::db::{load_daemon_state, open_database, semantic_pool};
use opbox::app::runtime::{AppRuntime, AppRuntimeConfig, RunMode};
use opbox::fs::fio::local::LocalFileIO;
use opbox::semantic::service::SemanticService;
use opbox::semantic::table::daemon_state;
use opbox::types::{DaemonWriterId, OutboxId, WorkspaceId};
use s2_sdk::types::{BasinName, CreateStreamInput, S2Config, S2Endpoints, S2Error, StreamName};
use s2_sdk::{S2, S2Basin};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use tracing::{debug, info};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand, Clone, Debug)]
enum Command {
    Init {
        #[arg(long)]
        db: Option<PathBuf>,
        #[arg(long)]
        basin: BasinName,
        sync_root: PathBuf,
    },
    Clone {
        #[arg(long)]
        db: Option<PathBuf>,
        #[arg(long)]
        basin: BasinName,
        #[arg(long)]
        workspace: WorkspaceId,
        sync_root: PathBuf,
    },
    Sync {
        #[arg(long)]
        db: PathBuf,
        sync_root: PathBuf,
    },
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .try_init();
}

fn s2_client_from_env(access_token: &str) -> eyre::Result<S2> {
    let mut config = S2Config::new(access_token);
    if let Ok(endpoints) = S2Endpoints::from_env() {
        config = config.with_endpoints(endpoints);
    }

    Ok(S2::new(config)?)
}

struct Bootstrap {
    mode: RunMode,
    db_path: PathBuf,
    sync_root: PathBuf,
    daemon_row: daemon_state::Row,
    s2_basin: S2Basin,
}

fn default_db_path(workspace_id: &WorkspaceId) -> eyre::Result<PathBuf> {
    let home = std::env::var_os("HOME").ok_or_else(|| eyre!("HOME is not set"))?;
    Ok(PathBuf::from(home)
        .join(".opbox")
        .join("checkouts")
        .join(format!("{}.db", workspace_id)))
}

fn ensure_new_db_path_available(db_path: &Path) -> eyre::Result<()> {
    if db_path.try_exists()? {
        eyre::bail!("db path already exists: {}", db_path.display());
    }
    Ok(())
}

fn fresh_daemon_state_row(workspace_id: WorkspaceId, basin: BasinName) -> daemon_state::Row {
    let writer_id = rand::random::<[u8; 16]>();

    daemon_state::Row {
        workspace_id,
        s2_basin: basin,
        daemon_writer_id: DaemonWriterId(bytes::Bytes::copy_from_slice(&writer_id)),
        stable_cursor: ..0,
        next_outbox_id: OutboxId::new(0),
    }
}

fn ops_stream_name(workspace_id: &WorkspaceId) -> eyre::Result<StreamName> {
    StreamName::from_str(&format!("{}/ops", workspace_id.0)).map_err(|err| {
        eyre!(
            "invalid ops stream name for workspace {}: {err}",
            workspace_id.0
        )
    })
}

async fn create_workspace_stream(
    s2_basin: &S2Basin,
    workspace_id: &WorkspaceId,
) -> eyre::Result<()> {
    let stream_name = ops_stream_name(workspace_id)?;
    match s2_basin
        .create_stream(CreateStreamInput::new(stream_name))
        .await
    {
        Ok(_) => Ok(()),
        Err(S2Error::Server(err)) if err.code == "resource_already_exists" => {
            Err(eyre!("workspace {} already exists", workspace_id.0))
        }
        Err(err) => Err(err.into()),
    }
}

async fn ensure_workspace_stream_exists(
    s2_basin: &S2Basin,
    workspace_id: &WorkspaceId,
) -> eyre::Result<()> {
    let stream_name = ops_stream_name(workspace_id)?;
    let stream = s2_basin.stream(stream_name);
    match stream.check_tail().await {
        Ok(_) => Ok(()),
        Err(S2Error::Server(err)) if err.code == "stream_not_found" => {
            Err(eyre!("workspace {} does not exist", workspace_id.0))
        }
        Err(err) => Err(err.into()),
    }
}

fn ensure_sync_root_exists(sync_root: &Path) -> eyre::Result<()> {
    let metadata = std::fs::metadata(sync_root)?;
    if !metadata.is_dir() {
        eyre::bail!("sync root is not a directory: {}", sync_root.display());
    }
    Ok(())
}

fn ensure_clean_clone_root(sync_root: &Path) -> eyre::Result<()> {
    if sync_root.try_exists()? {
        ensure_sync_root_exists(sync_root)?;
        if std::fs::read_dir(sync_root)?.next().transpose()?.is_some() {
            eyre::bail!("clone sync root is not empty: {}", sync_root.display());
        }
    } else {
        std::fs::create_dir_all(sync_root)?;
    }

    Ok(())
}

async fn create_initialized_database(
    db_path: &Path,
    daemon_row: &daemon_state::Row,
) -> eyre::Result<()> {
    ensure_new_db_path_available(db_path)?;
    opbox::app::db::create_initialized_database(db_path, daemon_row).await?;

    debug!(?db_path, "database initialized");

    Ok(())
}

async fn s2_basin_from_env(basin: BasinName) -> eyre::Result<S2Basin> {
    let token = std::env::var("S2_ACCESS_TOKEN")?;
    let s2 = s2_client_from_env(&token)?;
    Ok(s2.basin(basin))
}

async fn bootstrap_command(command: Command) -> eyre::Result<Bootstrap> {
    match command {
        Command::Init {
            db,
            basin,
            sync_root,
        } => {
            ensure_sync_root_exists(&sync_root)?;
            let s2_basin = s2_basin_from_env(basin.clone()).await?;
            let workspace_id = WorkspaceId::generate();
            info!(?workspace_id, "generated workspace id");
            let db_path = match db {
                Some(db_path) => db_path,
                None => default_db_path(&workspace_id)?,
            };
            ensure_new_db_path_available(&db_path)?;
            create_workspace_stream(&s2_basin, &workspace_id).await?;
            let daemon_row = fresh_daemon_state_row(workspace_id, basin);
            create_initialized_database(&db_path, &daemon_row).await?;
            Ok(Bootstrap {
                mode: RunMode::Init,
                db_path,
                sync_root,
                daemon_row,
                s2_basin,
            })
        }
        Command::Clone {
            db,
            basin,
            workspace,
            sync_root,
        } => {
            ensure_clean_clone_root(&sync_root)?;
            let db_path = match db {
                Some(db_path) => db_path,
                None => default_db_path(&workspace)?,
            };
            ensure_new_db_path_available(&db_path)?;
            let s2_basin = s2_basin_from_env(basin.clone()).await?;
            ensure_workspace_stream_exists(&s2_basin, &workspace).await?;
            let daemon_row = fresh_daemon_state_row(workspace, basin);
            create_initialized_database(&db_path, &daemon_row).await?;
            Ok(Bootstrap {
                mode: RunMode::Clone,
                db_path,
                sync_root,
                daemon_row,
                s2_basin,
            })
        }
        Command::Sync { db, sync_root } => {
            ensure_sync_root_exists(&sync_root)?;
            if !db.try_exists()? {
                eyre::bail!("db path does not exist: {}", db.display());
            }
            let daemon_row = load_daemon_state(&db).await?;
            let s2_basin = s2_basin_from_env(daemon_row.s2_basin.clone()).await?;
            ensure_workspace_stream_exists(&s2_basin, &daemon_row.workspace_id).await?;
            Ok(Bootstrap {
                mode: RunMode::Sync,
                db_path: db,
                sync_root,
                daemon_row,
                s2_basin,
            })
        }
    }
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    init_tracing();

    let args = Args::parse();
    let bootstrap = bootstrap_command(args.command).await?;

    let db = open_database(&bootstrap.db_path).await?;
    let pool = semantic_pool(db).await?;
    let semantic_service = SemanticService::new(pool);

    AppRuntime::new(AppRuntimeConfig {
        mode: bootstrap.mode,
        file_io: LocalFileIO::new(&bootstrap.sync_root),
        semantic_service,
        daemon_row: bootstrap.daemon_row,
        s2_basin: bootstrap.s2_basin,
    })
    .run_until_shutdown()
    .await
}
