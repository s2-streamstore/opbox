mod in_memory_file_io;
mod s2_connector;
mod s2_server;

use bytes::Bytes;
use clap::{Parser, Subcommand, ValueEnum};
use eyre::eyre;
use in_memory_file_io::InMemoryFileIO;
use opbox::app::db::{create_initialized_database, open_database, semantic_pool};
use opbox::app::runtime::{AppRuntime, AppRuntimeConfig, RunMode};
use opbox::semantic::service::SemanticService;
use opbox::semantic::table::daemon_state;
use opbox::types::{DaemonWriterId, OutboxId, WorkspaceId};
use rand::SeedableRng;
use s2_sdk::S2;
use s2_sdk::types::{
    AccountEndpoint, BasinEndpoint, BasinName, CreateBasinInput, S2Config, S2Endpoints, S2Error,
};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command as ProcessCommand, Stdio};
use std::time::{Duration, SystemTime};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

const SIM_BASIN: &str = "opbox-sim";

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Single(SingleArgs),
    Meta(MetaArgs),
}

impl Args {
    fn quiet(&self) -> bool {
        match &self.command {
            Command::Single(args) => args.quiet,
            Command::Meta(_) => false,
        }
    }
}

#[derive(Parser, Debug, Clone)]
struct CommonRunArgs {
    #[arg(value_enum, default_value_t = Workload::General)]
    workload: Workload,
    #[arg(long)]
    seed: Option<u64>,
    #[arg(long, default_value_t = 80)]
    steps: u64,
    #[arg(long, default_value_t = 0.0)]
    failure_rate: f64,
}

#[derive(Parser, Debug)]
struct SingleArgs {
    #[command(flatten)]
    common: CommonRunArgs,
    #[arg(long)]
    quiet: bool,
}

#[derive(Parser, Debug)]
struct MetaArgs {
    #[command(flatten)]
    common: CommonRunArgs,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Workload {
    General,
}

impl Workload {
    fn as_str(self) -> &'static str {
        match self {
            Workload::General => "general",
        }
    }
}

fn main() -> eyre::Result<()> {
    let args = Args::parse();
    init_tracing(args.quiet());
    match args.command {
        Command::Single(args) => run_single(args),
        Command::Meta(args) => run_meta(args),
    }
}

fn run_single(args: SingleArgs) -> eyre::Result<()> {
    let seed = args.common.seed.unwrap_or_else(random_seed);
    seed_rng(seed);
    info!(seed, ?args.common.workload, steps = args.common.steps, "opbox sim starting");

    let mut sim = init_sim(seed, args.common.failure_rate);
    sim.host("s2-lite", || async {
        s2_server::run_s2_lite_server()
            .await
            .map_err(|err| Box::new(std::io::Error::other(err.to_string())) as Box<_>)?;
        Ok(())
    });

    match args.common.workload {
        Workload::General => run_general_workload(&mut sim, seed, args.common.steps)?,
    }

    sim.run().map_err(|err| eyre!("simulation failed: {err}"))?;
    Ok(())
}

fn run_meta(args: MetaArgs) -> eyre::Result<()> {
    let seed = args.common.seed.unwrap_or_else(random_seed);
    let first = run_single_child(&args.common, seed, "first")?;
    let second = run_single_child(&args.common, seed, "second")?;

    if first.output != second.output {
        eyre::bail!(
            "meta output mismatch for seed {seed}\n--- first ---\n{}\n--- second ---\n{}",
            first.output,
            second.output
        );
    }

    println!(
        "META_OK workload={} seed={seed} lines={}",
        args.common.workload.as_str(),
        first.output.lines().count()
    );
    Ok(())
}

fn run_single_child(
    args: &CommonRunArgs,
    seed: u64,
    label: &'static str,
) -> eyre::Result<ChildRunOutput> {
    let exe = std::env::current_exe()?;
    let output = ProcessCommand::new(exe)
        .arg("single")
        .arg(args.workload.as_str())
        .arg("--seed")
        .arg(seed.to_string())
        .arg("--steps")
        .arg(args.steps.to_string())
        .arg("--failure-rate")
        .arg(args.failure_rate.to_string())
        .arg("--quiet")
        .env("RUST_LOG", "error")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    if !output.status.success() {
        eyre::bail!(
            "meta child {label} failed with status {}\n--- stdout ---\n{}\n--- stderr ---\n{}",
            output.status,
            stdout,
            stderr
        );
    }

    Ok(ChildRunOutput {
        output: normalize_child_output(&stdout, &stderr),
    })
}

struct ChildRunOutput {
    output: String,
}

fn normalize_child_output(stdout: &str, stderr: &str) -> String {
    stdout
        .lines()
        .chain(stderr.lines())
        .filter(|line| line.starts_with("SIM_"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn run_general_workload(
    sim: &mut turmoil::Sim<'static>,
    seed: u64,
    steps: u64,
) -> eyre::Result<()> {
    let workspace_id = WorkspaceId("00000000000000000000000000000001".to_string());
    let daemon_a = spawn_daemon(sim, "daemon-a", seed, 0, workspace_id.clone())?;
    let daemon_b = spawn_daemon(sim, "daemon-b", seed, 1, workspace_id)?;

    sim.client("controller", async move {
        daemon_a
            .write_file("a.txt", Bytes::from_static(b"from daemon a\n"))
            .await?;
        daemon_b
            .write_file("b.txt", Bytes::from_static(b"from daemon b\n"))
            .await?;

        let expected = BTreeMap::from([
            ("a.txt".to_string(), "from daemon a\n".to_string()),
            ("b.txt".to_string(), "from daemon b\n".to_string()),
        ]);

        let mut last_a = BTreeMap::new();
        let mut last_b = BTreeMap::new();
        for _ in 0..steps {
            last_a = daemon_a.snapshot_text_files().await?;
            last_b = daemon_b.snapshot_text_files().await?;
            if last_a == expected && last_b == expected {
                println!(
                    "SIM_OK workload=general seed={seed} files={}",
                    expected.len()
                );
                daemon_a.shutdown().await;
                daemon_b.shutdown().await;
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        daemon_a.shutdown().await;
        daemon_b.shutdown().await;
        Err(io_err(format!(
            "local trees did not converge; a={last_a:?} b={last_b:?}"
        )))
    });

    Ok(())
}

fn spawn_daemon(
    sim: &mut turmoil::Sim<'static>,
    name: &'static str,
    seed: u64,
    daemon_index: u8,
    workspace_id: WorkspaceId,
) -> eyre::Result<SimDaemonHandle> {
    let db_path = sim_db_path(seed, name);
    remove_db_family(&db_path);
    let daemon_row = daemon_state::Row {
        workspace_id,
        s2_basin: SIM_BASIN.parse()?,
        daemon_writer_id: DaemonWriterId(Bytes::copy_from_slice(&[daemon_index; 16])),
        stable_cursor: ..0,
        next_outbox_id: OutboxId::new(0),
    };

    let file_io = InMemoryFileIO::new();
    let handle_io = file_io.clone();
    let (command_tx, command_rx) = mpsc::channel(100);
    sim.client(name, async move {
        run_daemon_client(name, db_path, daemon_row, file_io, command_rx).await
    });

    Ok(SimDaemonHandle {
        command_tx,
        _io: handle_io,
    })
}

async fn run_daemon_client(
    name: &'static str,
    db_path: PathBuf,
    daemon_row: daemon_state::Row,
    file_io: InMemoryFileIO,
    mut command_rx: mpsc::Receiver<SimDaemonCommand>,
) -> Result<(), Box<dyn std::error::Error>> {
    ensure_basin_exists().await?;
    create_initialized_database(&db_path, &daemon_row)
        .await
        .map_err(io_err)?;
    let db = open_database(&db_path).await.map_err(io_err)?;
    let pool = semantic_pool(db).await.map_err(io_err)?;
    let semantic_service = SemanticService::new(pool);
    let s2_basin = sim_s2_client()
        .map_err(io_err)?
        .basin(daemon_row.s2_basin.clone());
    let cancellation_token = CancellationToken::new();
    let runtime = AppRuntime::new(AppRuntimeConfig {
        mode: RunMode::Sync,
        file_io: file_io.clone(),
        semantic_service,
        daemon_row,
        s2_basin,
    });
    let mut actors = runtime.spawn(cancellation_token.clone());

    loop {
        tokio::select! {
            actor_error = actors.wait_for_actor_stop() => {
                let error = actor_error.unwrap_or_else(|| eyre!("all actors exited"));
                return Err(io_err(error));
            }
            command = command_rx.recv() => {
                let Some(command) = command else {
                    warn!(name, "daemon command channel closed");
                    if let Some(error) = actors.shutdown(cancellation_token).await {
                        return Err(io_err(error));
                    }
                    return Ok(());
                };

                match command {
                    SimDaemonCommand::WriteFile { path, bytes, reply } => {
                        let _ = reply.send(file_io.write_file(path, bytes).map_err(|err| err.to_string()));
                    }
                    SimDaemonCommand::SnapshotTextFiles { reply } => {
                        let _ = reply.send(file_io.snapshot_text_files().map_err(|err| err.to_string()));
                    }
                    SimDaemonCommand::Shutdown { reply } => {
                        let result = actors.shutdown(cancellation_token).await;
                        let _ = reply.send(result.map(|err| err.to_string()));
                        return Ok(());
                    }
                }
            }
        }
    }
}

#[derive(Clone)]
struct SimDaemonHandle {
    command_tx: mpsc::Sender<SimDaemonCommand>,
    _io: InMemoryFileIO,
}

impl SimDaemonHandle {
    async fn write_file(
        &self,
        path: impl Into<String>,
        bytes: Bytes,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (reply, recv) = oneshot::channel();
        self.command_tx
            .send(SimDaemonCommand::WriteFile {
                path: path.into(),
                bytes,
                reply,
            })
            .await
            .map_err(io_err)?;
        recv.await.map_err(io_err)?.map_err(io_err)
    }

    async fn snapshot_text_files(
        &self,
    ) -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
        let (reply, recv) = oneshot::channel();
        self.command_tx
            .send(SimDaemonCommand::SnapshotTextFiles { reply })
            .await
            .map_err(io_err)?;
        recv.await.map_err(io_err)?.map_err(io_err)
    }

    async fn shutdown(&self) {
        let (reply, recv) = oneshot::channel();
        let _ = self
            .command_tx
            .send(SimDaemonCommand::Shutdown { reply })
            .await;
        let _ = recv.await;
    }
}

enum SimDaemonCommand {
    WriteFile {
        path: String,
        bytes: Bytes,
        reply: oneshot::Sender<Result<(), String>>,
    },
    SnapshotTextFiles {
        reply: oneshot::Sender<Result<BTreeMap<String, String>, String>>,
    },
    Shutdown {
        reply: oneshot::Sender<Option<String>>,
    },
}

fn sim_s2_client() -> eyre::Result<S2> {
    let endpoints = S2Endpoints::new(
        AccountEndpoint::new("http://s2-lite:80")?,
        BasinEndpoint::new("http://s2-lite:80")?,
    )?;

    S2::new_with_connector(
        S2Config::new("ignored").with_endpoints(endpoints),
        s2_connector::TurmoilConnector,
    )
    .map_err(Into::into)
}

async fn ensure_basin_exists() -> Result<(), Box<dyn std::error::Error>> {
    let basin_name: BasinName = SIM_BASIN.parse().map_err(io_err)?;
    let s2 = sim_s2_client().map_err(io_err)?;
    match s2.create_basin(CreateBasinInput::new(basin_name)).await {
        Ok(_) => Ok(()),
        Err(S2Error::Server(err)) if err.code == "resource_already_exists" => Ok(()),
        Err(err) => Err(io_err(err)),
    }
}

fn init_sim(seed: u64, failure_rate: f64) -> turmoil::Sim<'static> {
    let mut builder = turmoil::Builder::new();
    builder
        .rng_seed(seed)
        .simulation_duration(Duration::MAX)
        .min_message_latency(Duration::from_millis(2))
        .max_message_latency(Duration::from_millis(300))
        .tcp_capacity(10_000)
        .epoch(SystemTime::UNIX_EPOCH);

    if failure_rate > 0.0 {
        builder.fail_rate(failure_rate);
    }

    builder.build()
}

fn seed_rng(seed: u64) {
    mad_turmoil::rand::set_rng(rand::rngs::StdRng::seed_from_u64(seed));
}

fn sim_db_path(seed: u64, name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("opbox-sim-{seed}-{name}.db"))
}

fn remove_db_family(path: &Path) {
    for suffix in ["", "-shm", "-wal", "-log"] {
        let path = PathBuf::from(format!("{}{}", path.display(), suffix));
        let _ = std::fs::remove_file(path);
    }
}

fn init_tracing(quiet: bool) {
    let filter = if quiet {
        tracing_subscriber::EnvFilter::new("error")
    } else {
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
    };
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .try_init();
}

fn random_seed() -> u64 {
    use std::io::Read;

    let mut bytes = [0u8; 8];
    if let Ok(mut urandom) = std::fs::File::open("/dev/urandom")
        && urandom.read_exact(&mut bytes).is_ok()
    {
        return u64::from_le_bytes(bytes);
    }

    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos() as u64
}

fn io_err(err: impl std::fmt::Display) -> Box<dyn std::error::Error> {
    Box::new(std::io::Error::other(err.to_string()))
}
