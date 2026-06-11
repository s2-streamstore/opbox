use clap::builder::styling;
use clap::{Parser, Subcommand};
use opbox_core::app::db::{open_database, semantic_pool};
use opbox_core::app::ipc;
use opbox_core::app::runtime::{AppRuntime, AppRuntimeConfig, RunMode};
use opbox_core::app::s2::{
    create_workspace_stream, ensure_workspace_stream_exists, s2_basin_from_env,
};
use opbox_core::app::workspace::{
    NotInWorkspace, canonicalize_existing_dir, create_metadata_dir, current_dir, daemon_log_path,
    ensure_clean_clone_root, ensure_sync_root_unconfigured, find_workspace_root,
    load_configured_daemon_state, remove_socket_pointer, storage_db_path, workspace_env_path,
};
use opbox_core::fs::fio::local::LocalFileIO;
use opbox_core::fs::ignore::{IGNORE_FILE_NAME, default_ignore_file_contents};
use opbox_core::semantic::service::SemanticService;
use opbox_core::semantic::table::daemon_state;
use opbox_core::spy::{SpyEvent, SpySharedMessageKind};
use opbox_core::types::{DaemonWriterId, OutboxId, WorkspaceId};
use s2_sdk::{S2Basin, types::BasinName};
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::process::Command as TokioCommand;
use tracing::{debug, info};

const CLIENT_COMMAND: &str = "ob";

const STYLES: styling::Styles = styling::Styles::styled()
    .header(styling::AnsiColor::Green.on_default().bold())
    .usage(styling::AnsiColor::Green.on_default().bold())
    .literal(styling::AnsiColor::Blue.on_default().bold())
    .placeholder(styling::AnsiColor::Cyan.on_default());

// const GENERAL_USAGE: &str = color_print::cstr!(
//     r#"
//     <dim>$</dim> <bold>ob init/bold>
//     <dim>$</dim> <bold>s2 list-basins --prefix "foo" --limit 100</bold>
//     "#
// );

#[derive(Parser, Debug)]
#[command(name = "ob", version, styles = STYLES)]
struct Args {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand, Clone, Debug)]
enum Command {
    /// Initialize a new opbox workspace in the basin named by $S2_BASIN.
    /// Uses the $PWD unless a sync root is specified.
    Init {
        sync_root: Option<PathBuf>,
    },
    /// Clone an existing workspace from the basin named by $S2_BASIN.
    Clone {
        #[arg(long)]
        workspace: WorkspaceId,
        sync_root: Option<PathBuf>,
    },
    Start {
        sync_root: Option<PathBuf>,
    },
    Stop {
        sync_root: Option<PathBuf>,
    },
    Status {
        sync_root: Option<PathBuf>,
    },
    ///
    Spy {
        sync_root: Option<PathBuf>,
    },
    /// Inspect the daemon for a workspace.
    Logs {
        /// Tail the log (via `tail -f`).
        #[arg(short, long)]
        follow: bool,
        sync_root: Option<PathBuf>,
    },
}

struct Bootstrap {
    mode: RunMode,
    db_path: PathBuf,
    sync_root: PathBuf,
    daemon_row: daemon_state::Row,
    s2_basin: S2Basin,
}

fn init_tracing() {
    // The CLI communicates through stdout, not logs; default to warnings only.
    // RUST_LOG still overrides for debugging.
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .try_init();
}

fn root_or_current(sync_root: Option<PathBuf>) -> eyre::Result<PathBuf> {
    match sync_root {
        Some(path) => Ok(path),
        None => current_dir(),
    }
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

fn create_default_ignore_file(sync_root: &Path) -> eyre::Result<()> {
    let path = sync_root.join(IGNORE_FILE_NAME);
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
    {
        Ok(mut file) => {
            use std::io::Write;
            file.write_all(default_ignore_file_contents().as_bytes())?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(error.into()),
    }
}

async fn create_initialized_database(
    sync_root: &Path,
    daemon_row: &daemon_state::Row,
) -> eyre::Result<()> {
    let db_path = storage_db_path(sync_root);
    opbox_core::app::db::create_initialized_database(&db_path, daemon_row).await?;

    debug!(?db_path, "database initialized");

    Ok(())
}

fn basin_from_env() -> eyre::Result<BasinName> {
    let value = std::env::var("S2_BASIN")
        .map_err(|_| eyre::eyre!("S2_BASIN is not set; export the s2.dev basin name to use"))?;
    value
        .parse()
        .map_err(|err| eyre::eyre!("invalid S2_BASIN {value:?}: {err}"))
}

async fn bootstrap_init(sync_root: Option<PathBuf>) -> eyre::Result<Bootstrap> {
    let basin = basin_from_env()?;
    let sync_root = canonicalize_existing_dir(&root_or_current(sync_root)?)?;
    ensure_sync_root_unconfigured(&sync_root).await?;
    let s2_basin = s2_basin_from_env(basin.clone()).await?;
    let workspace_id = WorkspaceId::generate();
    debug!(?workspace_id, "generated workspace id");
    create_workspace_stream(&s2_basin, &workspace_id).await?;
    let daemon_row = fresh_daemon_state_row(workspace_id, basin);
    create_metadata_dir(&sync_root)?;
    create_default_ignore_file(&sync_root)?;
    create_initialized_database(&sync_root, &daemon_row).await?;
    Ok(Bootstrap {
        mode: RunMode::Init,
        db_path: storage_db_path(&sync_root),
        sync_root,
        daemon_row,
        s2_basin,
    })
}

async fn bootstrap_clone(
    workspace: WorkspaceId,
    sync_root: Option<PathBuf>,
) -> eyre::Result<Bootstrap> {
    let basin = basin_from_env()?;
    let requested_root = root_or_current(sync_root)?;
    if requested_root.try_exists()? && requested_root.is_dir() {
        ensure_sync_root_unconfigured(&requested_root).await?;
    }
    let sync_root = ensure_clean_clone_root(&requested_root)?;
    ensure_sync_root_unconfigured(&sync_root).await?;
    let s2_basin = s2_basin_from_env(basin.clone()).await?;
    ensure_workspace_stream_exists(&s2_basin, &workspace).await?;
    let daemon_row = fresh_daemon_state_row(workspace, basin);
    create_metadata_dir(&sync_root)?;
    create_initialized_database(&sync_root, &daemon_row).await?;
    Ok(Bootstrap {
        mode: RunMode::Clone,
        db_path: storage_db_path(&sync_root),
        sync_root,
        daemon_row,
        s2_basin,
    })
}

async fn run_bootstrap(bootstrap: Bootstrap) -> eyre::Result<()> {
    let mode = bootstrap.mode;
    let sync_root = bootstrap.sync_root.clone();
    let workspace_id = bootstrap.daemon_row.workspace_id.clone();
    let basin = bootstrap.daemon_row.s2_basin.clone();

    let db = open_database(&bootstrap.db_path).await?;
    let pool = semantic_pool(db).await?;
    let semantic_service = SemanticService::new(pool);

    AppRuntime::new(AppRuntimeConfig {
        mode: bootstrap.mode,
        file_io: LocalFileIO::new(&bootstrap.sync_root),
        notify_io: None::<opbox_core::notify::nio::LocalNotifyIO>,
        semantic_service,
        daemon_row: bootstrap.daemon_row,
        s2_basin: bootstrap.s2_basin,
        spy_tx: None,
    })
    .run_until_shutdown()
    .await?;

    if mode == RunMode::Clone {
        create_default_ignore_file(&sync_root)?;
    }
    let persisted_env = persist_s2_env(&sync_root)?;

    let style = CliStyle::for_stdout();
    let title = match mode {
        RunMode::Init => "initialized opbox workspace",
        RunMode::Clone => "cloned opbox workspace",
        RunMode::Sync => unreachable!("bootstrap never runs in sync mode"),
    };
    println!("{}", style.bold(title));
    if mode == RunMode::Clone {
        print_status_row("workspace", style.bold(&workspace_id.0), style);
    }
    print_status_row("basin", basin.as_ref(), style);
    print_status_row("root", sync_root.display(), style);
    if !persisted_env.is_empty() {
        println!(
            "\nsaved {} to {} for the daemon; delete that file to use the shell environment instead",
            style.bold(persisted_env.join(", ")),
            style.bold(".opbox/env"),
        );
    }
    if mode == RunMode::Init {
        println!();
        println!(
            "your workspace is: {}",
            style.bold(style.green(&workspace_id.0))
        );
        println!();
        println!("share this with others who want to sync with it:");
        println!();
        println!(
            "  {} {}",
            style.dim("$"),
            style.bold(format!(
                "S2_BASIN={} {CLIENT_COMMAND} clone --workspace {} <dir>",
                basin.as_ref(),
                workspace_id.0
            ))
        );
        println!();
    }
    println!(
        "run {} to begin syncing",
        style.bold(format!("{CLIENT_COMMAND} start"))
    );

    Ok(())
}

async fn run_status(sync_root: Option<PathBuf>) -> eyre::Result<()> {
    let root = find_workspace_root(&root_or_current(sync_root)?)?;
    let status = match request_valid_status(&root).await {
        Ok(status) => status,
        Err(_) => exit_daemon_not_running(&root),
    };

    print_daemon_status("opbox daemon running", &status, CliStyle::for_stdout());
    Ok(())
}

async fn run_spy(sync_root: Option<PathBuf>) -> eyre::Result<()> {
    let root = find_workspace_root(&root_or_current(sync_root)?)?;
    let status = match request_valid_status(&root).await {
        Ok(status) => status,
        Err(_) => exit_daemon_not_running(&root),
    };
    eprintln!(
        "spying on opbox workspace {} (pid {})",
        status.workspace_id, status.pid
    );

    let mut stream = ipc::open_spy_stream(&root).await?;
    let style = CliStyle::for_stdout();
    loop {
        tokio::select! {
            ctrl_c = tokio::signal::ctrl_c() => {
                ctrl_c?;
                return Ok(());
            }
            event = stream.next_event() => {
                match event? {
                    Some(event) => print_spy_event(event, style),
                    None => return Ok(()),
                }
            }
        }
    }
}

fn print_spy_event(event: SpyEvent, style: CliStyle) {
    match event {
        SpyEvent::Lagged { skipped } => {
            println!(
                "{} {}={skipped}",
                style.yellow("lagged"),
                style.dim("skipped")
            );
        }
        SpyEvent::SharedMessage(message) => match message.message {
            SpySharedMessageKind::NamespaceUpdate => {
                println!(
                    "{}  {}  {}  {}  {}  {}",
                    style.seq(message.sequence_number),
                    style.yellow(format!("{:<10}", "namespace")),
                    format_kv("from", short_id(&message.origin_writer_id_b64), style),
                    format_kv("outbox", message.origin_outbox_id, style),
                    style.bytes(message.payload_size_bytes),
                    format_kv("ts", message.timestamp_ns, style),
                );
            }
            SpySharedMessageKind::TextUpdate {
                object_id_b64,
                summary,
            } => {
                println!(
                    "{}  {}  {}  {}  {}  {}  {}{}",
                    style.seq(message.sequence_number),
                    style.cyan(format!("{:<10}", "text")),
                    format_kv("obj", short_id(&object_id_b64), style),
                    format_kv("from", short_id(&message.origin_writer_id_b64), style),
                    format_kv("outbox", message.origin_outbox_id, style),
                    style.bytes(message.payload_size_bytes),
                    format_kv("ts", message.timestamp_ns, style),
                    format_text_summary(summary.as_ref(), style),
                );
            }
            SpySharedMessageKind::BinaryPut {
                object_id_b64,
                wall_time_ns,
                writer_id_b64,
            } => {
                println!(
                    "{}  {}  {}  {}  {}  {}  {}  {}  {}",
                    style.seq(message.sequence_number),
                    style.magenta(format!("{:<10}", "binary")),
                    format_kv("obj", short_id(&object_id_b64), style),
                    format_kv("from", short_id(&message.origin_writer_id_b64), style),
                    format_kv("outbox", message.origin_outbox_id, style),
                    format_kv("writer", short_id(&writer_id_b64), style),
                    format_kv("wall", wall_time_ns, style),
                    style.bytes(message.payload_size_bytes),
                    format_kv("ts", message.timestamp_ns, style),
                );
            }
        },
    }
}

fn format_text_summary(
    summary: Option<&opbox_core::spy::TextUpdateSummary>,
    style: CliStyle,
) -> String {
    let Some(summary) = summary else {
        return String::new();
    };

    let mut out = format!(
        "  {}{} {}{}",
        style.green("+"),
        style.green(format!("{}ch", summary.inserted_chars)),
        style.red("-"),
        style.red(summary.deleted_items)
    );
    if let Some(preview) = &summary.inserted_preview {
        out.push_str("  ");
        out.push_str(&style.dim("insert"));
        out.push('=');
        out.push('"');
        out.push_str(&style.green(preview));
        out.push('"');
    }
    out
}

fn format_kv(label: &str, value: impl std::fmt::Display, style: CliStyle) -> String {
    format!("{}={}", style.dim(label), value)
}

fn short_id(value: &str) -> &str {
    if value.len() <= 8 { value } else { &value[..8] }
}

#[derive(Clone, Copy)]
struct CliStyle {
    enabled: bool,
}

impl CliStyle {
    fn for_stdout() -> Self {
        Self {
            enabled: std::io::stdout().is_terminal() && std::env::var_os("NO_COLOR").is_none(),
        }
    }

    fn for_stderr() -> Self {
        Self {
            enabled: std::io::stderr().is_terminal() && std::env::var_os("NO_COLOR").is_none(),
        }
    }

    fn seq(self, sequence_number: u64) -> String {
        self.dim(format!("#{sequence_number:<6}"))
    }

    fn bytes(self, bytes: usize) -> String {
        self.dim(format!("{bytes}B"))
    }

    fn dim(self, value: impl std::fmt::Display) -> String {
        self.paint("2", value)
    }

    fn bold(self, value: impl std::fmt::Display) -> String {
        self.paint("1", value)
    }

    fn cyan(self, value: impl std::fmt::Display) -> String {
        self.paint("36", value)
    }

    fn yellow(self, value: impl std::fmt::Display) -> String {
        self.paint("33", value)
    }

    fn magenta(self, value: impl std::fmt::Display) -> String {
        self.paint("35", value)
    }

    fn green(self, value: impl std::fmt::Display) -> String {
        self.paint("32", value)
    }

    fn red(self, value: impl std::fmt::Display) -> String {
        self.paint("31", value)
    }

    fn paint(self, code: &str, value: impl std::fmt::Display) -> String {
        if self.enabled {
            format!("\x1b[{code}m{value}\x1b[0m")
        } else {
            value.to_string()
        }
    }
}

fn print_daemon_status(title: &str, status: &ipc::DaemonStatus, style: CliStyle) {
    println!("{}", style.bold(title));
    print_status_row("workspace", &status.workspace_id, style);
    print_status_row("root", &status.root, style);
    print_status_row("pid", status.pid, style);
    print_status_row("stable cursor", status.stable_cursor_end, style);
}

fn print_status_row(label: &str, value: impl std::fmt::Display, style: CliStyle) {
    println!("  {}  {}", style.dim(format!("{label:<13}")), value);
}

async fn run_start(sync_root: Option<PathBuf>) -> eyre::Result<()> {
    let root = find_workspace_root(&root_or_current(sync_root)?)?;
    if let Ok(status) = request_valid_status(&root).await {
        print_daemon_status(
            "opbox daemon already running",
            &status,
            CliStyle::for_stdout(),
        );
        return Ok(());
    }
    let (_db_path, daemon_row) = load_configured_daemon_state(&root).await?;

    let log_path = daemon_log_path(&root);
    let log_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)?;
    let stderr = log_file.try_clone()?;
    let mut command = daemon_command(&root)?;
    let mut child = command
        .stdin(Stdio::null())
        .stdout(Stdio::from(log_file))
        .stderr(Stdio::from(stderr))
        .spawn()?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(status) = request_valid_status(&root).await
            && status.workspace_id == daemon_row.workspace_id.0
        {
            print_daemon_status("opbox daemon started", &status, CliStyle::for_stdout());
            println!(
                "  {}  {}",
                CliStyle::for_stdout().dim(format!("{:<13}", "log")),
                log_path.display()
            );
            return Ok(());
        }

        if let Some(status) = child.try_wait()? {
            eyre::bail!(
                "opbox-daemon exited before becoming ready with status {status}; see {}",
                log_path.display()
            );
        }

        if tokio::time::Instant::now() >= deadline {
            eyre::bail!(
                "timed out waiting for opbox-daemon to become ready; see {}",
                log_path.display()
            );
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn run_stop(sync_root: Option<PathBuf>) -> eyre::Result<()> {
    let root = find_workspace_root(&root_or_current(sync_root)?)?;
    if request_valid_status(&root).await.is_err() {
        exit_daemon_not_running(&root);
    }
    let response = match ipc::request_stop(&root).await {
        Ok(response) => response,
        Err(_) => exit_daemon_not_running(&root),
    };

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if request_valid_status(&root).await.is_err() {
            println!(
                "stopped opbox daemon for workspace {} (pid {})",
                response.workspace_id, response.pid
            );
            return Ok(());
        }

        if tokio::time::Instant::now() >= deadline {
            eyre::bail!(
                "timed out waiting for opbox daemon {} (pid {}) to stop",
                response.workspace_id,
                response.pid
            );
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn run_daemon_logs(follow: bool, sync_root: Option<PathBuf>) -> eyre::Result<()> {
    let root = find_workspace_root(&root_or_current(sync_root)?)?;
    let log_path = daemon_log_path(&root);

    if !follow {
        println!("{}", log_path.display());
        return Ok(());
    }

    if !log_path.try_exists()? {
        eyre::bail!(
            "no daemon log at {} yet; run {} to start the daemon",
            log_path.display(),
            CliStyle::for_stderr().bold(format!("{CLIENT_COMMAND} start")),
        );
    }

    use std::os::unix::process::CommandExt;
    let error = std::process::Command::new("tail")
        .arg("-f")
        .arg(&log_path)
        .exec();
    Err(eyre::eyre!("failed to exec tail -f: {error}"))
}

/// Snapshot `S2_*` environment variables into `.opbox/env` so the daemon can
/// run without the user's shell environment. Returns the persisted names.
/// Never overwrites an existing env file.
fn persist_s2_env(sync_root: &Path) -> eyre::Result<Vec<String>> {
    let mut vars: Vec<(String, String)> = std::env::vars_os()
        .filter_map(|(key, value)| Some((key.into_string().ok()?, value.into_string().ok()?)))
        .filter(|(key, value)| key.starts_with("S2_") && !value.contains('\n'))
        .collect();
    if vars.is_empty() {
        return Ok(Vec::new());
    }
    vars.sort();

    let mut body = String::from(
        "# S2_* environment variables captured by `ob init`/`ob clone`.\n\
         # Loaded by the opbox daemon at startup; the process environment\n\
         # takes precedence over entries here.\n",
    );
    for (key, value) in &vars {
        body.push_str(key);
        body.push('=');
        body.push_str(value);
        body.push('\n');
    }

    let path = workspace_env_path(sync_root);
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    match options.open(&path) {
        Ok(mut file) => {
            use std::io::Write;
            file.write_all(body.as_bytes())?;
            Ok(vars.into_iter().map(|(key, _)| key).collect())
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(Vec::new()),
        Err(error) => Err(eyre::eyre!("failed to write {}: {error}", path.display())),
    }
}

fn with_failure_banner(command: &'static str, result: eyre::Result<()>) -> eyre::Result<()> {
    if result.is_err() {
        let style = CliStyle::for_stderr();
        eprintln!("{}", style.red(format!("{command} failed")));
    }
    result
}

fn exit_daemon_not_running(root: &Path) -> ! {
    let style = CliStyle::for_stderr();
    eprintln!(
        "{}",
        style.yellow(format!(
            "opbox daemon is not running for {}",
            root.display()
        ))
    );
    eprintln!(
        "run {} to start it",
        style.bold(format!("{CLIENT_COMMAND} start"))
    );
    std::process::exit(1);
}

async fn request_valid_status(root: &Path) -> eyre::Result<ipc::DaemonStatus> {
    let status = ipc::request_status(root).await?;
    let status_root = PathBuf::from(&status.root);
    let status_root = status_root.canonicalize().map_err(|error| {
        let _ = remove_socket_pointer(root);
        eyre::eyre!(
            "daemon reported non-canonicalizable root {}: {error}; removed stale socket pointer",
            status.root
        )
    })?;
    let root = root.canonicalize()?;
    if status_root != root {
        let _ = remove_socket_pointer(&root);
        eyre::bail!(
            "daemon socket belongs to {}; expected {}; removed stale socket pointer",
            status_root.display(),
            root.display()
        );
    }
    Ok(status)
}

fn daemon_command(root: &Path) -> eyre::Result<TokioCommand> {
    let current_exe = std::env::current_exe()?;
    let sibling =
        current_exe.with_file_name(format!("opbox-daemon{}", std::env::consts::EXE_SUFFIX));
    if sibling.exists() {
        let mut command = TokioCommand::new(sibling);
        command.arg("--root").arg(root);
        return Ok(command);
    }

    if cfg!(debug_assertions)
        && let Some(manifest_dir) = option_env!("CARGO_MANIFEST_DIR")
    {
        let workspace_manifest = Path::new(manifest_dir)
            .parent()
            .and_then(Path::parent)
            .map(|workspace_root| workspace_root.join("Cargo.toml"))
            .ok_or_else(|| {
                eyre::eyre!("could not derive workspace manifest from {manifest_dir}")
            })?;
        let mut command = TokioCommand::new("cargo");
        command
            .arg("run")
            .arg("--quiet")
            .arg("--bin")
            .arg("opbox-daemon")
            .arg("--manifest-path")
            .arg(workspace_manifest)
            .arg("--")
            .arg("--root")
            .arg(root);
        return Ok(command);
    }

    let mut command = TokioCommand::new("opbox-daemon");
    command.arg("--root").arg(root);
    Ok(command)
}

#[tokio::main]
async fn main() {
    init_tracing();

    let result = match Args::parse().command {
        Command::Init { sync_root } => with_failure_banner(
            "init",
            async { run_bootstrap(bootstrap_init(sync_root).await?).await }.await,
        ),
        Command::Clone {
            workspace,
            sync_root,
        } => with_failure_banner(
            "clone",
            async { run_bootstrap(bootstrap_clone(workspace, sync_root).await?).await }.await,
        ),
        Command::Start { sync_root } => run_start(sync_root).await,
        Command::Stop { sync_root } => run_stop(sync_root).await,
        Command::Status { sync_root } => run_status(sync_root).await,
        Command::Spy { sync_root } => run_spy(sync_root).await,
        Command::Logs { follow, sync_root } => run_daemon_logs(follow, sync_root),
    };

    if let Err(error) = result {
        render_error(&error);
        std::process::exit(1);
    }
}

/// Render CLI failures for humans: known situations get guidance, everything
/// else gets a single `error:` line with the cause chain (no report/Location
/// noise; set RUST_LOG for diagnostics).
fn render_error(error: &eyre::Report) {
    let style = CliStyle::for_stderr();
    if let Some(not_in_workspace) = error.downcast_ref::<NotInWorkspace>() {
        eprintln!("{}", style.yellow(not_in_workspace.to_string()));
        eprintln!(
            "run {} to create a workspace here, or {} to fetch an existing one",
            style.bold(format!("{CLIENT_COMMAND} init")),
            style.bold(format!("{CLIENT_COMMAND} clone --workspace <ID>")),
        );
        return;
    }
    eprintln!("{} {error:#}", style.red("error:"));
}

#[cfg(test)]
mod tests {
    use super::*;
    use opbox_core::app::workspace::metadata_dir;

    #[test]
    fn persists_s2_vars_to_workspace_env_once() -> eyre::Result<()> {
        let sync_root =
            std::env::temp_dir().join(format!("ob-env-persist-test-{}", rand::random::<u64>()));
        std::fs::create_dir(&sync_root)?;
        std::fs::create_dir(metadata_dir(&sync_root))?;

        let key = format!("S2_PERSIST_TEST_{}", rand::random::<u32>());
        unsafe { std::env::set_var(&key, "tok-123") };

        let persisted = persist_s2_env(&sync_root)?;
        assert!(persisted.contains(&key));
        let body = std::fs::read_to_string(workspace_env_path(&sync_root))?;
        assert!(body.contains(&format!("{key}=tok-123")));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(workspace_env_path(&sync_root))?
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600, "env file must not be world-readable");
        }

        // A second run must not clobber the existing file.
        unsafe { std::env::set_var(&key, "tok-456") };
        assert!(persist_s2_env(&sync_root)?.is_empty());
        let body = std::fs::read_to_string(workspace_env_path(&sync_root))?;
        assert!(body.contains("tok-123"));

        unsafe { std::env::remove_var(&key) };
        let _ = std::fs::remove_dir_all(&sync_root);
        Ok(())
    }
}
