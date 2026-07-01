use clap::builder::styling;
use clap::{Parser, Subcommand, ValueEnum};
use indicatif::{ProgressBar, ProgressStyle};
use opbox_core::app::connectivity::{ConnectivityOverallState, LinkState, LinkStatus};
use opbox_core::app::db::{open_database, semantic_pool};
use opbox_core::app::ipc;
use opbox_core::app::runtime::{AppRuntime, AppRuntimeConfig, RunMode};
use opbox_core::app::s2::{
    S2ConnectionConfig, basin_default_stream_retention_warning, create_workspace_stream,
    ensure_workspace_stream_exists, s2_client_from_config, workspace_stream_retention_warning,
};
use opbox_core::app::user_config::{
    UserConfig, UserConfigKey, load_user_config, load_user_config_from_path,
    parse_bool_config_value, save_user_config, save_user_config_to_path, skip_retention_checks,
    user_config_path,
};
use opbox_core::app::workspace::{
    NotInWorkspace, canonicalize_existing_dir, create_metadata_dir, current_dir, daemon_log_path,
    ensure_clean_clone_root, ensure_sync_root_unconfigured, find_workspace_root,
    load_configured_daemon_state, pid_path, remove_pid, remove_socket_pointer,
    remove_stale_socket_files, socket_link_path, storage_db_path, workspace_config_path,
};
use opbox_core::fs::fio::local::LocalFileIO;
use opbox_core::fs::ignore::{IGNORE_FILE_NAME, METADATA_DIR_NAME, default_ignore_file_contents};
use opbox_core::log::types::LogReadStop;
use opbox_core::semantic::service::SemanticService;
use opbox_core::semantic::table::daemon_state;
use opbox_core::spy::{NamespaceSpyTracker, SpyEvent, SpySharedMessageKind};
use opbox_core::types::{
    DaemonWriterId, OutboxId, WorkspaceId, short_crockford_base32_lower_from_b64,
};
use s2_sdk::{
    S2, S2Basin,
    types::{
        AccessTokenId, AccessTokenIdPrefix, AccessTokenMatcher, AccessTokenScopeInput,
        AccountEndpoint, BasinEndpoint, BasinMatcher, BasinName, IssueAccessTokenInput,
        ListAccessTokensInput, Operation, S2DateTime, S2Error, StreamMatcher, StreamNamePrefix,
    },
};
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::str::FromStr;
use std::time::Duration;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::process::Command as TokioCommand;
use tracing::debug;

const CLIENT_COMMAND: &str = "ob";
const GITIGNORE_FILE_NAME: &str = ".gitignore";
const SHARE_TOKEN_NAMESPACE: &str = "opbox";
const BOOTSTRAP_SHARE_ID: &str = "bootstrap";

const STYLES: styling::Styles = styling::Styles::styled()
    .header(styling::AnsiColor::Green.on_default().bold())
    .usage(styling::AnsiColor::Green.on_default().bold())
    .literal(styling::AnsiColor::Blue.on_default().bold())
    .placeholder(styling::AnsiColor::Cyan.on_default());

#[derive(Parser, Debug)]
#[command(
    name = "ob",
    version = concat!(env!("CARGO_PKG_VERSION"), " ", env!("OPBOX_BUILD_HASH")),
    styles = STYLES
)]
struct Args {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand, Clone, Debug)]
enum Command {
    /// Manage opbox configuration.
    Config {
        /// Manage the current workspace config instead of the user-wide config.
        #[arg(long)]
        workspace: bool,
        #[command(subcommand)]
        command: ConfigCommand,
    },

    /// Initialize a new opbox workspace.
    /// Uses the $PWD unless a sync root is specified.
    Init {
        #[command(flatten)]
        config: WorkspaceConfigOverrides,
        sync_root: Option<PathBuf>,
    },

    /// Clone an existing workspace.
    Clone {
        #[arg(long)]
        workspace: WorkspaceId,
        /// Clone only shared log records at or before this RFC3339 timestamp.
        #[arg(long, value_name = "RFC3339")]
        as_of: Option<CloneAsOf>,
        #[command(flatten)]
        config: WorkspaceConfigOverrides,
        sync_root: Option<PathBuf>,
    },

    /// Start the daemon in an existing workspace.
    Start { sync_root: Option<PathBuf> },

    /// Stop the daemon.
    Stop { sync_root: Option<PathBuf> },

    /// Get status of current workspace.
    Status { sync_root: Option<PathBuf> },

    /// Attach to daemon in order to see CRDT ops as they are received.
    Spy { sync_root: Option<PathBuf> },

    /// Manage share tokens for the current workspace.
    Share {
        #[command(subcommand)]
        command: ShareCommand,
    },

    /// Inspect the daemon process logs.
    Logs {
        /// Tail the log (via `tail -f`).
        #[arg(short, long)]
        follow: bool,
        sync_root: Option<PathBuf>,
    },
}

#[derive(Subcommand, Clone, Debug)]
enum ShareCommand {
    /// Create a limited S2 access token and print a clone command.
    New {
        /// Human-readable share id. The S2 token id will be opbox-$workspace-$id.
        #[arg(long)]
        id: Option<String>,
        /// Token expiration timestamp. Defaults to the issuer token's expiration.
        #[arg(long, value_name = "RFC3339")]
        expires_at: Option<S2DateTime>,
        sync_root: Option<PathBuf>,
    },

    /// List share tokens for the current workspace.
    List { sync_root: Option<PathBuf> },

    /// Revoke a share token by share id.
    Revoke {
        id: String,
        sync_root: Option<PathBuf>,
    },
}

#[derive(clap::Args, Clone, Debug, Default)]
struct WorkspaceConfigOverrides {
    /// Persist a workspace-local basin override.
    #[arg(long)]
    basin: Option<String>,
    /// Persist a workspace-local S2 access token override.
    #[arg(long)]
    access_token: Option<String>,
    /// Persist a workspace-local S2 account endpoint override.
    #[arg(long)]
    account_endpoint: Option<String>,
    /// Persist a workspace-local S2 basin endpoint override.
    #[arg(long)]
    basin_endpoint: Option<String>,
}

#[derive(Subcommand, Clone, Debug)]
enum ConfigCommand {
    /// Print the config file path.
    Path,

    /// List configured values.
    List,

    /// List supported config keys.
    Keys,

    /// Get one configured value.
    Get {
        #[arg(value_enum)]
        key: ConfigKey,
    },

    /// Set one configured value.
    Set {
        #[arg(value_enum)]
        key: ConfigKey,
        value: String,
    },

    /// Unset one configured value.
    Unset {
        #[arg(value_enum)]
        key: ConfigKey,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
#[clap(rename_all = "kebab-case")]
enum ConfigKey {
    Basin,
    #[value(alias = "default_basin")]
    DefaultBasin,
    #[value(alias = "access_token")]
    AccessToken,
    #[value(alias = "account_endpoint")]
    AccountEndpoint,
    #[value(alias = "basin_endpoint")]
    BasinEndpoint,
    #[value(alias = "daemon_log_level")]
    DaemonLogLevel,
    #[value(alias = "client_log_level")]
    ClientLogLevel,
    #[value(alias = "skip_retention_checks")]
    SkipRetentionChecks,
}

impl ConfigKey {
    fn user_config_key(self) -> UserConfigKey {
        match self {
            Self::Basin => UserConfigKey::Basin,
            Self::DefaultBasin => UserConfigKey::DefaultBasin,
            Self::AccessToken => UserConfigKey::AccessToken,
            Self::AccountEndpoint => UserConfigKey::AccountEndpoint,
            Self::BasinEndpoint => UserConfigKey::BasinEndpoint,
            Self::DaemonLogLevel => UserConfigKey::DaemonLogLevel,
            Self::ClientLogLevel => UserConfigKey::ClientLogLevel,
            Self::SkipRetentionChecks => UserConfigKey::SkipRetentionChecks,
        }
    }
}

struct Bootstrap {
    mode: RunMode,
    db_path: PathBuf,
    sync_root: PathBuf,
    daemon_row: daemon_state::Row,
    s2_connection: S2ConnectionConfig,
    s2_basin: S2Basin,
    clone_log_read_stop: Option<LogReadStop>,
}

#[derive(Clone, Copy, Debug)]
struct CloneAsOf {
    exclusive_until_timestamp_ms: u64,
}

impl CloneAsOf {
    fn log_read_stop(self) -> LogReadStop {
        LogReadStop::UntilTimestampMs(self.exclusive_until_timestamp_ms)
    }
}

impl FromStr for CloneAsOf {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.as_bytes().get(10) {
            Some(b'T' | b't') => {}
            _ => {
                return Err(
                    "timestamp must use RFC3339 format, e.g. 2026-06-18T01:30:00Z".to_string(),
                );
            }
        }
        let timestamp = OffsetDateTime::parse(value, &Rfc3339)
            .map_err(|err| format!("invalid RFC3339 timestamp: {err}"))?;
        let timestamp_ns = timestamp.unix_timestamp_nanos();
        if timestamp_ns < 0 {
            return Err("as-of timestamp must be at or after the Unix epoch".to_string());
        }

        let inclusive_timestamp_ms = u64::try_from(timestamp_ns / 1_000_000)
            .map_err(|err| format!("as-of timestamp is out of range: {err}"))?;
        let exclusive_until_timestamp_ms = inclusive_timestamp_ms
            .checked_add(1)
            .ok_or_else(|| "as-of timestamp is out of range".to_string())?;

        Ok(Self {
            exclusive_until_timestamp_ms,
        })
    }
}

fn init_tracing(client_log_level: Option<&str>) -> eyre::Result<()> {
    // The CLI communicates through stdout, not logs; default to warnings only.
    let filter = tracing_subscriber::EnvFilter::try_new(client_log_level.unwrap_or("warn"))
        .map_err(|err| eyre::eyre!("invalid client-log-level: {err}"))?;
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .try_init();
    Ok(())
}

fn root_or_current(sync_root: Option<PathBuf>) -> eyre::Result<PathBuf> {
    match sync_root {
        Some(path) => Ok(path),
        None => current_dir(),
    }
}

fn save_workspace_config(root: &Path, config: &UserConfig) -> eyre::Result<()> {
    save_user_config_to_path(config, &workspace_config_path(root))
}

fn fresh_daemon_state_row(
    workspace_id: WorkspaceId,
    basin: BasinName,
    connection: &S2ConnectionConfig,
) -> daemon_state::Row {
    let writer_id = rand::random::<[u8; 16]>();
    let (s2_account_endpoint, s2_basin_endpoint) = connection.endpoint_pair_for_metadata();

    daemon_state::Row {
        workspace_id,
        s2_basin: basin,
        s2_account_endpoint,
        s2_basin_endpoint,
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
            file.write_all(default_ignore_file_contents_for_root(sync_root)?.as_bytes())?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn default_ignore_file_contents_for_root(sync_root: &Path) -> eyre::Result<String> {
    let mut contents = default_ignore_file_contents().to_string();
    let gitignore_contents = gitignore_contents_for_opboxignore(sync_root)?;
    if gitignore_contents.trim().is_empty() {
        return Ok(contents);
    }

    if !contents.ends_with('\n') {
        contents.push('\n');
    }
    contents.push_str("\n# Imported from .gitignore by ob init.\n");
    contents.push_str("# Edit this file if opbox should track a different set of paths.\n");
    contents.push_str(gitignore_contents.trim_end());
    contents.push('\n');
    Ok(contents)
}

fn gitignore_contents_for_opboxignore(sync_root: &Path) -> eyre::Result<String> {
    let path = sync_root.join(GITIGNORE_FILE_NAME);
    match std::fs::read_to_string(&path) {
        Ok(contents) => {
            let mut normalized = contents.replace("\r\n", "\n");
            if !normalized.ends_with('\n') {
                normalized.push('\n');
            }
            Ok(normalized)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(String::new()),
        Err(error) => Err(error.into()),
    }
}

fn gitignore_contains_metadata_dir(contents: &str) -> bool {
    contents.lines().map(str::trim).any(|line| {
        if line.is_empty() || line.starts_with('#') {
            return false;
        }
        line == METADATA_DIR_NAME
            || line == format!("{METADATA_DIR_NAME}/")
            || line == format!("/{METADATA_DIR_NAME}")
            || line == format!("/{METADATA_DIR_NAME}/")
    })
}

fn ensure_gitignore_metadata_entry(contents: &mut String) {
    if gitignore_contains_metadata_dir(contents) {
        return;
    }

    if !contents.is_empty() && !contents.ends_with('\n') {
        contents.push('\n');
    }
    contents.push_str(METADATA_DIR_NAME);
    contents.push('\n');
}

fn add_metadata_dir_to_gitignore_if_present(sync_root: &Path) -> eyre::Result<()> {
    let path = sync_root.join(GITIGNORE_FILE_NAME);
    let mut contents = match std::fs::read_to_string(&path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };

    ensure_gitignore_metadata_entry(&mut contents);
    std::fs::write(&path, contents)?;
    Ok(())
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

fn parse_basin_config_value(key: &str, value: &str) -> eyre::Result<BasinName> {
    value
        .parse()
        .map_err(|err| eyre::eyre!("invalid {key} {value:?}: {err}"))
}

fn basin_from_config_stack(
    workspace_config: &UserConfig,
    user_config: &UserConfig,
) -> eyre::Result<BasinName> {
    if let Some(value) = &workspace_config.basin {
        return parse_basin_config_value("basin", value);
    }
    if let Some(value) = &user_config.default_basin {
        return parse_basin_config_value("default-basin", value);
    }
    eyre::bail!(
        "no basin configured; run `ob config set default-basin <basin>` \
         or pass `--basin <basin>`"
    );
}

fn workspace_config_from_overrides(
    overrides: &WorkspaceConfigOverrides,
    user_config: &UserConfig,
) -> eyre::Result<(UserConfig, BasinName, S2ConnectionConfig)> {
    let mut workspace_config = UserConfig::default();
    if let Some(value) = &overrides.basin {
        validate_config_value(ConfigKey::Basin, value)?;
        workspace_config.basin = Some(value.clone());
    }
    if let Some(value) = &overrides.access_token {
        validate_config_value(ConfigKey::AccessToken, value)?;
        workspace_config.access_token = Some(value.clone());
    }
    if let Some(value) = &overrides.account_endpoint {
        validate_config_value(ConfigKey::AccountEndpoint, value)?;
        workspace_config.account_endpoint = Some(value.clone());
    }
    if let Some(value) = &overrides.basin_endpoint {
        validate_config_value(ConfigKey::BasinEndpoint, value)?;
        workspace_config.basin_endpoint = Some(value.clone());
    }

    let basin = basin_from_config_stack(&workspace_config, user_config)?;
    workspace_config.basin = Some(basin.as_ref().to_string());
    let s2_connection = S2ConnectionConfig::from_workspace_or_user_config(
        &workspace_config,
        None,
        None,
        user_config,
    )?;
    Ok((workspace_config, basin, s2_connection))
}

async fn bootstrap_init(
    sync_root: Option<PathBuf>,
    user_config: &UserConfig,
    overrides: &WorkspaceConfigOverrides,
    progress: &BootstrapProgress,
) -> eyre::Result<Bootstrap> {
    progress.set("resolving configuration");
    let (workspace_config, basin, s2_connection) =
        workspace_config_from_overrides(overrides, user_config)?;
    progress.set("checking workspace root");
    let sync_root = canonicalize_existing_dir(&root_or_current(sync_root)?)?;
    ensure_sync_root_unconfigured(&sync_root).await?;
    progress.set(format!("connecting to basin {}", basin.as_ref()));
    let s2 = s2_client_from_config(&s2_connection)?;
    let s2_basin = s2.basin(basin.clone());
    let workspace_id = WorkspaceId::generate();
    debug!(?workspace_id, "generated workspace id");
    progress.set("creating shared log stream");
    create_workspace_stream(&s2_basin, &workspace_id).await?;
    progress.set("checking shared log retention");
    warn_if_retention_not_infinite(
        &s2,
        &s2_basin,
        &basin,
        &workspace_id,
        &workspace_config,
        user_config,
        progress,
    )
    .await?;
    let daemon_row = fresh_daemon_state_row(workspace_id, basin, &s2_connection);
    progress.set("writing local metadata");
    create_metadata_dir(&sync_root)?;
    save_workspace_config(&sync_root, &workspace_config)?;
    create_default_ignore_file(&sync_root)?;
    add_metadata_dir_to_gitignore_if_present(&sync_root)?;
    create_initialized_database(&sync_root, &daemon_row).await?;
    Ok(Bootstrap {
        mode: RunMode::Init,
        db_path: storage_db_path(&sync_root),
        sync_root,
        daemon_row,
        s2_connection,
        s2_basin,
        clone_log_read_stop: None,
    })
}

async fn bootstrap_clone(
    workspace: WorkspaceId,
    sync_root: Option<PathBuf>,
    clone_log_read_stop: Option<LogReadStop>,
    user_config: &UserConfig,
    overrides: &WorkspaceConfigOverrides,
    progress: &BootstrapProgress,
) -> eyre::Result<Bootstrap> {
    progress.set("resolving configuration");
    let (workspace_config, basin, s2_connection) =
        workspace_config_from_overrides(overrides, user_config)?;
    progress.set("checking destination");
    let requested_root = root_or_current(sync_root)?;
    if requested_root.try_exists()? && requested_root.is_dir() {
        ensure_sync_root_unconfigured(&requested_root).await?;
    }
    let sync_root = ensure_clean_clone_root(&requested_root)?;
    ensure_sync_root_unconfigured(&sync_root).await?;
    progress.set(format!("connecting to basin {}", basin.as_ref()));
    let s2 = s2_client_from_config(&s2_connection)?;
    let s2_basin = s2.basin(basin.clone());
    progress.set("checking shared log stream");
    ensure_workspace_stream_exists(&s2_basin, &workspace).await?;
    progress.set("checking shared log retention");
    warn_if_retention_not_infinite(
        &s2,
        &s2_basin,
        &basin,
        &workspace,
        &workspace_config,
        user_config,
        progress,
    )
    .await?;
    let daemon_row = fresh_daemon_state_row(workspace, basin, &s2_connection);
    progress.set("writing local metadata");
    create_metadata_dir(&sync_root)?;
    save_workspace_config(&sync_root, &workspace_config)?;
    create_initialized_database(&sync_root, &daemon_row).await?;
    Ok(Bootstrap {
        mode: RunMode::Clone,
        db_path: storage_db_path(&sync_root),
        sync_root,
        daemon_row,
        s2_connection,
        s2_basin,
        clone_log_read_stop,
    })
}

async fn warn_if_retention_not_infinite(
    s2: &S2,
    s2_basin: &S2Basin,
    basin: &BasinName,
    workspace_id: &WorkspaceId,
    workspace_config: &UserConfig,
    user_config: &UserConfig,
    progress: &BootstrapProgress,
) -> eyre::Result<()> {
    if skip_retention_checks(workspace_config, user_config)? {
        return Ok(());
    }
    let context = RetentionWarningContext {
        basin: basin.as_ref(),
        workspace_id: &workspace_id.0,
    };

    match workspace_stream_retention_warning(s2_basin, workspace_id).await {
        Ok(Some(warning)) => {
            progress.suspend(|| print_warning(&warning, context, CliStyle::for_stderr()));
        }
        Ok(None) => {}
        Err(error) => {
            progress.suspend(|| {
                print_retention_check_warning(
                    "workspace ops stream",
                    &error,
                    CliStyle::for_stderr(),
                )
            });
        }
    }

    match basin_default_stream_retention_warning(s2, basin.clone()).await {
        Ok(Some(warning)) => {
            progress.suspend(|| print_warning(&warning, context, CliStyle::for_stderr()));
        }
        Ok(None) => {}
        Err(error) => {
            progress.suspend(|| {
                print_retention_check_warning(
                    "basin default stream",
                    &error,
                    CliStyle::for_stderr(),
                )
            });
        }
    }

    Ok(())
}

struct ShareContext {
    root: PathBuf,
    workspace_id: WorkspaceId,
    basin: BasinName,
    connection: S2ConnectionConfig,
}

struct IssuedShareToken {
    id: AccessTokenId,
    access_token: String,
}

async fn load_share_context(sync_root: Option<PathBuf>) -> eyre::Result<ShareContext> {
    let root = find_workspace_root(&root_or_current(sync_root)?)?;
    let status = match request_valid_status(&root).await {
        Ok(status) => status,
        Err(error) => exit_daemon_not_running_or_report(&root, error),
    };
    let workspace_config = load_user_config_from_path(&workspace_config_path(&root))?;
    let user_config = load_user_config()?;
    let workspace_id = status.workspace_id.parse::<WorkspaceId>()?;
    let basin = parse_basin_config_value("basin", &status.basin)?;
    let connection = S2ConnectionConfig::from_workspace_or_user_config(
        &workspace_config,
        None,
        None,
        &user_config,
    )?;

    Ok(ShareContext {
        root,
        workspace_id,
        basin,
        connection,
    })
}

fn share_token_prefix(workspace_id: &WorkspaceId) -> String {
    format!("{SHARE_TOKEN_NAMESPACE}-{}-", workspace_id.0)
}

fn generated_share_id() -> String {
    format!("share-{:016x}", rand::random::<u64>())
}

fn share_token_id(workspace_id: &WorkspaceId, share_id: &str) -> eyre::Result<AccessTokenId> {
    if share_id.is_empty() {
        eyre::bail!("share id cannot be empty");
    }

    let token_id = format!("{}{share_id}", share_token_prefix(workspace_id));
    token_id
        .parse()
        .map_err(|err| eyre::eyre!("invalid share token id {token_id:?}: {err}"))
}

fn share_token_id_prefix(workspace_id: &WorkspaceId) -> eyre::Result<AccessTokenIdPrefix> {
    let prefix = share_token_prefix(workspace_id);
    prefix
        .parse()
        .map_err(|err| eyre::eyre!("invalid share token prefix {prefix:?}: {err}"))
}

fn share_token_stream_prefix(workspace_id: &WorkspaceId) -> eyre::Result<StreamNamePrefix> {
    let prefix = format!("{}/", workspace_id.0);
    prefix
        .parse()
        .map_err(|err| eyre::eyre!("invalid workspace stream prefix {prefix:?}: {err}"))
}

fn share_id_from_token_id<'a>(workspace_id: &WorkspaceId, token_id: &'a str) -> &'a str {
    token_id
        .strip_prefix(&share_token_prefix(workspace_id))
        .unwrap_or(token_id)
}

fn share_token_scope(
    workspace_id: &WorkspaceId,
    basin: &BasinName,
) -> eyre::Result<AccessTokenScopeInput> {
    Ok(AccessTokenScopeInput::from_ops([
        Operation::GetBasinConfig,
        Operation::CreateStream,
        Operation::ListStreams,
        Operation::GetStreamConfig,
        Operation::CheckTail,
        Operation::Append,
        Operation::Read,
    ])
    .with_basins(BasinMatcher::Exact(basin.clone()))
    .with_streams(StreamMatcher::Prefix(share_token_stream_prefix(
        workspace_id,
    )?))
    .with_access_tokens(AccessTokenMatcher::None))
}

fn share_token_management_error(
    action: &'static str,
    required_permission: &'static str,
    error: impl std::fmt::Display,
) -> eyre::Report {
    eyre::eyre!(
        "failed to {action} share token: {error}\n\
         share token management requires S2 Cloud and an access token allowed to {required_permission} access tokens; \
         use the workspace owner/admin token"
    )
}

fn share_token_management_report(
    action: &'static str,
    required_permission: &'static str,
    error: eyre::Report,
) -> eyre::Report {
    if error.downcast_ref::<S2Error>().is_some() {
        share_token_management_error(action, required_permission, format!("{error:#}"))
    } else {
        error
    }
}

async fn issue_share_token(
    ctx: &ShareContext,
    share_id: &str,
    expires_at: Option<S2DateTime>,
) -> eyre::Result<IssuedShareToken> {
    let token_id = share_token_id(&ctx.workspace_id, share_id)?;
    let scope = share_token_scope(&ctx.workspace_id, &ctx.basin)?;
    let mut input = IssueAccessTokenInput::new(token_id.clone(), scope);
    if let Some(expires_at) = expires_at {
        input = input.with_expires_at(expires_at);
    }

    let s2 = s2_client_from_config(&ctx.connection)?;
    let access_token = s2.issue_access_token(input).await?;
    Ok(IssuedShareToken {
        id: token_id,
        access_token,
    })
}

async fn run_share(command: ShareCommand) -> eyre::Result<()> {
    match command {
        ShareCommand::New {
            id,
            expires_at,
            sync_root,
        } => {
            let ctx = load_share_context(sync_root).await?;
            let share_id = id.unwrap_or_else(generated_share_id);
            let token = issue_share_token(&ctx, &share_id, expires_at)
                .await
                .map_err(|error| share_token_management_report("create", "issue", error))?;
            let style = CliStyle::for_stdout();
            println!("{}", style.bold("created opbox share token"));
            print_status_row("workspace", &ctx.workspace_id.0, style);
            print_status_row("basin", ctx.basin.as_ref(), style);
            print_status_row("share id", &share_id, style);
            print_status_row("token id", &token.id, style);
            println!();
            print_share_clone_command(
                &ctx.workspace_id,
                &ctx.basin,
                &token.access_token,
                &ctx.connection,
                style,
            );
            Ok(())
        }
        ShareCommand::List { sync_root } => {
            let ctx = load_share_context(sync_root).await?;
            let s2 = s2_client_from_config(&ctx.connection)?;
            let prefix = share_token_id_prefix(&ctx.workspace_id)?;
            let page = s2
                .list_access_tokens(
                    ListAccessTokensInput::new()
                        .with_prefix(prefix)
                        .with_limit(1000),
                )
                .await
                .map_err(|error| share_token_management_error("list", "list", error))?;
            let style = CliStyle::for_stdout();
            println!("{}", style.bold("opbox share tokens"));
            print_status_row("workspace", &ctx.workspace_id.0, style);
            print_status_row("basin", ctx.basin.as_ref(), style);
            print_status_row("root", ctx.root.display(), style);
            if page.values.is_empty() {
                print_status_row("tokens", "none", style);
                return Ok(());
            }
            println!();
            for token in page.values {
                let token_id = token.id.to_string();
                println!(
                    "  {}  {}  {}",
                    style.bold(format!(
                        "{:<20}",
                        share_id_from_token_id(&ctx.workspace_id, &token_id)
                    )),
                    style.dim("expires"),
                    token.expires_at
                );
                println!("  {}  {}", style.dim(format!("{:<20}", "")), token_id);
            }
            if page.has_more {
                eprintln!(
                    "{} more share tokens exist; prefix listing is currently limited to 1000 results",
                    CliStyle::for_stderr().yellow("warning:")
                );
            }
            Ok(())
        }
        ShareCommand::Revoke { id, sync_root } => {
            let ctx = load_share_context(sync_root).await?;
            let token_id = share_token_id(&ctx.workspace_id, &id)?;
            let s2 = s2_client_from_config(&ctx.connection)?;
            s2.revoke_access_token(token_id.clone())
                .await
                .map_err(|error| share_token_management_error("revoke", "revoke", error))?;
            println!("revoked share token {token_id}");
            Ok(())
        }
    }
}

async fn run_bootstrap(bootstrap: Bootstrap, progress: &BootstrapProgress) -> eyre::Result<()> {
    let mode = bootstrap.mode;
    let sync_root = bootstrap.sync_root.clone();
    let workspace_id = bootstrap.daemon_row.workspace_id.clone();
    let basin = bootstrap.daemon_row.s2_basin.clone();
    let s2_connection = bootstrap.s2_connection.clone();

    progress.set(match mode {
        RunMode::Init => "uploading initial workspace snapshot",
        RunMode::Clone => "downloading shared log and materializing files",
        RunMode::Sync => unreachable!("bootstrap never runs in sync mode"),
    });
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
        clone_log_read_stop: bootstrap.clone_log_read_stop,
        engine_status: None,
        spy_tx: None,
    })
    .run_until_shutdown()
    .await?;

    let bootstrap_share_token = if mode == RunMode::Init {
        progress.set("creating bootstrap share token");
        let ctx = ShareContext {
            root: sync_root.clone(),
            workspace_id: workspace_id.clone(),
            basin: basin.clone(),
            connection: s2_connection.clone(),
        };
        match issue_share_token(&ctx, BOOTSTRAP_SHARE_ID, None).await {
            Ok(token) => Some(token),
            Err(error) => {
                progress.suspend(|| print_bootstrap_share_warning(&error, CliStyle::for_stderr()));
                None
            }
        }
    } else {
        None
    };

    if mode == RunMode::Clone {
        progress.set("writing local ignore rules");
        create_default_ignore_file(&sync_root)?;
    }

    progress.finish();
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
    if mode == RunMode::Init {
        println!();
        println!(
            "your workspace is: {}",
            style.bold(style.green(&workspace_id.0))
        );
        println!();
        if let Some(token) = bootstrap_share_token {
            print_status_row("share token", token.id, style);
            println!();
            print_share_clone_command(
                &workspace_id,
                &basin,
                &token.access_token,
                &s2_connection,
                style,
            );
            println!();
        } else {
            print_bootstrap_share_next_step(style);
            println!();
        }
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
        Err(error) => exit_daemon_not_running_or_report(&root, error),
    };

    print_daemon_status("opbox daemon running", &status, CliStyle::for_stdout());
    Ok(())
}

async fn run_spy(sync_root: Option<PathBuf>) -> eyre::Result<()> {
    let root = find_workspace_root(&root_or_current(sync_root)?)?;
    let status = match request_valid_status(&root).await {
        Ok(status) => status,
        Err(error) => exit_daemon_not_running_or_report(&root, error),
    };
    eprintln!(
        "spying on opbox workspace {} (pid {})",
        status.workspace_id, status.pid
    );

    let mut stream = ipc::open_spy_stream(&root).await?;
    let style = CliStyle::for_stdout();
    let mut spy_state = SpyPrintState::new();
    loop {
        tokio::select! {
            ctrl_c = tokio::signal::ctrl_c() => {
                ctrl_c?;
                return Ok(());
            }
            event = stream.next_event() => {
                match event? {
                    Some(event) => print_spy_event(event, style, &mut spy_state),
                    None => return Ok(()),
                }
            }
        }
    }
}

struct SpyPrintState {
    ns_tracker: NamespaceSpyTracker,
    daemon_writer_id_b64: Option<String>,
}

impl SpyPrintState {
    fn new() -> Self {
        Self {
            ns_tracker: NamespaceSpyTracker::new(),
            daemon_writer_id_b64: None,
        }
    }

    fn format_origin_writer(&self, origin_writer_id_b64: &str, style: CliStyle) -> String {
        let short = short_crockford_base32_lower_from_b64(origin_writer_id_b64);
        match &self.daemon_writer_id_b64 {
            Some(daemon_writer_id_b64) if daemon_writer_id_b64 == origin_writer_id_b64 => {
                format_kv("from", format!("{short}{}", style.green("(you)")), style)
            }
            Some(_) => format_kv(
                "from",
                format!("{short}{}", style.magenta("(remote)")),
                style,
            ),
            _ => format_kv("from", short, style),
        }
    }
}

fn print_spy_event(event: SpyEvent, style: CliStyle, state: &mut SpyPrintState) {
    match event {
        SpyEvent::SessionStarted {
            daemon_writer_id_b64,
        } => {
            println!(
                "{}  {}",
                style.dim("session"),
                format_kv(
                    "daemon",
                    short_crockford_base32_lower_from_b64(&daemon_writer_id_b64),
                    style
                ),
            );
            state.daemon_writer_id_b64 = Some(daemon_writer_id_b64);
        }
        SpyEvent::Lagged { skipped } => {
            println!(
                "{} {}={skipped}",
                style.yellow("lagged"),
                style.dim("skipped")
            );
        }
        SpyEvent::NamespaceSnapshot { yjs_state_b64 } => {
            state.ns_tracker.seed_b64(&yjs_state_b64);
        }
        SpyEvent::SharedMessage(message) => match message.message {
            SpySharedMessageKind::NamespaceUpdate {
                yjs_update_b64,
                summary,
            } => {
                let local_summary = state.ns_tracker.apply_b64(&yjs_update_b64);
                let summary = summary.as_ref().or(local_summary.as_ref());
                let shared_object_id = summary.and_then(namespace_summary_single_object_id);
                let shared_object_field = shared_object_id
                    .map(|object_id| {
                        format!(
                            "  {}",
                            format_kv(
                                "obj",
                                short_crockford_base32_lower_from_b64(object_id),
                                style
                            )
                        )
                    })
                    .unwrap_or_default();
                let summary_field = format_namespace_summary(summary, shared_object_id, style);
                println!(
                    "{}  {}  {}  {}{}{}",
                    style.spy_position(message.sequence_number, message.timestamp_ns),
                    style.spy_bytes(message.payload_size_bytes),
                    style.yellow(format!("{:<10}", "namespace")),
                    state.format_origin_writer(&message.origin_writer_id_b64, style),
                    shared_object_field,
                    summary_field,
                );
            }
            SpySharedMessageKind::TextUpdate {
                object_id_b64,
                summary,
            } => {
                println!(
                    "{}  {}  {}  {}  {}{}",
                    style.spy_position(message.sequence_number, message.timestamp_ns),
                    style.spy_bytes(message.payload_size_bytes),
                    style.cyan(format!("{:<10}", "text")),
                    state.format_origin_writer(&message.origin_writer_id_b64, style),
                    format_kv(
                        "obj",
                        short_crockford_base32_lower_from_b64(&object_id_b64),
                        style
                    ),
                    format_text_summary(summary.as_ref(), style),
                );
            }
            SpySharedMessageKind::BinaryPut {
                object_id_b64,
                wall_time_ns,
                writer_id_b64,
            } => {
                println!(
                    "{}  {}  {}  {}  {}  {}  {}",
                    style.spy_position(message.sequence_number, message.timestamp_ns),
                    style.spy_bytes(message.payload_size_bytes),
                    style.magenta(format!("{:<10}", "binary")),
                    state.format_origin_writer(&message.origin_writer_id_b64, style),
                    format_kv(
                        "obj",
                        short_crockford_base32_lower_from_b64(&object_id_b64),
                        style
                    ),
                    format_kv(
                        "writer",
                        short_crockford_base32_lower_from_b64(&writer_id_b64),
                        style
                    ),
                    format_kv("wall", wall_time_ns, style),
                );
            }
        },
    }
}

fn format_namespace_summary(
    summary: Option<&opbox_core::spy::NamespaceUpdateSummary>,
    shared_object_id: Option<&str>,
    style: CliStyle,
) -> String {
    let Some(summary) = summary else {
        return String::new();
    };

    let mut out = String::new();
    for claim in &summary.added_claims {
        out.push_str(&format!("  {}{}", style.green("+"), style.green("claim")));
        if shared_object_id == Some(claim.object_id_b64.as_str()) {
            out.push('=');
            out.push_str(&style.green(format!("\"{}\" ({})", claim.path, claim.kind)));
        } else {
            out.push_str(&style.green(format!(
                "=(\"{}\", obj={}, {})",
                claim.path,
                short_crockford_base32_lower_from_b64(&claim.object_id_b64),
                claim.kind
            )));
        }
    }
    for claim in &summary.removed_claims {
        out.push_str(&format!("  {}{}", style.red("-"), style.red("claim")));
        if shared_object_id == Some(claim.object_id_b64.as_str()) {
            out.push('=');
            out.push_str(&style.red(format!("\"{}\" ({})", claim.path, claim.kind)));
        } else {
            out.push_str(&style.red(format!(
                "=(\"{}\", obj={}, {})",
                claim.path,
                short_crockford_base32_lower_from_b64(&claim.object_id_b64),
                claim.kind
            )));
        }
    }
    out
}

fn namespace_summary_single_object_id(
    summary: &opbox_core::spy::NamespaceUpdateSummary,
) -> Option<&str> {
    let mut object_ids = summary
        .added_claims
        .iter()
        .chain(summary.removed_claims.iter())
        .map(|claim| claim.object_id_b64.as_str());
    let first = object_ids.next()?;
    object_ids
        .all(|object_id| object_id == first)
        .then_some(first)
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

struct BootstrapProgress {
    bar: Option<ProgressBar>,
}

impl BootstrapProgress {
    fn new(mode: RunMode) -> eyre::Result<Self> {
        if !std::io::stderr().is_terminal() {
            return Ok(Self { bar: None });
        }

        let bar = ProgressBar::new_spinner();
        let template = if std::env::var_os("NO_COLOR").is_some() {
            "{spinner} {prefix} {msg}"
        } else {
            "{spinner:.green} {prefix:.cyan} {msg}"
        };
        bar.set_style(ProgressStyle::with_template(template)?);
        bar.set_prefix(match mode {
            RunMode::Init => "init",
            RunMode::Clone => "clone",
            RunMode::Sync => unreachable!("bootstrap never runs in sync mode"),
        });
        bar.enable_steady_tick(Duration::from_millis(120));
        Ok(Self { bar: Some(bar) })
    }

    fn set(&self, message: impl Into<String>) {
        if let Some(bar) = &self.bar {
            bar.set_message(message.into());
        }
    }

    fn suspend(&self, action: impl FnOnce()) {
        if let Some(bar) = &self.bar {
            bar.suspend(action);
        } else {
            action();
        }
    }

    fn finish(&self) {
        if let Some(bar) = &self.bar {
            bar.finish_and_clear();
        }
    }
}

impl Drop for BootstrapProgress {
    fn drop(&mut self) {
        self.finish();
    }
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

    fn spy_position(self, sequence_number: u64, timestamp_ns: i64) -> String {
        let timestamp_ms = timestamp_ns / 1_000_000;
        let position = format!("#{sequence_number}@{timestamp_ms}");
        self.dim(format!("{position:<24}"))
    }

    fn spy_bytes(self, bytes: usize) -> String {
        self.dim(format!("{bytes:>7}B"))
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
    print_status_row("basin", &status.basin, style);
    print_status_row("root", &status.root, style);
    print_status_row("pid", status.pid, style);
    print_status_row("engine phase", status.engine_phase, style);
    print_status_row("stable cursor", status.stable_cursor_end, style);
    print_status_row("connectivity", connectivity_status_text(status), style);
    let context = RetentionWarningContext {
        basin: &status.basin,
        workspace_id: &status.workspace_id,
    };
    for warning in &status.warnings {
        print_status_warning(warning, context, style);
    }
}

fn print_status_row(label: &str, value: impl std::fmt::Display, style: CliStyle) {
    println!("  {}  {}", style.dim(format!("{label:<13}")), value);
}

fn print_bootstrap_share_warning(error: &eyre::Report, style: CliStyle) {
    eprintln!(
        "{} could not create bootstrap share token: {error:#}",
        style.yellow("warning:")
    );
    eprintln!(
        "         share token creation requires S2 Cloud and an access token that can issue access tokens"
    );
}

fn print_bootstrap_share_next_step(style: CliStyle) {
    println!("{}", style.yellow("bootstrap share token was not created"));
    println!(
        "run {} after configuring a workspace owner/admin token",
        style.bold(format!(
            "{CLIENT_COMMAND} share new --id {BOOTSTRAP_SHARE_ID}"
        ))
    );
}

fn print_share_clone_command(
    workspace_id: &WorkspaceId,
    basin: &BasinName,
    access_token: &str,
    connection: &S2ConnectionConfig,
    style: CliStyle,
) {
    println!(
        "{}",
        style.bold("share this clone command (contains limited access token):")
    );
    println!();
    for line in share_clone_command_lines(workspace_id, basin, access_token, connection) {
        println!("  {line}");
    }
}

fn share_clone_command_lines(
    workspace_id: &WorkspaceId,
    basin: &BasinName,
    access_token: &str,
    connection: &S2ConnectionConfig,
) -> Vec<String> {
    let mut lines = vec![
        format!("{CLIENT_COMMAND} clone \\"),
        format!("  --workspace {} \\", shell_quote(&workspace_id.0)),
        format!("  --access-token {} \\", shell_quote(access_token)),
    ];
    let (account_endpoint, basin_endpoint) = connection.endpoint_pair_for_metadata();
    if let Some(account_endpoint) = account_endpoint {
        lines.push(format!(
            "  --account-endpoint {} \\",
            shell_quote(&account_endpoint)
        ));
    }
    if let Some(basin_endpoint) = basin_endpoint {
        lines.push(format!(
            "  --basin-endpoint {} \\",
            shell_quote(&basin_endpoint)
        ));
    }
    lines.push(format!("  --basin {}", shell_quote(basin.as_ref())));
    lines
}

fn shell_quote(value: &str) -> String {
    if value
        .bytes()
        .all(|byte| matches!(byte, b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_' | b'-' | b'.' | b'/' | b':' | b'=' | b'@'))
    {
        return value.to_string();
    }

    format!("'{}'", value.replace('\'', "'\\''"))
}

#[derive(Clone, Copy)]
struct RetentionWarningContext<'a> {
    basin: &'a str,
    workspace_id: &'a str,
}

fn print_status_warning(
    warning: &ipc::DaemonWarning,
    context: RetentionWarningContext<'_>,
    style: CliStyle,
) {
    let lines = daemon_warning_lines(warning, context);
    let Some((first, rest)) = lines.split_first() else {
        return;
    };
    print_status_row("warning", style.yellow(first), style);
    for line in rest {
        print_status_row("", style.yellow(line), style);
    }
}

fn print_warning(
    warning: &ipc::DaemonWarning,
    context: RetentionWarningContext<'_>,
    style: CliStyle,
) {
    let lines = daemon_warning_lines(warning, context);
    let Some((first, rest)) = lines.split_first() else {
        return;
    };
    eprintln!("{} {}", style.yellow("warning:"), first);
    for line in rest {
        eprintln!("         {line}");
    }
}

fn print_retention_check_warning(target: &str, error: &eyre::Report, style: CliStyle) {
    eprintln!(
        "{} could not verify {target} retention: {error:#}. \
         Opbox workspaces should use Infinite retention for the ops stream and basin default stream config.",
        style.yellow("warning:")
    );
}

fn daemon_warning_lines(
    warning: &ipc::DaemonWarning,
    context: RetentionWarningContext<'_>,
) -> Vec<String> {
    match warning {
        ipc::DaemonWarning::OpsStreamRetentionNotInfinite { retention } => vec![
            format!(
                "ops stream retention is {}; future clones may fail after records expire.",
                retention_summary_text(retention)
            ),
            "if this workspace is disposable, fix the basin default and recreate it.".to_string(),
            "to keep this workspace, reconfigure the ops stream:".to_string(),
            format!(
                "$ s2 reconfigure-stream s2://{}/{}/ops --retention-policy infinite",
                context.basin, context.workspace_id
            ),
            "this does not restore expired records; existing object streams may also need reconfiguration.".to_string(),
        ],
        ipc::DaemonWarning::BasinDefaultStreamRetentionNotInfinite { retention } => vec![
            format!(
                "basin default stream retention is {}; future multipart object streams may expire.",
                retention_summary_text(retention)
            ),
            "for new/prototype workspaces, fix the basin default and recreate the opbox workspace:"
                .to_string(),
            format!(
                "$ s2 reconfigure-basin {} --retention-policy infinite",
                context.basin
            ),
            "this does not change existing streams or restore records that may already have expired."
                .to_string(),
        ],
    }
}

fn retention_summary_text(retention: &ipc::StreamRetentionSummary) -> String {
    match retention {
        ipc::StreamRetentionSummary::Age { seconds } => duration_text(*seconds),
        ipc::StreamRetentionSummary::Unspecified => "not confirmed as Infinite".to_string(),
    }
}

fn duration_text(seconds: u64) -> String {
    const MINUTE: u64 = 60;
    const HOUR: u64 = 60 * MINUTE;
    const DAY: u64 = 24 * HOUR;

    if seconds != 0 && seconds % DAY == 0 {
        let days = seconds / DAY;
        format!("{days} day{}", plural(days))
    } else if seconds != 0 && seconds % HOUR == 0 {
        let hours = seconds / HOUR;
        format!("{hours} hour{}", plural(hours))
    } else if seconds != 0 && seconds % MINUTE == 0 {
        let minutes = seconds / MINUTE;
        format!("{minutes} minute{}", plural(minutes))
    } else {
        format!("{seconds} second{}", plural(seconds))
    }
}

fn plural(value: u64) -> &'static str {
    if value == 1 { "" } else { "s" }
}

fn connectivity_status_text(status: &ipc::DaemonStatus) -> String {
    let now = time::OffsetDateTime::now_utc();
    match status.connectivity.overall {
        ConnectivityOverallState::Online => "online".to_string(),
        ConnectivityOverallState::Reconnecting => "reconnecting to S2".to_string(),
        ConnectivityOverallState::Offline => {
            let retry = retry_after_text(&status.connectivity.reader, now)
                .or_else(|| retry_after_text(&status.connectivity.writer, now));
            match retry {
                Some(retry) => format!("offline, retrying in {retry}"),
                None => "offline".to_string(),
            }
        }
        ConnectivityOverallState::Degraded => {
            let mut parts = Vec::new();
            if status.connectivity.reader.state != LinkState::Online {
                parts.push(format!(
                    "reader {}",
                    link_detail_text(&status.connectivity.reader, now)
                ));
            }
            if status.connectivity.writer.state != LinkState::Online {
                parts.push(format!(
                    "writer {}",
                    link_detail_text(&status.connectivity.writer, now)
                ));
            }
            format!("degraded, {}", parts.join(", "))
        }
    }
}

fn link_detail_text(status: &LinkStatus, now: time::OffsetDateTime) -> String {
    match status.state {
        LinkState::Online => "online".to_string(),
        LinkState::Reconnecting => "reconnecting".to_string(),
        LinkState::Offline => retry_after_text(status, now)
            .map(|retry| format!("offline, retrying in {retry}"))
            .unwrap_or_else(|| "offline".to_string()),
    }
}

fn retry_after_text(status: &LinkStatus, now: time::OffsetDateTime) -> Option<String> {
    let retry_after = status.retry_after(now)?;
    let secs = retry_after.as_secs();
    if secs == 0 {
        Some("now".to_string())
    } else {
        Some(format!("{secs}s"))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConfigScope {
    Global,
    Workspace,
}

impl ConfigScope {
    fn from_workspace_flag(workspace: bool) -> Self {
        if workspace {
            Self::Workspace
        } else {
            Self::Global
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Global => "user config",
            Self::Workspace => "workspace config",
        }
    }

    fn keys(self) -> &'static [ConfigKey] {
        match self {
            Self::Global => &[
                ConfigKey::DefaultBasin,
                ConfigKey::AccessToken,
                ConfigKey::AccountEndpoint,
                ConfigKey::BasinEndpoint,
                ConfigKey::DaemonLogLevel,
                ConfigKey::ClientLogLevel,
                ConfigKey::SkipRetentionChecks,
            ],
            Self::Workspace => &[
                ConfigKey::Basin,
                ConfigKey::AccessToken,
                ConfigKey::AccountEndpoint,
                ConfigKey::BasinEndpoint,
                ConfigKey::DaemonLogLevel,
                ConfigKey::SkipRetentionChecks,
            ],
        }
    }

    fn contains(self, key: ConfigKey) -> bool {
        self.keys().contains(&key)
    }
}

fn config_scope_path(scope: ConfigScope) -> eyre::Result<PathBuf> {
    match scope {
        ConfigScope::Global => user_config_path(),
        ConfigScope::Workspace => {
            let root = find_workspace_root(&current_dir()?)?;
            Ok(workspace_config_path(&root))
        }
    }
}

fn load_scoped_config(scope: ConfigScope) -> eyre::Result<(UserConfig, PathBuf)> {
    let path = config_scope_path(scope)?;
    Ok((load_user_config_from_path(&path)?, path))
}

fn save_scoped_config(scope: ConfigScope, config: &UserConfig) -> eyre::Result<PathBuf> {
    let path = config_scope_path(scope)?;
    match scope {
        ConfigScope::Global => save_user_config(config).map(|_| path),
        ConfigScope::Workspace => {
            save_user_config_to_path(config, &path)?;
            Ok(path)
        }
    }
}

fn ensure_config_key_allowed(scope: ConfigScope, key: ConfigKey) -> eyre::Result<()> {
    if scope.contains(key) {
        return Ok(());
    }

    match scope {
        ConfigScope::Global if key == ConfigKey::Basin => {
            eyre::bail!("basin is workspace-local; use `ob config --workspace set basin <basin>`")
        }
        ConfigScope::Workspace if key == ConfigKey::DefaultBasin => {
            eyre::bail!("default-basin is user-wide; use `ob config set default-basin <basin>`")
        }
        ConfigScope::Workspace if key == ConfigKey::ClientLogLevel => {
            eyre::bail!(
                "client-log-level is user-wide; use `ob config set client-log-level <filter>`"
            )
        }
        _ => eyre::bail!(
            "{} is not supported in {}",
            key.user_config_key().as_str(),
            scope.label()
        ),
    }
}

fn validate_config_value(key: ConfigKey, value: &str) -> eyre::Result<()> {
    if value.is_empty() {
        eyre::bail!("{} cannot be empty", key.user_config_key().as_str());
    }
    if value.contains('\n') {
        eyre::bail!("{} cannot contain newlines", key.user_config_key().as_str());
    }

    match key {
        ConfigKey::Basin | ConfigKey::DefaultBasin => {
            parse_basin_config_value(key.user_config_key().as_str(), value)?;
        }
        ConfigKey::AccessToken => {}
        ConfigKey::AccountEndpoint => {
            AccountEndpoint::new(value)
                .map_err(|err| eyre::eyre!("invalid account-endpoint {value:?}: {err}"))?;
        }
        ConfigKey::BasinEndpoint => {
            BasinEndpoint::new(value)
                .map_err(|err| eyre::eyre!("invalid basin-endpoint {value:?}: {err}"))?;
        }
        ConfigKey::DaemonLogLevel | ConfigKey::ClientLogLevel => {
            tracing_subscriber::EnvFilter::try_new(value).map_err(|err| {
                eyre::eyre!(
                    "invalid {} {value:?}: {err}",
                    key.user_config_key().as_str()
                )
            })?;
        }
        ConfigKey::SkipRetentionChecks => {
            parse_bool_config_value(key.user_config_key().as_str(), value)?;
        }
    }

    Ok(())
}

fn config_display_value(key: UserConfigKey, value: &str, reveal_secret: bool) -> String {
    if key == UserConfigKey::AccessToken && !reveal_secret {
        "<set>".to_string()
    } else {
        value.to_string()
    }
}

fn run_config(workspace: bool, command: ConfigCommand) -> eyre::Result<()> {
    let scope = ConfigScope::from_workspace_flag(workspace);
    match command {
        ConfigCommand::Path => {
            println!("{}", config_scope_path(scope)?.display());
        }
        ConfigCommand::List => {
            let (config, _) = load_scoped_config(scope)?;
            for key in scope.keys().iter().copied().map(ConfigKey::user_config_key) {
                if let Some(value) = config.get(key) {
                    println!(
                        "{} = {}",
                        key.as_str(),
                        config_display_value(key, value, false)
                    );
                }
            }
        }
        ConfigCommand::Keys => {
            for key in scope.keys().iter().copied().map(ConfigKey::user_config_key) {
                println!("{}", key.as_str());
            }
        }
        ConfigCommand::Get { key } => {
            ensure_config_key_allowed(scope, key)?;
            let (config, _) = load_scoped_config(scope)?;
            let key = key.user_config_key();
            if let Some(value) = config.get(key) {
                println!("{}", config_display_value(key, value, true));
            }
        }
        ConfigCommand::Set { key, value } => {
            ensure_config_key_allowed(scope, key)?;
            validate_config_value(key, &value)?;
            let (mut config, _) = load_scoped_config(scope)?;
            let user_key = key.user_config_key();
            config.set(user_key, value);
            let path = save_scoped_config(scope, &config)?;
            println!("set {} in {}", user_key.as_str(), path.display());
        }
        ConfigCommand::Unset { key } => {
            ensure_config_key_allowed(scope, key)?;
            let (mut config, _) = load_scoped_config(scope)?;
            let user_key = key.user_config_key();
            config.unset(user_key);
            let path = save_scoped_config(scope, &config)?;
            println!("unset {} in {}", user_key.as_str(), path.display());
        }
    }

    Ok(())
}

async fn run_start(sync_root: Option<PathBuf>) -> eyre::Result<()> {
    let root = find_workspace_root(&root_or_current(sync_root)?)?;
    match request_valid_status(&root).await {
        Ok(status) => {
            print_daemon_status(
                "opbox daemon already running",
                &status,
                CliStyle::for_stdout(),
            );
            return Ok(());
        }
        Err(error) => {
            if let Some(mismatch) = error.downcast_ref::<ipc::DaemonBuildMismatch>() {
                eyre::bail!("{mismatch}");
            }
        }
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
        match request_valid_status(&root).await {
            Ok(status) if status.workspace_id == daemon_row.workspace_id.0 => {
                print_daemon_status("opbox daemon started", &status, CliStyle::for_stdout());
                println!(
                    "  {}  {}",
                    CliStyle::for_stdout().dim(format!("{:<13}", "log")),
                    log_path.display()
                );
                return Ok(());
            }
            Err(error) => {
                if let Some(mismatch) = error.downcast_ref::<ipc::DaemonBuildMismatch>() {
                    let _ = child.kill().await;
                    eyre::bail!("{mismatch}");
                }
            }
            _ => {}
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
    let response = match request_valid_status(&root).await {
        Ok(_) => request_stop_with_mismatch_fallback(&root).await?,
        Err(error) => {
            if error.downcast_ref::<ipc::DaemonBuildMismatch>().is_some() {
                request_stop_with_mismatch_fallback(&root).await?
            } else {
                exit_daemon_not_running_or_report(&root, error);
            }
        }
    };

    wait_for_daemon_process_exit(&root, &response).await
}

async fn request_stop_with_mismatch_fallback(root: &Path) -> eyre::Result<ipc::StopResponse> {
    match ipc::request_stop(root).await {
        Ok(response) => Ok(response),
        Err(error) => {
            if error.downcast_ref::<ipc::DaemonBuildMismatch>().is_some() {
                stop_mismatched_daemon_by_signal(root, &error).await
            } else {
                exit_daemon_not_running_or_report(root, error);
            }
        }
    }
}

async fn stop_mismatched_daemon_by_signal(
    root: &Path,
    error: &eyre::Report,
) -> eyre::Result<ipc::StopResponse> {
    let style = CliStyle::for_stderr();
    eprintln!("{}", style.yellow(error));
    let pid = read_daemon_pid(root)?;
    let workspace_id = load_configured_daemon_state(root)
        .await
        .map(|(_, row)| row.workspace_id.0)
        .unwrap_or_else(|_| "unknown".to_string());
    terminate_daemon_process(pid)?;
    eprintln!(
        "{}",
        style.yellow(format!(
            "sent termination signal to mismatched opbox daemon pid {pid}"
        ))
    );
    Ok(ipc::StopResponse { workspace_id, pid })
}

async fn wait_for_daemon_process_exit(
    root: &Path,
    response: &ipc::StopResponse,
) -> eyre::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if !daemon_process_is_alive(response.pid) {
            let _ = remove_stale_socket_files(root);
            remove_pid(root);
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

fn read_daemon_pid(root: &Path) -> eyre::Result<u32> {
    let path = pid_path(root);
    let contents = std::fs::read_to_string(&path).map_err(|error| {
        eyre::eyre!("failed to read daemon pid from {}: {error}", path.display())
    })?;
    contents
        .trim()
        .parse()
        .map_err(|error| eyre::eyre!("invalid daemon pid in {}: {error}", path.display()))
}

#[cfg(unix)]
fn terminate_daemon_process(pid: u32) -> eyre::Result<()> {
    let status = std::process::Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status()
        .map_err(|error| eyre::eyre!("failed to signal daemon pid {pid}: {error}"))?;
    if !status.success() {
        eyre::bail!("failed to signal daemon pid {pid}: kill exited with {status}");
    }
    Ok(())
}

#[cfg(windows)]
fn terminate_daemon_process(pid: u32) -> eyre::Result<()> {
    let status = std::process::Command::new("taskkill")
        .args(["/PID", &pid.to_string(), "/F"])
        .status()
        .map_err(|error| eyre::eyre!("failed to terminate daemon pid {pid}: {error}"))?;
    if !status.success() {
        eyre::bail!("failed to terminate daemon pid {pid}: taskkill exited with {status}");
    }
    Ok(())
}

#[cfg(unix)]
fn daemon_process_is_alive(pid: u32) -> bool {
    std::process::Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

#[cfg(windows)]
fn daemon_process_is_alive(pid: u32) -> bool {
    std::process::Command::new("tasklist")
        .args(["/FI", &format!("PID eq {pid}"), "/NH"])
        .output()
        .map(|output| {
            let stdout = String::from_utf8_lossy(&output.stdout);
            stdout.contains(&pid.to_string())
        })
        .unwrap_or(false)
}

#[cfg(unix)]
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

#[cfg(windows)]
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

    let status = std::process::Command::new("powershell")
        .args([
            "-NoProfile",
            "-Command",
            &format!("Get-Content -Path '{}' -Wait -Tail 50", log_path.display()),
        ])
        .status()
        .map_err(|error| eyre::eyre!("failed to tail log: {error}"))?;

    if !status.success() {
        eyre::bail!("log follower exited with {status}");
    }

    Ok(())
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

fn exit_daemon_not_running_or_report(root: &Path, error: eyre::Report) -> ! {
    if !socket_link_path(root).exists() {
        exit_daemon_not_running(root);
    }

    let style = CliStyle::for_stderr();
    eprintln!(
        "{}",
        style.red(format!(
            "failed to contact opbox daemon for {}",
            root.display()
        ))
    );
    eprintln!("{error:#}");
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
    let args = Args::parse();
    let tracing_config = load_user_config().ok();
    let client_log_level = match &args.command {
        Command::Config { .. } => None,
        _ => tracing_config
            .as_ref()
            .and_then(|config| config.client_log_level.as_deref()),
    };
    if let Err(error) = init_tracing(client_log_level) {
        render_error(&error);
        std::process::exit(1);
    }

    let result = match args.command {
        Command::Config { workspace, command } => run_config(workspace, command),
        Command::Init { config, sync_root } => with_failure_banner(
            "init",
            async {
                let progress = BootstrapProgress::new(RunMode::Init)?;
                let user_config = load_user_config()?;
                let bootstrap = bootstrap_init(sync_root, &user_config, &config, &progress).await?;
                run_bootstrap(bootstrap, &progress).await
            }
            .await,
        ),
        Command::Clone {
            workspace,
            as_of,
            config,
            sync_root,
        } => with_failure_banner(
            "clone",
            async {
                let progress = BootstrapProgress::new(RunMode::Clone)?;
                let user_config = load_user_config()?;
                let bootstrap = bootstrap_clone(
                    workspace,
                    sync_root,
                    as_of.map(CloneAsOf::log_read_stop),
                    &user_config,
                    &config,
                    &progress,
                )
                .await?;
                run_bootstrap(bootstrap, &progress).await
            }
            .await,
        ),
        Command::Start { sync_root } => run_start(sync_root).await,
        Command::Stop { sync_root } => run_stop(sync_root).await,
        Command::Status { sync_root } => run_status(sync_root).await,
        Command::Spy { sync_root } => run_spy(sync_root).await,
        Command::Share { command } => with_failure_banner("share", run_share(command).await),
        Command::Logs { follow, sync_root } => run_daemon_logs(follow, sync_root),
    };

    if let Err(error) = result {
        render_error(&error);
        std::process::exit(1);
    }
}

/// Render CLI failures for humans: known situations get guidance, everything
/// else gets a single `error:` line with the cause chain (no report/Location
/// noise; set client-log-level or daemon-log-level for diagnostics).
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

    #[test]
    fn fresh_daemon_state_pins_complete_endpoint_pair() -> eyre::Result<()> {
        let workspace_id = WorkspaceId("0123456789abcdefghijklmnopqrstuv".to_string());
        let basin: BasinName = "test-basin".parse()?;
        let connection = S2ConnectionConfig {
            access_token: "tok-123".to_string(),
            account_endpoint: Some("account.s2.test".to_string()),
            basin_endpoint: Some("{basin}.s2.test".to_string()),
        };

        let row = fresh_daemon_state_row(workspace_id, basin, &connection);
        assert_eq!(row.s2_account_endpoint.as_deref(), Some("account.s2.test"));
        assert_eq!(row.s2_basin_endpoint.as_deref(), Some("{basin}.s2.test"));

        Ok(())
    }

    #[test]
    fn share_clone_command_includes_effective_connection_config() -> eyre::Result<()> {
        let workspace_id = WorkspaceId("dwwbav5ypjgxra25s7hjzt81gvdbsbmm".to_string());
        let basin: BasinName = "opbox-dev-2".parse()?;
        let connection = S2ConnectionConfig {
            access_token: "R3QAAAAAAABq+secret/token".to_string(),
            account_endpoint: Some("account.example.test".to_string()),
            basin_endpoint: Some("{basin}.example.test".to_string()),
        };

        assert_eq!(
            share_clone_command_lines(&workspace_id, &basin, "limited+share/token", &connection),
            vec![
                "ob clone \\".to_string(),
                "  --workspace dwwbav5ypjgxra25s7hjzt81gvdbsbmm \\".to_string(),
                "  --access-token 'limited+share/token' \\".to_string(),
                "  --account-endpoint account.example.test \\".to_string(),
                "  --basin-endpoint '{basin}.example.test' \\".to_string(),
                "  --basin opbox-dev-2".to_string(),
            ]
        );

        Ok(())
    }

    #[test]
    fn share_clone_command_omits_partial_endpoint_config() -> eyre::Result<()> {
        let workspace_id = WorkspaceId("dwwbav5ypjgxra25s7hjzt81gvdbsbmm".to_string());
        let basin: BasinName = "opbox-dev-2".parse()?;
        let connection = S2ConnectionConfig {
            access_token: "tok-123".to_string(),
            account_endpoint: Some("account.example.test".to_string()),
            basin_endpoint: None,
        };

        let command =
            share_clone_command_lines(&workspace_id, &basin, "limited-token", &connection)
                .join("\n");
        assert!(!command.contains("--account-endpoint"));
        assert!(!command.contains("--basin-endpoint"));

        Ok(())
    }

    #[test]
    fn share_token_id_is_scoped_to_workspace() -> eyre::Result<()> {
        let workspace_id = WorkspaceId("dwwbav5ypjgxra25s7hjzt81gvdbsbmm".to_string());

        assert_eq!(
            share_token_id(&workspace_id, "bootstrap")?.to_string(),
            "opbox-dwwbav5ypjgxra25s7hjzt81gvdbsbmm-bootstrap"
        );
        assert_eq!(
            share_id_from_token_id(
                &workspace_id,
                "opbox-dwwbav5ypjgxra25s7hjzt81gvdbsbmm-alice"
            ),
            "alice"
        );
        assert!(share_token_id(&workspace_id, "").is_err());

        Ok(())
    }

    #[test]
    fn shell_quote_escapes_single_quotes() {
        assert_eq!(shell_quote("simple-token_123"), "simple-token_123");
        assert_eq!(shell_quote("needs space"), "'needs space'");
        assert_eq!(shell_quote("a'b"), "'a'\\''b'");
    }

    #[test]
    fn clone_as_of_converts_to_exclusive_s2_millisecond() {
        let as_of: CloneAsOf = "1970-01-01T00:00:00Z".parse().unwrap();
        assert_eq!(as_of.log_read_stop(), LogReadStop::UntilTimestampMs(1));

        let as_of: CloneAsOf = "1970-01-01T00:00:00.999999999Z".parse().unwrap();
        assert_eq!(as_of.log_read_stop(), LogReadStop::UntilTimestampMs(1000));

        assert!("1969-12-31T23:59:59Z".parse::<CloneAsOf>().is_err());
        assert!("2026-06-18:01:30:00Z".parse::<CloneAsOf>().is_err());
    }

    #[test]
    fn init_opboxignore_imports_gitignore_contents() -> eyre::Result<()> {
        let root =
            std::env::temp_dir().join(format!("opbox-ignore-test-{}", rand::random::<u64>()));
        std::fs::create_dir(&root)?;
        std::fs::write(
            root.join(GITIGNORE_FILE_NAME),
            "\
# already in default
target

.next
dist/
!dist/keep.txt
**/*.dst.out
foo/**/bar
.opbox
",
        )?;

        create_default_ignore_file(&root)?;
        let contents = std::fs::read_to_string(root.join(IGNORE_FILE_NAME))?;
        assert!(contents.contains("target\n"));
        assert!(contents.contains("# Imported from .gitignore by ob init.\n"));
        assert!(contents.contains(".next\n"));
        assert!(contents.contains("dist/\n"));
        assert!(contents.contains("!dist/keep.txt\n"));
        assert!(contents.contains("**/*.dst.out\n"));
        assert!(contents.contains("foo/**/bar\n"));
        assert!(contents.contains("\n.opbox\n"));

        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[test]
    fn gitignore_metadata_detection_handles_anchored_forms() {
        assert!(gitignore_contains_metadata_dir(".opbox\n"));
        assert!(gitignore_contains_metadata_dir(".opbox/\n"));
        assert!(gitignore_contains_metadata_dir("/.opbox\n"));
        assert!(gitignore_contains_metadata_dir("/.opbox/\n"));
        assert!(!gitignore_contains_metadata_dir("!.opbox\n"));
        assert!(!gitignore_contains_metadata_dir(".opbox-other\n"));
    }

    #[test]
    fn init_gitignore_update_appends_metadata_dir_once() -> eyre::Result<()> {
        let root =
            std::env::temp_dir().join(format!("opbox-gitignore-test-{}", rand::random::<u64>()));
        std::fs::create_dir(&root)?;
        let gitignore = root.join(".gitignore");
        std::fs::write(&gitignore, "target")?;

        add_metadata_dir_to_gitignore_if_present(&root)?;
        assert_eq!(std::fs::read_to_string(&gitignore)?, "target\n.opbox\n");

        add_metadata_dir_to_gitignore_if_present(&root)?;
        assert_eq!(std::fs::read_to_string(&gitignore)?, "target\n.opbox\n");

        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[test]
    fn init_gitignore_update_leaves_missing_or_existing_ignore_alone() -> eyre::Result<()> {
        let root =
            std::env::temp_dir().join(format!("opbox-gitignore-test-{}", rand::random::<u64>()));
        std::fs::create_dir(&root)?;

        add_metadata_dir_to_gitignore_if_present(&root)?;
        assert!(!root.join(".gitignore").exists());

        let gitignore = root.join(".gitignore");
        std::fs::write(&gitignore, "  .opbox/  \n")?;
        add_metadata_dir_to_gitignore_if_present(&root)?;
        assert_eq!(std::fs::read_to_string(&gitignore)?, "  .opbox/  \n");

        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }
}
