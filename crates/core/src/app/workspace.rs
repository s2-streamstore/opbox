use crate::app::db::load_daemon_state;
use crate::fs::ignore::METADATA_DIR_NAME;
use crate::semantic::table::daemon_state;
use crate::types::{DaemonWriterId, WorkspaceId};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

pub const STORAGE_DB_FILE_NAME: &str = "storage.db";
pub const SOCKET_LINK_FILE_NAME: &str = "socket";
pub const PID_FILE_NAME: &str = "daemon.pid";
pub const LOCK_FILE_NAME: &str = "daemon.lock";
pub const DAEMON_LOG_FILE_NAME: &str = "daemon.log";
pub const WORKSPACE_ENV_FILE_NAME: &str = "env";

pub fn metadata_dir(sync_root: &Path) -> PathBuf {
    sync_root.join(METADATA_DIR_NAME)
}

pub fn storage_db_path(sync_root: &Path) -> PathBuf {
    metadata_dir(sync_root).join(STORAGE_DB_FILE_NAME)
}

pub fn socket_link_path(sync_root: &Path) -> PathBuf {
    metadata_dir(sync_root).join(SOCKET_LINK_FILE_NAME)
}

pub fn pid_path(sync_root: &Path) -> PathBuf {
    metadata_dir(sync_root).join(PID_FILE_NAME)
}

pub fn daemon_lock_path(sync_root: &Path) -> PathBuf {
    metadata_dir(sync_root).join(LOCK_FILE_NAME)
}

pub fn daemon_log_path(sync_root: &Path) -> PathBuf {
    metadata_dir(sync_root).join(DAEMON_LOG_FILE_NAME)
}

pub fn workspace_env_path(sync_root: &Path) -> PathBuf {
    metadata_dir(sync_root).join(WORKSPACE_ENV_FILE_NAME)
}

pub fn real_socket_path(workspace_id: &WorkspaceId, daemon_writer_id: &DaemonWriterId) -> PathBuf {
    PathBuf::from("/tmp").join(format!(
        "opbox-{}-{}.sock",
        workspace_id.0,
        hex_component(daemon_writer_id.0.as_ref())
    ))
}

fn hex_component(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

pub fn remove_socket_pointer(sync_root: &Path) -> eyre::Result<()> {
    match std::fs::remove_file(socket_link_path(sync_root)) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }

    Ok(())
}

pub fn remove_stale_socket_files(sync_root: &Path) -> eyre::Result<()> {
    let link_path = socket_link_path(sync_root);
    let target = std::fs::read_link(&link_path).ok();

    remove_socket_pointer(sync_root)?;

    if let Some(target) = target {
        match std::fs::remove_file(target) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }

    Ok(())
}

pub fn current_dir() -> eyre::Result<PathBuf> {
    Ok(std::env::current_dir()?)
}

pub fn canonicalize_existing_dir(path: &Path) -> eyre::Result<PathBuf> {
    let root = path.canonicalize()?;
    ensure_sync_root_exists(&root)?;
    Ok(root)
}

/// No `.opbox` directory exists at or above the starting path. Typed so CLI
/// frontends can render it as guidance rather than an error report.
#[derive(Debug)]
pub struct NotInWorkspace {
    pub start: PathBuf,
}

impl std::fmt::Display for NotInWorkspace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "not in an opbox workspace (searched upward from {})",
            self.start.display()
        )
    }
}

impl std::error::Error for NotInWorkspace {}

pub fn find_workspace_root(start: &Path) -> eyre::Result<PathBuf> {
    let start = start.canonicalize()?;
    let mut current = if start.is_dir() {
        start.clone()
    } else {
        start
            .parent()
            .ok_or_else(|| eyre::eyre!("path has no parent: {}", start.display()))?
            .to_path_buf()
    };

    loop {
        if metadata_dir(&current).is_dir() {
            return Ok(current);
        }

        if !current.pop() {
            return Err(NotInWorkspace { start }.into());
        }
    }
}

pub fn ensure_sync_root_exists(sync_root: &Path) -> eyre::Result<()> {
    let metadata = std::fs::metadata(sync_root)?;
    if !metadata.is_dir() {
        eyre::bail!("sync root is not a directory: {}", sync_root.display());
    }
    Ok(())
}

pub async fn configured_workspace_id(sync_root: &Path) -> eyre::Result<Option<WorkspaceId>> {
    let db_path = storage_db_path(sync_root);
    if !db_path.try_exists()? {
        return Ok(None);
    }

    Ok(Some(load_daemon_state(&db_path).await?.workspace_id))
}

pub async fn ensure_sync_root_unconfigured(sync_root: &Path) -> eyre::Result<()> {
    let metadata_dir = metadata_dir(sync_root);
    if !metadata_dir.try_exists()? {
        return Ok(());
    }
    if !metadata_dir.is_dir() {
        eyre::bail!(
            "sync root contains reserved path {}, but it is not a directory; remove it before initializing opbox",
            metadata_dir.display()
        );
    }

    match configured_workspace_id(sync_root).await {
        Ok(Some(workspace_id)) => {
            let partial_init = crate::app::db::init_appears_incomplete(&storage_db_path(sync_root))
                .await
                .unwrap_or(false);
            if partial_init {
                eyre::bail!(
                    "a previous init of workspace {} appears to have failed before completing; \
                     your files are untouched — delete the {} directory and run init again \
                     (if this workspace previously synced successfully, run start instead)",
                    workspace_id.0,
                    metadata_dir.display()
                );
            }
            eyre::bail!(
                "sync root is already configured to sync workspace {}; if you mean to reinitialize with a new workspace, delete the {} directory",
                workspace_id.0,
                metadata_dir.display()
            );
        }
        Ok(None) => {}
        Err(error) => {
            eyre::bail!(
                "sync root already contains {}, but opbox could not read its workspace metadata: {error}; if you mean to reinitialize with a new workspace, delete the {} directory",
                metadata_dir.display(),
                metadata_dir.display()
            );
        }
    }

    eyre::bail!(
        "sync root already contains {}; if you mean to initialize this directory, delete it first",
        metadata_dir.display()
    );
}

pub fn create_metadata_dir(sync_root: &Path) -> eyre::Result<()> {
    std::fs::create_dir(metadata_dir(sync_root))?;
    Ok(())
}

pub fn ensure_clean_clone_root(sync_root: &Path) -> eyre::Result<PathBuf> {
    if sync_root.try_exists()? {
        ensure_sync_root_exists(sync_root)?;
        if std::fs::read_dir(sync_root)?.next().transpose()?.is_some() {
            eyre::bail!("clone sync root is not empty: {}", sync_root.display());
        }
    } else {
        std::fs::create_dir_all(sync_root)?;
    }

    canonicalize_existing_dir(sync_root)
}

pub async fn load_configured_daemon_state(
    sync_root: &Path,
) -> eyre::Result<(PathBuf, daemon_state::Row)> {
    ensure_sync_root_exists(sync_root)?;
    let db_path = storage_db_path(sync_root);
    if !db_path.try_exists()? {
        eyre::bail!(
            "sync root is not configured for opbox: {} is missing; run `ob init` or `ob clone --workspace WORKSPACE_ID` first",
            db_path.display()
        );
    }
    let daemon_row = load_daemon_state(&db_path).await?;
    Ok((db_path, daemon_row))
}

#[derive(Clone, Default, Eq, PartialEq)]
pub struct WorkspaceEnv {
    entries: BTreeMap<String, String>,
}

impl WorkspaceEnv {
    pub fn get(&self, key: &str) -> Option<&str> {
        self.entries.get(key).map(String::as_str)
    }
}

pub fn load_workspace_env(sync_root: &Path) -> eyre::Result<WorkspaceEnv> {
    let path = workspace_env_path(sync_root);
    let contents = match std::fs::read_to_string(&path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(WorkspaceEnv::default());
        }
        Err(error) => return Err(error.into()),
    };

    parse_workspace_env(&path, &contents)
}

fn parse_workspace_env(path: &Path, contents: &str) -> eyre::Result<WorkspaceEnv> {
    let mut entries = BTreeMap::new();
    for (line_index, raw_line) in contents.lines().enumerate() {
        let line_number = line_index + 1;
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let assignment = line.strip_prefix("export ").unwrap_or(line);
        let (key, value) = assignment
            .split_once('=')
            .ok_or_else(|| eyre::eyre!("{}:{line_number}: expected KEY=VALUE", path.display()))?;
        let key = key.trim();
        if !workspace_env_key_is_valid(key) {
            eyre::bail!(
                "{}:{line_number}: invalid environment key {key:?}",
                path.display()
            );
        }

        let value = parse_workspace_env_value(path, line_number, value.trim())?;
        entries.insert(key.to_string(), value);
    }

    Ok(WorkspaceEnv { entries })
}

fn workspace_env_key_is_valid(key: &str) -> bool {
    let mut chars = key.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn parse_workspace_env_value(path: &Path, line_number: usize, value: &str) -> eyre::Result<String> {
    let Some(quote) = value.as_bytes().first().copied() else {
        return Ok(String::new());
    };
    if quote != b'\'' && quote != b'"' {
        return Ok(value.to_string());
    }

    if !value.as_bytes().ends_with(&[quote]) || value.len() == 1 {
        eyre::bail!(
            "{}:{line_number}: unterminated quoted environment value",
            path.display()
        );
    }

    let inner = &value[1..value.len() - 1];
    if quote == b'\'' {
        Ok(inner.to_string())
    } else {
        parse_double_quoted_workspace_env_value(path, line_number, inner)
    }
}

fn parse_double_quoted_workspace_env_value(
    path: &Path,
    line_number: usize,
    value: &str,
) -> eyre::Result<String> {
    let mut parsed = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            parsed.push(ch);
            continue;
        }

        let escaped = chars.next().ok_or_else(|| {
            eyre::eyre!(
                "{}:{line_number}: trailing escape in quoted environment value",
                path.display()
            )
        })?;
        parsed.push(match escaped {
            'n' => '\n',
            'r' => '\r',
            't' => '\t',
            '\\' | '"' | '$' => escaped,
            _ => escaped,
        });
    }

    Ok(parsed)
}

pub struct DaemonLock {
    path: PathBuf,
    pid: u32,
}

impl DaemonLock {
    pub fn acquire(sync_root: &Path) -> eyre::Result<Self> {
        let path = daemon_lock_path(sync_root);
        let pid = std::process::id();

        loop {
            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(mut file) => {
                    writeln!(file, "{pid}")?;
                    return Ok(Self { path, pid });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    if let Some(existing_pid) = read_pid_file(&path)?
                        && process_is_alive(existing_pid)
                    {
                        eyre::bail!(
                            "opbox daemon already appears to be running for this workspace (pid {existing_pid})"
                        );
                    }
                    std::fs::remove_file(&path)?;
                }
                Err(error) => return Err(error.into()),
            }
        }
    }
}

impl Drop for DaemonLock {
    fn drop(&mut self) {
        if read_pid_file(&self.path).ok().flatten() == Some(self.pid) {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

pub fn write_pid(sync_root: &Path) -> eyre::Result<()> {
    let mut file = File::create(pid_path(sync_root))?;
    writeln!(file, "{}", std::process::id())?;
    Ok(())
}

pub fn remove_pid(sync_root: &Path) {
    let _ = std::fs::remove_file(pid_path(sync_root));
}

fn read_pid_file(path: &Path) -> eyre::Result<Option<u32>> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    Ok(contents.trim().parse().ok())
}

fn process_is_alive(pid: u32) -> bool {
    std::process::Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .status()
        .is_ok_and(|status| status.success())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn workspace_env_parses_supported_assignment_forms() -> eyre::Result<()> {
        let env = parse_workspace_env(
            Path::new(".opbox/env"),
            r#"
# daemon-local overrides
RUST_LOG=opbox_core=debug,opbox_daemon=trace
export S2_ACCOUNT_ENDPOINT = "http://127.0.0.1:8080"
S2_BASIN_ENDPOINT='http://127.0.0.1:8081'
EMPTY=
"#,
        )?;

        assert_eq!(
            env.get("RUST_LOG"),
            Some("opbox_core=debug,opbox_daemon=trace")
        );
        assert_eq!(
            env.get("S2_ACCOUNT_ENDPOINT"),
            Some("http://127.0.0.1:8080")
        );
        assert_eq!(env.get("S2_BASIN_ENDPOINT"), Some("http://127.0.0.1:8081"));
        assert_eq!(env.get("EMPTY"), Some(""));

        Ok(())
    }

    #[test]
    fn workspace_env_rejects_non_assignments() {
        let error = match parse_workspace_env(Path::new(".opbox/env"), "RUST_LOG\n") {
            Ok(_) => panic!("missing assignment should be rejected"),
            Err(error) => error,
        };

        assert!(
            error.to_string().contains("expected KEY=VALUE"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn partial_init_is_detected_and_reported() -> eyre::Result<()> {
        use crate::app::db::{
            configure_connection, create_initialized_database, init_appears_incomplete,
            open_database,
        };
        use crate::types::OutboxId;

        let sync_root =
            std::env::temp_dir().join(format!("opbox-partial-init-test-{}", rand::random::<u64>()));
        std::fs::create_dir(&sync_root)?;
        std::fs::create_dir(metadata_dir(&sync_root))?;

        let daemon_row = crate::semantic::table::daemon_state::Row {
            workspace_id: WorkspaceId("0123456789abcdefghijklmnopqrstuv".to_string()),
            s2_basin: "test-basin".parse()?,
            s2_account_endpoint: None,
            s2_basin_endpoint: None,
            daemon_writer_id: DaemonWriterId(Bytes::from_static(b"0123456789abcdef")),
            stable_cursor: ..0,
            next_outbox_id: OutboxId::new(0),
        };
        let db_path = storage_db_path(&sync_root);
        create_initialized_database(&db_path, &daemon_row).await?;

        // Fresh init that completed (drained outbox): configured, not partial.
        assert!(!init_appears_incomplete(&db_path).await?);
        let error = ensure_sync_root_unconfigured(&sync_root)
            .await
            .expect_err("configured root must be rejected");
        assert!(error.to_string().contains("already configured"));

        // Crash mid-init: outbox rows remain with the cursor still at 0.
        let db = open_database(&db_path).await?;
        let conn = db.connect()?;
        configure_connection(&conn).await?;
        conn.execute(
            "INSERT INTO outbox (outbox_id, record_kind, object_id, payload, created_at_ns, inflight)
             VALUES (0, 'namespace_update', NULL, x'00', 0, 0)",
            (),
        )
        .await?;
        assert!(init_appears_incomplete(&db_path).await?);
        let error = ensure_sync_root_unconfigured(&sync_root)
            .await
            .expect_err("partially initialized root must be rejected");
        assert!(error.to_string().contains("failed before completing"));

        // Synced workspace with an outbox backlog: cursor has advanced.
        conn.execute("UPDATE daemon_state SET stable_cursor = 5 WHERE id = 1", ())
            .await?;
        assert!(!init_appears_incomplete(&db_path).await?);

        let _ = std::fs::remove_dir_all(&sync_root);
        Ok(())
    }

    #[test]
    fn socket_path_is_per_checkout_for_same_workspace() {
        let workspace_id = WorkspaceId("0123456789abcdefghijklmnopqrstuv".to_string());
        let left = DaemonWriterId(Bytes::from_static(b"left-writer-0001"));
        let right = DaemonWriterId(Bytes::from_static(b"right-writer-001"));

        assert_ne!(
            real_socket_path(&workspace_id, &left),
            real_socket_path(&workspace_id, &right)
        );
    }

    #[test]
    fn socket_path_stays_short_for_unix_socket_limits() {
        let workspace_id = WorkspaceId("0123456789abcdefghijklmnopqrstuv".to_string());
        let writer_id = DaemonWriterId(Bytes::from_static(b"0123456789abcdef"));
        let path = real_socket_path(&workspace_id, &writer_id);
        let path = path.to_str().expect("socket path is valid utf-8");

        assert!(
            path.len() < 100,
            "socket path should stay below common Unix SUN_LEN limits: {path}"
        );
    }
}
