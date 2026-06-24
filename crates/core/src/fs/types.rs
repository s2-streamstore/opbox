use bytes::Bytes;
use compact_str::CompactString;
use std::fmt::Debug;
use std::str::FromStr;
use time::OffsetDateTime;
use tokio::sync::oneshot;

#[derive(Debug)]
pub enum ExpectedBefore {
    Missing,
    /// The path must exist and match this fingerprint. `StatOnly` guards use
    /// metadata only; `StatAndContent` guards include the content hash.
    PresentWithFingerprint(FileFingerprint),
}

pub enum FsRequest {
    Scan {
        scope: ScanScope,
        reply: oneshot::Sender<eyre::Result<ScanResult>>,
    },
    Stat {
        path: RelativePath,
        reply: oneshot::Sender<eyre::Result<Option<TreeEntry>>>,
    },
    /// Read bytes only if the path still matches the fingerprint observed by semantic state.
    GuardedRead {
        path: RelativePath,
        expected: FileFingerprint,
        reply: oneshot::Sender<eyre::Result<GuardedReadResult>>,
    },
    /// Write a full file.
    GuardedWrite {
        path: RelativePath,
        bytes: Bytes,
        expected_before: ExpectedBefore,
        reply: oneshot::Sender<eyre::Result<GuardedWriteResult>>,
    },
    /// Delete a file only if the path still matches the expected pre-delete state.
    GuardedDelete {
        path: RelativePath,
        expected_before: ExpectedBefore,
        reply: oneshot::Sender<eyre::Result<GuardedDeleteResult>>,
    },
    /// Best-effort cleanup for daemon-owned temporary paths.
    DeleteIfExists {
        path: RelativePath,
        reply: oneshot::Sender<eyre::Result<DeleteIfExistsResult>>,
    },
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RelativePath {
    components: Vec<CompactString>,
}

impl Debug for RelativePath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RelativePath")
            .field("components", &self.components.join("/"))
            .finish()
    }
}

impl RelativePath {
    pub fn new(components: Vec<CompactString>) -> eyre::Result<Self> {
        if components.is_empty() {
            eyre::bail!("relative path must have at least one component");
        }

        for component in &components {
            validate_component(component.as_str())?;
        }

        Ok(Self { components })
    }

    pub fn parse(value: &str) -> eyre::Result<Self> {
        if value.is_empty() {
            eyre::bail!("relative path must not be empty");
        }
        if value.starts_with('/') {
            eyre::bail!("relative path must not start with /: {value:?}");
        }
        if value.ends_with('/') {
            eyre::bail!("relative path must not end with /: {value:?}");
        }

        let components = value.split('/').map(CompactString::from).collect();
        Self::new(components)
    }

    pub fn as_components(&self) -> &[CompactString] {
        &self.components
    }

    pub fn file_name(&self) -> &str {
        self.components
            .last()
            .expect("relative path has at least one component")
            .as_str()
    }

    pub fn parent(&self) -> Option<Self> {
        if self.components.len() <= 1 {
            return None;
        }

        Some(Self {
            components: self.components[..self.components.len() - 1].to_vec(),
        })
    }

    pub fn with_file_name(&self, name: CompactString) -> eyre::Result<Self> {
        validate_component(name.as_str())?;
        let mut components = self.components.clone();
        *components
            .last_mut()
            .expect("relative path has at least one component") = name;
        Ok(Self { components })
    }

    pub fn to_db_path(&self) -> String {
        self.components
            .iter()
            .map(CompactString::as_str)
            .collect::<Vec<_>>()
            .join("/")
    }
}

impl std::fmt::Display for RelativePath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_db_path())
    }
}

impl FromStr for RelativePath {
    type Err = eyre::Report;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

fn validate_component(component: &str) -> eyre::Result<()> {
    if component.is_empty() {
        eyre::bail!("relative path component must not be empty");
    }
    if component == "." || component == ".." {
        eyre::bail!("relative path component must not be {component:?}");
    }
    if component.contains('/') {
        eyre::bail!("relative path component must not contain /: {component:?}");
    }
    if component.contains('\0') {
        eyre::bail!("relative path component must not contain NUL");
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ScanScope {
    Full,
    Subtree(RelativePath),
    SingleFile(RelativePath),
}

impl ScanScope {
    pub fn contains_path(&self, path: &RelativePath) -> bool {
        match self {
            ScanScope::Full => true,
            ScanScope::SingleFile(single) => path == single,
            ScanScope::Subtree(subtree) => {
                path == subtree || path.as_components().starts_with(subtree.as_components())
            }
        }
    }
}

#[derive(Debug)]
pub struct ScanResult {
    pub finished_at: OffsetDateTime,
    pub scope: ScanScope,
    pub tree: Tree,
}

/// Flat, deterministic snapshot for a scan scope.
///
/// Invariants:
/// - entries are sorted by canonical relative path
/// - every path is inside `scope`
/// - no duplicate paths
/// - paths are full relative paths from the sync root
#[derive(Debug)]
pub struct Tree {
    entries: Vec<TreeEntry>,
}

impl Tree {
    pub fn new(mut entries: Vec<TreeEntry>) -> eyre::Result<Self> {
        entries.sort_by(|left, right| left.path.cmp(&right.path));

        for pair in entries.windows(2) {
            let [left, right] = pair else {
                unreachable!("window length is 2");
            };
            if left.path == right.path {
                eyre::bail!("duplicate tree path: {}", left.path);
            }
        }

        Ok(Self { entries })
    }

    pub fn empty() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn entries(&self) -> &[TreeEntry] {
        &self.entries
    }

    pub fn into_entries(self) -> Vec<TreeEntry> {
        self.entries
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeEntry {
    pub path: RelativePath,
    pub kind: TreeEntryKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TreeEntryKind {
    Directory,
    File { fingerprint: FileFingerprint },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FileKey {
    pub device_id: u64,
    pub inode: u64,
}

impl FileKey {
    pub fn new(device_id: u64, inode: u64) -> Self {
        Self { device_id, inode }
    }

    pub fn encode(&self) -> String {
        format!("{}:{}", self.device_id, self.inode)
    }

    pub fn decode(value: &str) -> eyre::Result<Self> {
        let (device_id, inode) = value
            .split_once(':')
            .ok_or_else(|| eyre::eyre!("invalid file key: {value:?}"))?;
        Ok(Self {
            device_id: device_id.parse()?,
            inode: inode.parse()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileFingerprint {
    StatOnly(FileStatFingerprint),
    StatAndContent {
        stat: FileStatFingerprint,
        content: FileContentFingerprint,
    },
}

impl FileFingerprint {
    pub fn stat(&self) -> &FileStatFingerprint {
        match self {
            Self::StatOnly(stat) | Self::StatAndContent { stat, .. } => stat,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileStatFingerprint {
    pub file_key: FileKey,
    pub size: u64,
    pub mtime: OffsetDateTime,
}

impl FileStatFingerprint {
    pub fn new(file_key: FileKey, size: u64, mtime: OffsetDateTime) -> Self {
        Self {
            file_key,
            size,
            mtime,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileContentFingerprint {
    pub hash: FileHash,
}

impl FileContentFingerprint {
    pub fn new(hash: FileHash) -> Self {
        Self { hash }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct FileHash(Bytes);

impl FileHash {
    pub fn new(bytes: Bytes) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &Bytes {
        &self.0
    }
}

impl Debug for FileHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("FileHash").field(&self.0).finish()
    }
}

#[derive(Debug)]
pub enum GuardedReadResult {
    Read {
        bytes: Bytes,
        fingerprint: FileFingerprint,
    },
    ChangedBetweenStats {
        before: Option<TreeEntry>,
        after: Option<TreeEntry>,
    },
    ConflictBeforeRead {
        observed: Option<TreeEntry>,
    },
}

#[derive(strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum GuardedWriteResult {
    Written {
        fingerprint: FileFingerprint,
    },
    AlreadyApplied {
        fingerprint: FileFingerprint,
    },
    ConflictBeforeSwap {
        /// Can be used for GC later.
        swap_path: RelativePath,
        observed: Option<TreeEntry>,
    },
    /// Ambiguous state.
    ConflictAfterSwap {
        observed: Option<TreeEntry>,
    },
    Conflict {
        observed: Option<TreeEntry>,
    },
}

pub enum GuardedDeleteResult {
    Deleted,
    AlreadyDeleted,
    Conflict { observed: Option<TreeEntry> },
}

#[derive(Debug)]
pub enum DeleteIfExistsResult {
    Deleted,
    AlreadyMissing,
}
