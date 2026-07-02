use async_trait::async_trait;
use bytes::Bytes;
use compact_str::CompactString;
use opbox_core::fs::fio::FileIO;
use opbox_core::fs::ignore::is_hard_ignored;
use opbox_core::fs::types::{
    DeleteIfExistsResult, ExpectedBefore, FileContentFingerprint, FileFingerprint, FileHash,
    FileKey, FileStatFingerprint, GuardedDeleteResult, GuardedReadResult, GuardedWriteResult,
    RelativePath, ScanResult, ScanScope, Tree, TreeEntry, TreeEntryKind,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use time::OffsetDateTime;
use xxhash_rust::xxh3::xxh3_64;

const DEVICE_ID: u64 = 1;

#[derive(Debug, Clone)]
pub struct InMemoryFileIO {
    state: Arc<Mutex<State>>,
}

#[derive(Debug)]
struct State {
    files: BTreeMap<RelativePath, Entry>,
    next_inode: u64,
    logical_time_ns: i128,
    stats: InMemoryFileIOStats,
}

#[derive(Debug, Clone)]
struct Entry {
    bytes: Bytes,
    inode: u64,
    mtime: OffsetDateTime,
}

#[derive(Debug, Clone)]
struct StableRead {
    bytes: Bytes,
    fingerprint: FileFingerprint,
}

impl Default for InMemoryFileIO {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryFileIO {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                files: BTreeMap::new(),
                next_inode: 1,
                logical_time_ns: 0,
                stats: InMemoryFileIOStats::default(),
            })),
        }
    }

    pub fn write_file(&self, path: impl AsRef<str>, bytes: impl Into<Bytes>) -> eyre::Result<()> {
        let path = RelativePath::parse(path.as_ref())?;
        let mut state = self.state.lock().expect("in-memory file io poisoned");
        Self::insert_file(&mut state, path, bytes.into());
        Ok(())
    }

    pub fn append_file(&self, path: impl AsRef<str>, bytes: impl AsRef<[u8]>) -> eyre::Result<()> {
        let path = RelativePath::parse(path.as_ref())?;
        let mut state = self.state.lock().expect("in-memory file io poisoned");
        state.logical_time_ns += 1;
        let mtime = OffsetDateTime::from_unix_timestamp_nanos(state.logical_time_ns)
            .expect("logical sim timestamp is valid");
        match state.files.get_mut(&path) {
            Some(entry) => {
                let mut next = entry.bytes.to_vec();
                next.extend_from_slice(bytes.as_ref());
                entry.bytes = Bytes::from(next);
                entry.mtime = mtime;
            }
            None => {
                let inode = state.next_inode;
                state.next_inode += 1;
                state.files.insert(
                    path,
                    Entry {
                        bytes: Bytes::copy_from_slice(bytes.as_ref()),
                        inode,
                        mtime,
                    },
                );
            }
        }
        Ok(())
    }

    pub fn replace_file_contents(
        &self,
        path: impl AsRef<str>,
        bytes: impl Into<Bytes>,
    ) -> eyre::Result<()> {
        let path = RelativePath::parse(path.as_ref())?;
        let mut state = self.state.lock().expect("in-memory file io poisoned");
        state.logical_time_ns += 1;
        let mtime = OffsetDateTime::from_unix_timestamp_nanos(state.logical_time_ns)
            .expect("logical sim timestamp is valid");
        match state.files.get_mut(&path) {
            Some(entry) => {
                entry.bytes = bytes.into();
                entry.mtime = mtime;
            }
            None => {
                let inode = state.next_inode;
                state.next_inode += 1;
                state.files.insert(
                    path,
                    Entry {
                        bytes: bytes.into(),
                        inode,
                        mtime,
                    },
                );
            }
        }
        Ok(())
    }

    pub fn prepend_file(&self, path: impl AsRef<str>, bytes: impl AsRef<[u8]>) -> eyre::Result<()> {
        let path = RelativePath::parse(path.as_ref())?;
        let mut state = self.state.lock().expect("in-memory file io poisoned");
        state.logical_time_ns += 1;
        let mtime = OffsetDateTime::from_unix_timestamp_nanos(state.logical_time_ns)
            .expect("logical sim timestamp is valid");
        match state.files.get_mut(&path) {
            Some(entry) => {
                let mut next = bytes.as_ref().to_vec();
                next.extend_from_slice(entry.bytes.as_ref());
                entry.bytes = Bytes::from(next);
                entry.mtime = mtime;
            }
            None => {
                let inode = state.next_inode;
                state.next_inode += 1;
                state.files.insert(
                    path,
                    Entry {
                        bytes: Bytes::copy_from_slice(bytes.as_ref()),
                        inode,
                        mtime,
                    },
                );
            }
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub fn delete_file(&self, path: impl AsRef<str>) -> eyre::Result<()> {
        let path = RelativePath::parse(path.as_ref())?;
        let mut state = self.state.lock().expect("in-memory file io poisoned");
        state.files.remove(&path);
        state.logical_time_ns += 1;
        Ok(())
    }

    pub fn rename_file(&self, from: impl AsRef<str>, to: impl AsRef<str>) -> eyre::Result<()> {
        let from = RelativePath::parse(from.as_ref())?;
        let to = RelativePath::parse(to.as_ref())?;
        let mut state = self.state.lock().expect("in-memory file io poisoned");
        let Some(entry) = state.files.remove(&from) else {
            eyre::bail!("rename source does not exist: {from}");
        };
        state.files.insert(to, entry);
        state.logical_time_ns += 1;
        Ok(())
    }

    pub fn snapshot_text_files(&self) -> eyre::Result<BTreeMap<String, String>> {
        let state = self.state.lock().expect("in-memory file io poisoned");
        state
            .files
            .iter()
            .map(|(path, entry)| {
                let content = String::from_utf8(entry.bytes.to_vec())?;
                Ok((path.to_string(), content))
            })
            .collect()
    }

    pub fn snapshot_utf8_text_files(&self) -> BTreeMap<String, String> {
        let state = self.state.lock().expect("in-memory file io poisoned");
        state
            .files
            .iter()
            .filter_map(|(path, entry)| {
                String::from_utf8(entry.bytes.to_vec())
                    .ok()
                    .map(|content| (path.to_string(), content))
            })
            .collect()
    }

    pub fn stats(&self) -> InMemoryFileIOStats {
        let state = self.state.lock().expect("in-memory file io poisoned");
        state.stats
    }

    fn insert_file(state: &mut State, path: RelativePath, bytes: Bytes) {
        let inode = state.next_inode;
        state.next_inode += 1;
        state.logical_time_ns += 1;
        let mtime = OffsetDateTime::from_unix_timestamp_nanos(state.logical_time_ns)
            .expect("logical sim timestamp is valid");
        state.files.insert(
            path,
            Entry {
                bytes,
                inode,
                mtime,
            },
        );
    }

    fn stat_entry(path: RelativePath, entry: &Entry) -> TreeEntry {
        TreeEntry {
            path,
            kind: TreeEntryKind::File {
                fingerprint: FileFingerprint::StatOnly(Self::stat_fingerprint(entry)),
            },
        }
    }

    fn stat_fingerprint(entry: &Entry) -> FileStatFingerprint {
        FileStatFingerprint::new(
            FileKey::new(DEVICE_ID, entry.inode),
            entry.bytes.len() as u64,
            entry.mtime,
        )
    }

    fn stat_and_content_fingerprint(entry: &Entry) -> FileFingerprint {
        let stat = Self::stat_fingerprint(entry);
        FileFingerprint::StatAndContent {
            stat,
            content: Self::content_fingerprint(&entry.bytes),
        }
    }

    fn content_fingerprint(bytes: &Bytes) -> FileContentFingerprint {
        let hash = xxh3_64(bytes);
        FileContentFingerprint::new(FileHash::new(Bytes::copy_from_slice(&hash.to_be_bytes())))
    }

    fn fingerprint_matches_observed(
        observed: &FileFingerprint,
        expected: &FileFingerprint,
    ) -> bool {
        match expected {
            FileFingerprint::StatOnly(expected_stat) => observed.stat() == expected_stat,
            FileFingerprint::StatAndContent {
                stat: expected_stat,
                content: expected_content,
            } => match observed {
                FileFingerprint::StatAndContent { stat, content } => {
                    stat == expected_stat && content == expected_content
                }
                FileFingerprint::StatOnly(_) => false,
            },
        }
    }

    fn entry_matches_fingerprint(entry: &Entry, expected: &FileFingerprint) -> bool {
        match expected {
            FileFingerprint::StatOnly(expected_stat) => {
                &Self::stat_fingerprint(entry) == expected_stat
            }
            FileFingerprint::StatAndContent { .. } => Self::fingerprint_matches_observed(
                &Self::stat_and_content_fingerprint(entry),
                expected,
            ),
        }
    }

    fn observed_matches_expected(observed: Option<&Entry>, expected: &ExpectedBefore) -> bool {
        match expected {
            ExpectedBefore::Anything => true,
            ExpectedBefore::Missing => observed.is_none(),
            ExpectedBefore::PresentWithFingerprint(expected) => {
                observed.is_some_and(|entry| Self::entry_matches_fingerprint(entry, expected))
            }
        }
    }

    fn stable_read(state: &State, path: &RelativePath) -> Option<StableRead> {
        state.files.get(path).map(|entry| StableRead {
            bytes: entry.bytes.clone(),
            fingerprint: Self::stat_and_content_fingerprint(entry),
        })
    }

    fn tree_entry_for_observed(path: RelativePath, observed: Option<&Entry>) -> Option<TreeEntry> {
        observed.map(|entry| Self::stat_entry(path, entry))
    }

    fn temp_path_for(path: &RelativePath) -> eyre::Result<RelativePath> {
        path.with_file_name(CompactString::from(format!(
            ".{}.opbox-sim-tmp",
            path.file_name()
        )))
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct InMemoryFileIOStats {
    pub guarded_read_read_count: u64,
    pub guarded_read_changed_between_stats_count: u64,
    pub guarded_read_conflict_before_read_count: u64,
    pub guarded_write_written_count: u64,
    pub guarded_write_already_applied_count: u64,
    pub guarded_write_conflict_before_swap_count: u64,
    pub guarded_write_conflict_after_swap_count: u64,
    pub guarded_write_conflict_count: u64,
    pub guarded_delete_deleted_count: u64,
    pub guarded_delete_already_deleted_count: u64,
    pub guarded_delete_conflict_count: u64,
}

impl InMemoryFileIOStats {
    pub fn combine(self, other: Self) -> Self {
        Self {
            guarded_read_read_count: self.guarded_read_read_count + other.guarded_read_read_count,
            guarded_read_changed_between_stats_count: self.guarded_read_changed_between_stats_count
                + other.guarded_read_changed_between_stats_count,
            guarded_read_conflict_before_read_count: self.guarded_read_conflict_before_read_count
                + other.guarded_read_conflict_before_read_count,
            guarded_write_written_count: self.guarded_write_written_count
                + other.guarded_write_written_count,
            guarded_write_already_applied_count: self.guarded_write_already_applied_count
                + other.guarded_write_already_applied_count,
            guarded_write_conflict_before_swap_count: self.guarded_write_conflict_before_swap_count
                + other.guarded_write_conflict_before_swap_count,
            guarded_write_conflict_after_swap_count: self.guarded_write_conflict_after_swap_count
                + other.guarded_write_conflict_after_swap_count,
            guarded_write_conflict_count: self.guarded_write_conflict_count
                + other.guarded_write_conflict_count,
            guarded_delete_deleted_count: self.guarded_delete_deleted_count
                + other.guarded_delete_deleted_count,
            guarded_delete_already_deleted_count: self.guarded_delete_already_deleted_count
                + other.guarded_delete_already_deleted_count,
            guarded_delete_conflict_count: self.guarded_delete_conflict_count
                + other.guarded_delete_conflict_count,
        }
    }
}

#[async_trait]
impl FileIO for InMemoryFileIO {
    async fn scan(&self, scope: ScanScope) -> eyre::Result<ScanResult> {
        let state = self.state.lock().expect("in-memory file io poisoned");
        let entries = state
            .files
            .iter()
            .filter(|(path, _)| scope.contains_path(path) && !is_hard_ignored(path))
            .map(|(path, entry)| Self::stat_entry(path.clone(), entry))
            .collect();
        let finished_at = OffsetDateTime::from_unix_timestamp_nanos(state.logical_time_ns)
            .expect("logical sim timestamp is valid");

        Ok(ScanResult {
            finished_at,
            scope,
            tree: Tree::new(entries)?,
        })
    }

    async fn stat(&self, path: RelativePath) -> eyre::Result<Option<TreeEntry>> {
        let state = self.state.lock().expect("in-memory file io poisoned");
        Ok(Self::tree_entry_for_observed(
            path.clone(),
            state.files.get(&path),
        ))
    }

    async fn guarded_read(
        &self,
        path: RelativePath,
        expected: FileFingerprint,
    ) -> eyre::Result<GuardedReadResult> {
        let mut state = self.state.lock().expect("in-memory file io poisoned");
        let Some(entry) = state.files.get(&path) else {
            state.stats.guarded_read_conflict_before_read_count += 1;
            return Ok(GuardedReadResult::ConflictBeforeRead { observed: None });
        };
        if Self::stat_fingerprint(entry) != *expected.stat() {
            let observed = Some(Self::stat_entry(path, entry));
            state.stats.guarded_read_conflict_before_read_count += 1;
            return Ok(GuardedReadResult::ConflictBeforeRead { observed });
        }

        let bytes = entry.bytes.clone();
        let fingerprint = Self::stat_and_content_fingerprint(entry);
        if !Self::fingerprint_matches_observed(&fingerprint, &expected) {
            let observed = Some(TreeEntry {
                path,
                kind: TreeEntryKind::File {
                    fingerprint: fingerprint.clone(),
                },
            });
            state.stats.guarded_read_conflict_before_read_count += 1;
            return Ok(GuardedReadResult::ConflictBeforeRead { observed });
        }

        state.stats.guarded_read_read_count += 1;
        Ok(GuardedReadResult::Read { bytes, fingerprint })
    }

    async fn guarded_write(
        &self,
        path: RelativePath,
        bytes: Bytes,
        expected_before: ExpectedBefore,
    ) -> eyre::Result<GuardedWriteResult> {
        let mut state = self.state.lock().expect("in-memory file io poisoned");

        if let Some(current) = Self::stable_read(&state, &path)
            && current.bytes == bytes
        {
            state.stats.guarded_write_already_applied_count += 1;
            return Ok(GuardedWriteResult::AlreadyApplied {
                fingerprint: current.fingerprint,
            });
        }

        let observed = state.files.get(&path);
        if !Self::observed_matches_expected(observed, &expected_before) {
            let observed = Self::tree_entry_for_observed(path.clone(), observed);
            let swap_path = Self::temp_path_for(&path)?;
            state.stats.guarded_write_conflict_before_swap_count += 1;
            return Ok(GuardedWriteResult::ConflictBeforeSwap {
                swap_path,
                observed,
            });
        }

        Self::insert_file(&mut state, path.clone(), bytes);
        let entry = state
            .files
            .get(&path)
            .expect("just inserted requested path");
        let fingerprint = Self::stat_and_content_fingerprint(entry);
        state.stats.guarded_write_written_count += 1;
        Ok(GuardedWriteResult::Written { fingerprint })
    }

    async fn guarded_delete(
        &self,
        path: RelativePath,
        expected_before: ExpectedBefore,
    ) -> eyre::Result<GuardedDeleteResult> {
        let mut state = self.state.lock().expect("in-memory file io poisoned");
        let observed = state.files.get(&path);
        if !Self::observed_matches_expected(observed, &expected_before) {
            let observed = Self::tree_entry_for_observed(path, observed);
            state.stats.guarded_delete_conflict_count += 1;
            return Ok(GuardedDeleteResult::Conflict { observed });
        }
        if observed.is_none() {
            state.stats.guarded_delete_already_deleted_count += 1;
            return Ok(GuardedDeleteResult::AlreadyDeleted);
        }

        state.files.remove(&path);
        state.logical_time_ns += 1;
        state.stats.guarded_delete_deleted_count += 1;
        Ok(GuardedDeleteResult::Deleted)
    }

    async fn delete_if_exists(&self, path: RelativePath) -> eyre::Result<DeleteIfExistsResult> {
        let mut state = self.state.lock().expect("in-memory file io poisoned");
        if state.files.remove(&path).is_some() {
            state.logical_time_ns += 1;
            Ok(DeleteIfExistsResult::Deleted)
        } else {
            Ok(DeleteIfExistsResult::AlreadyMissing)
        }
    }
}
