use async_trait::async_trait;
use bytes::Bytes;
use compact_str::CompactString;
use opbox::fs::fio::FileIO;
use opbox::fs::types::{
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
            })),
        }
    }

    pub fn write_file(&self, path: impl AsRef<str>, bytes: impl Into<Bytes>) -> eyre::Result<()> {
        let path = RelativePath::parse(path.as_ref())?;
        let mut state = self.state.lock().expect("in-memory file io poisoned");
        Self::insert_file(&mut state, path, bytes.into());
        Ok(())
    }

    #[allow(dead_code)]
    pub fn append_file(&self, path: impl AsRef<str>, bytes: impl AsRef<[u8]>) -> eyre::Result<()> {
        let path = RelativePath::parse(path.as_ref())?;
        let mut state = self.state.lock().expect("in-memory file io poisoned");
        let mut next = state
            .files
            .get(&path)
            .map(|entry| entry.bytes.to_vec())
            .unwrap_or_default();
        next.extend_from_slice(bytes.as_ref());
        Self::insert_file(&mut state, path, Bytes::from(next));
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

    fn in_scope(path: &RelativePath, scope: &ScanScope) -> bool {
        match scope {
            ScanScope::Full => true,
            ScanScope::SingleFile(single) => path == single,
            ScanScope::Subtree(subtree) => {
                path == subtree || path.as_components().starts_with(subtree.as_components())
            }
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
            .filter(|(path, _)| Self::in_scope(path, &scope))
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
        let state = self.state.lock().expect("in-memory file io poisoned");
        let Some(entry) = state.files.get(&path) else {
            return Ok(GuardedReadResult::ConflictBeforeRead { observed: None });
        };
        if Self::stat_fingerprint(entry) != *expected.stat() {
            return Ok(GuardedReadResult::ConflictBeforeRead {
                observed: Some(Self::stat_entry(path, entry)),
            });
        }

        let bytes = entry.bytes.clone();
        let fingerprint = Self::stat_and_content_fingerprint(entry);
        if !Self::fingerprint_matches_observed(&fingerprint, &expected) {
            return Ok(GuardedReadResult::ConflictBeforeRead {
                observed: Some(TreeEntry {
                    path,
                    kind: TreeEntryKind::File {
                        fingerprint: fingerprint.clone(),
                    },
                }),
            });
        }

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
            return Ok(GuardedWriteResult::AlreadyApplied {
                fingerprint: current.fingerprint,
            });
        }

        let observed = state.files.get(&path);
        if !Self::observed_matches_expected(observed, &expected_before) {
            return Ok(GuardedWriteResult::ConflictBeforeSwap {
                swap_path: Self::temp_path_for(&path)?,
                observed: Self::tree_entry_for_observed(path, observed),
            });
        }

        Self::insert_file(&mut state, path.clone(), bytes);
        let entry = state
            .files
            .get(&path)
            .expect("just inserted requested path");
        Ok(GuardedWriteResult::Written {
            fingerprint: Self::stat_and_content_fingerprint(entry),
        })
    }

    async fn guarded_delete(
        &self,
        path: RelativePath,
        expected_before: ExpectedBefore,
    ) -> eyre::Result<GuardedDeleteResult> {
        let mut state = self.state.lock().expect("in-memory file io poisoned");
        let observed = state.files.get(&path);
        if !Self::observed_matches_expected(observed, &expected_before) {
            return Ok(GuardedDeleteResult::Conflict {
                observed: Self::tree_entry_for_observed(path, observed),
            });
        }
        if observed.is_none() {
            return Ok(GuardedDeleteResult::AlreadyDeleted);
        }

        state.files.remove(&path);
        state.logical_time_ns += 1;
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
