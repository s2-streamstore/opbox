use crate::fs::fio::FileIO;
use crate::fs::ignore::IgnoreRules;
use crate::fs::types::{
    DeleteIfExistsResult, ExpectedBefore, FileContentFingerprint, FileFingerprint, FileHash,
    FileKey, FileStatFingerprint, GuardedDeleteResult, GuardedReadResult, GuardedWriteResult,
    RelativePath, ScanResult, ScanScope, Tree, TreeEntry, TreeEntryKind,
};
use async_trait::async_trait;
use bytes::Bytes;
use compact_str::CompactString;
use std::io::ErrorKind;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use time::OffsetDateTime;
use tracing::debug;
use xxhash_rust::xxh3::xxh3_64;

#[derive(Clone)]
pub struct LocalFileIO {
    absolute_path_root: PathBuf,
}

impl LocalFileIO {
    pub fn new(absolute_path_root: impl Into<PathBuf>) -> Self {
        Self {
            absolute_path_root: absolute_path_root.into(),
        }
    }

    fn absolute_path(&self, path: &RelativePath) -> PathBuf {
        self.absolute_path_root.join(path.to_db_path())
    }

    fn absolute_parent(&self, path: &RelativePath) -> PathBuf {
        path.parent()
            .map(|parent| self.absolute_path(&parent))
            .unwrap_or_else(|| self.absolute_path_root.clone())
    }

    fn temp_path_for(path: &RelativePath) -> eyre::Result<RelativePath> {
        let temp_name = CompactString::from(format!(
            ".{}.opbox-tmp-{:016x}",
            path.file_name(),
            rand::random::<u64>()
        ));
        path.with_file_name(temp_name)
    }

    fn child_path(parent: Option<&RelativePath>, name: &std::ffi::OsStr) -> eyre::Result<SelfPath> {
        let Some(name) = name.to_str() else {
            eyre::bail!("non-utf-8 path component is not supported");
        };

        let mut components = parent
            .map(|path| path.as_components().to_vec())
            .unwrap_or_default();
        components.push(CompactString::from(name));

        Ok(SelfPath(RelativePath::new(components)?))
    }

    async fn stat_path(&self, path: RelativePath) -> eyre::Result<Option<TreeEntry>> {
        let metadata = match tokio::fs::symlink_metadata(self.absolute_path(&path)).await {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };

        if metadata.is_dir() {
            return Ok(Some(TreeEntry {
                path,
                kind: TreeEntryKind::Directory,
            }));
        }

        if !metadata.is_file() {
            debug!(%path, "skipping non-file, non-directory path (e.g. symlink)");
            return Ok(None);
        }

        let abs_path = self.absolute_path(&path);
        Ok(Some(TreeEntry {
            path,
            kind: TreeEntryKind::File {
                fingerprint: FileFingerprint::StatOnly(Self::stat_fingerprint(
                    &metadata, &abs_path,
                )?),
            },
        }))
    }

    #[cfg(unix)]
    fn stat_fingerprint(
        metadata: &std::fs::Metadata,
        _path: &std::path::Path,
    ) -> eyre::Result<FileStatFingerprint> {
        Ok(FileStatFingerprint::new(
            FileKey::new(metadata.dev(), metadata.ino()),
            metadata.len(),
            OffsetDateTime::from(metadata.modified()?),
        ))
    }

    #[cfg(windows)]
    fn stat_fingerprint(
        metadata: &std::fs::Metadata,
        path: &std::path::Path,
    ) -> eyre::Result<FileStatFingerprint> {
        use std::os::windows::io::AsRawHandle;

        #[repr(C)]
        #[allow(non_snake_case)]
        struct BY_HANDLE_FILE_INFORMATION {
            dwFileAttributes: u32,
            ftCreationTime: [u32; 2],
            ftLastAccessTime: [u32; 2],
            ftLastWriteTime: [u32; 2],
            dwVolumeSerialNumber: u32,
            nFileSizeHigh: u32,
            nFileSizeLow: u32,
            nNumberOfLinks: u32,
            nFileIndexHigh: u32,
            nFileIndexLow: u32,
        }

        unsafe extern "system" {
            fn GetFileInformationByHandle(
                h_file: *mut std::ffi::c_void,
                lp_file_information: *mut BY_HANDLE_FILE_INFORMATION,
            ) -> i32;
        }

        let file = std::fs::File::open(path)?;
        let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
        let ret = unsafe { GetFileInformationByHandle(file.as_raw_handle() as *mut _, &mut info) };
        if ret == 0 {
            eyre::bail!("GetFileInformationByHandle failed for {}", path.display());
        }

        let file_index = ((info.nFileIndexHigh as u64) << 32) | (info.nFileIndexLow as u64);
        Ok(FileStatFingerprint::new(
            FileKey::new(info.dwVolumeSerialNumber as u64, file_index),
            metadata.len(),
            OffsetDateTime::from(metadata.modified()?),
        ))
    }

    fn content_fingerprint(bytes: &Bytes) -> FileContentFingerprint {
        let hash = xxh3_64(bytes);
        FileContentFingerprint::new(FileHash::new(Bytes::copy_from_slice(&hash.to_be_bytes())))
    }

    fn stat_and_content_fingerprint(stat: FileStatFingerprint, bytes: &Bytes) -> FileFingerprint {
        FileFingerprint::StatAndContent {
            stat,
            content: Self::content_fingerprint(bytes),
        }
    }

    fn file_stat(entry: &TreeEntry) -> Option<&FileStatFingerprint> {
        match &entry.kind {
            TreeEntryKind::File { fingerprint } => Some(fingerprint.stat()),
            TreeEntryKind::Directory => None,
        }
    }

    fn entry_for_file_fingerprint(path: RelativePath, fingerprint: FileFingerprint) -> TreeEntry {
        TreeEntry {
            path,
            kind: TreeEntryKind::File { fingerprint },
        }
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

    fn entry_matches_fingerprint(entry: &TreeEntry, expected: &FileFingerprint) -> bool {
        match &entry.kind {
            TreeEntryKind::File { fingerprint } => {
                Self::fingerprint_matches_observed(fingerprint, expected)
            }
            TreeEntryKind::Directory => false,
        }
    }

    fn entry_stat_matches_fingerprint(entry: &TreeEntry, expected: &FileFingerprint) -> bool {
        Self::file_stat(entry).is_some_and(|stat| stat == expected.stat())
    }

    fn observed_matches_expected(observed: &Option<TreeEntry>, expected: &ExpectedBefore) -> bool {
        match expected {
            ExpectedBefore::Missing => observed.is_none(),
            ExpectedBefore::PresentWithFingerprint(expected) => observed
                .as_ref()
                .is_some_and(|entry| Self::entry_matches_fingerprint(entry, expected)),
        }
    }

    async fn observe_for_expected(
        &self,
        path: RelativePath,
        expected: &ExpectedBefore,
    ) -> eyre::Result<Option<TreeEntry>> {
        let ExpectedBefore::PresentWithFingerprint(FileFingerprint::StatAndContent { .. }) =
            expected
        else {
            return self.stat_path(path).await;
        };

        Ok(match self.stable_read_file(path).await? {
            StableFileRead::Read { fingerprint, .. } => Some(Self::entry_for_file_fingerprint(
                fingerprint.stat_path.clone(),
                fingerprint.fingerprint,
            )),
            StableFileRead::Changed { after, .. } => after,
            StableFileRead::NotFile { observed } => observed,
        })
    }

    async fn stable_read_file(&self, path: RelativePath) -> eyre::Result<StableFileRead> {
        let before = self.stat_path(path.clone()).await?;
        let Some(before_entry) = before.as_ref() else {
            return Ok(StableFileRead::NotFile { observed: before });
        };
        let Some(before_stat) = Self::file_stat(before_entry).cloned() else {
            return Ok(StableFileRead::NotFile { observed: before });
        };

        let bytes = match tokio::fs::read(self.absolute_path(&path)).await {
            Ok(bytes) => Bytes::from(bytes),
            Err(error) if error.kind() == ErrorKind::NotFound => {
                return Ok(StableFileRead::Changed {
                    after: self.stat_path(path).await?,
                });
            }
            Err(error) => return Err(error.into()),
        };

        let after = self.stat_path(path.clone()).await?;
        if after != before {
            return Ok(StableFileRead::Changed { after });
        }

        Ok(StableFileRead::Read {
            bytes: bytes.clone(),
            fingerprint: StableFileFingerprint {
                stat_path: path,
                fingerprint: Self::stat_and_content_fingerprint(before_stat, &bytes),
            },
        })
    }
}

#[async_trait]
impl FileIO for LocalFileIO {
    async fn scan(&self, scope: ScanScope) -> eyre::Result<ScanResult> {
        let mut entries = Vec::new();
        let mut pending_dirs = Vec::new();
        let ignore_rules = IgnoreRules::load(&self.absolute_path_root)?;

        match &scope {
            ScanScope::Full => pending_dirs.push(None),
            ScanScope::Subtree(path) => {
                if ignore_rules.is_ignored(path) {
                    // Ignored subtrees are local-only implementation details.
                } else if let Some(entry) = self.stat_path(path.clone()).await? {
                    if matches!(entry.kind, TreeEntryKind::Directory) {
                        pending_dirs.push(Some(path.clone()));
                    }
                    entries.push(entry);
                }
            }
            ScanScope::SingleFile(path) => {
                if !ignore_rules.is_ignored(path)
                    && let Some(entry) = self.stat_path(path.clone()).await?
                {
                    entries.push(entry);
                }
            }
        }

        while let Some(dir) = pending_dirs.pop() {
            let absolute_dir = dir
                .as_ref()
                .map(|path| self.absolute_path(path))
                .unwrap_or_else(|| self.absolute_path_root.clone());
            let mut read_dir = match tokio::fs::read_dir(absolute_dir).await {
                Ok(read_dir) => read_dir,
                Err(error) if error.kind() == ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };

            loop {
                let entry = match read_dir.next_entry().await {
                    Ok(Some(entry)) => entry,
                    Ok(None) => break,
                    Err(error) if error.kind() == ErrorKind::NotFound => break,
                    Err(error) => return Err(error.into()),
                };
                let SelfPath(path) = Self::child_path(dir.as_ref(), &entry.file_name())?;
                if ignore_rules.is_ignored(&path) {
                    continue;
                }
                let Some(tree_entry) = self.stat_path(path.clone()).await? else {
                    continue;
                };

                if matches!(tree_entry.kind, TreeEntryKind::Directory) {
                    pending_dirs.push(Some(path));
                }
                entries.push(tree_entry);
            }
        }

        Ok(ScanResult {
            finished_at: OffsetDateTime::now_utc(),
            scope,
            tree: Tree::new(entries)?,
        })
    }

    async fn stat(&self, path: RelativePath) -> eyre::Result<Option<TreeEntry>> {
        self.stat_path(path).await
    }

    async fn guarded_read(
        &self,
        path: RelativePath,
        expected: FileFingerprint,
    ) -> eyre::Result<GuardedReadResult> {
        let before = self.stat_path(path.clone()).await?;
        let Some(before_entry) = before.as_ref() else {
            return Ok(GuardedReadResult::ConflictBeforeRead { observed: before });
        };
        let Some(before_stat) = Self::file_stat(before_entry).cloned() else {
            return Ok(GuardedReadResult::ConflictBeforeRead { observed: before });
        };

        if !Self::entry_stat_matches_fingerprint(before_entry, &expected) {
            return Ok(GuardedReadResult::ConflictBeforeRead { observed: before });
        }

        let bytes = match tokio::fs::read(self.absolute_path(&path)).await {
            Ok(bytes) => Bytes::from(bytes),
            Err(error) if error.kind() == ErrorKind::NotFound => {
                return Ok(GuardedReadResult::ChangedBetweenStats {
                    before,
                    after: self.stat_path(path).await?,
                });
            }
            Err(error) => return Err(error.into()),
        };

        let after = self.stat_path(path.clone()).await?;
        if after != before {
            return Ok(GuardedReadResult::ChangedBetweenStats { before, after });
        }

        let fingerprint = Self::stat_and_content_fingerprint(before_stat, &bytes);
        if !Self::fingerprint_matches_observed(&fingerprint, &expected) {
            return Ok(GuardedReadResult::ConflictBeforeRead {
                observed: Some(Self::entry_for_file_fingerprint(path, fingerprint)),
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
        if let StableFileRead::Read {
            bytes: current_bytes,
            fingerprint,
        } = self.stable_read_file(path.clone()).await?
            && current_bytes == bytes
        {
            return Ok(GuardedWriteResult::AlreadyApplied {
                fingerprint: fingerprint.fingerprint,
            });
        }

        let swap_path = Self::temp_path_for(&path)?;
        tokio::fs::create_dir_all(self.absolute_parent(&path)).await?;
        tokio::fs::write(self.absolute_path(&swap_path), &bytes).await?;

        let observed = self
            .observe_for_expected(path.clone(), &expected_before)
            .await?;
        if !Self::observed_matches_expected(&observed, &expected_before) {
            return Ok(GuardedWriteResult::ConflictBeforeSwap {
                swap_path,
                observed,
            });
        }

        if let Err(error) =
            tokio::fs::rename(self.absolute_path(&swap_path), self.absolute_path(&path)).await
        {
            let _ = tokio::fs::remove_file(self.absolute_path(&swap_path)).await;
            return Err(error.into());
        }

        match self.stable_read_file(path).await? {
            StableFileRead::Read {
                bytes: current_bytes,
                fingerprint,
            } if current_bytes == bytes => Ok(GuardedWriteResult::Written {
                fingerprint: fingerprint.fingerprint,
            }),
            StableFileRead::Read { fingerprint, .. } => Ok(GuardedWriteResult::ConflictAfterSwap {
                observed: Some(Self::entry_for_file_fingerprint(
                    fingerprint.stat_path,
                    fingerprint.fingerprint,
                )),
            }),
            StableFileRead::Changed { after } => {
                Ok(GuardedWriteResult::ConflictAfterSwap { observed: after })
            }
            StableFileRead::NotFile { observed } => {
                Ok(GuardedWriteResult::ConflictAfterSwap { observed })
            }
        }
    }

    async fn guarded_delete(
        &self,
        path: RelativePath,
        expected_before: ExpectedBefore,
    ) -> eyre::Result<GuardedDeleteResult> {
        let observed = self
            .observe_for_expected(path.clone(), &expected_before)
            .await?;

        if !Self::observed_matches_expected(&observed, &expected_before) {
            return Ok(GuardedDeleteResult::Conflict { observed });
        }

        if observed.is_none() {
            return Ok(GuardedDeleteResult::AlreadyDeleted);
        }

        match tokio::fs::remove_file(self.absolute_path(&path)).await {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {
                return Ok(GuardedDeleteResult::Conflict { observed: None });
            }
            Err(error) => return Err(error.into()),
        }

        let after = self.stat_path(path).await?;
        if after.is_none() {
            Ok(GuardedDeleteResult::Deleted)
        } else {
            Ok(GuardedDeleteResult::Conflict { observed: after })
        }
    }

    async fn delete_if_exists(&self, path: RelativePath) -> eyre::Result<DeleteIfExistsResult> {
        match tokio::fs::remove_file(self.absolute_path(&path)).await {
            Ok(()) => Ok(DeleteIfExistsResult::Deleted),
            Err(error) if error.kind() == ErrorKind::NotFound => {
                Ok(DeleteIfExistsResult::AlreadyMissing)
            }
            Err(error) => Err(error.into()),
        }
    }
}

struct SelfPath(RelativePath);

struct StableFileFingerprint {
    stat_path: RelativePath,
    fingerprint: FileFingerprint,
}

enum StableFileRead {
    Read {
        bytes: Bytes,
        fingerprint: StableFileFingerprint,
    },
    Changed {
        after: Option<TreeEntry>,
    },
    NotFile {
        observed: Option<TreeEntry>,
    },
}
