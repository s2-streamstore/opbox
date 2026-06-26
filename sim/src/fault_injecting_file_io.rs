use crate::in_memory_file_io::{InMemoryFileIO, InMemoryFileIOStats};
use async_trait::async_trait;
use bytes::Bytes;
use compact_str::CompactString;
use opbox_core::fs::fio::FileIO;
use opbox_core::fs::types::{
    DeleteIfExistsResult, ExpectedBefore, FileFingerprint, GuardedDeleteResult, GuardedReadResult,
    GuardedWriteResult, RelativePath, ScanResult, ScanScope, TreeEntry,
};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct FaultInjectingFileIO {
    inner: InMemoryFileIO,
    state: Arc<Mutex<FaultState>>,
}

#[derive(Debug, Default)]
struct FaultState {
    faults: VecDeque<Fault>,
    stats: InMemoryFileIOStats,
}

#[derive(Debug)]
enum Fault {
    GuardedReadChangedBetweenStats {
        path: String,
        replacement: Bytes,
    },
    GuardedWriteConflictBeforeSwap {
        path: String,
        replacement: Bytes,
    },
    GuardedWriteConflictAfterSwap {
        path: String,
        replacement: Bytes,
    },
    GuardedWriteTempLeakAndFail {
        path: String,
        temp_path: String,
        bytes: Bytes,
    },
    DeleteIfExistsFailure {
        path: String,
    },
}

impl FaultInjectingFileIO {
    pub fn new(inner: InMemoryFileIO) -> Self {
        Self {
            inner,
            state: Arc::new(Mutex::new(FaultState::default())),
        }
    }

    pub fn write_file(&self, path: impl AsRef<str>, bytes: impl Into<Bytes>) -> eyre::Result<()> {
        self.inner.write_file(path, bytes)
    }

    pub fn append_file(&self, path: impl AsRef<str>, bytes: impl AsRef<[u8]>) -> eyre::Result<()> {
        self.inner.append_file(path, bytes)
    }

    pub fn replace_file_contents(
        &self,
        path: impl AsRef<str>,
        bytes: impl Into<Bytes>,
    ) -> eyre::Result<()> {
        self.inner.replace_file_contents(path, bytes)
    }

    pub fn prepend_file(&self, path: impl AsRef<str>, bytes: impl AsRef<[u8]>) -> eyre::Result<()> {
        self.inner.prepend_file(path, bytes)
    }

    pub fn delete_file(&self, path: impl AsRef<str>) -> eyre::Result<()> {
        self.inner.delete_file(path)
    }

    pub fn rename_file(&self, from: impl AsRef<str>, to: impl AsRef<str>) -> eyre::Result<()> {
        self.inner.rename_file(from, to)
    }

    pub fn snapshot_text_files(&self) -> eyre::Result<std::collections::BTreeMap<String, String>> {
        self.inner.snapshot_text_files()
    }

    pub fn snapshot_utf8_text_files(&self) -> std::collections::BTreeMap<String, String> {
        self.inner.snapshot_utf8_text_files()
    }

    pub fn stats(&self) -> InMemoryFileIOStats {
        let state = self.state.lock().expect("fault-injecting file io poisoned");
        self.inner.stats().combine(state.stats)
    }

    pub fn inject_guarded_read_changed_between_stats(
        &self,
        path: impl AsRef<str>,
        replacement: Bytes,
    ) -> eyre::Result<()> {
        let path = validate_path(path)?;
        self.push_fault(Fault::GuardedReadChangedBetweenStats { path, replacement });
        Ok(())
    }

    pub fn inject_guarded_write_conflict_before_swap(
        &self,
        path: impl AsRef<str>,
        replacement: Bytes,
    ) -> eyre::Result<()> {
        let path = validate_path(path)?;
        self.push_fault(Fault::GuardedWriteConflictBeforeSwap { path, replacement });
        Ok(())
    }

    pub fn inject_guarded_write_conflict_after_swap(
        &self,
        path: impl AsRef<str>,
        replacement: Bytes,
    ) -> eyre::Result<()> {
        let path = validate_path(path)?;
        self.push_fault(Fault::GuardedWriteConflictAfterSwap { path, replacement });
        Ok(())
    }

    pub fn inject_guarded_write_temp_leak_and_fail(
        &self,
        path: impl AsRef<str>,
        temp_path: impl AsRef<str>,
        bytes: Bytes,
    ) -> eyre::Result<()> {
        let path = validate_path(path)?;
        let temp_path = validate_path(temp_path)?;
        self.push_fault(Fault::GuardedWriteTempLeakAndFail {
            path,
            temp_path,
            bytes,
        });
        Ok(())
    }

    pub fn inject_delete_if_exists_failure(&self, path: impl AsRef<str>) -> eyre::Result<()> {
        let path = validate_path(path)?;
        self.push_fault(Fault::DeleteIfExistsFailure { path });
        Ok(())
    }

    fn push_fault(&self, fault: Fault) {
        self.state
            .lock()
            .expect("fault-injecting file io poisoned")
            .faults
            .push_back(fault);
    }

    fn take_fault(&self, predicate: impl Fn(&Fault) -> bool) -> Option<Fault> {
        let mut state = self.state.lock().expect("fault-injecting file io poisoned");
        let index = state.faults.iter().position(predicate)?;
        state.faults.remove(index)
    }

    fn add_stats(&self, update: impl FnOnce(&mut InMemoryFileIOStats)) {
        let mut state = self.state.lock().expect("fault-injecting file io poisoned");
        update(&mut state.stats);
    }

    fn temp_path_for(path: &RelativePath) -> eyre::Result<RelativePath> {
        path.with_file_name(CompactString::from(format!(
            ".{}.opbox-tmp-injected",
            path.file_name()
        )))
    }
}

#[async_trait]
impl FileIO for FaultInjectingFileIO {
    async fn scan(&self, scope: ScanScope) -> eyre::Result<ScanResult> {
        self.inner.scan(scope).await
    }

    async fn stat(&self, path: RelativePath) -> eyre::Result<Option<TreeEntry>> {
        self.inner.stat(path).await
    }

    async fn guarded_read(
        &self,
        path: RelativePath,
        expected: FileFingerprint,
    ) -> eyre::Result<GuardedReadResult> {
        if let Some(Fault::GuardedReadChangedBetweenStats { replacement, .. }) =
            self.take_fault(|fault| {
                matches!(
                    fault,
                    Fault::GuardedReadChangedBetweenStats { path: fault_path, .. }
                        if fault_path == &path.to_string()
                )
            })
        {
            let before = self.inner.stat(path.clone()).await?;
            self.inner
                .replace_file_contents(path.to_string(), replacement)?;
            let after = self.inner.stat(path).await?;
            self.add_stats(|stats| stats.guarded_read_changed_between_stats_count += 1);
            return Ok(GuardedReadResult::ChangedBetweenStats { before, after });
        }

        self.inner.guarded_read(path, expected).await
    }

    async fn guarded_write(
        &self,
        path: RelativePath,
        bytes: Bytes,
        expected_before: ExpectedBefore,
    ) -> eyre::Result<GuardedWriteResult> {
        if let Some(Fault::GuardedWriteTempLeakAndFail {
            temp_path,
            bytes: temp_bytes,
            ..
        }) = self.take_fault(|fault| {
            matches!(
                fault,
                Fault::GuardedWriteTempLeakAndFail { path: fault_path, .. }
                    if fault_path == &path.to_string()
            )
        }) {
            self.inner.write_file(temp_path, temp_bytes)?;
            self.add_stats(|stats| stats.guarded_write_conflict_count += 1);
            eyre::bail!("injected guarded write failure after temp creation for {path}");
        }

        if let Some(Fault::GuardedWriteConflictBeforeSwap { replacement, .. }) =
            self.take_fault(|fault| {
                matches!(
                    fault,
                    Fault::GuardedWriteConflictBeforeSwap { path: fault_path, .. }
                        if fault_path == &path.to_string()
                )
            })
        {
            self.inner
                .replace_file_contents(path.to_string(), replacement)?;
            let observed = self.inner.stat(path.clone()).await?;
            let swap_path = Self::temp_path_for(&path)?;
            self.add_stats(|stats| stats.guarded_write_conflict_before_swap_count += 1);
            return Ok(GuardedWriteResult::ConflictBeforeSwap {
                swap_path,
                observed,
            });
        }

        let result = self
            .inner
            .guarded_write(path.clone(), bytes, expected_before)
            .await?;
        if !matches!(
            result,
            GuardedWriteResult::Written { .. } | GuardedWriteResult::AlreadyApplied { .. }
        ) {
            return Ok(result);
        }

        if let Some(Fault::GuardedWriteConflictAfterSwap { replacement, .. }) =
            self.take_fault(|fault| {
                matches!(
                    fault,
                    Fault::GuardedWriteConflictAfterSwap { path: fault_path, .. }
                        if fault_path == &path.to_string()
                )
            })
        {
            self.inner
                .replace_file_contents(path.to_string(), replacement)?;
            let observed = self.inner.stat(path).await?;
            self.add_stats(|stats| stats.guarded_write_conflict_after_swap_count += 1);
            return Ok(GuardedWriteResult::ConflictAfterSwap { observed });
        }

        Ok(result)
    }

    async fn guarded_delete(
        &self,
        path: RelativePath,
        expected_before: ExpectedBefore,
    ) -> eyre::Result<GuardedDeleteResult> {
        self.inner.guarded_delete(path, expected_before).await
    }

    async fn delete_if_exists(&self, path: RelativePath) -> eyre::Result<DeleteIfExistsResult> {
        if let Some(Fault::DeleteIfExistsFailure { .. }) = self.take_fault(|fault| {
            matches!(
                fault,
                Fault::DeleteIfExistsFailure { path: fault_path } if fault_path == &path.to_string()
            )
        }) {
            eyre::bail!("injected cleanup delete failure for {path}");
        }

        self.inner.delete_if_exists(path).await
    }
}

fn validate_path(path: impl AsRef<str>) -> eyre::Result<String> {
    let path = RelativePath::parse(path.as_ref())?;
    Ok(path.to_string())
}
