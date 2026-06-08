use crate::fs::types::{
    DeleteIfExistsResult, ExpectedBefore, FileFingerprint, GuardedDeleteResult, GuardedReadResult,
    GuardedWriteResult, RelativePath, ScanResult, ScanScope, TreeEntry,
};
use async_trait::async_trait;
use bytes::Bytes;

#[async_trait]
pub trait FileIO {
    async fn scan(&self, scope: ScanScope) -> eyre::Result<ScanResult>;
    async fn stat(&self, path: RelativePath) -> eyre::Result<Option<TreeEntry>>;
    async fn guarded_read(
        &self,
        path: RelativePath,
        expected: FileFingerprint,
    ) -> eyre::Result<GuardedReadResult>;
    async fn guarded_write(
        &self,
        path: RelativePath,
        bytes: Bytes,
        expected_before: ExpectedBefore,
    ) -> eyre::Result<GuardedWriteResult>;
    async fn guarded_delete(
        &self,
        path: RelativePath,
        expected_before: ExpectedBefore,
    ) -> eyre::Result<GuardedDeleteResult>;
    async fn delete_if_exists(&self, path: RelativePath) -> eyre::Result<DeleteIfExistsResult>;
}

pub mod local;
