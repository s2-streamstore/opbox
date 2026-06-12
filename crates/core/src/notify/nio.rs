use crate::fs::ignore::{IgnoreRules, IGNORE_FILE_NAME};
use crate::fs::types::ScanScope;
use async_trait::async_trait;
use compact_str::CompactString;
use notify_debouncer_full::notify::RecursiveMode;
use notify_debouncer_full::{DebounceEventResult, RecommendedCache, new_debouncer};
use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::warn;

#[derive(Debug)]
pub struct NotifyEventBatch {
    pub scopes: Vec<ScanScope>,
}

impl NotifyEventBatch {
    pub fn new(scopes: Vec<ScanScope>) -> Self {
        Self { scopes }
    }

    pub fn is_empty(&self) -> bool {
        self.scopes.is_empty()
    }
}

#[async_trait]
pub trait NotifyIO: Send + 'static {
    async fn next(&mut self) -> eyre::Result<NotifyEventBatch>;
}

pub struct ChannelNotifyIO {
    rx: mpsc::UnboundedReceiver<NotifyEventBatch>,
}

#[derive(Clone)]
pub struct ChannelNotifyHandle {
    tx: mpsc::UnboundedSender<NotifyEventBatch>,
}

pub fn channel_notify_io() -> (ChannelNotifyIO, ChannelNotifyHandle) {
    let (tx, rx) = mpsc::unbounded_channel();
    (ChannelNotifyIO { rx }, ChannelNotifyHandle { tx })
}

impl ChannelNotifyHandle {
    pub fn send(&self, batch: NotifyEventBatch) -> eyre::Result<()> {
        self.tx.send(batch)?;
        Ok(())
    }

    pub fn send_scope(&self, scope: ScanScope) -> eyre::Result<()> {
        self.send(NotifyEventBatch::new(vec![scope]))
    }
}

#[async_trait]
impl NotifyIO for ChannelNotifyIO {
    async fn next(&mut self) -> eyre::Result<NotifyEventBatch> {
        self.rx
            .recv()
            .await
            .ok_or_else(|| eyre::eyre!("notify channel closed"))
    }
}

pub struct LocalNotifyIO {
    root: PathBuf,
    _debouncer: notify_debouncer_full::Debouncer<
        notify_debouncer_full::notify::RecommendedWatcher,
        RecommendedCache,
    >,
    rx: mpsc::UnboundedReceiver<DebounceEventResult>,
    ignore_rules: IgnoreRules,
}

impl LocalNotifyIO {
    pub fn new(root: impl Into<PathBuf>, debounce: Duration) -> eyre::Result<Self> {
        let root = std::fs::canonicalize(root.into())?;
        let (tx, rx) = mpsc::unbounded_channel();
        let mut debouncer = new_debouncer(debounce, None, move |result| {
            let _ = tx.send(result);
        })?;

        debouncer.watch(&root, RecursiveMode::Recursive)?;

        let ignore_rules = IgnoreRules::load(&root)?;
        Ok(Self {
            root,
            _debouncer: debouncer,
            rx,
            ignore_rules,
        })
    }

    fn scopes_for_result(&mut self, result: DebounceEventResult) -> NotifyEventBatch {
        match result {
            Ok(events) => {
                let ignore_file = self.root.join(IGNORE_FILE_NAME);
                let needs_reload = events.iter().any(|e| e.event.paths.contains(&ignore_file));
                if needs_reload {
                    match IgnoreRules::load(&self.root) {
                        Ok(rules) => self.ignore_rules = rules,
                        Err(error) => {
                            warn!(?error, "failed to reload ignore rules");
                        }
                    }
                }

                let mut scopes = BTreeSet::new();
                for event in events {
                    for path in event.event.paths {
                        match self.scope_for_path(&path) {
                            Ok(Some(scope)) => {
                                scopes.insert(scope);
                            }
                            Ok(None) => {}
                            Err(error) => {
                                warn!(?path, ?error, "failed to map notify path to scan scope");
                                scopes.insert(ScanScope::Full);
                            }
                        }
                    }
                }

                NotifyEventBatch::new(scopes.into_iter().collect())
            }
            Err(errors) => {
                for error in errors {
                    warn!(?error, "filesystem notify error");
                }
                NotifyEventBatch::new(vec![ScanScope::Full])
            }
        }
    }

    fn scope_for_path(&self, path: &Path) -> eyre::Result<Option<ScanScope>> {
        let absolute = if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.root.join(path)
        };
        let relative = absolute.strip_prefix(&self.root)?;

        if relative.as_os_str().is_empty() {
            return Ok(Some(ScanScope::Full));
        }

        let relative_path = relative_path_from_path(relative)?;
        if self.ignore_rules.is_ignored(&relative_path) {
            return Ok(None);
        }
        if absolute.is_file() {
            Ok(Some(ScanScope::SingleFile(relative_path)))
        } else {
            // Missing paths may be deletes, so scan the prior subtree rooted at the path.
            Ok(Some(ScanScope::Subtree(relative_path)))
        }
    }
}

#[async_trait]
impl NotifyIO for LocalNotifyIO {
    async fn next(&mut self) -> eyre::Result<NotifyEventBatch> {
        loop {
            let result = self
                .rx
                .recv()
                .await
                .ok_or_else(|| eyre::eyre!("notify watcher channel closed"))?;
            let batch = self.scopes_for_result(result);
            if !batch.is_empty() {
                return Ok(batch);
            }
        }
    }
}

fn relative_path_from_path(path: &Path) -> eyre::Result<crate::fs::types::RelativePath> {
    let mut components = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(value) => {
                let Some(value) = value.to_str() else {
                    eyre::bail!("notify path contains non-utf-8 component: {path:?}");
                };
                components.push(CompactString::from(value));
            }
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                eyre::bail!("notify path is not a normalized relative path: {path:?}");
            }
        }
    }

    crate::fs::types::RelativePath::new(components)
}

#[async_trait]
impl NotifyIO for () {
    async fn next(&mut self) -> eyre::Result<NotifyEventBatch> {
        std::future::pending::<()>().await;
        unreachable!("pending future never resolves")
    }
}
