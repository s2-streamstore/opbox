use crate::engine::actor::EngineCommand;
use crate::notify::nio::NotifyIO;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::debug;

pub struct NotifyActor<IO> {
    io: IO,
    engine_tx: mpsc::UnboundedSender<EngineCommand>,
}

impl<IO> NotifyActor<IO>
where
    IO: NotifyIO,
{
    pub fn new(io: IO, engine_tx: mpsc::UnboundedSender<EngineCommand>) -> Self {
        Self { io, engine_tx }
    }

    pub async fn run(mut self, token: CancellationToken) -> eyre::Result<()> {
        loop {
            tokio::select! {
                _ = token.cancelled() => {
                    debug!("cancelled");
                    return Ok(());
                }
                batch = self.io.next() => {
                    let batch = batch?;
                    if batch.is_empty() {
                        continue;
                    }
                    for scope in batch.scopes {
                        self.engine_tx.send(EngineCommand::Scan(scope))?;
                    }
                }
            }
        }
    }
}
