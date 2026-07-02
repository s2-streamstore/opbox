use crate::engine::actor::{
    Engine, EngineClients, EngineCommand, EngineConfig, EngineEvents, EngineStatusConfig,
};
use crate::engine::clone as engine_clone;
use crate::engine::init as engine_init;
use crate::fs::actor::FsActor;
use crate::fs::client::FsClient;
use crate::fs::fio::FileIO;
use crate::log::encrypt::NonceRng;
use crate::log::reader::LogReaderActor;
use crate::log::types::{LOG_READER_EVENT_CHANNEL_CAPACITY, LogReadStop};
use crate::log::writer::LogWriterActor;
use crate::notify::actor::NotifyActor;
use crate::notify::nio::NotifyIO;
use crate::semantic::actor::SemanticActor;
use crate::semantic::client::SemanticClient;
use crate::semantic::service::SemanticService;
use crate::semantic::table::daemon_state;
use crate::spy::SpyEvent;
use s2_sdk::S2Basin;
use tokio::sync::{broadcast, mpsc};
use tokio::task::{JoinError, JoinSet};
use tokio::time::{Duration, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

pub type ActorResult = (&'static str, eyre::Result<()>);

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunMode {
    Init,
    Clone,
    Sync,
}

pub struct AppRuntimeConfig<IO, NIO = ()> {
    pub mode: RunMode,
    pub file_io: IO,
    pub notify_io: Option<NIO>,
    pub semantic_service: SemanticService,
    pub daemon_row: daemon_state::Row,
    pub s2_basin: S2Basin,
    pub nonce_rng: NonceRng,
    pub clone_log_read_stop: Option<LogReadStop>,
    pub clone_clobber: bool,
    pub engine_status: Option<EngineStatusConfig>,
    pub spy_tx: Option<broadcast::Sender<SpyEvent>>,
}

pub struct AppRuntime<IO, NIO = ()> {
    config: AppRuntimeConfig<IO, NIO>,
}

impl<IO, NIO> AppRuntime<IO, NIO>
where
    IO: FileIO + Clone + Send + 'static,
    NIO: NotifyIO,
{
    pub fn new(config: AppRuntimeConfig<IO, NIO>) -> Self {
        Self { config }
    }

    pub fn spawn(self, cancellation_token: CancellationToken) -> AppActors {
        let AppRuntimeConfig {
            mode,
            file_io,
            notify_io,
            semantic_service,
            daemon_row,
            s2_basin,
            nonce_rng,
            clone_log_read_stop,
            clone_clobber,
            engine_status,
            spy_tx,
        } = self.config;

        let (semantic_request_tx, semantic_request_rx) = mpsc::unbounded_channel();
        let (semantic_event_tx, semantic_event_rx) = mpsc::unbounded_channel();
        let semantic_actor =
            SemanticActor::new(semantic_request_rx, semantic_event_tx, semantic_service);

        let semantic_client = SemanticClient::new(semantic_request_tx);
        let (fs_request_tx, fs_request_rx) = mpsc::unbounded_channel();
        let fs_client = FsClient::new(fs_request_tx);
        let fs_actor = FsActor::new(file_io, fs_request_rx);

        let mut actors = JoinSet::<ActorResult>::new();

        actors.spawn({
            let token = cancellation_token.clone();
            async move { ("semantic", semantic_actor.run(token).await) }
        });
        actors.spawn({
            let token = cancellation_token.clone();
            async move { ("fs", fs_actor.run(token).await) }
        });
        let mut sync_engine_command_tx = None;

        match mode {
            RunMode::Init => {
                let (log_writer_req_tx, log_writer_req_rx) = mpsc::unbounded_channel();
                let (log_writer_resp_tx, log_writer_resp_rx) = mpsc::unbounded_channel();
                let log_writer = LogWriterActor::new(
                    s2_basin.clone(),
                    daemon_row.workspace_id.clone(),
                    daemon_row.daemon_writer_id.clone(),
                    daemon_row.encryption_key.clone(),
                    nonce_rng,
                    log_writer_req_rx,
                    log_writer_resp_tx,
                );

                actors.spawn({
                    let token = cancellation_token.clone();
                    async move { ("log_writer", log_writer.run(token).await) }
                });
                actors.spawn(async move {
                    let _semantic_event_rx = semantic_event_rx;
                    let result = engine_init::run(engine_init::InitConfig {
                        clients: engine_init::InitClients {
                            fs: fs_client,
                            semantic: semantic_client,
                            log_writer: log_writer_req_tx,
                        },
                        events: engine_init::InitEvents {
                            log_writer: log_writer_resp_rx,
                        },
                    })
                    .await
                    .map(|result| {
                        info!(?result, "init completed");
                    });

                    ("init", result)
                });
            }
            RunMode::Clone => {
                let (log_reader_req_tx, log_reader_req_rx) = mpsc::unbounded_channel();
                let (log_reader_resp_tx, log_reader_resp_rx) =
                    mpsc::channel(LOG_READER_EVENT_CHANNEL_CAPACITY);
                let log_reader = LogReaderActor::new(
                    s2_basin.clone(),
                    daemon_row.workspace_id.clone(),
                    daemon_row.encryption_key.clone(),
                    daemon_row.stable_cursor.end,
                    clone_log_read_stop,
                    log_reader_req_rx,
                    log_reader_resp_tx,
                );

                actors.spawn({
                    let token = cancellation_token.clone();
                    async move { ("log_reader", log_reader.run(token).await) }
                });
                actors.spawn(async move {
                    let _semantic_event_rx = semantic_event_rx;
                    let result = engine_clone::run(engine_clone::CloneConfig {
                        clients: engine_clone::CloneClients {
                            fs: fs_client,
                            semantic: semantic_client,
                            log_reader: log_reader_req_tx,
                        },
                        events: engine_clone::CloneEvents {
                            log_reader: log_reader_resp_rx,
                        },
                        log_read_stop: clone_log_read_stop,
                        clobber: clone_clobber,
                    })
                    .await
                    .map(|result| {
                        info!(?result, "clone completed");
                    });

                    ("clone", result)
                });
            }
            RunMode::Sync => {
                let (log_writer_req_tx, log_writer_req_rx) = mpsc::unbounded_channel();
                let (log_writer_resp_tx, log_writer_resp_rx) = mpsc::unbounded_channel();
                let log_writer = LogWriterActor::new(
                    s2_basin.clone(),
                    daemon_row.workspace_id.clone(),
                    daemon_row.daemon_writer_id.clone(),
                    daemon_row.encryption_key.clone(),
                    nonce_rng,
                    log_writer_req_rx,
                    log_writer_resp_tx,
                );

                let (log_reader_req_tx, log_reader_req_rx) = mpsc::unbounded_channel();
                let (log_reader_resp_tx, log_reader_resp_rx) =
                    mpsc::channel(LOG_READER_EVENT_CHANNEL_CAPACITY);
                let (engine_command_tx, engine_command_rx) = mpsc::unbounded_channel();
                let sync_engine_status =
                    engine_status.expect("sync mode requires engine status config");
                let log_reader = LogReaderActor::new(
                    s2_basin.clone(),
                    daemon_row.workspace_id.clone(),
                    daemon_row.encryption_key.clone(),
                    daemon_row.stable_cursor.end,
                    None,
                    log_reader_req_rx,
                    log_reader_resp_tx,
                );

                let engine_clients = EngineClients {
                    fs: fs_client,
                    semantic: semantic_client,
                    log_reader: log_reader_req_tx,
                    log_writer: log_writer_req_tx,
                };

                let engine_events = EngineEvents {
                    log_reader: log_reader_resp_rx,
                    log_writer: log_writer_resp_rx,
                    semantic: semantic_event_rx,
                    commands: engine_command_rx,
                    spy: spy_tx,
                };

                let engine = Engine::new(EngineConfig {
                    clients: engine_clients,
                    events: engine_events,
                    status: sync_engine_status,
                });

                sync_engine_command_tx = Some(engine_command_tx.clone());

                actors.spawn({
                    let token = cancellation_token.clone();
                    async move { ("engine", engine.run(token).await) }
                });
                actors.spawn({
                    let token = cancellation_token.clone();
                    async move { ("log_reader", log_reader.run(token).await) }
                });
                actors.spawn({
                    let token = cancellation_token.clone();
                    async move { ("log_writer", log_writer.run(token).await) }
                });
                if let Some(notify_io) = notify_io {
                    let notify_actor = NotifyActor::new(notify_io, engine_command_tx.clone());
                    actors.spawn({
                        let token = cancellation_token.clone();
                        async move { ("notify", notify_actor.run(token).await) }
                    });
                }
            }
        }

        AppActors {
            actors,
            engine_command_tx: sync_engine_command_tx,
        }
    }

    pub async fn run_until_shutdown(self) -> eyre::Result<()> {
        let cancellation_token = CancellationToken::new();
        let mut actors = self.spawn(cancellation_token.clone());
        actors
            .wait_for_ctrl_c_or_actor_stop(cancellation_token)
            .await
    }
}

pub struct AppActors {
    actors: JoinSet<ActorResult>,
    engine_command_tx: Option<mpsc::UnboundedSender<EngineCommand>>,
}

impl AppActors {
    pub fn engine_command_tx(&self) -> Option<mpsc::UnboundedSender<EngineCommand>> {
        self.engine_command_tx.clone()
    }

    pub async fn wait_for_actor_stop(&mut self) -> Option<eyre::Report> {
        actor_stopped("initial", self.actors.join_next().await)
    }

    pub async fn wait_for_ctrl_c_or_actor_stop(
        &mut self,
        cancellation_token: CancellationToken,
    ) -> eyre::Result<()> {
        let mut interrupted = false;
        let mut shutdown_error = tokio::select! {
            ctrl_c = tokio::signal::ctrl_c() => {
                ctrl_c?;
                info!("ctrl-c received");
                interrupted = true;
                None
            }
            result = self.actors.join_next() => {
                actor_stopped("initial", result)
            }
        };

        cancellation_token.cancel();
        let shutdown_timed_out = self.drain_with_timeout(&mut shutdown_error).await;
        if shutdown_timed_out && shutdown_error.is_none() {
            shutdown_error = Some(eyre::eyre!("shutdown timed out"));
        }
        if interrupted && shutdown_error.is_none() {
            shutdown_error = Some(eyre::eyre!("operation interrupted"));
        }

        info!("exiting");

        match shutdown_error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    pub async fn shutdown(
        &mut self,
        cancellation_token: CancellationToken,
    ) -> Option<eyre::Report> {
        cancellation_token.cancel();
        let mut shutdown_error = None;
        let shutdown_timed_out = self.drain_with_timeout(&mut shutdown_error).await;
        if shutdown_timed_out && shutdown_error.is_none() {
            shutdown_error = Some(eyre::eyre!("shutdown timed out"));
        }
        shutdown_error
    }

    async fn drain_with_timeout(&mut self, shutdown_error: &mut Option<eyre::Report>) -> bool {
        let drain = async {
            while let Some(result) = self.actors.join_next().await {
                if shutdown_error.is_none() {
                    *shutdown_error = actor_stopped("shutdown", Some(result));
                } else {
                    let _ = actor_stopped("shutdown", Some(result));
                }
            }
        };

        if timeout(SHUTDOWN_TIMEOUT, drain).await.is_err() {
            warn!(
                timeout_ms = SHUTDOWN_TIMEOUT.as_millis(),
                "shutdown timed out; aborting actors"
            );
            self.actors.abort_all();
            true
        } else {
            false
        }
    }
}

fn actor_stopped(
    phase: &'static str,
    result: Option<Result<ActorResult, JoinError>>,
) -> Option<eyre::Report> {
    let Some(result) = result else {
        return Some(eyre::eyre!("all actors exited"));
    };

    match result {
        Ok((name, Ok(()))) => {
            info!(phase, name, "actor stopped");
            None
        }
        Ok((name, Err(err))) => {
            error!(phase, name, ?err, "actor failed");
            Some(err)
        }
        Err(err) => {
            error!(phase, ?err, "actor task join failed");
            Some(err.into())
        }
    }
}
