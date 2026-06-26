use crate::app::workspace::{real_socket_path, remove_stale_socket_files, socket_link_path};
use crate::engine::actor::EngineCommand;
use crate::semantic::table::daemon_state;
use crate::spy::{SpyEvent, SpyOpen};
use bytes::Bytes;
use eyre::WrapErr;
use futures::{StreamExt, stream};
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Empty, Full, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::client::conn::http1 as client_http1;
use hyper::server::conn::http1 as server_http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

type IpcBody = UnsyncBoxBody<Bytes, Infallible>;

pub use crate::app::control::{DaemonStatus, EnginePhaseStatus};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StopResponse {
    pub workspace_id: String,
    pub pid: u32,
}

#[derive(Debug, Clone)]
pub struct ControlServerConfig {
    pub sync_root: PathBuf,
    pub daemon_state: daemon_state::Row,
    pub engine_tx: mpsc::UnboundedSender<EngineCommand>,
}

pub async fn request_status(sync_root: &Path) -> eyre::Result<DaemonStatus> {
    request_json(sync_root, Method::GET, "/status").await
}

pub async fn request_stop(sync_root: &Path) -> eyre::Result<StopResponse> {
    request_json(sync_root, Method::POST, "/stop").await
}

pub async fn open_spy_stream(sync_root: &Path) -> eyre::Result<SpyStream> {
    let request = Request::builder()
        .method(Method::GET)
        .uri("/spy")
        .header(hyper::header::HOST, "opbox")
        .header(hyper::header::ACCEPT, "text/event-stream")
        .body(Empty::<Bytes>::new())?;
    let (response, connection_task) = send_request(sync_root, request).await?;
    if response.status() != StatusCode::OK {
        eyre::bail!("daemon returned non-200 status: {}", response.status());
    }
    Ok(SpyStream {
        body: response.into_body(),
        buffer: Vec::new(),
        connection_task,
    })
}

pub struct SpyStream {
    body: Incoming,
    buffer: Vec<u8>,
    connection_task: JoinHandle<()>,
}

impl SpyStream {
    pub async fn next_event(&mut self) -> eyre::Result<Option<SpyEvent>> {
        loop {
            if let Some(event) = self.try_pop_event()? {
                return Ok(Some(event));
            }

            match self.body.frame().await {
                Some(Ok(frame)) => {
                    if let Ok(data) = frame.into_data() {
                        self.buffer.extend_from_slice(data.as_ref());
                    }
                }
                Some(Err(error)) => return Err(error.into()),
                None => {
                    if self.buffer.iter().all(|byte| byte.is_ascii_whitespace()) {
                        return Ok(None);
                    }
                    eyre::bail!("daemon spy stream closed with partial event");
                }
            }
        }
    }

    fn try_pop_event(&mut self) -> eyre::Result<Option<SpyEvent>> {
        let Some(event_end) = self.buffer.windows(2).position(|window| window == b"\n\n") else {
            return Ok(None);
        };
        let frame = self.buffer.drain(..event_end + 2).collect::<Vec<_>>();
        let frame = std::str::from_utf8(&frame)?;
        let data = frame
            .lines()
            .filter_map(|line| {
                let line = line.strip_suffix('\r').unwrap_or(line);
                line.strip_prefix("data:").map(str::trim_start)
            })
            .collect::<Vec<_>>()
            .join("\n");
        if data.is_empty() {
            return Ok(None);
        }
        Ok(Some(serde_json::from_str(&data)?))
    }
}

impl Drop for SpyStream {
    fn drop(&mut self) {
        self.connection_task.abort();
    }
}

async fn request_json<T>(sync_root: &Path, method: Method, uri: &str) -> eyre::Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header(hyper::header::HOST, "opbox")
        .body(Empty::<Bytes>::new())?;
    let (response, connection_task) = send_request(sync_root, request).await?;
    if response.status() != StatusCode::OK {
        eyre::bail!("daemon returned non-200 status: {}", response.status());
    }
    let body = response.into_body().collect().await?.to_bytes();
    connection_task.abort();
    Ok(serde_json::from_slice(&body)?)
}

async fn send_request(
    sync_root: &Path,
    request: Request<Empty<Bytes>>,
) -> eyre::Result<(Response<Incoming>, JoinHandle<()>)> {
    let socket_path = resolve_socket_path(sync_root)?;
    let stream = match UnixStream::connect(&socket_path).await {
        Ok(stream) => stream,
        Err(error) => {
            if matches!(
                error.kind(),
                ErrorKind::NotFound | ErrorKind::ConnectionRefused
            ) {
                let _ = remove_stale_socket_files(sync_root);
            }
            return Err(error).wrap_err_with(|| {
                format!("connect to daemon control socket {}", socket_path.display())
            });
        }
    };
    let io = TokioIo::new(stream);
    let (mut sender, connection) = client_http1::handshake(io)
        .await
        .wrap_err("handshake with daemon control socket")?;
    let connection_task = tokio::spawn(async move {
        if let Err(error) = connection.await {
            debug!(?error, "ipc client connection failed");
        }
    });
    let response = match sender.send_request(request).await {
        Ok(response) => response,
        Err(error) => {
            connection_task.abort();
            return Err(error).wrap_err("send IPC request to daemon");
        }
    };
    Ok((response, connection_task))
}

fn resolve_socket_path(sync_root: &Path) -> eyre::Result<PathBuf> {
    let link_path = socket_link_path(sync_root);
    match std::fs::read_link(&link_path) {
        Ok(path) => Ok(path),
        Err(error) if error.kind() == std::io::ErrorKind::InvalidInput => Ok(link_path),
        Err(error) => Err(error.into()),
    }
}

pub async fn serve_control(
    config: ControlServerConfig,
    token: CancellationToken,
) -> eyre::Result<()> {
    let socket_path = real_socket_path(
        &config.daemon_state.workspace_id,
        &config.daemon_state.daemon_writer_id,
    );
    remove_stale_socket(&socket_path)?;
    let listener = UnixListener::bind(&socket_path)?;
    replace_socket_symlink(&socket_path, &socket_link_path(&config.sync_root))?;
    let _guard = SocketGuard {
        socket_path,
        link_path: socket_link_path(&config.sync_root),
    };

    loop {
        tokio::select! {
            () = token.cancelled() => {
                debug!("status server cancelled");
                return Ok(());
            }
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let config = config.clone();
                let token = token.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |request| {
                        handle_control_request(
                            request,
                            config.clone(),
                            token.clone(),
                        )
                    });
                    if let Err(error) = server_http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await
                    {
                        warn!(?error, "control connection failed");
                    }
                });
            }
        }
    }
}

async fn handle_control_request(
    request: Request<Incoming>,
    config: ControlServerConfig,
    token: CancellationToken,
) -> Result<Response<IpcBody>, Infallible> {
    let response = match (request.method(), request.uri().path()) {
        (&Method::GET, "/status") => match request_engine_status(&config).await {
            Ok(status) => json_response(StatusCode::OK, &status),
            Err(error) => {
                warn!(?error, "status request failed");
                text_response(StatusCode::INTERNAL_SERVER_ERROR, "internal server error")
            }
        },
        (&Method::GET, "/spy") => match request_engine_spy(&config).await {
            Ok(open) => Ok(spy_response(open, token)),
            Err(error) => {
                warn!(?error, "spy request failed");
                text_response(StatusCode::INTERNAL_SERVER_ERROR, "internal server error")
            }
        },
        (&Method::POST, "/stop") => match request_engine_stop(&config).await {
            Ok(status) => json_response(
                StatusCode::OK,
                &StopResponse {
                    workspace_id: status.workspace_id,
                    pid: status.pid,
                },
            ),
            Err(error) => {
                warn!(?error, "stop request failed");
                text_response(StatusCode::INTERNAL_SERVER_ERROR, "internal server error")
            }
        },
        _ => text_response(StatusCode::NOT_FOUND, "not found"),
    };

    match response {
        Ok(response) => Ok(response),
        Err(error) => {
            warn!(?error, "control response serialization failed");
            Ok(
                text_response(StatusCode::INTERNAL_SERVER_ERROR, "internal server error")
                    .expect("static error response is valid"),
            )
        }
    }
}

async fn request_engine_status(config: &ControlServerConfig) -> eyre::Result<DaemonStatus> {
    let (reply, rx) = oneshot::channel();
    config.engine_tx.send(EngineCommand::Status { reply })?;
    Ok(rx.await?)
}

async fn request_engine_spy(config: &ControlServerConfig) -> eyre::Result<SpyOpen> {
    let (reply, rx) = oneshot::channel();
    config.engine_tx.send(EngineCommand::OpenSpy { reply })?;
    match rx.await? {
        Ok(open) => Ok(open),
        Err(error) => eyre::bail!(error),
    }
}

async fn request_engine_stop(config: &ControlServerConfig) -> eyre::Result<DaemonStatus> {
    let (reply, rx) = oneshot::channel();
    config.engine_tx.send(EngineCommand::Stop { reply })?;
    Ok(rx.await?)
}

fn spy_response(open: SpyOpen, token: CancellationToken) -> Response<IpcBody> {
    let session_started = SpyEvent::SessionStarted {
        daemon_writer_id_b64: open.daemon_writer_id_b64,
    };
    let snapshot = SpyEvent::NamespaceSnapshot {
        yjs_state_b64: open.namespace_snapshot_b64,
    };
    let session_frame =
        spy_sse_frame(&session_started).expect("session-started spy event is serializable");
    let snapshot_frame =
        spy_sse_frame(&snapshot).expect("namespace snapshot spy event is serializable");
    let startup_stream = stream::iter([Ok::<_, Infallible>(session_frame), Ok(snapshot_frame)]);

    let live_stream = stream::unfold((open.events, token), |(mut rx, token)| async move {
        let cancel_token = token.clone();
        let event = tokio::select! {
            () = cancel_token.cancelled() => return None,
            event = rx.recv() => event,
        };

        let event = match event {
            Ok(event) => event,
            Err(broadcast::error::RecvError::Lagged(skipped)) => SpyEvent::Lagged { skipped },
            Err(broadcast::error::RecvError::Closed) => return None,
        };
        let frame = match spy_sse_frame(&event) {
            Ok(frame) => frame,
            Err(error) => {
                warn!(?error, "failed to serialize spy event");
                return None;
            }
        };
        Some((Ok::<_, Infallible>(frame), (rx, token)))
    });
    let event_stream = startup_stream.chain(live_stream);

    Response::builder()
        .status(StatusCode::OK)
        .header(hyper::header::CONTENT_TYPE, "text/event-stream")
        .header(hyper::header::CACHE_CONTROL, "no-cache")
        .body(StreamBody::new(event_stream).boxed_unsync())
        .expect("spy response is valid")
}

fn spy_sse_frame(event: &SpyEvent) -> Result<Frame<Bytes>, serde_json::Error> {
    serde_json::to_string(event).map(|json| Frame::data(Bytes::from(format!("data: {json}\n\n"))))
}

fn json_response<T>(status: StatusCode, body: &T) -> eyre::Result<Response<IpcBody>>
where
    T: Serialize,
{
    Ok(response(
        status,
        "application/json",
        Bytes::from(serde_json::to_vec(body)?),
    ))
}

fn text_response(status: StatusCode, body: &'static str) -> eyre::Result<Response<IpcBody>> {
    Ok(response(
        status,
        "text/plain; charset=utf-8",
        Bytes::from_static(body.as_bytes()),
    ))
}

fn response(status: StatusCode, content_type: &'static str, body: Bytes) -> Response<IpcBody> {
    Response::builder()
        .status(status)
        .header(hyper::header::CONTENT_TYPE, content_type)
        .body(Full::new(body).boxed_unsync())
        .expect("control response is valid")
}

fn remove_stale_socket(socket_path: &Path) -> eyre::Result<()> {
    match std::fs::remove_file(socket_path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(unix)]
fn replace_socket_symlink(socket_path: &Path, link_path: &Path) -> eyre::Result<()> {
    match std::fs::remove_file(link_path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    std::os::unix::fs::symlink(socket_path, link_path)?;
    Ok(())
}

struct SocketGuard {
    socket_path: PathBuf,
    link_path: PathBuf,
}

impl Drop for SocketGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.socket_path);
        if std::fs::read_link(&self.link_path).ok().as_ref() == Some(&self.socket_path) {
            let _ = std::fs::remove_file(&self.link_path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::connectivity::ConnectivitySnapshot;
    use crate::app::control::EnginePhaseStatus;
    use crate::app::workspace::metadata_dir;
    use crate::engine::actor::EngineCommand;
    use crate::types::{DaemonWriterId, OutboxId, WorkspaceId};
    use std::time::Duration;

    #[tokio::test]
    async fn status_round_trips_through_engine_mailbox() -> eyre::Result<()> {
        let sync_root =
            std::env::temp_dir().join(format!("opbox-ipc-test-{}", rand::random::<u64>()));
        std::fs::create_dir(&sync_root)?;
        std::fs::create_dir(metadata_dir(&sync_root))?;
        let writer_bytes = rand::random::<[u8; 16]>();

        let daemon_row = daemon_state::Row {
            workspace_id: WorkspaceId("0123456789abcdefghijklmnopqrstuv".to_string()),
            s2_basin: "test-basin".parse()?,
            s2_account_endpoint: None,
            s2_basin_endpoint: None,
            daemon_writer_id: DaemonWriterId(Bytes::copy_from_slice(&writer_bytes)),
            stable_cursor: ..0,
            next_outbox_id: OutboxId::new(0),
        };

        let (engine_tx, mut engine_rx) = mpsc::unbounded_channel();
        let expected_status = DaemonStatus {
            workspace_id: daemon_row.workspace_id.0.clone(),
            root: sync_root.display().to_string(),
            pid: 123,
            stable_cursor_end: 42,
            daemon_writer_id_b64: daemon_row.daemon_writer_id.encode_b64(),
            started_at_ns: 0,
            engine_phase: EnginePhaseStatus::Scanning,
            connectivity: ConnectivitySnapshot::starting(),
        };
        let engine = tokio::spawn({
            let expected_status = expected_status.clone();
            async move {
                match engine_rx.recv().await {
                    Some(EngineCommand::Status { reply }) => {
                        let _ = reply.send(expected_status);
                    }
                    Some(EngineCommand::Scan(_)) => panic!("unexpected scan command"),
                    Some(EngineCommand::OpenSpy { .. }) => panic!("unexpected open-spy command"),
                    Some(EngineCommand::Stop { .. }) => panic!("unexpected stop command"),
                    None => panic!("engine command channel closed"),
                }
            }
        });
        let token = CancellationToken::new();
        let server = tokio::spawn({
            let token = token.clone();
            let config = ControlServerConfig {
                sync_root: sync_root.clone(),
                daemon_state: daemon_row,
                engine_tx,
            };
            async move { serve_control(config, token).await }
        });

        wait_for_socket(&sync_root).await?;

        let status = request_status(&sync_root).await?;
        assert_eq!(status.workspace_id, expected_status.workspace_id);
        assert_eq!(status.root, expected_status.root);
        assert_eq!(status.pid, expected_status.pid);
        assert_eq!(status.stable_cursor_end, expected_status.stable_cursor_end);
        assert_eq!(status.engine_phase, expected_status.engine_phase);
        assert_eq!(status.connectivity, expected_status.connectivity);

        token.cancel();
        server.await??;
        engine.await?;
        let _ = std::fs::remove_dir_all(&sync_root);
        Ok(())
    }

    #[tokio::test]
    async fn spy_stream_opens_through_engine_mailbox() -> eyre::Result<()> {
        let sync_root =
            std::env::temp_dir().join(format!("opbox-ipc-test-{}", rand::random::<u64>()));
        std::fs::create_dir(&sync_root)?;
        std::fs::create_dir(metadata_dir(&sync_root))?;
        let writer_bytes = rand::random::<[u8; 16]>();

        let daemon_row = daemon_state::Row {
            workspace_id: WorkspaceId("0123456789abcdefghijklmnopqrstuv".to_string()),
            s2_basin: "test-basin".parse()?,
            s2_account_endpoint: None,
            s2_basin_endpoint: None,
            daemon_writer_id: DaemonWriterId(Bytes::copy_from_slice(&writer_bytes)),
            stable_cursor: ..0,
            next_outbox_id: OutboxId::new(0),
        };

        let (spy_tx, _) = broadcast::channel(8);
        let spy_tx_for_test = spy_tx.clone();
        let (engine_tx, mut engine_rx) = mpsc::unbounded_channel();
        let daemon_writer_id_b64 = daemon_row.daemon_writer_id.encode_b64();
        let expected_daemon_writer_id_b64 = daemon_writer_id_b64.clone();
        let engine = tokio::spawn(async move {
            match engine_rx.recv().await {
                Some(EngineCommand::OpenSpy { reply }) => {
                    let _ = reply.send(Ok(SpyOpen {
                        daemon_writer_id_b64: daemon_writer_id_b64,
                        namespace_snapshot_b64: "dGVzdA==".to_string(),
                        events: spy_tx.subscribe(),
                    }));
                }
                Some(EngineCommand::Status { .. }) => panic!("unexpected status command"),
                Some(EngineCommand::Scan(_)) => panic!("unexpected scan command"),
                Some(EngineCommand::Stop { .. }) => panic!("unexpected stop command"),
                None => panic!("engine command channel closed"),
            }
        });
        let token = CancellationToken::new();
        let server = tokio::spawn({
            let token = token.clone();
            let config = ControlServerConfig {
                sync_root: sync_root.clone(),
                daemon_state: daemon_row,
                engine_tx,
            };
            async move { serve_control(config, token).await }
        });

        wait_for_socket(&sync_root).await?;

        let mut stream = open_spy_stream(&sync_root).await?;
        let event = stream.next_event().await?;
        assert!(matches!(
            event,
            Some(SpyEvent::SessionStarted {
                daemon_writer_id_b64
            }) if daemon_writer_id_b64 == expected_daemon_writer_id_b64
        ));
        let event = stream.next_event().await?;
        assert!(matches!(
            event,
            Some(SpyEvent::NamespaceSnapshot { yjs_state_b64 }) if yjs_state_b64 == "dGVzdA=="
        ));
        spy_tx_for_test.send(SpyEvent::Lagged { skipped: 9 })?;
        let event = stream.next_event().await?;
        assert!(matches!(event, Some(SpyEvent::Lagged { skipped: 9 })));

        token.cancel();
        drop(stream);
        server.await??;
        engine.await?;
        let _ = std::fs::remove_dir_all(&sync_root);
        Ok(())
    }

    #[tokio::test]
    async fn stop_round_trips_through_engine_mailbox() -> eyre::Result<()> {
        let sync_root =
            std::env::temp_dir().join(format!("opbox-ipc-test-{}", rand::random::<u64>()));
        std::fs::create_dir(&sync_root)?;
        std::fs::create_dir(metadata_dir(&sync_root))?;
        let writer_bytes = rand::random::<[u8; 16]>();

        let daemon_row = daemon_state::Row {
            workspace_id: WorkspaceId("0123456789abcdefghijklmnopqrstuv".to_string()),
            s2_basin: "test-basin".parse()?,
            s2_account_endpoint: None,
            s2_basin_endpoint: None,
            daemon_writer_id: DaemonWriterId(Bytes::copy_from_slice(&writer_bytes)),
            stable_cursor: ..0,
            next_outbox_id: OutboxId::new(0),
        };

        let (engine_tx, mut engine_rx) = mpsc::unbounded_channel();
        let expected_status = DaemonStatus {
            workspace_id: daemon_row.workspace_id.0.clone(),
            root: sync_root.display().to_string(),
            pid: 123,
            stable_cursor_end: 42,
            daemon_writer_id_b64: daemon_row.daemon_writer_id.encode_b64(),
            started_at_ns: 0,
            engine_phase: EnginePhaseStatus::Scanning,
            connectivity: ConnectivitySnapshot::starting(),
        };
        let engine = tokio::spawn({
            let expected_status = expected_status.clone();
            async move {
                match engine_rx.recv().await {
                    Some(EngineCommand::Stop { reply }) => {
                        let _ = reply.send(expected_status);
                    }
                    Some(EngineCommand::Status { .. }) => panic!("unexpected status command"),
                    Some(EngineCommand::Scan(_)) => panic!("unexpected scan command"),
                    Some(EngineCommand::OpenSpy { .. }) => panic!("unexpected open-spy command"),
                    None => panic!("engine command channel closed"),
                }
            }
        });
        let token = CancellationToken::new();
        let server = tokio::spawn({
            let token = token.clone();
            let config = ControlServerConfig {
                sync_root: sync_root.clone(),
                daemon_state: daemon_row,
                engine_tx,
            };
            async move { serve_control(config, token).await }
        });

        wait_for_socket(&sync_root).await?;

        let response = request_stop(&sync_root).await?;
        assert_eq!(response.workspace_id, expected_status.workspace_id);
        assert_eq!(response.pid, expected_status.pid);

        token.cancel();
        server.await??;
        engine.await?;
        let _ = std::fs::remove_dir_all(&sync_root);
        Ok(())
    }

    #[tokio::test]
    async fn ipc_request_error_after_connect_does_not_unlink_socket() -> eyre::Result<()> {
        let sync_root =
            std::env::temp_dir().join(format!("opbox-ipc-test-{}", rand::random::<u64>()));
        std::fs::create_dir(&sync_root)?;
        std::fs::create_dir(metadata_dir(&sync_root))?;
        let socket_path =
            std::env::temp_dir().join(format!("opbox-ipc-test-{}.sock", rand::random::<u64>()));
        let listener = UnixListener::bind(&socket_path)?;
        std::os::unix::fs::symlink(&socket_path, socket_link_path(&sync_root))?;

        let server = tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                drop(stream);
            }
        });

        let error = request_status(&sync_root)
            .await
            .expect_err("server drops connection before response");
        assert!(
            error.to_string().contains("send IPC request")
                || error
                    .to_string()
                    .contains("handshake with daemon control socket"),
            "unexpected error: {error:?}"
        );
        assert!(socket_link_path(&sync_root).exists());
        assert!(socket_path.exists());

        server.await?;
        let _ = std::fs::remove_file(&socket_path);
        let _ = std::fs::remove_dir_all(&sync_root);
        Ok(())
    }

    async fn wait_for_socket(sync_root: &Path) -> eyre::Result<()> {
        let link_path = socket_link_path(sync_root);
        for _ in 0..100 {
            if link_path.exists() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        eyre::bail!("timed out waiting for control socket")
    }
}
