use crate::app::db::load_daemon_state;
use crate::app::workspace::{real_socket_path, remove_stale_socket_files, socket_link_path};
use crate::semantic::table::daemon_state;
use crate::spy::SpyEvent;
use bytes::Bytes;
use futures::stream;
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
use std::path::{Path, PathBuf};
use std::time::Duration;
use time::OffsetDateTime;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

type IpcBody = UnsyncBoxBody<Bytes, Infallible>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonStatus {
    pub workspace_id: String,
    pub root: String,
    pub pid: u32,
    pub stable_cursor_end: u64,
    pub daemon_writer_id_b64: String,
    pub started_at_ns: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StopResponse {
    pub workspace_id: String,
    pub pid: u32,
}

#[derive(Debug, Clone)]
pub struct ControlServerConfig {
    pub sync_root: PathBuf,
    pub db_path: PathBuf,
    pub daemon_state: daemon_state::Row,
    pub started_at: OffsetDateTime,
    pub spy_tx: broadcast::Sender<SpyEvent>,
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
            let _ = remove_stale_socket_files(sync_root);
            return Err(error.into());
        }
    };
    let io = TokioIo::new(stream);
    let (mut sender, connection) = match client_http1::handshake(io).await {
        Ok(parts) => parts,
        Err(error) => {
            let _ = remove_stale_socket_files(sync_root);
            return Err(error.into());
        }
    };
    let connection_task = tokio::spawn(async move {
        if let Err(error) = connection.await {
            debug!(?error, "ipc client connection failed");
        }
    });
    let response = match sender.send_request(request).await {
        Ok(response) => response,
        Err(error) => {
            let _ = remove_stale_socket_files(sync_root);
            connection_task.abort();
            return Err(error.into());
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
    stop_tx: mpsc::UnboundedSender<()>,
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
                let stop_tx = stop_tx.clone();
                let token = token.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |request| {
                        handle_control_request(
                            request,
                            config.clone(),
                            stop_tx.clone(),
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
    stop_tx: mpsc::UnboundedSender<()>,
    token: CancellationToken,
) -> Result<Response<IpcBody>, Infallible> {
    let response = match (request.method(), request.uri().path()) {
        (&Method::GET, "/status") => match load_status(&config).await {
            Ok(status) => json_response(StatusCode::OK, &status),
            Err(error) => {
                warn!(?error, "status request failed");
                text_response(StatusCode::INTERNAL_SERVER_ERROR, "internal server error")
            }
        },
        (&Method::GET, "/spy") => Ok(spy_response(config.spy_tx.subscribe(), token)),
        (&Method::POST, "/stop") => match load_status(&config).await {
            Ok(status) => {
                let response = json_response(
                    StatusCode::OK,
                    &StopResponse {
                        workspace_id: status.workspace_id,
                        pid: status.pid,
                    },
                );
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    let _ = stop_tx.send(());
                });
                response
            }
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

async fn load_status(config: &ControlServerConfig) -> eyre::Result<DaemonStatus> {
    let daemon_state = load_daemon_state(&config.db_path).await?;
    Ok(DaemonStatus {
        workspace_id: daemon_state.workspace_id.0.clone(),
        root: config.sync_root.display().to_string(),
        pid: std::process::id(),
        stable_cursor_end: daemon_state.stable_cursor.end,
        daemon_writer_id_b64: daemon_state.daemon_writer_id.encode_b64(),
        started_at_ns: i64::try_from(config.started_at.unix_timestamp_nanos())
            .expect("started_at timestamp nanos fit in i64"),
    })
}

fn spy_response(rx: broadcast::Receiver<SpyEvent>, token: CancellationToken) -> Response<IpcBody> {
    let event_stream = stream::unfold((rx, token), |(mut rx, token)| async move {
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
        let frame = match serde_json::to_string(&event) {
            Ok(json) => Frame::data(Bytes::from(format!("data: {json}\n\n"))),
            Err(error) => {
                warn!(?error, "failed to serialize spy event");
                return None;
            }
        };
        Some((Ok::<_, Infallible>(frame), (rx, token)))
    });

    Response::builder()
        .status(StatusCode::OK)
        .header(hyper::header::CONTENT_TYPE, "text/event-stream")
        .header(hyper::header::CACHE_CONTROL, "no-cache")
        .body(StreamBody::new(event_stream).boxed_unsync())
        .expect("spy response is valid")
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
    use crate::app::db::{configure_connection, create_initialized_database, open_database};
    use crate::app::workspace::metadata_dir;
    use crate::types::{DaemonWriterId, OutboxId, WorkspaceId};

    #[tokio::test]
    async fn status_reads_current_daemon_state_from_db() -> eyre::Result<()> {
        let sync_root =
            std::env::temp_dir().join(format!("opbox-ipc-test-{}", rand::random::<u64>()));
        std::fs::create_dir(&sync_root)?;
        std::fs::create_dir(metadata_dir(&sync_root))?;

        let daemon_row = daemon_state::Row {
            workspace_id: WorkspaceId("0123456789abcdefghijklmnopqrstuv".to_string()),
            s2_basin: "test-basin".parse()?,
            daemon_writer_id: DaemonWriterId(Bytes::from_static(b"ipc-test-writer-1")),
            stable_cursor: ..0,
            next_outbox_id: OutboxId::new(0),
        };
        let db_path = metadata_dir(&sync_root).join("storage.db");
        create_initialized_database(&db_path, &daemon_row).await?;

        let (spy_tx, _) = broadcast::channel(8);
        let token = CancellationToken::new();
        let (stop_tx, _stop_rx) = mpsc::unbounded_channel();
        let server = tokio::spawn({
            let token = token.clone();
            let config = ControlServerConfig {
                sync_root: sync_root.clone(),
                db_path: db_path.clone(),
                daemon_state: daemon_row,
                started_at: OffsetDateTime::UNIX_EPOCH,
                spy_tx,
            };
            async move { serve_control(config, token, stop_tx).await }
        });

        wait_for_socket(&sync_root).await?;

        let db = open_database(&db_path).await?;
        let conn = db.connect()?;
        configure_connection(&conn).await?;
        conn.execute(
            "UPDATE daemon_state SET stable_cursor = 42 WHERE id = 1",
            (),
        )
        .await?;

        let status = request_status(&sync_root).await?;
        assert_eq!(status.stable_cursor_end, 42);

        token.cancel();
        server.await??;
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
