#[cfg(unix)]
use crate::app::workspace::real_socket_path;
use crate::app::workspace::{remove_stale_socket_files, socket_link_path};
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
use hyper::header::HeaderMap;
use hyper::server::conn::http1 as server_http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
#[cfg(unix)]
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

type IpcBody = UnsyncBoxBody<Bytes, Infallible>;

pub use crate::app::control::{
    DaemonStatus, DaemonWarning, EnginePhaseStatus, StreamRetentionSummary,
};

pub const IPC_PROTOCOL_VERSION: u32 = 1;

const IPC_PROTOCOL_VERSION_HEADER_VALUE: &str = "1";
const HEADER_IPC_PROTOCOL: &str = "x-opbox-ipc-protocol";
const HEADER_VERSION: &str = "x-opbox-version";
const HEADER_BUILD: &str = "x-opbox-build";
const HEADER_CLIENT_IPC_PROTOCOL: &str = "x-opbox-client-ipc-protocol";
const HEADER_CLIENT_VERSION: &str = "x-opbox-client-version";
const HEADER_CLIENT_BUILD: &str = "x-opbox-client-build";

pub fn package_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

pub fn build_hash() -> &'static str {
    env!("OPBOX_BUILD_HASH")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StopResponse {
    pub workspace_id: String,
    pub pid: u32,
}

/// Context `ob share` needs from a running daemon. The daemon holds an
/// exclusive lock on the workspace DB, so the CLI must not open it directly
/// while the daemon runs; this response carries the DB-backed fields instead.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShareContextResponse {
    pub workspace_id: String,
    pub basin: String,
    pub root: String,
    pub encryption_key: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DaemonBuildMismatch {
    pub client_version: String,
    pub client_build: String,
    pub daemon_version: String,
    pub daemon_build: String,
}

impl std::fmt::Display for DaemonBuildMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "opbox daemon build does not match ob\n  ob:     {} {}\n  daemon: {} {}\nrestart the daemon with the current opbox build",
            self.client_version, self.client_build, self.daemon_version, self.daemon_build
        )
    }
}

impl std::error::Error for DaemonBuildMismatch {}

#[derive(Debug, Clone)]
pub struct ControlServerConfig {
    pub sync_root: PathBuf,
    pub daemon_state: daemon_state::Row,
    pub engine_tx: mpsc::UnboundedSender<EngineCommand>,
}

pub async fn request_status(sync_root: &Path) -> eyre::Result<DaemonStatus> {
    request_json(
        sync_root,
        Method::GET,
        "/status",
        DaemonMetadataPolicy::Strict,
    )
    .await
}

pub async fn request_share_context(sync_root: &Path) -> eyre::Result<ShareContextResponse> {
    request_json(
        sync_root,
        Method::GET,
        "/share-context",
        DaemonMetadataPolicy::Strict,
    )
    .await
}

pub async fn request_stop(sync_root: &Path) -> eyre::Result<StopResponse> {
    request_json(
        sync_root,
        Method::POST,
        "/stop",
        DaemonMetadataPolicy::AllowBuildMismatch,
    )
    .await
}

pub async fn open_spy_stream(sync_root: &Path) -> eyre::Result<SpyStream> {
    let request = ipc_request_builder(Method::GET, "/spy")
        .header(hyper::header::HOST, "opbox")
        .header(hyper::header::ACCEPT, "text/event-stream")
        .body(Empty::<Bytes>::new())?;
    let (response, connection_task) = send_request(sync_root, request).await?;
    validate_daemon_metadata(&response, DaemonMetadataPolicy::Strict)?;
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DaemonMetadataPolicy {
    Strict,
    AllowBuildMismatch,
}

async fn request_json<T>(
    sync_root: &Path,
    method: Method,
    uri: &str,
    metadata_policy: DaemonMetadataPolicy,
) -> eyre::Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let request = ipc_request_builder(method, uri)
        .header(hyper::header::HOST, "opbox")
        .body(Empty::<Bytes>::new())?;
    let (response, connection_task) = send_request(sync_root, request).await?;
    validate_daemon_metadata(&response, metadata_policy)?;
    if response.status() != StatusCode::OK {
        if metadata_policy == DaemonMetadataPolicy::AllowBuildMismatch
            && response.status() == StatusCode::CONFLICT
            && let Some(mismatch) = daemon_build_mismatch(response.headers())
        {
            connection_task.abort();
            return Err(mismatch.into());
        }
        eyre::bail!("daemon returned non-200 status: {}", response.status());
    }
    let body = response.into_body().collect().await?.to_bytes();
    connection_task.abort();
    Ok(serde_json::from_slice(&body)?)
}

fn ipc_request_builder(method: Method, uri: &str) -> hyper::http::request::Builder {
    Request::builder()
        .method(method)
        .uri(uri)
        .header(
            HEADER_CLIENT_IPC_PROTOCOL,
            IPC_PROTOCOL_VERSION_HEADER_VALUE,
        )
        .header(HEADER_CLIENT_VERSION, package_version())
        .header(HEADER_CLIENT_BUILD, build_hash())
}

fn validate_daemon_metadata(
    response: &Response<Incoming>,
    policy: DaemonMetadataPolicy,
) -> eyre::Result<()> {
    let headers = response.headers();
    let Some(protocol) = header_value(headers, HEADER_IPC_PROTOCOL) else {
        eyre::bail!(
            "daemon did not include opbox IPC metadata; it is likely from an older build. Restart the daemon with the current opbox build."
        );
    };
    if protocol != IPC_PROTOCOL_VERSION_HEADER_VALUE {
        eyre::bail!(
            "opbox daemon IPC protocol mismatch\n  ob protocol: {}\n  daemon protocol: {protocol}\nrestart the daemon with the current opbox build",
            IPC_PROTOCOL_VERSION
        );
    }

    let Some(daemon_build) = header_value(headers, HEADER_BUILD) else {
        eyre::bail!(
            "daemon did not include opbox build metadata; it is likely from an older build. Restart the daemon with the current opbox build."
        );
    };
    if daemon_build != build_hash()
        && let Some(mismatch) = daemon_build_mismatch(headers)
    {
        if policy == DaemonMetadataPolicy::AllowBuildMismatch {
            return Ok(());
        }
        return Err(mismatch.into());
    }

    Ok(())
}

fn daemon_build_mismatch(headers: &HeaderMap) -> Option<DaemonBuildMismatch> {
    let daemon_build = header_value(headers, HEADER_BUILD)?;
    if daemon_build == build_hash() {
        return None;
    }
    Some(DaemonBuildMismatch {
        client_version: package_version().to_string(),
        client_build: build_hash().to_string(),
        daemon_version: header_value(headers, HEADER_VERSION)
            .unwrap_or("unknown")
            .to_string(),
        daemon_build: daemon_build.to_string(),
    })
}

async fn send_request(
    sync_root: &Path,
    request: Request<Empty<Bytes>>,
) -> eyre::Result<(Response<Incoming>, JoinHandle<()>)> {
    #[cfg(unix)]
    let io = {
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
        TokioIo::new(stream)
    };

    #[cfg(windows)]
    let io = {
        let addr = resolve_socket_address(sync_root)?;
        let stream = match tokio::net::TcpStream::connect(&addr).await {
            Ok(stream) => stream,
            Err(error) => {
                if matches!(error.kind(), ErrorKind::ConnectionRefused) {
                    let _ = remove_stale_socket_files(sync_root);
                }
                return Err(error)
                    .wrap_err_with(|| format!("connect to daemon control socket at {addr}"));
            }
        };
        TokioIo::new(stream)
    };

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

#[cfg(unix)]
fn resolve_socket_path(sync_root: &Path) -> eyre::Result<PathBuf> {
    let link_path = socket_link_path(sync_root);
    match std::fs::read_link(&link_path) {
        Ok(path) => Ok(path),
        Err(error) if error.kind() == std::io::ErrorKind::InvalidInput => Ok(link_path),
        Err(error) => Err(error.into()),
    }
}

#[cfg(windows)]
fn resolve_socket_address(sync_root: &Path) -> eyre::Result<std::net::SocketAddr> {
    let link_path = socket_link_path(sync_root);
    let contents = std::fs::read_to_string(&link_path).map_err(|error| {
        eyre::eyre!(
            "failed to read daemon address from {}: {error}",
            link_path.display()
        )
    })?;
    contents
        .trim()
        .parse()
        .map_err(|error| eyre::eyre!("invalid daemon address in {}: {error}", link_path.display()))
}

pub async fn serve_control(
    config: ControlServerConfig,
    token: CancellationToken,
) -> eyre::Result<()> {
    #[cfg(unix)]
    {
        let socket_path = real_socket_path(
            &config.daemon_state.workspace_id,
            &config.daemon_state.daemon_writer_id,
        );
        remove_stale_socket(&socket_path)?;
        let listener = UnixListener::bind(&socket_path)
            .wrap_err_with(|| format!("bind daemon control socket {}", socket_path.display()))?;
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

    #[cfg(windows)]
    {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        write_socket_address(&socket_link_path(&config.sync_root), &addr)?;
        let _guard = SocketGuard {
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
}

async fn handle_control_request(
    request: Request<Incoming>,
    config: ControlServerConfig,
    token: CancellationToken,
) -> Result<Response<IpcBody>, Infallible> {
    if let Some(response) =
        client_metadata_error(request.method(), request.uri().path(), request.headers())
    {
        return Ok(response);
    }

    let response = match (request.method(), request.uri().path()) {
        (&Method::GET, "/status") => match request_engine_status(&config).await {
            Ok(status) => json_response(StatusCode::OK, &status),
            Err(error) => {
                warn!(?error, "status request failed");
                text_response(StatusCode::INTERNAL_SERVER_ERROR, "internal server error")
            }
        },
        (&Method::GET, "/share-context") => json_response(
            StatusCode::OK,
            &ShareContextResponse {
                workspace_id: config.daemon_state.workspace_id.0.clone(),
                basin: config.daemon_state.s2_basin.as_ref().to_string(),
                root: config.sync_root.display().to_string(),
                encryption_key: config.daemon_state.encryption_key.to_string(),
            },
        ),
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

fn client_metadata_error(
    method: &Method,
    path: &str,
    headers: &HeaderMap,
) -> Option<Response<IpcBody>> {
    let allow_build_mismatch = method == Method::POST && path == "/stop";

    let Some(protocol) = header_value(headers, HEADER_CLIENT_IPC_PROTOCOL) else {
        return Some(
            text_response(
                StatusCode::UPGRADE_REQUIRED,
                "opbox IPC client metadata is missing; upgrade ob and restart the daemon",
            )
            .expect("metadata error response is valid"),
        );
    };
    if protocol != IPC_PROTOCOL_VERSION_HEADER_VALUE {
        return Some(
            text_response(
                StatusCode::UPGRADE_REQUIRED,
                format!(
                    "opbox IPC protocol mismatch; daemon protocol is {}, client protocol is {protocol}",
                    IPC_PROTOCOL_VERSION
                ),
            )
            .expect("protocol mismatch response is valid"),
        );
    }

    let Some(client_build) = header_value(headers, HEADER_CLIENT_BUILD) else {
        return Some(
            text_response(
                StatusCode::UPGRADE_REQUIRED,
                "opbox IPC client build metadata is missing; upgrade ob and restart the daemon",
            )
            .expect("build metadata response is valid"),
        );
    };
    if client_build != build_hash() && !allow_build_mismatch {
        let client_version = header_value(headers, HEADER_CLIENT_VERSION).unwrap_or("unknown");
        return Some(
            text_response(
                StatusCode::CONFLICT,
                format!(
                    "opbox client build does not match daemon\n  client: {client_version} {client_build}\n  daemon: {} {}",
                    package_version(),
                    build_hash()
                ),
            )
            .expect("build mismatch response is valid"),
        );
    }

    None
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
        .header(HEADER_IPC_PROTOCOL, IPC_PROTOCOL_VERSION_HEADER_VALUE)
        .header(HEADER_VERSION, package_version())
        .header(HEADER_BUILD, build_hash())
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

fn text_response(status: StatusCode, body: impl Into<String>) -> eyre::Result<Response<IpcBody>> {
    Ok(response(
        status,
        "text/plain; charset=utf-8",
        Bytes::from(body.into()),
    ))
}

fn response(status: StatusCode, content_type: &'static str, body: Bytes) -> Response<IpcBody> {
    Response::builder()
        .status(status)
        .header(hyper::header::CONTENT_TYPE, content_type)
        .header(HEADER_IPC_PROTOCOL, IPC_PROTOCOL_VERSION_HEADER_VALUE)
        .header(HEADER_VERSION, package_version())
        .header(HEADER_BUILD, build_hash())
        .body(Full::new(body).boxed_unsync())
        .expect("control response is valid")
}

fn header_value<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name)?.to_str().ok()
}

#[cfg(unix)]
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

#[cfg(windows)]
fn write_socket_address(link_path: &Path, addr: &std::net::SocketAddr) -> eyre::Result<()> {
    use std::io::Write;
    match std::fs::remove_file(link_path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let mut file = std::fs::File::create(link_path)?;
    write!(file, "{addr}")?;
    Ok(())
}

#[cfg(unix)]
struct SocketGuard {
    socket_path: PathBuf,
    link_path: PathBuf,
}

#[cfg(unix)]
impl Drop for SocketGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.socket_path);
        if std::fs::read_link(&self.link_path).ok().as_ref() == Some(&self.socket_path) {
            let _ = std::fs::remove_file(&self.link_path);
        }
    }
}

#[cfg(windows)]
struct SocketGuard {
    link_path: PathBuf,
}

#[cfg(windows)]
impl Drop for SocketGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.link_path);
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
    use tokio::io::AsyncWriteExt;

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
            encryption_key: crate::log::encrypt::CipherKey::from_bytes([0x42u8; 32]),
        };

        let (engine_tx, mut engine_rx) = mpsc::unbounded_channel();
        let expected_status = DaemonStatus {
            workspace_id: daemon_row.workspace_id.0.clone(),
            basin: daemon_row.s2_basin.as_ref().to_string(),
            root: sync_root.display().to_string(),
            pid: 123,
            stable_cursor_end: 42,
            daemon_writer_id_b64: daemon_row.daemon_writer_id.encode_b64(),
            started_at_ns: 0,
            engine_phase: EnginePhaseStatus::Scanning,
            connectivity: ConnectivitySnapshot::starting(),
            warnings: vec![DaemonWarning::OpsStreamRetentionNotInfinite {
                retention: StreamRetentionSummary::Age { seconds: 86_400 },
            }],
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
        assert_eq!(status.basin, expected_status.basin);
        assert_eq!(status.root, expected_status.root);
        assert_eq!(status.pid, expected_status.pid);
        assert_eq!(status.stable_cursor_end, expected_status.stable_cursor_end);
        assert_eq!(status.engine_phase, expected_status.engine_phase);
        assert_eq!(status.connectivity, expected_status.connectivity);
        assert_eq!(status.warnings, expected_status.warnings);

        token.cancel();
        server.await??;
        engine.await?;
        let _ = std::fs::remove_dir_all(&sync_root);
        Ok(())
    }

    #[tokio::test]
    async fn share_context_serves_daemon_state_without_engine_round_trip() -> eyre::Result<()> {
        let sync_root =
            std::env::temp_dir().join(format!("opbox-ipc-test-{}", rand::random::<u64>()));
        std::fs::create_dir(&sync_root)?;
        std::fs::create_dir(metadata_dir(&sync_root))?;
        let writer_bytes = rand::random::<[u8; 16]>();
        let encryption_key = crate::log::encrypt::CipherKey::from_bytes([0x42u8; 32]);

        let daemon_row = daemon_state::Row {
            workspace_id: WorkspaceId("0123456789abcdefghijklmnopqrstuv".to_string()),
            s2_basin: "test-basin".parse()?,
            s2_account_endpoint: None,
            s2_basin_endpoint: None,
            daemon_writer_id: DaemonWriterId(Bytes::copy_from_slice(&writer_bytes)),
            stable_cursor: ..0,
            next_outbox_id: OutboxId::new(0),
            encryption_key: encryption_key.clone(),
        };

        // No engine task: the share-context route must answer straight from
        // ControlServerConfig.daemon_state.
        let (engine_tx, _engine_rx) = mpsc::unbounded_channel();
        let token = CancellationToken::new();
        let server = tokio::spawn({
            let token = token.clone();
            let config = ControlServerConfig {
                sync_root: sync_root.clone(),
                daemon_state: daemon_row.clone(),
                engine_tx,
            };
            async move { serve_control(config, token).await }
        });

        wait_for_socket(&sync_root).await?;

        let share = request_share_context(&sync_root).await?;
        assert_eq!(share.workspace_id, daemon_row.workspace_id.0);
        assert_eq!(share.basin, daemon_row.s2_basin.as_ref());
        assert_eq!(share.root, sync_root.display().to_string());
        assert_eq!(share.encryption_key, encryption_key.to_string());

        token.cancel();
        server.await??;
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
            encryption_key: crate::log::encrypt::CipherKey::from_bytes([0x42u8; 32]),
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
            encryption_key: crate::log::encrypt::CipherKey::from_bytes([0x42u8; 32]),
        };

        let (engine_tx, mut engine_rx) = mpsc::unbounded_channel();
        let expected_status = DaemonStatus {
            workspace_id: daemon_row.workspace_id.0.clone(),
            basin: daemon_row.s2_basin.as_ref().to_string(),
            root: sync_root.display().to_string(),
            pid: 123,
            stable_cursor_end: 42,
            daemon_writer_id_b64: daemon_row.daemon_writer_id.encode_b64(),
            started_at_ns: 0,
            engine_phase: EnginePhaseStatus::Scanning,
            connectivity: ConnectivitySnapshot::starting(),
            warnings: Vec::new(),
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

    #[cfg(unix)]
    #[tokio::test]
    async fn stop_allows_daemon_build_mismatch_response() -> eyre::Result<()> {
        let sync_root =
            std::env::temp_dir().join(format!("opbox-ipc-test-{}", rand::random::<u64>()));
        std::fs::create_dir(&sync_root)?;
        std::fs::create_dir(metadata_dir(&sync_root))?;
        let socket_path =
            std::env::temp_dir().join(format!("opbox-ipc-test-{}.sock", rand::random::<u64>()));
        let listener = UnixListener::bind(&socket_path)?;
        std::os::unix::fs::symlink(&socket_path, socket_link_path(&sync_root))?;

        let expected = StopResponse {
            workspace_id: "0123456789abcdefghijklmnopqrstuv".to_string(),
            pid: 123,
        };
        let body = serde_json::to_vec(&expected)?;
        let response_head = format!(
            "HTTP/1.1 200 OK\r\n{HEADER_IPC_PROTOCOL}: {IPC_PROTOCOL_VERSION_HEADER_VALUE}\r\n{HEADER_VERSION}: {}\r\n{HEADER_BUILD}: old-build\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
            package_version(),
            body.len()
        );
        let server = tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let _ = stream.write_all(response_head.as_bytes()).await;
                let _ = stream.write_all(&body).await;
            }
        });

        let response = request_stop(&sync_root).await?;
        assert_eq!(response.workspace_id, expected.workspace_id);
        assert_eq!(response.pid, expected.pid);

        server.await?;
        let _ = std::fs::remove_file(&socket_path);
        let _ = std::fs::remove_dir_all(&sync_root);
        Ok(())
    }

    #[tokio::test]
    async fn daemon_allows_stop_with_client_build_mismatch() -> eyre::Result<()> {
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
            encryption_key: crate::log::encrypt::CipherKey::from_bytes([0x42u8; 32]),
        };

        let (engine_tx, mut engine_rx) = mpsc::unbounded_channel();
        let expected_status = DaemonStatus {
            workspace_id: daemon_row.workspace_id.0.clone(),
            basin: daemon_row.s2_basin.as_ref().to_string(),
            root: sync_root.display().to_string(),
            pid: 123,
            stable_cursor_end: 42,
            daemon_writer_id_b64: daemon_row.daemon_writer_id.encode_b64(),
            started_at_ns: 0,
            engine_phase: EnginePhaseStatus::Scanning,
            connectivity: ConnectivitySnapshot::starting(),
            warnings: Vec::new(),
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

        let request = Request::builder()
            .method(Method::POST)
            .uri("/stop")
            .header(hyper::header::HOST, "opbox")
            .header(
                HEADER_CLIENT_IPC_PROTOCOL,
                IPC_PROTOCOL_VERSION_HEADER_VALUE,
            )
            .header(HEADER_CLIENT_VERSION, package_version())
            .header(HEADER_CLIENT_BUILD, format!("{}-other", build_hash()))
            .body(Empty::<Bytes>::new())?;
        let (response, connection_task) = send_request(&sync_root, request).await?;
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await?.to_bytes();
        connection_task.abort();
        let stop_response: StopResponse = serde_json::from_slice(&body)?;
        assert_eq!(stop_response.workspace_id, expected_status.workspace_id);
        assert_eq!(stop_response.pid, expected_status.pid);

        token.cancel();
        server.await??;
        engine.await?;
        let _ = std::fs::remove_dir_all(&sync_root);
        Ok(())
    }

    #[cfg(unix)]
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

    #[tokio::test]
    async fn daemon_rejects_request_without_client_metadata() -> eyre::Result<()> {
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
            encryption_key: crate::log::encrypt::CipherKey::from_bytes([0x42u8; 32]),
        };
        let (engine_tx, mut engine_rx) = mpsc::unbounded_channel();
        let engine = tokio::spawn(async move {
            assert!(
                engine_rx.recv().await.is_none(),
                "metadata rejection should not reach engine"
            );
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

        let request = Request::builder()
            .method(Method::GET)
            .uri("/status")
            .header(hyper::header::HOST, "opbox")
            .body(Empty::<Bytes>::new())?;
        let (response, connection_task) = send_request(&sync_root, request).await?;
        assert_eq!(response.status(), StatusCode::UPGRADE_REQUIRED);
        assert_eq!(
            header_value(response.headers(), HEADER_IPC_PROTOCOL),
            Some(IPC_PROTOCOL_VERSION_HEADER_VALUE)
        );

        connection_task.abort();
        token.cancel();
        server.await??;
        engine.await?;
        let _ = std::fs::remove_dir_all(&sync_root);
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn legacy_daemon_response_reports_metadata_error_without_unlinking_socket()
    -> eyre::Result<()> {
        let sync_root =
            std::env::temp_dir().join(format!("opbox-ipc-test-{}", rand::random::<u64>()));
        std::fs::create_dir(&sync_root)?;
        std::fs::create_dir(metadata_dir(&sync_root))?;
        let socket_path =
            std::env::temp_dir().join(format!("opbox-ipc-test-{}.sock", rand::random::<u64>()));
        let listener = UnixListener::bind(&socket_path)?;
        std::os::unix::fs::symlink(&socket_path, socket_link_path(&sync_root))?;

        let server = tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let _ = stream
                    .write_all(
                        b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 2\r\n\r\n{}",
                    )
                    .await;
            }
        });

        let error = request_status(&sync_root)
            .await
            .expect_err("legacy daemon response should fail metadata validation");
        assert!(
            error
                .to_string()
                .contains("did not include opbox IPC metadata"),
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
