use hyper::rt::{Read, Write};
use hyper_util::client::legacy::connect::{Connected, Connection};
use hyper_util::rt::TokioIo;
use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::{Instant, Sleep};
use tower::Service;
use turmoil::net::TcpStream;

/// How long sent bytes may go undelivered before the connection is reset.
const UNDELIVERED_TIMEOUT: Duration = Duration::from_secs(3);
const CHECK_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Default)]
struct Direction {
    sent: AtomicU64,
    received: AtomicU64,
}

#[derive(Default)]
struct Link {
    to_server: Direction,
    to_client: Direction,
    reset: AtomicBool,
}

/// Links by client address, shared by both ends of a connection.
static LINKS: LazyLock<Mutex<HashMap<SocketAddr, Arc<Link>>>> = LazyLock::new(Default::default);

/// A turmoil TCP stream that is reset once bytes sent on it stay undelivered.
///
/// Turmoil never retransmits a dropped segment, so a connection that loses one
/// stalls in that direction forever while the other direction keeps flowing.
/// Real TCP either retransmits or gives up on the connection; this models the
/// latter, since a half-stalled connection cannot exist outside the simulation.
pub struct SimTcpStream {
    stream: TcpStream,
    link: Arc<Link>,
    client_addr: Option<SocketAddr>,
    is_client: bool,
    check: Pin<Box<Sleep>>,
    stalled: Option<(u64, Instant)>,
}

impl SimTcpStream {
    pub fn client(stream: TcpStream) -> io::Result<Self> {
        let addr = stream.local_addr()?;
        Ok(Self::new(stream, addr, true))
    }

    pub fn server(stream: TcpStream, client_addr: SocketAddr) -> Self {
        Self::new(stream, client_addr, false)
    }

    fn new(stream: TcpStream, client_addr: SocketAddr, is_client: bool) -> Self {
        let link = LINKS
            .lock()
            .unwrap()
            .entry(client_addr)
            .or_default()
            .clone();
        Self {
            stream,
            link,
            client_addr: is_client.then_some(client_addr),
            is_client,
            check: Box::pin(tokio::time::sleep(CHECK_INTERVAL)),
            stalled: None,
        }
    }

    fn directions(&self) -> (&Direction, &Direction) {
        if self.is_client {
            (&self.link.to_server, &self.link.to_client)
        } else {
            (&self.link.to_client, &self.link.to_server)
        }
    }

    fn poll_reset(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        while self.check.as_mut().poll(cx).is_ready() {
            let now = Instant::now();
            self.check.as_mut().reset(now + CHECK_INTERVAL);
            let (outbound, _) = self.directions();
            let delivered = outbound.received.load(Ordering::Relaxed);
            if delivered == outbound.sent.load(Ordering::Relaxed) {
                self.stalled = None;
                continue;
            }
            match self.stalled {
                Some((at, since)) if at == delivered => {
                    if now.duration_since(since) >= UNDELIVERED_TIMEOUT {
                        self.link.reset.store(true, Ordering::Relaxed);
                    }
                }
                _ => self.stalled = Some((delivered, now)),
            }
        }
        if self.link.reset.load(Ordering::Relaxed) {
            return Err(io::ErrorKind::ConnectionReset.into());
        }
        Ok(())
    }
}

impl Drop for SimTcpStream {
    fn drop(&mut self) {
        if let Some(addr) = self.client_addr {
            LINKS.lock().unwrap().remove(&addr);
        }
    }
}

impl AsyncRead for SimTcpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.poll_reset(cx)?;
        let before = buf.filled().len();
        let poll = Pin::new(&mut self.stream).poll_read(cx, buf);
        let read = (buf.filled().len() - before) as u64;
        self.directions()
            .1
            .received
            .fetch_add(read, Ordering::Relaxed);
        poll
    }
}

impl AsyncWrite for SimTcpStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_reset(cx)?;
        let poll = Pin::new(&mut self.stream).poll_write(cx, buf);
        if let Poll::Ready(Ok(written)) = poll {
            self.directions()
                .0
                .sent
                .fetch_add(written as u64, Ordering::Relaxed);
        }
        poll
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

#[derive(Clone)]
pub struct TurmoilConnector;

pub struct TurmoilConnection(TokioIo<SimTcpStream>);

impl Read for TurmoilConnection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl Write for TurmoilConnection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

impl Connection for TurmoilConnection {
    fn connected(&self) -> Connected {
        Connected::new()
    }
}

type ConnectorFut = Pin<Box<dyn Future<Output = Result<TurmoilConnection, io::Error>> + Send>>;

impl Service<http::Uri> for TurmoilConnector {
    type Response = TurmoilConnection;
    type Error = io::Error;
    type Future = ConnectorFut;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: http::Uri) -> Self::Future {
        Box::pin(async move {
            let port = uri.port_u16().unwrap_or(match uri.scheme_str() {
                Some("https") => 443,
                _ => 80,
            });
            let host = uri.host().expect("URI must have host");
            let addr = format!("{host}:{port}");
            let conn = TcpStream::connect(addr.as_str()).await?;
            Ok(TurmoilConnection(TokioIo::new(SimTcpStream::client(conn)?)))
        })
    }
}
