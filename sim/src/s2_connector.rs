use hyper::rt::{Read, Write};
use hyper_util::client::legacy::connect::{Connected, Connection};
use hyper_util::rt::TokioIo;
use std::future::Future;
use std::pin::Pin;
use std::task::Poll;
use tower::Service;
use turmoil::net::TcpStream;

#[derive(Clone)]
pub struct TurmoilConnector;

pub struct TurmoilConnection(TokioIo<TcpStream>);

impl Read for TurmoilConnection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl Write for TurmoilConnection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

impl Connection for TurmoilConnection {
    fn connected(&self) -> Connected {
        Connected::new()
    }
}

type ConnectorFut = Pin<Box<dyn Future<Output = Result<TurmoilConnection, std::io::Error>> + Send>>;

impl Service<http::Uri> for TurmoilConnector {
    type Response = TurmoilConnection;
    type Error = std::io::Error;
    type Future = ConnectorFut;

    fn poll_ready(&mut self, _cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
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
            Ok(TurmoilConnection(TokioIo::new(conn)))
        })
    }
}
