use hyper_util::rt::TokioIo;
use std::sync::Arc;
use std::time::Duration;
use tower_http::trace::{DefaultMakeSpan, DefaultOnRequest, DefaultOnResponse, TraceLayer};

pub async fn run_s2_lite_server() -> Result<(), Box<dyn std::error::Error>> {
    let object_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());

    let db = slatedb::Db::builder("", object_store)
        .with_settings(slatedb::Settings {
            flush_interval: Some(Duration::from_millis(5)),
            ..Default::default()
        })
        .build()
        .await
        .map_err(|err| format!("slatedb init: {err}"))?;

    let backend = s2_lite::backend::Backend::new(db, bytesize::ByteSize::mib(128));
    s2_lite::backend::bgtasks::spawn(&backend);

    let app = s2_lite::handlers::router().with_state(backend).layer(
        TraceLayer::new_for_http()
            .make_span_with(DefaultMakeSpan::new().level(tracing::Level::INFO))
            .on_request(DefaultOnRequest::new().level(tracing::Level::DEBUG))
            .on_response(DefaultOnResponse::new().level(tracing::Level::INFO)),
    );

    let listener = turmoil::net::TcpListener::bind("0.0.0.0:80").await?;
    tracing::info!("s2-lite listening on turmoil port 80");

    loop {
        let (stream, _addr) = listener.accept().await?;
        let app = app.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let svc = hyper::service::service_fn(move |req| {
                let app = app.clone();
                async move {
                    use tower::ServiceExt;
                    app.oneshot(req).await
                }
            });
            if let Err(err) =
                hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new())
                    .serve_connection(io, svc)
                    .await
            {
                tracing::warn!(error = %err, "s2-lite connection error");
            }
        });
    }
}
