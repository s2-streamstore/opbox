use crate::app::s2::s2_error_is_connectivity;
use crate::log::codec;
use crate::log::codec::{ObjectPointer, S2Package};
use crate::log::encrypt::{self, CipherKey};
use crate::log::types::{LogWriterRequest, LogWriterResponse, SharedMessageOrigin};
use crate::types::{DaemonWriterId, OutboxId, WorkspaceId};
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::FuturesOrdered;
use s2_sdk::append_session::AppendSessionConfig;
use s2_sdk::batching::BatchingConfig;
use s2_sdk::producer::{IndexedAppendAck, ProducerConfig};
use s2_sdk::types::{AppendInput, AppendRecord, AppendRecordBatch, CreateStreamInput, Header};
use s2_sdk::{
    S2Basin,
    types::{AppendAck, S2Error, StreamName},
};
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

enum WriterRunError {
    Disconnected(String),
    Fatal(eyre::Report),
}

impl WriterRunError {
    fn from_s2(error: S2Error) -> Self {
        if s2_error_is_connectivity(&error) {
            Self::Disconnected(error.to_string())
        } else {
            Self::Fatal(error.into())
        }
    }

    fn fatal(error: impl Into<eyre::Report>) -> Self {
        Self::Fatal(error.into())
    }
}

enum WriterRunOutcome {
    Cancelled,
}

pub struct LogWriterActor {
    basin: S2Basin,
    workspace: WorkspaceId,
    daemon_writer_id: DaemonWriterId,
    encryption_key: Option<CipherKey>,
    req_rx: mpsc::UnboundedReceiver<LogWriterRequest>,
    resp_tx: mpsc::UnboundedSender<LogWriterResponse>,
}

impl LogWriterActor {
    async fn ensure_stream_exists(basin: &S2Basin, stream_name: StreamName) -> Result<(), S2Error> {
        match basin
            .create_stream(CreateStreamInput::new(stream_name))
            .await
        {
            Ok(_) => Ok(()),
            Err(S2Error::Server(err)) if err.code == "resource_already_exists" => Ok(()),
            Err(err) => Err(err),
        }
    }

    pub fn new(
        basin: S2Basin,
        workspace: WorkspaceId,
        daemon_writer_id: DaemonWriterId,
        encryption_key: Option<CipherKey>,
        req_rx: mpsc::UnboundedReceiver<LogWriterRequest>,
        resp_tx: mpsc::UnboundedSender<LogWriterResponse>,
    ) -> Self {
        Self {
            basin,
            workspace,
            daemon_writer_id,
            encryption_key,
            req_rx,
            resp_tx,
        }
    }

    /// Return the now unblocked pointer record when completed.
    async fn upload_parts(
        s2: S2Basin,
        workspace_id: WorkspaceId,
        encryption_key: Option<CipherKey>,
        outbox_id: OutboxId,
        pointer_record: AppendRecord,
        pointer: ObjectPointer,
        parts: Vec<AppendRecord>,
    ) -> Result<(OutboxId, AppendRecord), WriterRunError> {
        let stream_name = pointer
            .stream_name(workspace_id)
            .map_err(WriterRunError::fatal)?;
        Self::ensure_stream_exists(&s2, stream_name.clone())
            .await
            .map_err(WriterRunError::from_s2)?;
        let stream = s2.stream(stream_name);

        let session = stream.append_session(AppendSessionConfig::new());
        let mut set = JoinSet::new();

        for (idx, part) in parts.into_iter().enumerate() {
            let part = encrypt_record(part, &encryption_key).map_err(WriterRunError::fatal)?;
            let batch = AppendRecordBatch::try_from_iter([part]).map_err(WriterRunError::fatal)?;
            let input = AppendInput::new(batch).with_match_seq_num(idx as u64);
            let ticket = session
                .submit(input)
                .await
                .map_err(WriterRunError::from_s2)?;
            set.spawn(ticket);
        }

        set.join_all()
            .await
            .into_iter()
            .collect::<Result<Vec<AppendAck>, S2Error>>()
            .map_err(WriterRunError::from_s2)?;

        Ok((outbox_id, pointer_record))
    }

    pub async fn run(mut self, token: CancellationToken) -> eyre::Result<()> {
        loop {
            match self.run_connected(&token).await {
                Ok(WriterRunOutcome::Cancelled) => return Ok(()),
                Err(WriterRunError::Disconnected(reason)) => {
                    self.resp_tx
                        .send(LogWriterResponse::Disconnected { reason })?;
                    if !self.wait_for_reconnect(&token).await? {
                        return Ok(());
                    }
                }
                Err(WriterRunError::Fatal(error)) => return Err(error),
            }
        }
    }

    async fn wait_for_reconnect(&mut self, token: &CancellationToken) -> eyre::Result<bool> {
        loop {
            tokio::select! {
                _ = token.cancelled() => {
                    debug!("cancelled");
                    return Ok(false);
                }
                req = self.req_rx.recv() => {
                    match req {
                        Some(LogWriterRequest::Reconnect) => return Ok(true),
                        Some(LogWriterRequest::Status) => {
                            self.resp_tx.send(LogWriterResponse::Ping)?;
                        }
                        Some(LogWriterRequest::Append { .. }) => {}
                        None => return Err(eyre::eyre!("log writer request channel closed")),
                    }
                }
            }
        }
    }

    async fn run_connected(
        &mut self,
        token: &CancellationToken,
    ) -> Result<WriterRunOutcome, WriterRunError> {
        let mut big_upload = None;
        let main_stream_name = StreamName::from_str(&format!("{}/ops", self.workspace.0.as_str()))
            .map_err(WriterRunError::fatal)?;
        Self::ensure_stream_exists(&self.basin, main_stream_name.clone())
            .await
            .map_err(WriterRunError::from_s2)?;

        let stream = self.basin.stream(main_stream_name);
        let producer = stream.producer(
            ProducerConfig::new()
                .with_max_unacked_bytes(1024 * 1024 * 100)
                .map_err(WriterRunError::fatal)?
                .with_batching(BatchingConfig::new().with_linger(Duration::from_millis(5))),
        );
        self.resp_tx
            .send(LogWriterResponse::Connected)
            .map_err(WriterRunError::fatal)?;

        let mut inflight = 0;
        let mut next_expected_outbox_id: Option<OutboxId> = None;
        let mut pending_appends: FuturesOrdered<
            BoxFuture<'static, (OutboxId, Result<IndexedAppendAck, S2Error>)>,
        > = FuturesOrdered::new();

        loop {
            tokio::select! {
                _ = token.cancelled() => {
                    debug!("cancelled");

                    return Ok(WriterRunOutcome::Cancelled);
                }

                Some((outbox_id, res)) = pending_appends.next(), if !pending_appends.is_empty() => {
                    inflight -= 1;
                    let _res = res.map_err(WriterRunError::from_s2)?;
                    self.resp_tx.send(LogWriterResponse::Durable {
                        outbox_range: ..=outbox_id
                    }).map_err(WriterRunError::fatal)?;
                }

                res = async {
                    match big_upload.as_mut() {
                        Some(handle) => Some(handle.await),
                        None => std::future::pending().await,
                    }
                } => {
                    let res = res.expect("big_upload branch only completes when a handle exists");
                    let _finished = big_upload
                        .take()
                        .expect("completed upload handle still present");

                    match res {
                        Ok(Ok((outbox_id, pointer_record))) => {
                            inflight += 1;
                            let ticket = producer
                                .submit(pointer_record)
                                .await
                                .map_err(WriterRunError::from_s2)?;
                            let f: BoxFuture<'static, (OutboxId, Result<IndexedAppendAck, S2Error>)> = Box::pin(async move {
                                (outbox_id, ticket.await)
                            });
                            pending_appends.push_back(f);
                        }
                        Ok(Err(err)) => return Err(err),
                        Err(join_err) => return Err(WriterRunError::fatal(eyre::Report::new(join_err))),
                    }
                }

                Some(req) = self.req_rx.recv(), if big_upload.is_none() && inflight < 1024 * 1024 => {
                    match req {
                        LogWriterRequest::Status => {
                            self.resp_tx
                                .send(LogWriterResponse::Ping)
                                .map_err(WriterRunError::fatal)?;
                        },
                        LogWriterRequest::Reconnect => {},
                        // only if big upload not in prog
                        LogWriterRequest::Append { outbox_id, shared_message } => {
                            if let Some(expected) = next_expected_outbox_id {
                                assert_eq!(
                                    outbox_id, expected,
                                    "log writer received non-contiguous outbox id"
                                );
                            }
                            next_expected_outbox_id = Some(
                                outbox_id
                                    .checked_next()
                                    .expect("outbox id overflow in log writer"),
                            );

                            let kind: &'static str = shared_message.kind().into();
                            let origin = SharedMessageOrigin {
                                daemon_writer_id: self.daemon_writer_id.clone(),
                                outbox_id,
                            };
                            match codec::shared_to_s2_package(shared_message, &origin)
                                .map_err(WriterRunError::fatal)? {
                                S2Package::Inlined{ record } => {
                                    let record = encrypt_record(record, &self.encryption_key).map_err(WriterRunError::fatal)?;
                                    inflight += 1;
                                    let ticket = producer
                                        .submit(record)
                                        .await
                                        .map_err(WriterRunError::from_s2)?;
                                    let f: BoxFuture<'static, (OutboxId, Result<IndexedAppendAck, S2Error>)> = Box::pin(async move {
                                        (outbox_id, ticket.await)
                                    });
                                    pending_appends.push_back(f);
                                }
                                S2Package::Pointer{ pointer_record, pointer, parts  } => {
                                    info!(
                                        kind,
                                        ?outbox_id,
                                        part_count = parts.len(),
                                        payload_bytes = pointer.size_bytes,
                                        checksum = pointer.checksum,
                                        "log writer using pointer package"
                                    );
                                    let upload = tokio::spawn(Self::upload_parts(self.basin.clone(), self.workspace.clone(), self.encryption_key.clone(), outbox_id, pointer_record, pointer, parts));
                                    big_upload = Some(upload);
                                }
                            }


                        }
                    }
                }

                else => {
                    return Err(WriterRunError::fatal(eyre::eyre!("log writer exiting")));
                }
            }
        }
    }
}

fn encrypt_record(record: AppendRecord, key: &Option<CipherKey>) -> eyre::Result<AppendRecord> {
    let Some(key) = key else {
        return Ok(record);
    };
    let encrypted_body = encrypt::encrypt(key, record.body())?;
    let headers: Vec<Header> = record.headers().to_vec();
    let mut new_record = AppendRecord::new(encrypted_body)?;
    if !headers.is_empty() {
        new_record = new_record.with_headers(headers)?;
    }
    Ok(new_record)
}
