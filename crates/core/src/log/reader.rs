use crate::app::s2::s2_error_is_connectivity;
use crate::crdt::types::SharedMessage;
use crate::log::codec::{self, ObjectPointer};
use crate::log::encrypt::{self, CipherKey};
use crate::log::types::{
    LogReadStop, LogReaderEvent, LogReaderRequest, SequenceNumber, SharedMessageEnvelope,
};
use crate::types::WorkspaceId;
use bytes::BytesMut;
use futures::StreamExt;
use s2_sdk::S2Basin;
use s2_sdk::types::ReadFrom::SeqNum;
use s2_sdk::types::{
    CreateStreamInput, Header, ReadBatch, ReadFrom, ReadInput, ReadLimits, ReadStart, ReadStop,
    S2Error, StreamName,
};
use std::str::FromStr;
use time::OffsetDateTime;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace};
use xxhash_rust::xxh3::xxh3_64;

enum ReaderRunError {
    Disconnected(String),
    Fatal(eyre::Report),
}

impl ReaderRunError {
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

enum ReaderRunOutcome {
    Cancelled,
}

pub struct LogReaderActor {
    basin: S2Basin,
    workspace: WorkspaceId,
    encryption_key: Option<CipherKey>,
    start_at: SequenceNumber,
    stop: Option<LogReadStop>,
    req_rx: mpsc::UnboundedReceiver<LogReaderRequest>,
    event_tx: Sender<LogReaderEvent>,
}

impl LogReaderActor {
    pub fn new(
        basin: S2Basin,
        workspace: WorkspaceId,
        encryption_key: Option<CipherKey>,
        start_at: SequenceNumber,
        stop: Option<LogReadStop>,
        req_rx: mpsc::UnboundedReceiver<LogReaderRequest>,
        event_tx: mpsc::Sender<LogReaderEvent>,
    ) -> Self {
        Self {
            basin,
            workspace,
            encryption_key,
            start_at,
            stop,
            req_rx,
            event_tx,
        }
    }

    async fn ensure_stream_exists(basin: &S2Basin, stream_name: StreamName) -> Result<(), S2Error> {
        match basin
            .create_stream(CreateStreamInput::new(stream_name))
            .await
        {
            Ok(_) => Ok(()),
            Err(s2_sdk::types::S2Error::Server(err)) if err.code == "resource_already_exists" => {
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    async fn read_full_multipart(
        basin: S2Basin,
        workspace_id: WorkspaceId,
        encryption_key: Option<CipherKey>,
        message_headers: Vec<Header>,
        object_pointer: ObjectPointer,
    ) -> Result<SharedMessage, ReaderRunError> {
        let kind = codec::header_str(&message_headers, "message_kind")
            .map_err(ReaderRunError::fatal)?
            .unwrap_or("unknown");
        info!(
            kind,
            payload_bytes = object_pointer.size_bytes,
            part_count = object_pointer.n_records,
            checksum = object_pointer.checksum,
            "log reader fetching pointer package"
        );

        let stream_name = object_pointer
            .stream_name(workspace_id)
            .map_err(ReaderRunError::fatal)?;
        let stream = basin.stream(stream_name);
        let mut read_session = stream
            .read_session(
                ReadInput::new()
                    .with_start(ReadStart::new().with_from(SeqNum(0)))
                    .with_stop(
                        ReadStop::new()
                            .with_limits(ReadLimits::new().with_count(object_pointer.n_records)),
                    ),
            )
            .await
            .map_err(ReaderRunError::from_s2)?;

        let expected_size = usize::try_from(object_pointer.size_bytes).map_err(|_| {
            ReaderRunError::fatal(eyre::eyre!("multipart payload size does not fit usize"))
        })?;
        let mut buf = BytesMut::with_capacity(expected_size);
        let mut record_count = 0usize;

        while let Some(batch) = read_session.next().await {
            let ReadBatch { records, .. } = batch.map_err(ReaderRunError::from_s2)?;
            for record in records {
                record_count += 1;
                let body =
                    decrypt_body(&record.body, &encryption_key).map_err(ReaderRunError::fatal)?;
                buf.extend_from_slice(&body);
            }
        }

        if record_count != object_pointer.n_records {
            return Err(ReaderRunError::fatal(eyre::eyre!(
                "multipart payload record count mismatch: expected {}, got {}",
                object_pointer.n_records,
                record_count
            )));
        }
        if buf.len() != expected_size {
            return Err(ReaderRunError::fatal(eyre::eyre!(
                "multipart payload size mismatch: expected {}, got {}",
                expected_size,
                buf.len()
            )));
        }

        let payload = buf.freeze();
        let checksum = xxh3_64(payload.as_ref());
        if checksum != object_pointer.checksum {
            return Err(ReaderRunError::fatal(eyre::eyre!(
                "multipart payload checksum mismatch: expected {}, got {}",
                object_pointer.checksum,
                checksum
            )));
        }

        info!(
            kind,
            payload_bytes = object_pointer.size_bytes,
            part_count = object_pointer.n_records,
            "log reader reconstructed pointer package"
        );

        codec::s2_payload_to_shared_message(&message_headers, payload)
            .map_err(ReaderRunError::fatal)
    }

    pub async fn run(mut self, token: CancellationToken) -> eyre::Result<()> {
        let mut start_at = self.start_at;
        loop {
            match self.run_connected(&token, start_at).await {
                Ok(ReaderRunOutcome::Cancelled) => return Ok(()),
                Err(ReaderRunError::Disconnected(reason)) => {
                    self.event_tx
                        .send(LogReaderEvent::Disconnected { reason })
                        .await?;
                    let Some(next_start_at) = self.wait_for_reconnect(&token).await? else {
                        return Ok(());
                    };
                    start_at = next_start_at;
                }
                Err(ReaderRunError::Fatal(error)) => return Err(error),
            }
        }
    }

    async fn wait_for_reconnect(
        &mut self,
        token: &CancellationToken,
    ) -> eyre::Result<Option<SequenceNumber>> {
        loop {
            tokio::select! {
                _ = token.cancelled() => {
                    debug!("cancelled");
                    return Ok(None);
                }
                req = self.req_rx.recv() => {
                    match req {
                        Some(LogReaderRequest::Reconnect { start_at }) => return Ok(Some(start_at)),
                        Some(LogReaderRequest::Status) => {}
                        None => return Err(eyre::eyre!("log reader request channel closed")),
                    }
                }
            }
        }
    }

    async fn run_connected(
        &mut self,
        token: &CancellationToken,
        start_at: SequenceNumber,
    ) -> Result<ReaderRunOutcome, ReaderRunError> {
        let main_stream_name = StreamName::from_str(&format!("{}/ops", self.workspace.0.as_str()))
            .map_err(ReaderRunError::fatal)?;
        Self::ensure_stream_exists(&self.basin, main_stream_name.clone())
            .await
            .map_err(ReaderRunError::from_s2)?;
        let stream = self.basin.stream(main_stream_name);

        let mut read_input =
            ReadInput::new().with_start(ReadStart::new().with_from(ReadFrom::SeqNum(start_at)));
        if let Some(stop) = self.stop {
            let read_stop = match stop {
                LogReadStop::UntilTimestampMs(until_ms) => ReadStop::new().with_until(..until_ms),
            };
            read_input = read_input.with_stop(read_stop);
        }
        let mut batches = stream
            .read_session(read_input)
            .await
            .map_err(ReaderRunError::from_s2)?;
        let mut next_sequence_number = start_at;
        self.event_tx
            .send(LogReaderEvent::Connected)
            .await
            .map_err(ReaderRunError::fatal)?;

        loop {
            tokio::select! {
                _ = token.cancelled() => {
                    debug!("cancelled");

                    return Ok(ReaderRunOutcome::Cancelled);
                }

                Some(req) = self.req_rx.recv() => {
                    match req {
                        LogReaderRequest::Status => {
                            let tail = stream
                                .check_tail()
                                .await
                                .map_err(ReaderRunError::from_s2)?;
                            self.event_tx.send(LogReaderEvent::Status {
                                tail: ..tail.seq_num,
                            }).await.map_err(ReaderRunError::fatal)?;
                        }
                        LogReaderRequest::Reconnect { .. } => {}
                    }
                }

                batch = batches.next() => {
                    let Some(batch) = batch else {
                        if self.stop.is_some() {
                            self.event_tx.send(LogReaderEvent::Ended {
                                cursor: ..next_sequence_number,
                            }).await.map_err(ReaderRunError::fatal)?;
                            token.cancelled().await;
                            return Ok(ReaderRunOutcome::Cancelled);
                        }
                        return Err(ReaderRunError::fatal(eyre::eyre!("log reader read session ended")));
                    };
                    let batch = batch.map_err(ReaderRunError::from_s2)?;

                    trace!(
                        record_count = batch.records.len(),
                        tail = ?batch.tail,
                        "log reader batch"
                    );

                    for record in batch.records {
                        let sequence_number = record.seq_num;
                        next_sequence_number = sequence_number.checked_add(1)
                            .ok_or_else(|| ReaderRunError::fatal(eyre::eyre!("log reader sequence number overflow")))?;
                        let timestamp =
                            OffsetDateTime::from_unix_timestamp_nanos(record.timestamp as i128 * 1_000_000)
                                .expect("valid timestamp");
                        let origin = codec::decode_origin(&record.headers)
                            .map_err(ReaderRunError::fatal)?;

                        let shared_message =
                            if let Some(pointer) = codec::header_value(&record.headers, "pointer") {
                                let object_pointer: ObjectPointer = serde_json::from_slice(pointer)
                                    .map_err(ReaderRunError::fatal)?;
                                Self::read_full_multipart(
                                    self.basin.clone(),
                                    self.workspace.clone(),
                                    self.encryption_key.clone(),
                                    record.headers,
                                    object_pointer,
                                )
                                .await?
                            } else {
                                let decrypted_body = decrypt_body(&record.body, &self.encryption_key)
                                    .map_err(ReaderRunError::fatal)?;
                                codec::s2_payload_to_shared_message(&record.headers, decrypted_body)
                                    .map_err(ReaderRunError::fatal)?
                            };

                        let envelope = SharedMessageEnvelope {
                            timestamp,
                            sequence_number,
                            origin,
                            shared_message
                        };

                        self.event_tx
                            .send(LogReaderEvent::Read(envelope))
                            .await
                            .map_err(ReaderRunError::fatal)?;
                    }
                }

                else => {
                    return Ok(ReaderRunOutcome::Cancelled);
                }

            }
        }
    }
}

fn decrypt_body(body: &bytes::Bytes, key: &Option<CipherKey>) -> eyre::Result<bytes::Bytes> {
    match key {
        Some(key) => encrypt::decrypt(key, body),
        None => Ok(body.clone()),
    }
}
