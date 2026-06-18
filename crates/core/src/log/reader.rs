use crate::crdt::types::SharedMessage;
use crate::log::codec::{self, ObjectPointer};
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
    StreamName,
};
use std::str::FromStr;
use time::OffsetDateTime;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace};
use xxhash_rust::xxh3::xxh3_64;

pub struct LogReaderActor {
    basin: S2Basin,
    workspace: WorkspaceId,
    start_at: SequenceNumber,
    stop: Option<LogReadStop>,
    req_rx: mpsc::UnboundedReceiver<LogReaderRequest>,
    event_tx: Sender<LogReaderEvent>,
}

impl LogReaderActor {
    pub fn new(
        basin: S2Basin,
        workspace: WorkspaceId,
        start_at: SequenceNumber,
        stop: Option<LogReadStop>,
        req_rx: mpsc::UnboundedReceiver<LogReaderRequest>,
        event_tx: mpsc::Sender<LogReaderEvent>,
    ) -> Self {
        Self {
            basin,
            workspace,
            start_at,
            stop,
            req_rx,
            event_tx,
        }
    }

    async fn ensure_stream_exists(
        basin: &S2Basin,
        stream_name: StreamName,
    ) -> Result<(), s2_sdk::types::S2Error> {
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
        message_headers: Vec<Header>,
        object_pointer: ObjectPointer,
    ) -> eyre::Result<SharedMessage> {
        let kind = codec::header_str(&message_headers, "message_kind")?.unwrap_or("unknown");
        info!(
            kind,
            payload_bytes = object_pointer.size_bytes,
            part_count = object_pointer.n_records,
            checksum = object_pointer.checksum,
            "log reader fetching pointer package"
        );

        let stream_name = object_pointer.stream_name(workspace_id)?;
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
            .await?;

        let expected_size = usize::try_from(object_pointer.size_bytes)
            .map_err(|_| eyre::eyre!("multipart payload size does not fit usize"))?;
        let mut buf = BytesMut::with_capacity(expected_size);
        let mut record_count = 0usize;

        while let Some(batch) = read_session.next().await {
            let ReadBatch { records, .. } = batch?;
            for record in records {
                record_count += 1;
                buf.extend_from_slice(&record.body);
            }
        }

        eyre::ensure!(
            record_count == object_pointer.n_records,
            "multipart payload record count mismatch: expected {}, got {}",
            object_pointer.n_records,
            record_count
        );
        eyre::ensure!(
            buf.len() == expected_size,
            "multipart payload size mismatch: expected {}, got {}",
            expected_size,
            buf.len()
        );

        let payload = buf.freeze();
        let checksum = xxh3_64(payload.as_ref());
        eyre::ensure!(
            checksum == object_pointer.checksum,
            "multipart payload checksum mismatch: expected {}, got {}",
            object_pointer.checksum,
            checksum
        );

        info!(
            kind,
            payload_bytes = object_pointer.size_bytes,
            part_count = object_pointer.n_records,
            "log reader reconstructed pointer package"
        );

        codec::s2_payload_to_shared_message(&message_headers, payload)
    }

    pub async fn run(mut self, token: CancellationToken) -> eyre::Result<()> {
        let main_stream_name = StreamName::from_str(&format!("{}/ops", self.workspace.0.as_str()))?;
        Self::ensure_stream_exists(&self.basin, main_stream_name.clone()).await?;
        let stream = self.basin.stream(main_stream_name);

        let mut read_input = ReadInput::new()
            .with_start(ReadStart::new().with_from(ReadFrom::SeqNum(self.start_at)));
        if let Some(stop) = self.stop {
            let read_stop = match stop {
                LogReadStop::UntilTimestampMs(until_ms) => ReadStop::new().with_until(..until_ms),
            };
            read_input = read_input.with_stop(read_stop);
        }
        let mut batches = stream.read_session(read_input).await?;
        let mut next_sequence_number = self.start_at;

        loop {
            tokio::select! {
                _ = token.cancelled() => {
                    debug!("cancelled");

                    return Ok(());
                }

                Some(req) = self.req_rx.recv() => {
                    match req {
                        LogReaderRequest::Status => {
                            let tail = stream.check_tail().await?;
                            self.event_tx.send(LogReaderEvent::Status {
                                tail: ..tail.seq_num,
                            }).await?;
                        }
                    }
                }

                batch = batches.next() => {
                    let Some(batch) = batch else {
                        if self.stop.is_some() {
                            self.event_tx.send(LogReaderEvent::Ended {
                                cursor: ..next_sequence_number,
                            }).await?;
                            token.cancelled().await;
                            return Ok(());
                        }
                        eyre::bail!("log reader read session ended");
                    };
                    let batch = batch?;

                    trace!(
                        record_count = batch.records.len(),
                        tail = ?batch.tail,
                        "log reader batch"
                    );

                    for record in batch.records {
                        let sequence_number = record.seq_num;
                        next_sequence_number = sequence_number.checked_add(1)
                            .ok_or_else(|| eyre::eyre!("log reader sequence number overflow"))?;
                        let timestamp =
                            OffsetDateTime::from_unix_timestamp_nanos(record.timestamp as i128 * 1_000_000)
                                .expect("valid timestamp");
                        let origin = codec::decode_origin(&record.headers)?;

                        let shared_message =
                            if let Some(pointer) = codec::header_value(&record.headers, "pointer") {
                                let object_pointer: ObjectPointer = serde_json::from_slice(pointer)?;
                                Self::read_full_multipart(
                                    self.basin.clone(),
                                    self.workspace.clone(),
                                    record.headers,
                                    object_pointer,
                                )
                                .await?
                            } else if let Some(shared_message) = codec::inline_record_to_shared_message(record)? {
                                shared_message
                            } else {
                                eyre::bail!("invalid log record");
                            };

                        let envelope = SharedMessageEnvelope {
                            timestamp,
                            sequence_number,
                            origin,
                            shared_message
                        };

                        self.event_tx
                            .send(LogReaderEvent::Read(envelope))
                            .await?;
                    }
                }

                else => {
                    return Ok(());
                }

            }
        }
    }
}
