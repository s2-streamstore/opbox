use crate::crdt::types::SharedMessage;
use crate::types::{DaemonWriterId, OutboxId};
use std::ops::{RangeTo, RangeToInclusive};
use time::OffsetDateTime;

pub type SequenceNumber = u64;

pub const LOG_READER_EVENT_CHANNEL_CAPACITY: usize = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogReadStop {
    UntilTimestampMs(u64),
}

pub enum LogReaderRequest {
    Status,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedMessageOrigin {
    pub daemon_writer_id: DaemonWriterId,
    pub outbox_id: OutboxId,
}

#[derive(Debug)]
pub struct SharedMessageEnvelope {
    pub timestamp: OffsetDateTime,
    pub sequence_number: SequenceNumber,
    pub origin: SharedMessageOrigin,
    pub shared_message: SharedMessage,
}

#[derive(Debug)]
pub enum LogReaderEvent {
    Status { tail: RangeTo<SequenceNumber> },
    Read(SharedMessageEnvelope),
    Ended { cursor: RangeTo<SequenceNumber> },
}

pub enum LogWriterRequest {
    Status,
    Append {
        outbox_id: OutboxId,
        shared_message: SharedMessage,
    },
}

pub enum LogWriterResponse {
    Ping,
    Durable {
        outbox_range: RangeToInclusive<OutboxId>,
    },
}
