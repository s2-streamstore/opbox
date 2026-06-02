use crate::crdt::types::SharedMessage;
use crate::types::OutboxId;
use std::ops::{RangeTo, RangeToInclusive};
use time::OffsetDateTime;

pub type SequenceNumber = u64;

pub const LOG_READER_EVENT_CHANNEL_CAPACITY: usize = 10;

pub enum LogReaderRequest {
    Status,
}

pub struct SharedMessageEnvelope {
    pub timestamp: OffsetDateTime,
    pub sequence_number: SequenceNumber,
    pub shared_message: SharedMessage,
}

pub enum LogReaderEvent {
    Status { tail: RangeTo<SequenceNumber> },
    Read(SharedMessageEnvelope),
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
