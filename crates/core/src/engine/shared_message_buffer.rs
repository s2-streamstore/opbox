use crate::log::types::{SequenceNumber, SharedMessageEnvelope};
use std::time::Duration;
use tokio::time::Instant;

// Can overshoot.
const MAX_SIZE_BYTES: usize = 1024 * 1024 * 10;

pub struct SharedMessageBuffer {
    max_coalesce: Duration,
    size_bytes: usize,
    earliest_timestamp: Option<Instant>,
    pub envelopes: Vec<SharedMessageEnvelope>,
}

impl SharedMessageBuffer {
    pub fn new(max_coalesce: Duration) -> Self {
        Self {
            max_coalesce,
            size_bytes: 0,
            earliest_timestamp: None,
            envelopes: Vec::new(),
        }
    }

    pub fn insert(&mut self, envelope: SharedMessageEnvelope) {
        if self.earliest_timestamp.is_none() {
            self.earliest_timestamp = Some(Instant::now());
        }
        self.size_bytes += envelope.shared_message.approximate_size_bytes();
        self.envelopes.push(envelope);
    }

    pub fn has_capacity(&self) -> bool {
        self.size_bytes < MAX_SIZE_BYTES
    }

    pub fn is_empty(&self) -> bool {
        self.envelopes.is_empty()
    }

    pub fn last_sequence_end(&self) -> Option<SequenceNumber> {
        self.envelopes
            .last()
            .map(|envelope| envelope.sequence_number + 1)
    }

    pub fn next_fire_at(&self) -> Option<Instant> {
        if self.has_capacity() {
            self.earliest_timestamp
                .map(|earliest| earliest + self.max_coalesce)
        } else {
            Some(Instant::now())
        }
    }

    /// Reset the buffer to empty, preserving the configured coalescing window.
    /// Returns the drained envelopes.
    pub fn drain(&mut self) -> Vec<SharedMessageEnvelope> {
        self.size_bytes = 0;
        self.earliest_timestamp = None;
        std::mem::take(&mut self.envelopes)
    }
}
