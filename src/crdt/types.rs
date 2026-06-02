use crate::types::DaemonWriterId;
use base64::Engine;
use bytes::Bytes;
use strum;
use time::OffsetDateTime;

/// Stable identity for a synced object (file or directory).
/// Never reused; a recreated path gets a new ObjectId.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectId(pub Bytes);

impl ObjectId {
    pub fn encode_b64(&self) -> String {
        base64::engine::general_purpose::STANDARD.encode(self.0.as_ref())
    }

    pub fn decode_b64(s: &str) -> Option<Self> {
        base64::engine::general_purpose::STANDARD
            .decode(s)
            .ok()
            .map(Bytes::from)
            .map(Self)
    }

    pub fn generate() -> Self {
        let bytes = rand::random::<[u8; 16]>();
        Self(Bytes::from_owner(bytes))
    }
}

/// Stable identity for an immutable namespace placement claim.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NamespaceClaimId(pub Bytes);

impl NamespaceClaimId {
    pub fn encode_b64(&self) -> String {
        base64::engine::general_purpose::STANDARD.encode(self.0.as_ref())
    }

    pub fn decode_b64(s: &str) -> Option<Self> {
        base64::engine::general_purpose::STANDARD
            .decode(s)
            .ok()
            .map(Bytes::from)
            .map(Self)
    }

    pub fn generate() -> Self {
        let bytes = rand::random::<[u8; 16]>();
        Self(Bytes::from_owner(bytes))
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    strum::IntoStaticStr,
    strum::EnumString,
)]
pub enum ObjectKind {
    #[strum(serialize = "text")]
    Text,
    #[strum(serialize = "binary")]
    Binary,
    #[strum(serialize = "dir")]
    Dir,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    strum::EnumString,
    strum::IntoStaticStr,
)]
pub enum SharedMessageKind {
    #[strum(serialize = "namespace_update")]
    NamespaceUpdate,
    #[strum(serialize = "text_update")]
    TextUpdate,
    #[strum(serialize = "binary_put")]
    BinaryPut,
}

/// The CRDT operation stored in our shared log.
#[derive(Clone, strum::IntoStaticStr, derivative::Derivative)]
#[derivative(Debug)]
pub enum SharedMessage {
    /// Yjs update bytes for the global namespace doc.
    #[strum(serialize = "namespace_update")]
    NamespaceUpdate {
        #[derivative(Debug = "ignore")]
        yjs_update: Bytes,
    },

    /// Yjs update bytes for a specific text object's doc.
    #[strum(serialize = "text_update")]
    TextObjectUpdate {
        object_id: ObjectId,
        #[derivative(Debug = "ignore")]
        yjs_update: Bytes,
    },

    /// LWW binary blob put.
    #[strum(serialize = "binary_put")]
    BinaryObjectPut {
        object_id: ObjectId,
        wall_time: OffsetDateTime,
        writer_id: DaemonWriterId,
        #[derivative(Debug = "ignore")]
        blob: Bytes,
    },
}
impl SharedMessage {
    pub fn kind(&self) -> SharedMessageKind {
        match self {
            Self::NamespaceUpdate { .. } => SharedMessageKind::NamespaceUpdate,
            Self::TextObjectUpdate { .. } => SharedMessageKind::TextUpdate,
            Self::BinaryObjectPut { .. } => SharedMessageKind::BinaryPut,
        }
    }

    pub fn payload(&self) -> &Bytes {
        match self {
            Self::NamespaceUpdate { yjs_update } => yjs_update,
            Self::TextObjectUpdate { yjs_update, .. } => yjs_update,
            Self::BinaryObjectPut { blob, .. } => blob,
        }
    }

    pub fn approximate_size_bytes(&self) -> usize {
        match self {
            Self::NamespaceUpdate { yjs_update } => yjs_update.len(),
            Self::TextObjectUpdate { yjs_update, .. } => yjs_update.len(),
            Self::BinaryObjectPut { blob, .. } => blob.len(),
        }
    }

    // pub fn from_row(row: outbox::Row) -> eyre::Result<(OutboxId, SharedMessage)> {
    //     let outbox::Row {
    //         outbox_id,
    //         kind,
    //         object_id,
    //         wall_time,
    //         writer_id,
    //         payload,
    //         created_at: _,
    //     } = row;
    //
    //     let msg = match kind {
    //         SharedMessageKind::NamespaceUpdate => SharedMessage::NamespaceUpdate {
    //             yjs_update: payload,
    //         },
    //         SharedMessageKind::TextUpdate => SharedMessage::TextObjectUpdate {
    //             object_id: object_id
    //                 .ok_or_else(|| eyre::eyre!("text outbox row missing object_id"))?,
    //             yjs_update: payload,
    //         },
    //         SharedMessageKind::BinaryPut => SharedMessage::BinaryObjectPut {
    //             object_id: object_id
    //                 .ok_or_else(|| eyre::eyre!("binary outbox row missing object_id"))?,
    //             wall_time: wall_time
    //                 .ok_or_else(|| eyre::eyre!("binary outbox row missing wall_time"))?,
    //             writer_id: writer_id
    //                 .ok_or_else(|| eyre::eyre!("binary outbox row missing writer_id"))?,
    //             blob: payload,
    //         },
    //     };
    //
    //     Ok((outbox_id, msg))
    // }
}
