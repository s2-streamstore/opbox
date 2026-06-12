//! Namespace CRDT wrapper: one global Y.Doc per workspace.
//!
//! opbox v0 uses full relative file paths in namespace claims. Directories are
//! structural path prefixes only; empty directories are not synced.

use crate::crdt::types::{NamespaceClaimId, ObjectId, ObjectKind};
use crate::fs::types::RelativePath;
use crate::types::DaemonWriterId;
use bytes::Bytes;
use eyre::{Result, WrapErr};
use fast32::base32;
use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::trace;
use yrs::block::ClientID;
use yrs::types::map::MapRef;
use yrs::updates::decoder::Decode;
use yrs::{Any, Doc, Map, MapPrelim, Options, ReadTxn, StateVector, Transact, Update};

pub use super::client_id_for_writer;

/// Wrapper around the global namespace Y.Doc.
#[derive(Debug)]
pub struct NamespaceDoc {
    doc: Doc,
    objects: MapRef,
    claims: MapRef,
    removed_claims: MapRef,
}

/// Materialized object metadata from the namespace doc.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    pub kind: ObjectKind,
    pub creator_writer_id: DaemonWriterId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimRecord {
    pub object_id: ObjectId,
    pub path: RelativePath,
}

/// An active claim: present in claims and absent from removed_claims.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveClaim {
    pub claim_id: NamespaceClaimId,
    pub record: ClaimRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfirmedObject {
    pub object_id: ObjectId,
    pub meta: ObjectMeta,
}

/// A deterministic filesystem projection derived from one namespace claim.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DesiredPlacement {
    pub path: RelativePath,
    pub claimed_path: RelativePath,
    pub claim_id: NamespaceClaimId,
    pub object_id: ObjectId,
    pub kind: ObjectKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementDelta {
    pub added: Vec<DesiredPlacement>,
    pub removed: Vec<DesiredPlacement>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceProjection {
    pub confirmed_objects: Vec<ConfirmedObject>,
    pub placements: Vec<DesiredPlacement>,
}

#[derive(Debug, Clone)]
struct MaterializationClaim {
    claim_id: NamespaceClaimId,
    object_id: ObjectId,
    path: RelativePath,
    meta: ObjectMeta,
}

impl NamespaceDoc {
    const OBJECTS_MAP: &'static str = "objects";
    const CLAIMS_MAP: &'static str = "claims";
    const REMOVED_CLAIMS_MAP: &'static str = "removed_claims";

    const FIELD_KIND: &'static str = "k";
    const FIELD_CREATOR: &'static str = "c";

    const FIELD_OBJECT_ID: &'static str = "o";
    const FIELD_PATH: &'static str = "p";

    pub fn new(client_id: ClientID) -> Self {
        let doc = Doc::with_options(Options {
            client_id,
            skip_gc: false,
            ..Default::default()
        });

        Self::from_doc(doc)
    }

    fn from_doc(doc: Doc) -> Self {
        let objects = doc.get_or_insert_map(Self::OBJECTS_MAP);
        let claims = doc.get_or_insert_map(Self::CLAIMS_MAP);
        let removed_claims = doc.get_or_insert_map(Self::REMOVED_CLAIMS_MAP);
        Self {
            doc,
            objects,
            claims,
            removed_claims,
        }
    }

    pub fn from_full_state(client_id: ClientID, state: impl AsRef<[u8]>) -> Result<Self> {
        let state = state.as_ref();
        if state.is_empty() {
            return Ok(Self::new(client_id));
        }

        let doc = Doc::with_options(Options {
            client_id,
            skip_gc: false,
            ..Default::default()
        });
        {
            // V2 is required: V1 truncates client ids to u32 during decode.
            let update = Update::decode_v2(state).wrap_err("namespace: decode full state")?;
            let mut txn = doc.transact_mut();
            txn.apply_update(update)
                .wrap_err("namespace: apply full state")?;
        }

        Ok(Self::from_doc(doc))
    }

    pub fn add_new_object(&self, object_id: &ObjectId, kind: ObjectKind, creator: &DaemonWriterId) {
        let mut txn = self.doc.transact_mut();
        let object_key = object_id.encode_b64();
        let kind: &'static str = kind.into();
        let nested = self
            .objects
            .insert(&mut txn, object_key.clone(), MapPrelim::default());
        // MapPrelim is backed by HashMap; insert nested fields explicitly so
        // full-state bytes are deterministic under trace-level DST comparison.
        nested.insert(&mut txn, Self::FIELD_CREATOR, Any::from(creator.0.to_vec()));
        nested.insert(&mut txn, Self::FIELD_KIND, Any::from(kind));

        trace!(object_id = %object_key, ?kind, "namespace: added object");
    }

    pub fn set_object_kind(&self, object_id: &ObjectId, kind: ObjectKind) -> Result<()> {
        let mut txn = self.doc.transact_mut();
        let object_key = object_id.encode_b64();
        let Some(value) = self.objects.get(&txn, &object_key) else {
            return Err(eyre::eyre!(
                "namespace object {} missing during kind update",
                object_id.encode_b64()
            ));
        };

        let yrs::Out::YMap(map) = value else {
            return Err(eyre::eyre!(
                "namespace object {} had non-map metadata during kind update",
                object_id.encode_b64()
            ));
        };

        let kind: &'static str = kind.into();
        map.insert(&mut txn, Self::FIELD_KIND, Any::from(kind));
        trace!(object_id = %object_key, ?kind, "namespace: updated object kind");
        Ok(())
    }

    /// Add an immutable placement claim.
    pub fn add_new_claim(
        &self,
        claim_id: &NamespaceClaimId,
        object_id: &ObjectId,
        path: &RelativePath,
    ) {
        let mut txn = self.doc.transact_mut();
        let claim_key = claim_id.encode_b64();

        let nested = self
            .claims
            .insert(&mut txn, claim_key.clone(), MapPrelim::default());
        // See add_new_object: avoid HashMap iteration order in MapPrelim.
        nested.insert(
            &mut txn,
            Self::FIELD_OBJECT_ID,
            Any::from(object_id.0.to_vec()),
        );
        nested.insert(&mut txn, Self::FIELD_PATH, Any::from(path.to_db_path()));

        trace!(claim_id = %claim_key, path = %path, "namespace: added claim");
    }

    /// Mark a claim as removed by claim id.
    pub fn remove_claim(&self, claim_id: &NamespaceClaimId) {
        let mut txn = self.doc.transact_mut();
        let claim_key = claim_id.encode_b64();
        self.removed_claims
            .insert(&mut txn, claim_key.clone(), Any::from(1i64));

        trace!(claim_id = %claim_key, "namespace: removed claim");
    }

    pub fn get_object(&self, object_id: &ObjectId) -> Option<ObjectMeta> {
        let txn = self.doc.transact();
        let object_key = object_id.encode_b64();
        let value = self.objects.get(&txn, &object_key)?;
        object_meta_from_out(&txn, value)
    }

    pub fn get_claim(&self, claim_id: &NamespaceClaimId) -> Option<ClaimRecord> {
        let txn = self.doc.transact();
        let claim_key = claim_id.encode_b64();
        let value = self.claims.get(&txn, &claim_key)?;
        claim_record_from_out(&txn, value)
    }

    pub fn is_claim_removed(&self, claim_id: &NamespaceClaimId) -> bool {
        let txn = self.doc.transact();
        let claim_key = claim_id.encode_b64();
        self.removed_claims.get(&txn, claim_key.as_str()).is_some()
    }

    pub fn active_claims(&self) -> Result<Vec<ActiveClaim>> {
        let txn = self.doc.transact();
        let mut result = Vec::new();

        for (claim_key, value) in self.claims.iter(&txn) {
            if self.removed_claims.get(&txn, claim_key).is_some() {
                continue;
            }

            let Some(record) = claim_record_from_out(&txn, value) else {
                return Err(eyre::eyre!(
                    "namespace claim {claim_key:?} has invalid record"
                ));
            };
            let claim_id = NamespaceClaimId::decode_b64(claim_key).ok_or_else(|| {
                eyre::eyre!("namespace claim key is not valid b64: {claim_key:?}")
            })?;

            result.push(ActiveClaim { claim_id, record });
        }

        Ok(result)
    }

    pub fn all_object_ids(&self) -> Result<Vec<ObjectId>> {
        let txn = self.doc.transact();
        self.objects
            .iter(&txn)
            .map(|(key, _)| {
                ObjectId::decode_b64(key)
                    .ok_or_else(|| eyre::eyre!("namespace object key is not valid b64: {key:?}"))
            })
            .collect()
    }

    pub fn encode_full_state(&self) -> Bytes {
        let txn = self.doc.transact();
        Bytes::from(txn.encode_state_as_update_v2(&StateVector::default()))
    }

    pub fn state_vector(&self) -> StateVector {
        let txn = self.doc.transact();
        txn.state_vector()
    }

    pub fn encode_update_since(&self, sv: &StateVector) -> Bytes {
        let txn = self.doc.transact();
        Bytes::from(txn.encode_state_as_update_v2(sv))
    }

    pub fn apply_update(&self, update_bytes: &[u8]) -> Result<()> {
        let update = Update::decode_v2(update_bytes).wrap_err("namespace: decode update")?;
        let mut txn = self.doc.transact_mut();
        txn.apply_update(update)
            .wrap_err("namespace: apply update")?;
        trace!("namespace: applied update ({} bytes)", update_bytes.len());
        Ok(())
    }

    pub fn materialize(&self) -> Result<NamespaceProjection> {
        materialize_namespace(self)
    }
}

static READ_ONLY_CLIENT_ID_COUNTER: AtomicU64 = AtomicU64::new(20_000_000);

/// Client id for docs that only decode/apply/encode existing state. A doc's
/// client id never appears in encoded output unless ops are generated under
/// it, so these ids are safe ONLY for paths that never mint ops. Any local
/// mutation that will be published must use [`client_id_for_writer`], or the
/// resulting op ids can collide across daemons and silently diverge.
fn read_only_client_id() -> u64 {
    READ_ONLY_CLIENT_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
}

pub fn empty_namespace_state(client_id: u64) -> Bytes {
    NamespaceDoc::new(client_id).encode_full_state()
}

pub fn apply_namespace_update(base_state: &[u8], update_bytes: &[u8]) -> Result<Bytes> {
    let doc = NamespaceDoc::from_full_state(read_only_client_id(), base_state)?;
    doc.apply_update(update_bytes)?;
    Ok(doc.encode_full_state())
}

pub fn encode_state_delta(base_state: &[u8], next_state: &[u8]) -> Result<Bytes> {
    let base_doc = NamespaceDoc::from_full_state(read_only_client_id(), base_state)?;
    let next_doc = NamespaceDoc::from_full_state(read_only_client_id(), next_state)?;
    Ok(next_doc.encode_update_since(&base_doc.state_vector()))
}

pub fn materialize_namespace(doc: &NamespaceDoc) -> Result<NamespaceProjection> {
    let confirmed_objects = confirmed_objects_from_doc(doc)?;
    let mut claims = collect_materialization_claims(doc)?;
    sort_claims_deterministically(&mut claims);
    let placements = derive_visible_placements(claims)?;

    Ok(NamespaceProjection {
        confirmed_objects,
        placements,
    })
}

pub fn placement_delta(previous: &[DesiredPlacement], next: &[DesiredPlacement]) -> PlacementDelta {
    let previous = previous.iter().cloned().collect::<BTreeSet<_>>();
    let next = next.iter().cloned().collect::<BTreeSet<_>>();

    let removed = previous
        .iter()
        .filter(|placement| !next.contains(*placement))
        .cloned()
        .collect();

    let added = next
        .iter()
        .filter(|placement| !previous.contains(*placement))
        .cloned()
        .collect();

    PlacementDelta { added, removed }
}

fn confirmed_objects_from_doc(doc: &NamespaceDoc) -> Result<Vec<ConfirmedObject>> {
    let mut object_ids = doc.all_object_ids()?;
    object_ids.sort();

    object_ids
        .into_iter()
        .map(|object_id| {
            let meta = doc.get_object(&object_id).ok_or_else(|| {
                eyre::eyre!(
                    "namespace object {} missing metadata during materialization",
                    object_id.encode_b64()
                )
            })?;
            Ok(ConfirmedObject { object_id, meta })
        })
        .collect()
}

fn collect_materialization_claims(doc: &NamespaceDoc) -> Result<Vec<MaterializationClaim>> {
    doc.active_claims()?
        .into_iter()
        .map(|claim| {
            let meta = doc.get_object(&claim.record.object_id).ok_or_else(|| {
                eyre::eyre!(
                    "namespace claim {} references object {} without metadata",
                    claim.claim_id.encode_b64(),
                    claim.record.object_id.encode_b64()
                )
            })?;

            Ok(MaterializationClaim {
                claim_id: claim.claim_id,
                object_id: claim.record.object_id,
                path: claim.record.path,
                meta,
            })
        })
        .collect()
}

fn sort_claims_deterministically(claims: &mut [MaterializationClaim]) {
    claims.sort_by(|a, b| {
        (
            &a.path,
            &a.meta.creator_writer_id,
            &a.object_id,
            &a.claim_id,
        )
            .cmp(&(
                &b.path,
                &b.meta.creator_writer_id,
                &b.object_id,
                &b.claim_id,
            ))
    });
}

fn derive_visible_placements(claims: Vec<MaterializationClaim>) -> Result<Vec<DesiredPlacement>> {
    let mut claims_by_requested_path = BTreeMap::<RelativePath, Vec<MaterializationClaim>>::new();

    for claim in claims {
        claims_by_requested_path
            .entry(claim.path.clone())
            .or_default()
            .push(claim);
    }

    let mut placements = Vec::new();
    let mut occupied_paths = BTreeSet::<RelativePath>::new();
    let mut losing_claims = Vec::new();

    for (requested_path, mut requested_claims) in claims_by_requested_path {
        debug_assert!(!requested_claims.is_empty());
        requested_claims.sort_by(|a, b| {
            (&a.meta.creator_writer_id, &a.object_id, &a.claim_id).cmp(&(
                &b.meta.creator_writer_id,
                &b.object_id,
                &b.claim_id,
            ))
        });

        let winner = requested_claims.remove(0);
        occupied_paths.insert(requested_path.clone());
        placements.push(DesiredPlacement {
            path: requested_path.clone(),
            claimed_path: requested_path,
            claim_id: winner.claim_id,
            object_id: winner.object_id,
            kind: winner.meta.kind,
        });
        losing_claims.extend(requested_claims);
    }

    for claim in losing_claims {
        let conflict_path = next_available_conflict_path(&claim, &occupied_paths)?;
        occupied_paths.insert(conflict_path.clone());
        placements.push(DesiredPlacement {
            path: conflict_path,
            claimed_path: claim.path,
            claim_id: claim.claim_id,
            object_id: claim.object_id,
            kind: claim.meta.kind,
        });
    }

    placements.sort();
    Ok(placements)
}

fn next_available_conflict_path(
    claim: &MaterializationClaim,
    occupied_paths: &BTreeSet<RelativePath>,
) -> Result<RelativePath> {
    let object_suffix = base32::CROCKFORD_LOWER.encode(claim.object_id.0.as_ref());
    let writer_suffix = base32::CROCKFORD_LOWER.encode(claim.meta.creator_writer_id.0.as_ref());
    let claim_suffix = base32::CROCKFORD_LOWER.encode(claim.claim_id.0.as_ref());

    let mut suffixes = vec![
        writer_suffix[..6.min(writer_suffix.len())].to_string(),
        object_suffix[..6.min(object_suffix.len())].to_string(),
        object_suffix[..8.min(object_suffix.len())].to_string(),
        object_suffix.clone(),
        format!(
            "{object_suffix}-{}",
            &claim_suffix[..6.min(claim_suffix.len())]
        ),
    ];
    suffixes.dedup();

    for suffix in suffixes {
        let candidate_name = format_conflict_name(claim.path.file_name(), &suffix);
        let candidate = claim.path.with_file_name(candidate_name.into())?;
        if !occupied_paths.contains(&candidate) {
            return Ok(candidate);
        }
    }

    Err(eyre::eyre!(
        "unable to synthesize conflict path for object {} from requested path {}",
        claim.object_id.encode_b64(),
        claim.path
    ))
}

fn format_conflict_name(requested_name: &str, suffix: &str) -> String {
    let (stem, ext) = split_filename_for_conflict(requested_name);
    format!("{stem} (conflict {suffix}){ext}")
}

fn split_filename_for_conflict(name: &str) -> (String, String) {
    match name.rsplit_once('.') {
        Some((stem, ext)) if !stem.is_empty() => (stem.to_string(), format!(".{ext}")),
        _ => (name.to_string(), String::new()),
    }
}

fn object_meta_from_out<T: ReadTxn>(txn: &T, value: yrs::Out) -> Option<ObjectMeta> {
    match value {
        yrs::Out::YMap(map_ref) => {
            let kind_val = map_ref.get(txn, NamespaceDoc::FIELD_KIND)?;
            let kind_str = kind_val.to_string(txn);
            let kind = ObjectKind::from_str(&kind_str).ok()?;

            let creator_writer_id = DaemonWriterId(any_to_bytes(
                &map_ref.get(txn, NamespaceDoc::FIELD_CREATOR)?,
            )?);

            Some(ObjectMeta {
                kind,
                creator_writer_id,
            })
        }
        yrs::Out::Any(Any::Map(entries)) => {
            let kind = entries
                .get(NamespaceDoc::FIELD_KIND)
                .and_then(any_value_to_str)
                .and_then(|kind| ObjectKind::from_str(&kind).ok())?;
            let creator_writer_id = DaemonWriterId(
                entries
                    .get(NamespaceDoc::FIELD_CREATOR)
                    .and_then(any_value_to_bytes)?,
            );

            Some(ObjectMeta {
                kind,
                creator_writer_id,
            })
        }
        _ => None,
    }
}

fn claim_record_from_out<T: ReadTxn>(txn: &T, value: yrs::Out) -> Option<ClaimRecord> {
    match value {
        yrs::Out::YMap(map_ref) => {
            let object_id = ObjectId(any_to_bytes(
                &map_ref.get(txn, NamespaceDoc::FIELD_OBJECT_ID)?,
            )?);
            let path_val = map_ref.get(txn, NamespaceDoc::FIELD_PATH)?;
            let path = RelativePath::parse(&path_val.to_string(txn)).ok()?;

            Some(ClaimRecord { object_id, path })
        }
        yrs::Out::Any(Any::Map(entries)) => {
            let object_id = ObjectId(
                entries
                    .get(NamespaceDoc::FIELD_OBJECT_ID)
                    .and_then(any_value_to_bytes)?,
            );
            let path = entries
                .get(NamespaceDoc::FIELD_PATH)
                .and_then(any_value_to_str)
                .and_then(|path| RelativePath::parse(&path).ok())?;

            Some(ClaimRecord { object_id, path })
        }
        _ => None,
    }
}

fn any_to_bytes(val: &yrs::Out) -> Option<Bytes> {
    match val {
        yrs::Out::Any(Any::Buffer(buf)) => Some(Bytes::copy_from_slice(buf)),
        _ => None,
    }
}

fn any_value_to_bytes(val: &Any) -> Option<Bytes> {
    match val {
        Any::Buffer(buf) => Some(Bytes::copy_from_slice(buf)),
        _ => None,
    }
}

fn any_value_to_str(val: &Any) -> Option<String> {
    match val {
        Any::String(value) => Some(value.to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn object_id(byte: u8) -> ObjectId {
        ObjectId(Bytes::from(vec![byte; 16]))
    }

    fn claim_id(byte: u8) -> NamespaceClaimId {
        NamespaceClaimId(Bytes::from(vec![byte; 16]))
    }

    fn writer_id(byte: u8) -> DaemonWriterId {
        DaemonWriterId(Bytes::from(vec![byte; 16]))
    }

    #[test]
    fn materializes_same_path_conflicts_deterministically() -> Result<()> {
        let doc = NamespaceDoc::new(1);
        let winner = object_id(1);
        let loser = object_id(2);
        let winner_claim = claim_id(11);
        let loser_claim = claim_id(12);
        let path = RelativePath::parse("notes/a.txt")?;

        doc.add_new_object(&winner, ObjectKind::Text, &writer_id(1));
        doc.add_new_object(&loser, ObjectKind::Text, &writer_id(2));
        doc.add_new_claim(&winner_claim, &winner, &path);
        doc.add_new_claim(&loser_claim, &loser, &path);

        let projection = doc.materialize()?;
        assert_eq!(projection.placements.len(), 2);
        assert!(projection.placements.iter().any(|placement| {
            placement.object_id == winner
                && placement.path == path
                && placement.claimed_path == path
                && placement.claim_id == winner_claim
        }));
        assert!(projection.placements.iter().any(|placement| {
            placement.object_id == loser
                && placement.claimed_path == path
                && placement.path.to_string().starts_with("notes/a (conflict ")
                && placement.path.to_string().ends_with(").txt")
                && placement.claim_id == loser_claim
        }));

        Ok(())
    }

    #[test]
    fn namespace_update_roundtrips() -> Result<()> {
        let base = NamespaceDoc::new(1);
        let sv = base.state_vector();
        let next = NamespaceDoc::from_full_state(2, base.encode_full_state())?;
        let object_id = object_id(42);
        let claim_id = claim_id(43);
        let path = RelativePath::parse("hello.txt")?;

        next.add_new_object(&object_id, ObjectKind::Text, &writer_id(7));
        next.add_new_claim(&claim_id, &object_id, &path);
        let update = next.encode_update_since(&sv);

        base.apply_update(&update)?;
        let projection = base.materialize()?;
        assert_eq!(projection.placements.len(), 1);
        assert_eq!(projection.placements[0].path, path);

        Ok(())
    }
}
