pub mod daemon_state;
pub mod import_staged_files;
pub mod objects;
pub mod outbox;
pub mod prior_namespace;
pub mod prior_text_objects;
pub mod prior_tree;
pub mod stable_namespace;
pub mod stable_text_objects;
pub mod stable_tree;

fn datetime_from_unix_ns(field: &str, ns: i64) -> eyre::Result<time::OffsetDateTime> {
    time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(ns))
        .map_err(|err| eyre::eyre!("{field} invalid unix timestamp ns {ns}: {err}"))
}

pub(crate) fn datetime_to_unix_ns(
    field: &str,
    datetime: time::OffsetDateTime,
) -> eyre::Result<i64> {
    i64::try_from(datetime.unix_timestamp_nanos())
        .map_err(|err| eyre::eyre!("{field} unix timestamp ns out of range: {err}"))
}
