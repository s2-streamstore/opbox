use crate::crdt::namespace;
use crate::semantic::service::TursoConnectionManager;
use crate::semantic::table::daemon_state;
use eyre::eyre;
use std::path::Path;
use turso::{Builder, Connection, Database};

pub async fn open_database(path: &Path) -> eyre::Result<Database> {
    Ok(Builder::new_local(path_to_str(path)?).build().await?)
}

pub async fn open_memory_database() -> eyre::Result<Database> {
    Ok(Builder::new_local(":memory:").build().await?)
}

pub async fn configure_connection(conn: &Connection) -> eyre::Result<()> {
    conn.pragma_update("journal_mode", "'mvcc'").await?;
    Ok(())
}

pub async fn create_initialized_database(
    db_path: &Path,
    daemon_row: &daemon_state::Row,
) -> eyre::Result<()> {
    ensure_parent_dir(db_path)?;

    let db = open_database(db_path).await?;
    initialize_database(&db, daemon_row).await
}

pub async fn initialize_database(
    db: &Database,
    daemon_row: &daemon_state::Row,
) -> eyre::Result<()> {
    let conn = db.connect()?;
    configure_connection(&conn).await?;
    conn.execute_batch(include_str!("../semantic/table/sql/schema.sql"))
        .await?;
    insert_daemon_state(&conn, daemon_row).await?;
    insert_empty_namespaces(&conn).await?;

    Ok(())
}

pub async fn load_daemon_state(db_path: &Path) -> eyre::Result<daemon_state::Row> {
    let db = open_database(db_path).await?;
    let conn = db.connect()?;
    configure_connection(&conn).await?;
    let mut rows = conn
        .query(
            "SELECT workspace_id, s2_basin, writer_id, stable_cursor, next_outbox_id,
                    s2_account_endpoint, s2_basin_endpoint, encryption_key
             FROM daemon_state
             WHERE id = 1",
            (),
        )
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| eyre!("daemon_state row missing"))?;

    daemon_state::Row::from_sql_row(&row)
}

/// Heuristic for a workspace whose `init` crashed before finishing: a
/// successful init drains the outbox before reporting success, so a stable
/// cursor still at 0 with outbox rows means the bootstrap never completed.
/// (A previously synced workspace with a backlog has a nonzero cursor.)
pub async fn init_appears_incomplete(db_path: &Path) -> eyre::Result<bool> {
    let db = open_database(db_path).await?;
    let conn = db.connect()?;
    configure_connection(&conn).await?;

    let mut rows = conn
        .query("SELECT stable_cursor FROM daemon_state WHERE id = 1", ())
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(false);
    };
    if row.get::<i64>(0)? != 0 {
        return Ok(false);
    }

    let mut rows = conn.query("SELECT COUNT(*) FROM outbox", ()).await?;
    let outbox_count = match rows.next().await? {
        Some(row) => row.get::<i64>(0)?,
        None => 0,
    };
    Ok(outbox_count > 0)
}

pub async fn clear_startup_scratch_tables(conn: &Connection) -> eyre::Result<()> {
    conn.execute("DELETE FROM import_staged_files", ()).await?;
    conn.execute("UPDATE outbox SET inflight = 0", ()).await?;
    Ok(())
}

pub async fn semantic_pool(db: Database) -> eyre::Result<bb8::Pool<TursoConnectionManager>> {
    let pool = bb8::Pool::builder()
        .build(TursoConnectionManager::new(db))
        .await?;
    {
        let conn = pool.get().await?;
        configure_connection(&conn).await?;
        // Schema DDL is IF NOT EXISTS throughout, so this creates tables added
        // after the workspace was initialized.
        conn.execute_batch(include_str!("../semantic/table/sql/schema.sql"))
            .await?;
        clear_startup_scratch_tables(&conn).await?;
    }
    Ok(pool)
}

pub async fn insert_empty_namespaces(conn: &Connection) -> eyre::Result<()> {
    let now = time::OffsetDateTime::now_utc();
    let now_ns = i64::try_from(now.unix_timestamp_nanos())
        .map_err(|err| eyre!("namespace timestamp out of range: {err}"))?;
    let namespace_blob = namespace::empty_namespace_state(1);

    conn.execute(
        "INSERT INTO stable_namespace (id, doc_blob, updated_at_ns)
         VALUES (1, ?1, ?2)",
        (namespace_blob.as_ref(), now_ns),
    )
    .await?;
    conn.execute(
        "INSERT INTO prior_namespace (id, doc_blob, accepted_at_ns)
         VALUES (1, ?1, ?2)",
        (namespace_blob.as_ref(), now_ns),
    )
    .await?;

    Ok(())
}

pub async fn insert_daemon_state(conn: &Connection, row: &daemon_state::Row) -> eyre::Result<()> {
    let stable_cursor = i64::try_from(row.stable_cursor.end)
        .map_err(|_| eyre!("stable_cursor out of range: {}", row.stable_cursor.end))?;
    let next_outbox_id = i64::try_from(row.next_outbox_id.get())
        .map_err(|_| eyre!("next_outbox_id out of range: {}", row.next_outbox_id.get()))?;

    let encryption_key_str = row
        .encryption_key
        .as_ref()
        .map(|k| k.to_header_value().to_str().unwrap().to_owned());

    conn.execute(
        "INSERT INTO daemon_state (
            id,
            workspace_id,
            s2_basin,
            s2_account_endpoint,
            s2_basin_endpoint,
            writer_id,
            stable_cursor,
            next_outbox_id,
            encryption_key
        ) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        (
            row.workspace_id.0.as_str(),
            row.s2_basin.as_ref(),
            row.s2_account_endpoint.as_deref(),
            row.s2_basin_endpoint.as_deref(),
            row.daemon_writer_id.0.as_ref(),
            stable_cursor,
            next_outbox_id,
            encryption_key_str.as_deref(),
        ),
    )
    .await?;

    Ok(())
}

fn path_to_str(path: &Path) -> eyre::Result<&str> {
    path.to_str()
        .ok_or_else(|| eyre!("path is not valid utf-8: {}", path.display()))
}

fn ensure_parent_dir(path: &Path) -> eyre::Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    Ok(())
}
