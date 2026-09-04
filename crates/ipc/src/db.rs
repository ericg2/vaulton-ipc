use crate::core::{UserSystem, VfsError, VfsPoint, VfsResult, VfsUser};
use async_trait::async_trait;
use opendal_core::ErrorKind;
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqliteSynchronous};
use std::path::Path;
use std::str::FromStr;

use opendal_vfs::layers::quota::*;

/// SQLite-backed implementation of [`VfsStore`] and [`QuotaTracker`].
///
/// Uses a connection pool in WAL journal mode so concurrent async tasks can
/// read simultaneously while writes are serialised — ideal for a read-heavy
/// VFS workload.
#[derive(Debug)]
pub struct DbManager {
    pool: SqlitePool,
}

impl DbManager {
    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    /// Open (or create) the SQLite database at `path` and apply schema
    /// migrations.
    ///
    /// Pass `":memory:"` for an in-process database useful in tests.
    pub async fn open(path: impl AsRef<Path>) -> VfsResult<Self> {
        let path_str = path
            .as_ref()
            .to_str()
            .expect("database path must be valid UTF-8");

        let options = SqliteConnectOptions::from_str(path_str)?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(std::time::Duration::from_secs(5));

        let pool = SqlitePool::connect_with(options).await?;
        let mgr = Self { pool };
        mgr.migrate().await?;
        Ok(mgr)
    }

    // -----------------------------------------------------------------------
    // Schema
    // -----------------------------------------------------------------------

    async fn migrate(&self) -> VfsResult<()> {
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS users (
            username      TEXT PRIMARY KEY NOT NULL,
            password      TEXT NOT NULL DEFAULT '',
            points        TEXT NOT NULL DEFAULT '[]'
        ) STRICT;

        CREATE TABLE IF NOT EXISTS quota (
            id            TEXT PRIMARY KEY NOT NULL,
            bytes_written INTEGER NOT NULL DEFAULT 0
        ) STRICT;
        "#,
        )
        .execute(&self.pool)
        .await?;

        let _ = sqlx::query("ALTER TABLE users ADD COLUMN password TEXT NOT NULL DEFAULT ''")
            .execute(&self.pool)
            .await;

        Ok(())
    }

    // -----------------------------------------------------------------------
    // User management
    // -----------------------------------------------------------------------

    /// Upsert a single user — inserts if absent, replaces all fields if present.
    pub async fn save_user(&self, user: &VfsUser) -> VfsResult<()> {
        let points_json = serde_json::to_string(&user.points)?;

        sqlx::query(
            r#"
        INSERT INTO users (username, password, points)
        VALUES (?1, ?2, ?3)
        ON CONFLICT(username) DO UPDATE SET
            password = excluded.password,
            points   = excluded.points
        "#,
        )
        .bind(&user.username)
        .bind(&user.password)
        .bind(&points_json)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Delete a user by username. No-op if the user does not exist.
    pub async fn delete_user(&self, username: &str) -> VfsResult<()> {
        sqlx::query("DELETE FROM users WHERE username = ?1")
            .bind(username)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// VfsStore impl
// ---------------------------------------------------------------------------

#[async_trait]
impl UserSystem for DbManager {
    async fn get_user(&self, username: &str) -> VfsResult<VfsUser> {
        sqlx::query_as::<_, UserRow>(
            "SELECT username, password, points FROM users WHERE username = ?1",
        )
        .bind(username)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(VfsError::UserNotFound)
        .and_then(VfsUser::try_from)
    }

    async fn set_users(&self, users: Vec<VfsUser>) -> VfsResult<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM users").execute(&mut *tx).await?;

        for user in &users {
            let points_json = serde_json::to_string(&user.points)?;

            sqlx::query(
                r#"
            INSERT INTO users (username, password, points)
            VALUES (?1, ?2, ?3)
            "#,
            )
            .bind(&user.username)
            .bind(&user.password)
            .bind(&points_json)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    async fn get_users(&self) -> VfsResult<Vec<VfsUser>> {
        let rows = sqlx::query_as::<_, UserRow>(
            "SELECT username, password, points FROM users ORDER BY username",
        )
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(VfsUser::try_from).collect()
    }
}

// ---------------------------------------------------------------------------
// QuotaTracker impl
// ---------------------------------------------------------------------------

#[async_trait]
impl QuotaTracker for DbManager {
    async fn get_bytes_written(&self, id: &str) -> opendal_core::Result<u64> {
        let row: Option<(i64,)> = sqlx::query_as("SELECT bytes_written FROM quota WHERE id = ?1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|x| {
                opendal_core::Error::new(ErrorKind::Unexpected, "Failed to get bytes written")
                    .set_source(x)
                    .set_temporary()
            })?;

        // Unknown id → 0. Cast i64 → u64 is safe: we never store negatives.
        Ok(row.map(|(b,)| b as u64).unwrap_or(0))
    }

    async fn clear(&self, id: &str) -> opendal_core::Result<()> {
        sqlx::query("DELETE FROM quota WHERE id = ?1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(db_err("clear quota"))?;

        Ok(())
    }

    async fn apply_delta(
        &self,
        id: &str,
        old_size: u64,
        new_size: u64,
        limit: u64,
    ) -> opendal_core::Result<u64> {
        // SQLite's INTEGER columns are signed 64-bit; these deltas are
        // realistic byte counts, so the casts below are safe.
        let old_size = old_size as i64;
        let new_size = new_size as i64;

        // Clamp an effectively-infinite limit (e.g. deletes, which pass
        // u64::MAX) to i64::MAX so the comparison below stays valid SQL.
        let limit = limit.min(i64::MAX as u64) as i64;

        // Acquire a connection and explicitly start an IMMEDIATE transaction.
        //
        // A normal `BEGIN` creates a deferred transaction: multiple
        // concurrent callers can all begin successfully and then race when
        // they attempt their first write. `BEGIN IMMEDIATE` acquires the
        // SQLite RESERVED/write lock immediately, causing concurrent callers
        // to wait rather than racing into SQLITE_BUSY errors.
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(db_err("acquire connection"))?;

        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *conn)
            .await
            .map_err(db_err("begin transaction"))?;

        // Make sure a row exists so the UPDATE below always has something
        // to match, without disturbing an existing value.
        sqlx::query(
            "INSERT INTO quota (id, bytes_written) VALUES (?1, 0) ON CONFLICT(id) DO NOTHING",
        )
        .bind(id)
        .execute(&mut *conn)
        .await
        .map_err(db_err("initialize quota row"))?;

        // The read/check/write is performed as one atomic UPDATE while the
        // IMMEDIATE transaction holds SQLite's writer lock.
        //
        // MAX(bytes_written - old_size, 0) mirrors the saturating_sub used
        // by MemoryTracker: the total is never allowed to go negative.
        let row: Option<(i64,)> = sqlx::query_as(
            r#"
        UPDATE quota
        SET bytes_written = MAX(bytes_written - ?2, 0) + ?3
        WHERE id = ?1
          AND (MAX(bytes_written - ?2, 0) + ?3) <= ?4
        RETURNING bytes_written
        "#,
        )
        .bind(id)
        .bind(old_size)
        .bind(new_size)
        .bind(limit)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err("apply quota delta"))?;

        match row {
            Some((new_total,)) => {
                sqlx::query("COMMIT")
                    .execute(&mut *conn)
                    .await
                    .map_err(db_err("commit quota delta"))?;

                Ok(new_total as u64)
            }

            None => {
                // The WHERE clause didn't match: applying the delta would
                // have exceeded `limit`. Roll back the transaction so the
                // quota value remains unchanged.
                sqlx::query("ROLLBACK")
                    .execute(&mut *conn)
                    .await
                    .map_err(db_err("rollback quota delta"))?;

                let current = self.get_bytes_written(id).await?;

                let hypothetical = current
                    .saturating_sub(old_size as u64)
                    .saturating_add(new_size as u64);

                Err(opendal_core::Error::new(
                    ErrorKind::RateLimited,
                    format!(
                        "write quota exceeded for '{id}': {current} used, \
                             {hypothetical} would be needed, {} limit",
                        limit as u64
                    ),
                )
                .with_context("quota_id", id.to_string())
                .with_context("quota_limit", limit.to_string())
                .with_context("quota_used", current.to_string()))
            }
        }
    }
}

/// Wrap a sqlx error as an `opendal_core::Error`, tagged as temporary since
/// these are all transient DB-layer failures (lock contention, I/O, etc.)
/// rather than a rejection of the operation itself.
fn db_err(context: &'static str) -> impl Fn(sqlx::Error) -> opendal_core::Error {
    move |x| {
        opendal_core::Error::new(ErrorKind::Unexpected, format!("Failed to {context}"))
            .set_source(x)
            .set_temporary()
    }
}

// ---------------------------------------------------------------------------
// Internal SQLite row type
// ---------------------------------------------------------------------------

#[derive(sqlx::FromRow)]
struct UserRow {
    username: String,
    password: String,
    points: String, // JSON-encoded Vec<VfsPoint>
}

impl TryFrom<UserRow> for VfsUser {
    type Error = VfsError;

    fn try_from(row: UserRow) -> Result<Self, Self::Error> {
        let points: Vec<VfsPoint> = serde_json::from_str(&row.points)?;

        Ok(VfsUser {
            username: row.username,
            password: row.password,
            points,
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn dummy_user(name: &str) -> VfsUser {
        VfsUser {
            username: name.to_string(),
            password: "password".to_string(),
            points: vec![VfsPoint {
                name: "primary".to_string(),
                max_bytes: Some(1_073_741_824),
                read_only: false,
                scheme: "s3".into(),
                config: BTreeMap::new(),
                is_repo: false,
                repo_password: None,
            }],
        }
    }

    async fn in_memory() -> DbManager {
        DbManager::open(":memory:")
            .await
            .expect("open in-memory db")
    }

    // --- VfsStore ---

    #[tokio::test]
    async fn load_missing_user_returns_none() {
        let mgr = in_memory().await;
        assert!(mgr.get_user("nobody").await.is_err());
    }

    #[tokio::test]
    async fn save_and_load_roundtrip() {
        let mgr = in_memory().await;
        mgr.save_user(&dummy_user("alice")).await.unwrap();

        let loaded = mgr.get_user("alice").await.expect("Should exist");
        assert_eq!(loaded.username, "alice");
        assert_eq!(loaded.password, "password");
        assert_eq!(loaded.points.len(), 1);
        assert_eq!(loaded.points[0].name, "primary");
    }

    #[tokio::test]
    async fn replace_users_is_atomic() {
        let mgr = in_memory().await;
        mgr.save_user(&dummy_user("old")).await.unwrap();

        mgr.set_users(vec![dummy_user("alice"), dummy_user("bob")])
            .await
            .unwrap();

        assert!(mgr.get_user("old").await.is_err());
        assert!(mgr.get_user("alice").await.is_ok());
        assert!(mgr.get_user("bob").await.is_ok());
    }

    #[tokio::test]
    async fn upsert_updates_existing_user() {
        let mgr = in_memory().await;
        mgr.save_user(&dummy_user("carol")).await.unwrap();

        let updated = VfsUser {
            username: "carol".to_string(),
            password: "new-password".to_string(),
            points: vec![],
        };

        mgr.save_user(&updated).await.unwrap();

        let loaded = mgr.get_user("carol").await.unwrap();
        assert_eq!(loaded.password, "new-password");
        assert!(loaded.points.is_empty());
    }

    #[tokio::test]
    async fn delete_user() {
        let mgr = in_memory().await;
        mgr.save_user(&dummy_user("dan")).await.unwrap();
        mgr.delete_user("dan").await.unwrap();
        assert!(mgr.get_user("dan").await.is_err());
    }

    #[tokio::test]
    async fn list_users_returns_all() {
        let mgr = in_memory().await;

        mgr.set_users(vec![dummy_user("a"), dummy_user("b"), dummy_user("c")])
            .await
            .unwrap();

        assert_eq!(mgr.get_users().await.unwrap().len(), 3);
    }

    // --- QuotaTracker ---

    #[tokio::test]
    async fn unknown_quota_id_returns_zero() {
        let mgr = in_memory().await;
        assert_eq!(mgr.get_bytes_written("ghost").await.unwrap(), 0);
    }

    #[tokio::test]
    async fn apply_delta_on_new_id_creates_row_and_returns_total() {
        let mgr = in_memory().await;

        let total = mgr
            .apply_delta("user:alice:primary", 0, 1_234_567, u64::MAX)
            .await
            .unwrap();

        assert_eq!(total, 1_234_567);
        assert_eq!(
            mgr.get_bytes_written("user:alice:primary").await.unwrap(),
            1_234_567
        );
    }

    #[tokio::test]
    async fn apply_delta_replaces_old_size_with_new_size() {
        let mgr = in_memory().await;
        mgr.apply_delta("id", 0, 100, u64::MAX).await.unwrap();
        mgr.apply_delta("id", 100, 999, u64::MAX).await.unwrap();
        assert_eq!(mgr.get_bytes_written("id").await.unwrap(), 999);
    }

    #[tokio::test]
    async fn quota_ids_are_isolated() {
        let mgr = in_memory().await;
        mgr.apply_delta("a", 0, 1, u64::MAX).await.unwrap();
        mgr.apply_delta("b", 0, 2, u64::MAX).await.unwrap();
        assert_eq!(mgr.get_bytes_written("a").await.unwrap(), 1);
        assert_eq!(mgr.get_bytes_written("b").await.unwrap(), 2);
    }

    #[tokio::test]
    async fn apply_delta_over_limit_is_rejected_and_does_not_mutate() {
        let mgr = in_memory().await;
        mgr.apply_delta("id", 0, 100, 1_000).await.unwrap();

        let err = mgr.apply_delta("id", 0, 950, 1_000).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::RateLimited);

        // Rejected delta must not have mutated the stored value.
        assert_eq!(mgr.get_bytes_written("id").await.unwrap(), 100);
    }

    #[tokio::test]
    async fn apply_delta_never_goes_negative() {
        let mgr = in_memory().await;
        mgr.apply_delta("id", 0, 50, u64::MAX).await.unwrap();

        // old_size larger than the current total should saturate at 0
        // rather than underflow.
        let total = mgr.apply_delta("id", 500, 0, u64::MAX).await.unwrap();
        assert_eq!(total, 0);
    }

    // --- Concurrency ---

    #[tokio::test]
    async fn concurrent_reads_do_not_deadlock() {
        use std::sync::Arc;

        let mgr = Arc::new(in_memory().await);
        mgr.save_user(&dummy_user("shared")).await.unwrap();
        mgr.apply_delta("shared", 0, 42, u64::MAX).await.unwrap();

        let handles: Vec<_> = (0..16)
            .map(|_| {
                let m = Arc::clone(&mgr);

                tokio::spawn(async move {
                    let _user = m.get_user("shared").await.unwrap();
                    let quota = m.get_bytes_written("shared").await.unwrap();
                    (true, quota)
                })
            })
            .collect();

        for h in handles {
            let (found, quota) = h.await.unwrap();
            assert!(found);
            assert_eq!(quota, 42);
        }
    }

    #[tokio::test]
    async fn concurrent_apply_deltas_never_exceed_the_limit() {
        use std::sync::Arc;

        let mgr = Arc::new(in_memory().await);
        let limit = 1_000u64;

        // 20 concurrent new-object writes of 100 bytes each against the
        // same quota id.
        //
        // BEGIN IMMEDIATE serialises the writers at transaction start,
        // allowing exactly 10 operations to commit and causing the other
        // 10 to be rejected by the atomic UPDATE's WHERE clause.
        let handles: Vec<_> = (0..20)
            .map(|_| {
                let m = Arc::clone(&mgr);
                tokio::spawn(async move { m.apply_delta("shared", 0, 100, limit).await })
            })
            .collect();

        let mut succeeded = 0;

        for h in handles {
            if h.await.unwrap().is_ok() {
                succeeded += 1;
            }
        }

        assert_eq!(succeeded, 10);
        assert_eq!(mgr.get_bytes_written("shared").await.unwrap(), 1_000);
    }
}
