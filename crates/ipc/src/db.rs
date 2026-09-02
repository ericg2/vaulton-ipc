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
            INSERT INTO users (username, points)
            VALUES (?1, ?2)
            ON CONFLICT(username) DO UPDATE SET
                points        = excluded.points
            "#,
        )
        .bind(&user.username)
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

    /// Return every user in the database, ordered by username.
    pub async fn list_users(&self) -> VfsResult<Vec<VfsUser>> {
        let rows = sqlx::query_as::<_, UserRow>(
            "SELECT username, points FROM users ORDER BY username",
        )
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(VfsUser::try_from).collect()
    }
}

// ---------------------------------------------------------------------------
// VfsStore impl
// ---------------------------------------------------------------------------

#[async_trait]
impl UserSystem for DbManager {
    async fn load_user(&self, username: &str) -> VfsResult<VfsUser> {
        sqlx::query_as::<_, UserRow>(
            "SELECT username, points FROM users WHERE username = ?1",
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
                INSERT INTO users (username, points)
                VALUES (?1, ?2)
                "#,
            )
            .bind(&user.username)
            .bind(&points_json)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
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

    async fn set_bytes_written(&self, id: &str, bytes: u64) -> opendal_core::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO quota (id, bytes_written)
            VALUES (?1, ?2)
            ON CONFLICT(id) DO UPDATE SET bytes_written = excluded.bytes_written
            "#,
        )
        .bind(id)
        .bind(bytes as i64) // SQLite integers are signed 64-bit; safe for any realistic size
        .execute(&self.pool)
        .await
        .map_err(|x| {
            opendal_core::Error::new(ErrorKind::Unexpected, "Failed to set bytes written")
                .set_source(x)
                .set_temporary()
        })?;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Internal SQLite row type
// ---------------------------------------------------------------------------

#[derive(sqlx::FromRow)]
struct UserRow {
    username: String,
    points: String, // JSON-encoded Vec<VfsPoint>
}

impl TryFrom<UserRow> for VfsUser {
    type Error = VfsError;

    fn try_from(row: UserRow) -> Result<Self, Self::Error> {
        let points: Vec<VfsPoint> = serde_json::from_str(&row.points)?;
        Ok(VfsUser {
            username: row.username,
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
    use std::collections::{BTreeMap, HashMap};

    fn dummy_user(name: &str) -> VfsUser {
        VfsUser {
            username: name.to_string(),
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
        assert!(mgr.load_user("nobody").await.is_err());
    }

    #[tokio::test]
    async fn save_and_load_roundtrip() {
        let mgr = in_memory().await;
        mgr.save_user(&dummy_user("alice")).await.unwrap();

        let loaded = mgr.load_user("alice").await.expect("Should exist");
        assert_eq!(loaded.username, "alice");
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

        assert!(mgr.load_user("old").await.is_err());
        assert!(mgr.load_user("alice").await.is_ok());
        assert!(mgr.load_user("bob").await.is_ok());
    }

    #[tokio::test]
    async fn upsert_updates_existing_user() {
        let mgr = in_memory().await;
        mgr.save_user(&dummy_user("carol")).await.unwrap();

        let updated = VfsUser {
            username: "carol".to_string(),
            points: vec![],
        };
        mgr.save_user(&updated).await.unwrap();

        let loaded = mgr.load_user("carol").await.unwrap();
        assert!(loaded.points.is_empty());
    }

    #[tokio::test]
    async fn delete_user() {
        let mgr = in_memory().await;
        mgr.save_user(&dummy_user("dan")).await.unwrap();
        mgr.delete_user("dan").await.unwrap();
        assert!(mgr.load_user("dan").await.is_err());
    }

    #[tokio::test]
    async fn list_users_returns_all() {
        let mgr = in_memory().await;
        mgr.set_users(vec![dummy_user("a"), dummy_user("b"), dummy_user("c")])
            .await
            .unwrap();
        assert_eq!(mgr.list_users().await.unwrap().len(), 3);
    }

    // --- QuotaTracker ---

    #[tokio::test]
    async fn unknown_quota_id_returns_zero() {
        let mgr = in_memory().await;
        assert_eq!(mgr.get_bytes_written("ghost").await.unwrap(), 0);
    }

    #[tokio::test]
    async fn quota_set_and_get_roundtrip() {
        let mgr = in_memory().await;
        mgr.set_bytes_written("user:alice:primary", 1_234_567)
            .await
            .unwrap();
        assert_eq!(
            mgr.get_bytes_written("user:alice:primary").await.unwrap(),
            1_234_567
        );
    }

    #[tokio::test]
    async fn quota_set_overwrites_previous_value() {
        let mgr = in_memory().await;
        mgr.set_bytes_written("id", 100).await.unwrap();
        mgr.set_bytes_written("id", 999).await.unwrap();
        assert_eq!(mgr.get_bytes_written("id").await.unwrap(), 999);
    }

    #[tokio::test]
    async fn quota_ids_are_isolated() {
        let mgr = in_memory().await;
        mgr.set_bytes_written("a", 1).await.unwrap();
        mgr.set_bytes_written("b", 2).await.unwrap();
        assert_eq!(mgr.get_bytes_written("a").await.unwrap(), 1);
        assert_eq!(mgr.get_bytes_written("b").await.unwrap(), 2);
    }

    // --- Concurrency ---

    #[tokio::test]
    async fn concurrent_reads_do_not_deadlock() {
        use std::sync::Arc;

        let mgr = Arc::new(in_memory().await);
        mgr.save_user(&dummy_user("shared")).await.unwrap();
        mgr.set_bytes_written("shared", 42).await.unwrap();

        let handles: Vec<_> = (0..16)
            .map(|_| {
                let m = Arc::clone(&mgr);
                tokio::spawn(async move {
                    let _user = m.load_user("shared").await.unwrap();
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
}
