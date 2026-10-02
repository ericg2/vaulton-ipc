//! Per-user virtual filesystem.
//!
//! Composes data-layer points and read-only rustic repository mounts into a
//! single [`Operator`] per user. Only local/fs-backed data points are ever
//! writable through the VFS; remote data points and repos are read-only there
//! (backup/restore jobs still reach them through dedicated operators).
//!
//! # Layout inside each user's operator
//!
//! ```text
//! /
//! ├── points/
//! │   ├── <name>/   ← raw data operator; writable only if local/fs-backed
//! │   └── ...
//! └── repos/
//!     ├── <name>/   ← rustic VFS operator; always read-only
//!     └── ...
//! ```
//!
//! Operators are built lazily on first access, cached per username via a
//! TTI-evicted [`moka`] cache, and safe for hot-loop use. After mutating a
//! user record in the store, call [`VfsManager::invalidate`] to drop the
//! stale entry so the next call rebuilds from fresh data.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use thiserror::Error;
use uuid::Uuid;
// ── Errors ────────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum VfsError {
    #[error("storage error: {0}")]
    Storage(#[from] Box<rustic_core::RusticError>),

    #[error("opendal error: {0}")]
    OpenDal(#[from] opendal_core::Error),

    #[error("sql error: {0}")]
    SqlError(#[from] sqlx::Error),

    #[error("serialization error: {0}")]
    SerdeError(#[from] serde_json::Error),

    #[error("user not found")]
    UserNotFound,

    #[error("point is a repo mount but has no `repo_password`")]
    RepoPasswordMissing,

    #[error("point '{name}' failed to load: {reason}")]
    PointFailed { name: String, reason: String },

    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("internal error: {0}")]
    Internal(String),
}

pub type VfsResult<T> = Result<T, VfsError>;

/// Load state of a single point, recorded whenever a user's VFS is (re)built.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PointHealth {
    Healthy,
    /// The point could not be initialized/opened; it is left out of the VFS.
    Failed(String),
}

// ── Domain types ──────────────────────────────────────────────────────────────

/// A single virtual mount point belonging to a [`VfsUser`].
///
/// Exposed as `points/<name>/**` (raw data) or `repos/<name>/**` (rustic
/// VFS) inside the user's composed [`Operator`].
#[derive(Hash, Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct VfsPoint {
    /// The ID of the point.
    pub id: Uuid,

    /// Mount name — becomes the path component under the namespace prefix.
    pub name: String,

    /// Maximum cumulative bytes this point may receive via writes.
    /// `None` means unlimited. Ignored when `read_only` is `true`.
    pub max_bytes: Option<u64>,

    /// When `true`, writes and deletes are rejected.
    ///
    /// For local data points this gates the VFS mount directly. Remote data
    /// points are never user-writable through the VFS regardless of this
    /// flag (see `utils::is_user_writable`); here it only gates restore jobs
    /// into them. For repo points the VFS mount is always read-only — the
    /// flag gates whether backup/forget jobs and password changes may write
    /// to the repo (see `utils::require_writable` in the server layer).
    pub read_only: bool,

    /// The storage scheme to use. Example: `s3`.
    pub scheme: String,

    /// All storage options.
    pub config: BTreeMap<String, String>,

    /// `true` → served via the rustic VFS ([`StorageSystem::get_vfs_operator`]).
    /// `false` → served as a raw data-layer operator ([`StorageSystem::get_data_operator`]).
    pub is_repo: bool,

    /// Decryption password for the rustic repository.
    /// Required (and only consulted) when `is_repo` is `true`.
    pub repo_password: Option<String>,
}

/// Checks the point-ID invariants that every other layer relies on.
///
/// A point ID is the *only* thing that identifies a point in indexed paths,
/// quota keys, health records and operator caches. Data points and repo points
/// share one ID space, so the same ID must never appear twice anywhere in the
/// configuration (not within a user, not across users, and not across the
/// data/repo kinds). Nil IDs are rejected as well.
pub fn validate_point_ids<'a>(users: impl IntoIterator<Item = &'a VfsUser>) -> VfsResult<()> {
    let mut seen: std::collections::HashMap<Uuid, (&str, &str)> = std::collections::HashMap::new();
    for user in users {
        for point in &user.points {
            if point.id.is_nil() {
                return Err(VfsError::InvalidConfig(format!(
                    "point '{}' of user '{}' has a nil ID",
                    point.name, user.username
                )));
            }
            if let Some((other_user, other_name)) =
                seen.insert(point.id, (user.username.as_str(), point.name.as_str()))
            {
                return Err(VfsError::InvalidConfig(format!(
                    "point ID {} is used by both '{}/{}' and '{}/{}'",
                    point.id, other_user, other_name, user.username, point.name
                )));
            }
        }
    }
    Ok(())
}

/// A user identity and its associated virtual filesystem mount points.
#[derive(Hash, Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct VfsUser {
    /// Unique username. Also used as a namespace prefix in quota keys so that
    /// one shared [`QuotaTracker`] correctly isolates every user.
    pub username: String,

    /// Ordered mount points owned by this user.
    pub points: Vec<VfsPoint>,
}

// ── VfsStore ──────────────────────────────────────────────────────────────────
/// Database persistence for [`VfsUser`] records.
#[async_trait]
pub trait UserSystem: Send + Sync + 'static {
    /// Returns a single user.
    async fn get_user(&self, username: &str) -> VfsResult<VfsUser>;

    /// Sets the list of users.
    async fn set_users(&self, users: Vec<VfsUser>) -> VfsResult<()>;

    /// Returns a list of all users.
    async fn get_users(&self) -> VfsResult<Vec<VfsUser>>;
}
