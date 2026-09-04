//! Per-user virtual filesystem.
//!
//! Composes quota-guarded data-layer points and read-only rustic repository
//! mounts into a single [`Operator`] per user.
//!
//! # Layout inside each user's operator
//!
//! ```text
//! /
//! ├── points/
//! │   ├── <name>/   ← raw data operator; quota-enforced when writable
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
use std::fmt::Formatter;
use thiserror::Error;
use unftp_core::auth::UserDetail;

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

    #[error("internal error: {0}")]
    Internal(String),
}

pub type VfsResult<T> = Result<T, VfsError>;

// ── Domain types ──────────────────────────────────────────────────────────────

/// A single virtual mount point belonging to a [`VfsUser`].
///
/// Exposed as `points/<name>/**` (raw data) or `repos/<name>/**` (rustic
/// VFS) inside the user's composed [`Operator`].
#[derive(Hash, Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct VfsPoint {
    /// Mount name — becomes the path component under the namespace prefix.
    pub name: String,

    /// Maximum cumulative bytes this point may receive via writes.
    /// `None` means unlimited. Ignored when `readonly` is `true`.
    pub max_bytes: Option<u64>,

    /// When `true`, writes and deletes are rejected at the mount level.
    /// Repo mounts are always read-only regardless of this flag.
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

/// A user identity and its associated virtual filesystem mount points.
#[derive(Hash, Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct VfsUser {
    /// Unique username. Also used as a namespace prefix in quota keys so that
    /// one shared [`QuotaTracker`] correctly isolates every user.
    pub username: String,

    /// The password to use.
    pub password: String,

    /// Ordered mount points owned by this user.
    pub points: Vec<VfsPoint>,
}

impl std::fmt::Display for VfsUser {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.username)
    }
}

impl UserDetail for VfsUser {}

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
