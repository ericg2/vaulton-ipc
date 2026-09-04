//! [`StorageManager`] — unified cache for rustic repositories, VFS operators,
//! and raw data-layer operators.

use crate::core::{VfsError, VfsResult, VfsUser};
use crate::db::DbManager;
use crate::ipc::job_event::Data;
use crate::progress::RusticProgressBars;
use async_trait::async_trait;
use crossbeam_channel::Sender;
use moka::sync::Cache;
use opendal_core::Operator;
use rustic_backend::opendal::*;
use rustic_backend::{BackendBuilder, BackendOptions};
use rustic_core::{
    ConfigOptions, Credentials, ErrorKind, IndexedFullStatus, KeyOptions, OpenStatus, Repository,
    RepositoryOptions, RusticError, RusticResult,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use unftp_core::storage::StorageBackend;
use uuid::Uuid;
use opendal_vfs::layers::vfs::VfsBuilder;

pub type RepoNoIndex = Repository<OpenStatus>;
pub type RepoIndexed = Repository<IndexedFullStatus>;

/// Unified storage interface covering three access patterns:
///
/// - **Indexed repositories** — full rustic repos used for backup/restore.
/// - **VFS operators** — OpenDAL [`Operator`]s backed by a rustic snapshot
///   tree for filesystem-style reads over repo contents.
/// - **Data-layer operators** — OpenDAL [`Operator`]s for raw scheme-level
///   storage (S3, local disk, etc.), independent of any rustic repo.
#[async_trait]
pub trait StorageSystem: Send + Sync + 'static {
    /// Returns a cached, indexed rustic repository for the given source.
    ///
    /// Opens the repository on first access; subsequent calls for the same
    /// source return the cached handle until the TTI window expires.
    async fn get_repo(&self, src: &RepoSource) -> RusticResult<Arc<RepoIndexed>>;

    /// Opens a fresh indexed repository tied to a background job, forwarding
    /// progress events over `tx`.
    ///
    /// Results are **not** cached — each call produces a new handle so that
    /// progress reporting is scoped to the job lifetime.
    async fn get_repo_job(
        &self,
        src: &RepoSource,
        job_id: Uuid,
        tx: Sender<Data>,
    ) -> RusticResult<RepoIndexed>;

    /// Creates a VFS for the given user.
    async fn get_vfs(&self, user: &VfsUser) -> VfsResult<Operator>;
}

/// Identifies a rustic repository by its storage scheme and decryption
/// password.
///
/// Used as the cache key for both [`get_repo`](StorageSystem::get_repo) and
/// [`get_vfs_operator`](StorageSystem::get_vfs_operator).
#[derive(Hash, Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct RepoSource {
    /// The OpenDAL scheme that locates the repository.
    pub scheme: String,
    pub config: BTreeMap<String, String>,
    pub password: String,
}

/// Concrete implementation of [`StorageSystem`] backed by three
/// [`moka`] caches with a shared time-to-idle eviction policy.
///
/// | Cache      | Key          | Value              | Purpose                |
/// |------------|--------------|--------------------|------------------------|
/// | `repos`    | `RepoSource` | `Arc<RepoIndexed>` | Indexed rustic repos   |
/// | `vfs_ops`  | `RepoSource` | `Arc<Operator>`    | VFS OpenDAL operators  |
/// | `data_ops` | `Scheme`     | `Arc<Operator>`    | Data-layer operators   |
#[derive(Clone)]
pub struct StorageManager {
    repos: Cache<RepoSource, Arc<RepoIndexed>>,
    vfs_ops: Cache<VfsUser, Operator>,
    db: Arc<DbManager>,
}

impl StorageManager {
    /// Creates a new [`StorageManager`] whose caches evict entries after
    /// `tti` of inactivity.
    pub fn new(db: Arc<DbManager>, tti: Duration) -> Self {
        Self {
            repos: Cache::builder()
                .time_to_idle(tti)
                .max_capacity(1000)
                .build(),

            vfs_ops: Cache::builder()
                .time_to_idle(tti)
                .max_capacity(1000)
                .build(),

            db,
        }
    }

    // ── Repository helpers ────────────────────────────────────────────────
    /// Opens or initialises a rustic repository **without** a progress bar.
    ///
    /// When `init` is `true` the repository is created fresh via
    /// [`Repository::init`]; otherwise it is opened from existing storage.
    fn create_indexed(&self, src: &RepoSource, init: bool) -> RusticResult<RepoIndexed> {
        let creds = Credentials::password(&src.password);
        let config = OpenDALConfig::default()
            .scheme(src.scheme.clone())
            .options(src.config.clone().into_iter().collect::<HashMap<_, _>>());
        let backend = BackendOptions::default().with_repo(&config).to_backends()?;
        let repo = Repository::new(&RepositoryOptions::default(), &backend)?;
        if init {
            repo.init(&creds, &KeyOptions::default(), &ConfigOptions::default())?
                .to_indexed()
        } else {
            repo.open(&creds)?.to_indexed()
        }
    }

    /// Opens or initialises a rustic repository **with** job-scoped progress
    /// events forwarded over `tx`.
    fn create_for_job(
        &self,
        src: &RepoSource,
        job_id: Uuid,
        tx: Sender<Data>,
        init: bool,
    ) -> RusticResult<RepoIndexed> {
        let creds = Credentials::password(&src.password);
        let config = OpenDALConfig::default()
            .scheme(src.scheme.clone())
            .options(src.config.clone().into_iter().collect::<HashMap<_, _>>());
        let backend = BackendOptions::default().with_repo(&config).to_backends()?;
        let pb = RusticProgressBars::new(job_id, tx);
        let repo = Repository::new_with_progress(&RepositoryOptions::default(), &backend, pb)?;
        if init {
            repo.init(&creds, &KeyOptions::default(), &ConfigOptions::default())?
                .to_indexed()
        } else {
            repo.open(&creds)?.to_indexed()
        }
    }

    /// Tries to open an existing repository, falling back to `init` on
    /// failure (e.g. first run against empty storage).
    fn get_raw_repo(&self, src: &RepoSource) -> RusticResult<RepoIndexed> {
        self.create_indexed(src, false)
            .or_else(|_| self.create_indexed(src, true))
    }

    // ── VFS operator helpers ──────────────────────────────────────────────

    /// Builds a fresh OpenDAL [`Operator`] for VFS access to the given
    /// repository source via [`RusticVfsBuilder`].
    fn create_vfs_operator(&self, src: &RepoSource) -> RusticResult<Operator> {
        // Ensure the repo exists, initializing it if this is the first access.
        self.get_raw_repo(src)?;

        let config = OpenDALConfig::default()
            .scheme(src.scheme.clone())
            .options(src.config.clone().into_iter().collect::<HashMap<_, _>>());
        let op = Operator::new(
            RusticVfsBuilder::default()
                .with_options(RepositoryOptions::default())
                .with_backend(BackendOptions::default().with_repo(&config))
                .with_credentials(Credentials::password(&src.password)),
        )
        .map_err(|e| RusticError::with_source(ErrorKind::Vfs, "Failed to initialize VFS", e))?;
        Ok(op)
    }

    fn create_for_vfs(&self, user: &VfsUser) -> VfsResult<Operator> {
        let mut vfs = VfsBuilder::new(self.db.clone());
        for point in user.points.iter() {
            if point.is_repo {
                let pass = point
                    .repo_password
                    .clone()
                    .ok_or(VfsError::RepoPasswordMissing)?;
                
                let config = OpenDALConfig::default()
                    .scheme(point.scheme.clone())
                    .options(point.config.clone().into_iter().collect::<HashMap<_, _>>());

                let scheme = RusticVfsConfig {
                    options: RepositoryOptions::default(),
                    backend: BackendOptions::default().with_repo(&config),
                    credentials: Some(Credentials::password(&pass)),
                    refresh_interval: Some(Duration::from_mins(2)),
                };

                let op = Operator::from_config(scheme)?;
                vfs = vfs.mount(format!("/repos/{}", &point.name), op).read_only(); // *** it won't allow anyway; but it prevents strange errors.
            } else {
                // If the point is a data point - it becomes better!
                let op = Operator::via_iter(&point.scheme, point.config.clone())?;
                vfs = vfs.mount(format!("/points/{}", &point.name), op);
                if point.read_only {
                    vfs = vfs.read_only();
                } else if let Some(max) = point.max_bytes {
                    vfs = vfs.quota(&point.name, max);
                }
            }
        }
        Ok(Operator::new(vfs)?)
    }
}

#[async_trait]
impl StorageSystem for StorageManager {
    async fn get_repo(&self, src: &RepoSource) -> RusticResult<Arc<RepoIndexed>> {
        if let Some(repo) = self.repos.get(src) {
            return Ok(repo);
        }
        let this = self.clone();
        let src = src.clone();
        tokio::task::spawn_blocking(move || {
            // check again inside — another task may have populated it while we waited
            if let Some(repo) = this.repos.get(&src) {
                return Ok(repo);
            }
            let repo = Arc::new(this.get_raw_repo(&src)?);
            this.repos.insert(src, repo.clone());
            Ok(repo)
        })
        .await
        .map_err(|e| RusticError::with_source(ErrorKind::Backend, "spawn_blocking panicked", e))?
    }

    async fn get_repo_job(
        &self,
        src: &RepoSource,
        job_id: Uuid,
        tx: Sender<Data>,
    ) -> RusticResult<RepoIndexed> {
        let this = self.clone();
        let src = src.clone();
        tokio::task::spawn_blocking(move || {
            this.create_for_job(&src, job_id, tx.clone(), false)
                .or_else(|_| this.create_for_job(&src, job_id, tx, true))
        })
        .await
        .map_err(|e| RusticError::with_source(ErrorKind::Backend, "spawn_blocking panicked", e))?
    }

    async fn get_vfs(&self, user: &VfsUser) -> VfsResult<Operator> {
        if let Some(op) = self.vfs_ops.get(user) {
            return Ok(op);
        }

        let this = self.clone();
        let user = user.clone();
        tokio::task::spawn_blocking(move || {
            let op = this.create_for_vfs(&user)?;
            this.vfs_ops.insert(user.clone(), op.clone());
            Ok(op)
        })
        .await
        .map_err(|e| VfsError::Internal(format!("spawn_blocking panicked: {e}")))?
    }
}
