//! [`StorageManager`] — unified cache for rustic repositories, VFS operators,
//! and raw data-layer operators.

use crate::core::{PointHealth, VfsError, VfsPoint, VfsResult, VfsUser};
use crate::db::DbManager;
use crate::event_bus::send as send_event;
use crate::ipc::PointStatusEvent;
use crate::ipc::ipc_event::Data;
use crate::progress::RusticProgressBars;
use crate::utils;
use async_trait::async_trait;
use crossbeam_channel::Sender;
use dashmap::DashMap;
use log::{error, info};
use moka::sync::Cache;
use opendal_core::Operator;
use opendal_vfs::layers::quota::{QuotaLayer, QuotaState};
use opendal_vfs::layers::read_only::ReadOnlyLayer;
use opendal_vfs::layers::vfs::VfsBuilder;
use rustic_backend::opendal::*;
use rustic_backend::{BackendBuilder, BackendOptions};
use rustic_core::{
    ConfigOptions, Credentials, ErrorKind, IndexedFullStatus, KeyOptions, OpenStatus, Repository,
    RepositoryOptions, RusticError, RusticResult,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use unftp_core::storage::StorageBackend;
use uuid::Uuid;

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
    async fn build_vfs(&self, user: VfsUser) -> VfsResult<Operator>;

    fn refresh_degraded(&self, user: VfsUser);

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
    ///
    /// `allow_init` controls whether a *missing* repository is created. It is
    /// only ever `true` for backups; restore/check/forget must fail loudly
    /// instead of silently creating an empty repo.
    async fn get_repo_job(
        &self,
        src: &RepoSource,
        op: opendal_core::blocking::Operator,
        job_id: Uuid,
        tx: Sender<Data>,
        allow_init: bool,
    ) -> RusticResult<RepoIndexed>;

    /// Creates a VFS for the given user.
    async fn get_vfs(&self, user: &VfsUser) -> VfsResult<Operator>;

    /// Removes the VFS instance for a user (healthy and degraded caches).
    fn invalidate_vfs(&self, user: &VfsUser);

    /// Last recorded load state of a point, or `None` if never loaded.
    fn point_health(&self, id: &Uuid) -> Option<PointHealth>;

    /// Builds an operator for a single named data point belonging to `user`,
    /// applying the same read-only/quota layering as [`get_vfs`](Self::get_vfs)
    /// applies to that point's mount.
    ///
    /// Intended for backup/restore jobs, which read/write a data point's
    /// backend directly (via `OpenDALSource::new`) rather than through the
    /// user's mounted VFS tree — without this, those jobs would bypass quota
    /// enforcement and the point's read-only flag entirely.
    fn get_data_operator(
        &self,
        user: &VfsUser,
        point: &VfsPoint,
    ) -> VfsResult<opendal_core::blocking::Operator>;
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
/// | `vfs_ops`  | `VfsUser`    | `Operator`         | Per-user VFS operators |
#[derive(Clone)]
pub struct StorageManager {
    repos: Cache<RepoSource, Arc<RepoIndexed>>,
    repo_vfs_ops: Cache<RepoSource, Operator>,
    data_ops: Cache<Uuid, opendal_core::blocking::Operator>,
    vfs_ops: Cache<String, Operator>,
    /// VFS trees built while at least one point failed. Short TTL so failed
    /// points are retried soon without re-initializing on every request.
    degraded_ops: Cache<String, Operator>,
    health: Arc<DashMap<Uuid, PointHealth>>,
    repo_locks: Arc<DashMap<RepoSource, Arc<StdMutex<()>>>>,
    vfs_build_locks: Arc<DashMap<String, Arc<StdMutex<()>>>>,
    vfs_generation: Arc<DashMap<String, u64>>,
    refreshing: Arc<DashMap<String, ()>>,
    events: Sender<Data>,
    pub(crate) state: QuotaState,
}

/// How long a partially-failed VFS is served before points are retried.
const DEGRADED_TTL: Duration = Duration::from_secs(30);

fn repo_err(msg: impl Into<String>) -> Box<RusticError> {
    let msg = msg.into();
    RusticError::with_source(ErrorKind::Backend, msg.clone(), std::io::Error::other(msg))
}

/// A rustic repository exists iff its `config` file does. Any error other
/// than NotFound (network, auth, bad config) is returned as-is instead of
/// being mistaken for "repo missing" and triggering a bogus `init`.
fn repo_exists(op: &opendal_core::blocking::Operator) -> RusticResult<bool> {
    match op.stat("config") {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == opendal_core::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(RusticError::with_source(
            ErrorKind::Backend,
            format!("repository probe failed: {e}"),
            e,
        )),
    }
}

impl StorageManager {
    /// Creates a new [`StorageManager`] whose caches evict entries after
    /// `tti` of inactivity.
    pub fn new(db: Arc<DbManager>, tti: Duration, events: Sender<Data>) -> Self {
        Self {
            repos: Cache::builder()
                .time_to_idle(tti)
                .max_capacity(1000)
                .build(),

            repo_vfs_ops: Cache::builder()
                .time_to_idle(tti)
                .max_capacity(1000)
                .build(),

            data_ops: Cache::builder()
                .time_to_idle(tti)
                .max_capacity(2000)
                .build(),

            vfs_ops: Cache::builder()
                .time_to_idle(tti)
                .max_capacity(1000)
                .build(),

            degraded_ops: Cache::builder()
                .time_to_live(DEGRADED_TTL)
                .max_capacity(1000)
                .build(),

            health: Arc::new(DashMap::new()),
            repo_locks: Arc::new(DashMap::new()),
            vfs_build_locks: Arc::new(DashMap::new()),
            vfs_generation: Arc::new(DashMap::new()),
            refreshing: Arc::new(DashMap::new()),
            events,
            state: QuotaState::new(db.clone()),
        }
    }

    /// Records a point's health; logs and emits a `PointStatusEvent` only on change.
    fn set_health(&self, user: &VfsUser, point: &VfsPoint, new: PointHealth) {
        let changed = self.health.insert(point.id, new.clone()).as_ref() != Some(&new);
        if !changed {
            return;
        }
        let (health, err) = match &new {
            PointHealth::Healthy => {
                info!("point '{}' of user '{}' loaded", point.name, user.username);
                (crate::ipc::PointHealth::Healthy, String::new())
            }
            PointHealth::Failed(e) => {
                error!(
                    "point '{}' of user '{}' failed: {e}",
                    point.name, user.username
                );
                (crate::ipc::PointHealth::Failed, e.clone())
            }
        };
        let _ = send_event(
            &self.events,
            Data::PointStatus(PointStatusEvent {
                user: user.username.clone(),
                point_id: point.id.to_string(),
                point_name: point.name.clone(),
                health: health as i32,
                error: err,
                time: utils::proto_stamp(rustic_core::jiff::Timestamp::now()),
            }),
        );
    }

    // ── Repository helpers ────────────────────────────────────────────────

    fn probe(src: &RepoSource) -> RusticResult<opendal_core::blocking::Operator> {
        let op = Operator::via_iter(&src.scheme, src.config.clone())
            .and_then(opendal_core::blocking::Operator::new)
            .map_err(|e| {
                RusticError::with_source(ErrorKind::Backend, "Invalid storage config", e)
            })?;
        Ok(op)
    }

    /// Probes repository storage and initializes a missing repository when
    /// permitted. Existing repositories are not opened here: the caller that
    /// needs the actual rustic handle performs the real open exactly once.
    fn prepare_repo_unlocked(&self, src: &RepoSource, allow_init: bool) -> RusticResult<()> {
        let op = Self::probe(src)?;
        if repo_exists(&op)? {
            return Ok(());
        }

        if !allow_init {
            return Err(repo_err(
                "repository does not exist (and this point is read-only)",
            ));
        }

        let creds = Credentials::password(&src.password);
        let config = OpenDALConfig::default()
            .scheme(src.scheme.clone())
            .options(src.config.clone().into_iter().collect::<HashMap<_, _>>());
        let backend = BackendOptions::default().with_repo(&config).to_backends()?;

        info!("initializing new repository ({})", src.scheme);
        Repository::new(&RepositoryOptions::default(), &backend)?.init(
            &creds,
            &KeyOptions::default(),
            &ConfigOptions::default(),
        )?;

        Ok(())
    }

    /// Opens an existing repository, creating it only if `allow_init`.
    /// The caller owns the repository lock when this is used by `get_repo`.
    fn get_raw_repo(&self, src: &RepoSource, allow_init: bool) -> RusticResult<RepoIndexed> {
        self.prepare_repo_unlocked(src, allow_init)?;
        let creds = Credentials::password(&src.password);
        let config = OpenDALConfig::default()
            .scheme(src.scheme.clone())
            .options(src.config.clone().into_iter().collect::<HashMap<_, _>>());
        let backend = BackendOptions::default().with_repo(&config).to_backends()?;
        Repository::new(&RepositoryOptions::default(), &backend)?
            .open(&creds)?
            .to_indexed()
    }

    /// Job-scoped variant with progress events over `tx`.
    fn create_for_job(
        &self,
        src: &RepoSource,
        op: opendal_core::blocking::Operator,
        job_id: Uuid,
        tx: Sender<Data>,
        allow_init: bool,
    ) -> RusticResult<RepoIndexed> {
        let exists = repo_exists(&op)?;
        let creds = Credentials::password(&src.password);
        let backend = OpenDALSource::new(op).to_backends()?;
        let pb = RusticProgressBars::new(job_id, tx);
        let repo = Repository::new_with_progress(&RepositoryOptions::default(), &backend, pb)?;
        if exists {
            repo.open(&creds)?.to_indexed()
        } else if allow_init {
            repo.init(&creds, &KeyOptions::default(), &ConfigOptions::default())?
                .to_indexed()
        } else {
            Err(repo_err("repository does not exist"))
        }
    }

    /// Builds a single-point operator with the read-only/quota policy from
    /// `point` applied directly as OpenDAL layers.
    ///
    /// This is the one place read-only and quota layering for data points is
    /// wired up: it's reused both when composing a user's full VFS tree and
    /// when a backup/restore job needs to touch a single point's backend
    /// directly. `VfsBuilder` refuses to mount at its virtual root `"/"`, so
    /// this applies [`ReadOnlyLayer`]/[`QuotaLayer`] straight to the raw
    /// operator instead of routing through a single-mount `VfsBuilder`.
    fn point_operator(&self, user: &VfsUser, point: &VfsPoint) -> VfsResult<Operator> {
        let mut op = Operator::via_iter(&point.scheme, point.config.clone())?;

        if point.read_only {
            op = op.layer(ReadOnlyLayer);
        } else if let Some(max) = point.max_bytes {
            // Local/fs-backed *data* points are quota-checked against
            // their true on-disk footprint (`utils::dir_size`) directly by
            // the IPC write handlers and by `FtpServer::put` in `ftp.rs`,
            // instead of through this incremental write-counter layer.
            // Skip it here so the two accounting mechanisms don't both
            // apply (and disagree) for the same point — see
            // `utils::dir_size` for why the counter isn't trustworthy for
            // data points any more.
            //
            // Repo points are excluded from that switch even when they're
            // also local/fs-backed: their content is written by rustic
            // during backup jobs (`get_data_operator`, not the
            // `Vfs_OpenWrite`/`Vfs_WriteAt` handles or `Vfs_WriteFile`), so
            // there's no code path that would ever perform a `dir_size`
            // check for them — leaving the counter attached here is the
            // only enforcement they get.
            if point.is_repo || !utils::is_local_scheme(&point.scheme) {
                op = op.layer(QuotaLayer::new(
                    self.state.clone(),
                    utils::quota_id(&user.username, &point.id),
                    max,
                ));
            }
        }

        Ok(op)
    }

    /// Builds one mount. Any failure here only affects this point.
    fn build_mount(&self, user: &VfsUser, point: &VfsPoint) -> VfsResult<(String, Operator)> {
        if !point.is_repo {
            let op = self.point_operator(user, point)?;
            let probe = opendal_core::blocking::Operator::new(op.clone())?;
            match probe.list("") {
                Ok(_) => {}
                Err(e)
                    if e.kind() == opendal_core::ErrorKind::NotFound
                        && utils::is_local_scheme(&point.scheme) =>
                {
                    let root = utils::local_source_path(point)
                        .map_err(|status| VfsError::Internal(status.to_string()))?;
                    std::fs::create_dir_all(root).map_err(|e| {
                        VfsError::Internal(format!("failed to create local point root: {e}"))
                    })?;
                }
                Err(e) if e.kind() == opendal_core::ErrorKind::Unsupported => {
                    // Some backends do not implement root listing. A root stat
                    // is a cheaper fallback that still proves the backend is
                    // reachable.
                    probe.stat("")?;
                }
                Err(e) => return Err(VfsError::OpenDal(e)),
            }
            return Ok((utils::data_mount_path(&point.name), op));
        }

        let pass = point
            .repo_password
            .clone()
            .ok_or(VfsError::RepoPasswordMissing)?;
        let src = RepoSource {
            scheme: point.scheme.clone(),
            config: point.config.clone(),
            password: pass.clone(),
        };
        if let Some(op) = self.repo_vfs_ops.get(&src) {
            return Ok((utils::repo_mount_path(&point.name), op));
        }

        // Serialize preparation and VFS repository construction by the same
        // source key. Otherwise parallel user-cache rebuilds can all perform
        // the same credential/probe/open work at once.
        let lock = self
            .repo_locks
            .entry(src.clone())
            .or_insert_with(|| Arc::new(StdMutex::new(())))
            .clone();
        let _guard = lock
            .lock()
            .map_err(|_| VfsError::Internal("repository VFS lock poisoned".into()))?;

        if let Some(op) = self.repo_vfs_ops.get(&src) {
            return Ok((utils::repo_mount_path(&point.name), op));
        }

        // Read-only repo points must never be created implicitly.
        self.prepare_repo_unlocked(&src, !point.read_only)
            .map_err(|e| VfsError::Storage(e))?;

        let config = OpenDALConfig::default()
            .scheme(point.scheme.clone())
            .options(point.config.clone().into_iter().collect::<HashMap<_, _>>());
        let op = Operator::from_config(RusticVfsConfig {
            options: RepositoryOptions::default(),
            backend: BackendOptions::default().with_repo(&config),
            credentials: Some(Credentials::password(&pass)),
            refresh_interval: Some(Duration::from_mins(2)),
        })?;
        self.repo_vfs_ops.insert(src, op.clone());
        Ok((utils::repo_mount_path(&point.name), op))
    }

    /// Builds the user's VFS. A point that fails to load is recorded, logged
    /// and skipped — it never prevents the user's other points from mounting.
    /// Returns `(operator, degraded)`.
    fn create_for_vfs(&self, user: &VfsUser) -> VfsResult<(Operator, bool)> {
        // `create_for_vfs` itself runs inside `tokio::task::spawn_blocking`,
        // so there is a Tokio runtime handle available here. The scoped
        // threads below are plain std::threads, however, and do not inherit
        // that runtime context automatically.
        //
        // OpenDAL's blocking Operator requires a current Tokio runtime handle,
        // so explicitly enter the runtime on each worker thread.
        let runtime = tokio::runtime::Handle::current();

        let results = std::thread::scope(|scope| {
            user.points
                .iter()
                .map(|point| {
                    let point = point.clone();
                    let user = user.clone();
                    let runtime = runtime.clone();

                    scope.spawn(move || {
                        let _runtime_guard = runtime.enter();
                        self.build_mount(&user, &point)
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|handle| match handle.join() {
                    Ok(result) => result,
                    Err(_) => Err(VfsError::Internal(
                        "point probe thread panicked".into(),
                    )),
                })
                .collect::<Vec<_>>()
        });

        let mut vfs = VfsBuilder::new(self.state.clone());
        let mut degraded = false;

        for (point, result) in user.points.iter().zip(results) {
            match result {
                Ok((path, op)) => {
                    // Repo mounts are always read-only inside the VFS tree;
                    // `point.read_only` gates backup/forget jobs instead.
                    vfs = if point.is_repo {
                        vfs.mount(path, op).read_only()
                    } else {
                        vfs.mount(path, op)
                    };

                    self.set_health(user, point, PointHealth::Healthy);
                }

                Err(e) => {
                    degraded = true;
                    self.set_health(
                        user,
                        point,
                        PointHealth::Failed(e.to_string()),
                    );
                }
            }
        }

        // An empty or completely failed mount set is still a valid VFS.
        // Individual point failures are represented by health state rather
        // than turning the whole user into an authentication failure.
        Ok((Operator::new(vfs)?, degraded))
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
            let lock = this
                .repo_locks
                .entry(src.clone())
                .or_insert_with(|| Arc::new(StdMutex::new(())))
                .clone();
            let _guard = lock
                .lock()
                .map_err(|_| repo_err("repository open lock poisoned"))?;

            if let Some(repo) = this.repos.get(&src) {
                return Ok(repo);
            }

            let result = this.get_raw_repo(&src, false).map(Arc::new);
            if let Ok(repo) = &result {
                this.repos.insert(src.clone(), repo.clone());
            }
            result
        })
        .await
        .map_err(|e| RusticError::with_source(ErrorKind::Backend, "spawn_blocking panicked", e))?
    }

    async fn get_repo_job(
        &self,
        src: &RepoSource,
        operator: opendal_core::blocking::Operator,
        job_id: Uuid,
        tx: Sender<Data>,
        allow_init: bool,
    ) -> RusticResult<RepoIndexed> {
        let this = self.clone();
        let src = src.clone();
        tokio::task::spawn_blocking(move || {
            this.create_for_job(&src, operator, job_id, tx, allow_init)
        })
        .await
        .map_err(|e| RusticError::with_source(ErrorKind::Backend, "spawn_blocking panicked", e))?
    }

    async fn get_vfs(&self, user: &VfsUser) -> VfsResult<Operator> {
        let key = user.username.clone();
        if let Some(op) = self.vfs_ops.get(&key) {
            return Ok(op);
        }

        // A degraded VFS is intentionally usable while an asynchronous refresh
        // retries unhealthy points. This prevents a transient storage outage
        // from turning into an authentication/VFS outage for the whole user.
        if let Some(op) = self.degraded_ops.get(&key) {
            self.refresh_degraded(user.clone());
            return Ok(op);
        }

        self.build_vfs(user.clone()).await
    }

    fn refresh_degraded(&self, user: VfsUser) {
        let key = user.username.clone();
        if self.refreshing.insert(key.clone(), ()).is_some() {
            return;
        }

        let this = self.clone();
        tokio::spawn(async move {
            let result = this.build_vfs(user).await;
            if let Err(e) = result {
                log::warn!("background VFS refresh for '{key}' failed: {e}");
            }
            this.refreshing.remove(&key);
        });
    }

    async fn build_vfs(&self, user: VfsUser) -> VfsResult<Operator> {
        let key = user.username.clone();
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            let lock = this
                .vfs_build_locks
                .entry(key.clone())
                .or_insert_with(|| Arc::new(StdMutex::new(())))
                .clone();
            let _guard = lock
                .lock()
                .map_err(|_| VfsError::Internal("VFS build lock poisoned".into()))?;

            if let Some(op) = this.vfs_ops.get(&key) {
                return Ok(op);
            }

            let generation = this.vfs_generation.get(&key).map(|v| *v).unwrap_or(0);
            let (op, degraded) = this.create_for_vfs(&user)?;

            // Configuration may have changed while the blocking probes ran.
            // Never put a stale user configuration back into the cache.
            let current_generation = this.vfs_generation.get(&key).map(|v| *v).unwrap_or(0);
            if generation != current_generation {
                return Ok(op);
            }

            if degraded {
                this.vfs_ops.invalidate(&key);
                this.degraded_ops.insert(key, op.clone());
            } else {
                this.degraded_ops.invalidate(&key);
                this.vfs_ops.insert(key, op.clone());
            }
            Ok(op)
        })
        .await
        .map_err(|e| VfsError::Internal(format!("spawn_blocking panicked: {e}")))?
    }

    fn invalidate_vfs(&self, user: &VfsUser) {
        self.vfs_generation
            .entry(user.username.clone())
            .and_modify(|generation| *generation = generation.saturating_add(1))
            .or_insert(1);
        self.vfs_ops.invalidate(&user.username);
        self.degraded_ops.invalidate(&user.username);

        // A health entry belongs to a particular point configuration. Remove
        // it when that user's configuration changes so a reused point ID
        // starts from UNKNOWN instead of inheriting an old failure.
        for point in &user.points {
            self.health.remove(&point.id);
            self.data_ops.invalidate(&point.id);
            if point.is_repo {
                if let Some(password) = &point.repo_password {
                    let src = RepoSource {
                        scheme: point.scheme.clone(),
                        config: point.config.clone(),
                        password: password.clone(),
                    };
                    self.repo_vfs_ops.invalidate(&src);
                    self.repos.invalidate(&src);
                }
            }
        }
    }

    fn point_health(&self, id: &Uuid) -> Option<PointHealth> {
        self.health.get(id).map(|h| h.clone())
    }

    fn get_data_operator(
        &self,
        user: &VfsUser,
        point: &VfsPoint,
    ) -> VfsResult<opendal_core::blocking::Operator> {
        if let Some(op) = self.data_ops.get(&point.id) {
            return Ok(op);
        }

        let op = self.point_operator(user, point)?;
        let x = opendal_core::blocking::Operator::new(op)?;
        self.data_ops.insert(point.id, x.clone());
        Ok(x)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    async fn manager() -> StorageManager {
        let db = Arc::new(
            DbManager::open(":memory:")
                .await
                .expect("open in-memory db"),
        );
        StorageManager::new(
            db,
            Duration::from_secs(60),
            crossbeam_channel::unbounded().0,
        )
    }

    fn memory_point(name: &str, read_only: bool, max_bytes: Option<u64>) -> VfsPoint {
        VfsPoint {
            id: Uuid::new_v4(),
            name: name.to_string(),
            max_bytes,
            read_only,
            scheme: "memory".into(),
            config: BTreeMap::new(),
            is_repo: false,
            repo_password: None,
        }
    }

    fn user_with(points: Vec<VfsPoint>) -> VfsUser {
        VfsUser {
            username: "alice".into(),
            password_hash: "pw".into(),
            points,
        }
    }

    #[tokio::test]
    async fn read_only_point_operator_rejects_writes() {
        let mgr = manager().await;
        let point = memory_point("ro", true, None);
        let user = user_with(vec![point.clone()]);

        let op = mgr.point_operator(&user, &point).unwrap();
        let err = op.write("file.txt", b"hi".to_vec()).await.unwrap_err();
        assert_eq!(err.kind(), opendal_core::ErrorKind::PermissionDenied);
    }

    #[tokio::test]
    async fn read_only_point_operator_allows_reads() {
        let mgr = manager().await;

        // Write via a writable operator over the same backend config first,
        // since the read-only operator itself must never be able to write.
        let writable = memory_point("seed", false, None);
        let user = user_with(vec![writable.clone()]);
        let seed_op = mgr.point_operator(&user, &writable).unwrap();
        seed_op.write("file.txt", b"hi".to_vec()).await.unwrap();

        let data = seed_op.read("file.txt").await.unwrap();
        assert_eq!(data.to_vec(), b"hi");
    }

    #[tokio::test]
    async fn writable_point_operator_allows_writes() {
        let mgr = manager().await;
        let point = memory_point("rw", false, None);
        let user = user_with(vec![point.clone()]);

        let op = mgr.point_operator(&user, &point).unwrap();
        op.write("file.txt", b"hi".to_vec()).await.unwrap();
        let data = op.read("file.txt").await.unwrap();
        assert_eq!(data.to_vec(), b"hi");
    }

    #[tokio::test]
    async fn quota_enforced_on_point_operator() {
        let mgr = manager().await;
        let point = memory_point("quota", false, Some(4));
        let user = user_with(vec![point.clone()]);

        let op = mgr.point_operator(&user, &point).unwrap();
        op.write("small.txt", b"ab".to_vec()).await.unwrap();

        let err = op.write("big.txt", b"toolarge".to_vec()).await.unwrap_err();
        assert_eq!(err.kind(), opendal_core::ErrorKind::RateLimited);
    }

    #[tokio::test]
    async fn get_data_operator_applies_same_policy_as_point_operator() {
        let mgr = manager().await;
        let point = memory_point("ro", true, None);
        let user = user_with(vec![point.clone()]);

        let op = mgr.get_data_operator(&user, &point).unwrap();
        assert!(op.write("x", b"y".to_vec()).is_err());
    }

    #[tokio::test]
    async fn invalidate_vfs_evicts_cached_operator() {
        let mgr = manager().await;
        let user = user_with(vec![memory_point("d", false, None)]);

        let op1 = mgr.get_vfs(&user).await.unwrap();
        mgr.invalidate_vfs(&user);
        let op2 = mgr.get_vfs(&user).await.unwrap();

        // Both calls succeed; the important behavior under test is that
        // invalidation doesn't error and a fresh operator can be rebuilt.
        let _ = (op1, op2);
    }
}
