//! gRPC server implementing every method in `IpcService`.

use crossbeam_channel as chan;
use dashmap::DashMap;
use log::warn;
use opendal_core::{Buffer, ErrorKind as DalErrorKind, Operator};
use rustic_backend::local::LocalSource;
use rustic_backend::opendal::OpenDALSource;
use std::collections::{HashSet, VecDeque};
use std::io::SeekFrom;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, Mutex as StdMutex};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex as TokioMutex;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::core::{UserSystem, VfsPoint, VfsUser};
use crate::ipc::ipc_service_server::IpcService as IpcServiceTrait;
use crate::ipc::job_event::Data;
use crate::ipc::vfs_path::Path;
use crate::ipc::vfs_point::Src as ProtoSrc;
use crate::ipc::{
    BackupArgs, CancelArgs, CheckArgs, CloseHandleArgs, Empty, ExistsResponse, FilePath,
    ForgetArgs, GetSnapshotArgs, InfoResponse, JobCancelResponse, JobEvent, JobFinishedEvent,
    JobNewMessageEvent, JobStartResponse, ListVfsResponse, OpenWriteArgs, OpenWriteResponse,
    PointSource as ProtoPoint, PollResponse, Priority, ReadVfsArgs, ReadVfsResponse,
    RepoSource as ProtoRepo, RestoreArgs, SetVfsArgs, Snapshot, SnapshotResponse, StatResponse,
    Summary, TransferArgs, VfsNode, VfsPath, VfsPoint as ProtoVfsPoint, VfsUser as ProtoVfsUser,
    WriteAtArgs,
};
use crate::store::StorageSystem;
use crate::utils;
use crate::utils::{fix_path, map_dal, map_vfs};
use opendal_vfs::layers::quota::{QuotaState, QuotaTracker};
use rustic_core::jiff::Zoned;
use rustic_core::repofile::{SnapshotFile, SnapshotId, SnapshotSummary};
use rustic_core::{
    CancelToken, CheckOptions, LsOptions, PathList, RestoreOptions, SnapshotOptions, StringList,
};

// ── Unified Write handles ──────────────────────────────────────────────
//
// Backing state for `Vfs_OpenWrite`/`Vfs_WriteAt`/`Vfs_CloseWrite`.
// Supports both random-access local writes and sequential remote streaming.
enum WriteStateBackend {
    Local(tokio::fs::File),
    Remote(opendal_core::Writer),
}

struct WriteState {
    backend: WriteStateBackend,
    len: u64,
}

struct ActiveWriteHandle {
    state: TokioMutex<WriteState>,
    max_bytes: Option<u64>,
    baseline_bytes: u64,
}

/// Maps an I/O error to the closest matching gRPC status, the same way
/// `utils::map_dal` does for OpenDAL errors.
fn io_status(e: std::io::Error) -> Status {
    match e.kind() {
        std::io::ErrorKind::NotFound => Status::not_found(e.to_string()),
        std::io::ErrorKind::PermissionDenied => Status::permission_denied(e.to_string()),
        _ => Status::internal(e.to_string()),
    }
}

fn parse_handle_id(id: &str) -> Result<Uuid, Status> {
    Uuid::parse_str(id).map_err(|_| Status::invalid_argument("malformed handle_id"))
}

/// Checks that growing a local point's on-disk footprint by `growth` bytes
/// stays within `max_bytes`, by walking the point's real directory tree
/// (`utils::dir_size`) — see that function's doc comment for why a
/// real-disk measurement is used instead of an incremental counter.
///
/// `growth` should already account for the file being written to (i.e. it's
/// the *net* increase in the file's length caused by this write, not the
/// number of bytes sent over the wire) so in-place overwrites that don't
/// extend the file don't get penalized.
///
/// A no-op (and no disk walk) when `max_bytes` is `None` or `growth` is 0.
async fn check_local_quota(
    root: &std::path::Path,
    max_bytes: Option<u64>,
    growth: u64,
) -> Result<(), Status> {
    let Some(max) = max_bytes else {
        return Ok(());
    };
    if growth == 0 {
        return Ok(());
    }
    let root = root.to_path_buf();
    let used = tokio::task::spawn_blocking(move || utils::dir_size(&root))
        .await
        .map_err(|e| Status::internal(format!("quota check panicked: {e}")))?
        .map_err(|e| Status::internal(format!("failed to measure quota usage: {e}")))?;
    if used + growth > max {
        return Err(Status::resource_exhausted(format!(
            "write would exceed point quota ({used} + {growth} > {max} bytes)"
        )));
    }
    Ok(())
}

// ── Error helpers ─────────────────────────────────────────────────────────────
impl TryFrom<ProtoVfsPoint> for VfsPoint {
    type Error = Status;

    fn try_from(p: ProtoVfsPoint) -> Result<Self, Status> {
        let (scheme, config, is_repo, repo_password) = match p.src {
            Some(ProtoSrc::Data(ps)) => (ps.scheme, ps.config, false, None),
            Some(ProtoSrc::Repo(rs)) => match rs.src {
                None => return Err(Status::invalid_argument("VfsPoint[repo].src is required")),
                Some(x) => (x.scheme, x.config, true, Some(rs.password)),
            },
            None => return Err(Status::invalid_argument("VfsPoint.src is required")),
        };

        Ok(VfsPoint {
            name: p.name,
            max_bytes: (p.max_bytes != 0).then_some(p.max_bytes),
            read_only: !p.can_write,
            scheme,
            config: config.into_iter().collect(),
            is_repo,
            repo_password,
        })
    }
}

impl From<&VfsPoint> for ProtoVfsPoint {
    fn from(point: &VfsPoint) -> Self {
        let p = ProtoPoint {
            scheme: point.scheme.clone(),
            config: point.config.clone().into_iter().collect(),
        };

        let src = if point.is_repo {
            ProtoSrc::Repo(ProtoRepo {
                src: Some(p),
                password: point.repo_password.clone().unwrap_or_default(),
            })
        } else {
            ProtoSrc::Data(p)
        };

        ProtoVfsPoint {
            name: point.name.clone(),
            max_bytes: point.max_bytes.unwrap_or(0),
            can_write: !point.read_only,
            src: Some(src),
        }
    }
}

impl TryFrom<ProtoVfsUser> for VfsUser {
    type Error = Status;

    fn try_from(p: ProtoVfsUser) -> Result<Self, Status> {
        Ok(VfsUser {
            username: p.name,
            password: p.password,
            points: p
                .points
                .into_iter()
                .map(VfsPoint::try_from)
                .collect::<Result<_, _>>()?,
        })
    }
}

impl From<SnapshotSummary> for Summary {
    fn from(s: SnapshotSummary) -> Self {
        Summary {
            files_new: s.files_new,
            files_changed: s.files_changed,
            files_unmodified: s.files_unmodified,
            total_files_processed: s.total_files_processed,
            total_bytes_processed: s.total_bytes_processed,
            dirs_new: s.dirs_new,
            dirs_changed: s.dirs_changed,
            dirs_unmodified: s.dirs_unmodified,
            total_dirs_processed: s.total_dirs_processed,
            total_dirsize_processed: s.total_dirsize_processed,
            data_blobs: s.data_blobs,
            tree_blobs: s.tree_blobs,
            data_added: s.data_added,
            data_added_packed: s.data_added_files_packed, // Adjusted based on context
            data_added_files: s.data_added_files,
            data_added_files_packed: s.data_added_files_packed,
            data_added_trees: s.data_added_trees,
            data_added_trees_packed: s.data_added_trees_packed,
            backup_start: Some(utils::to_ts(s.backup_start)),
            backup_end: Some(utils::to_ts(s.backup_end)),
        }
    }
}

impl From<SnapshotFile> for Snapshot {
    fn from(s: SnapshotFile) -> Self {
        Snapshot {
            id: s.id.to_string(),
            time: Some(utils::to_ts(s.time)),
            summary: s.summary.map(Into::into),
            tags: s.tags.iter().map(|t| t.to_string()).collect(),
            paths: s.paths.iter().map(|p| p.to_string()).collect(),
            app_version: s.program_version,
        }
    }
}

impl std::fmt::Display for VfsPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.path {
            None => Ok(()),
            Some(Path::Virtual(path)) => f.write_str(path),
            Some(Path::Indexed(path)) => {
                write!(
                    f,
                    "{}/{}",
                    utils::data_mount_path(&path.point_name),
                    path.point_path
                )
            }
        }
    }
}

/// Convert an opendal directory entry to the proto node type.
fn entry_to_node(entry: &opendal_core::Entry) -> VfsNode {
    let meta = entry.metadata();
    let name = entry
        .path()
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or(entry.path())
        .to_string();
    let mtime = meta.last_modified().map(chrono_to_ts);
    VfsNode {
        name,
        is_dir: meta.is_dir(),
        bytes: meta.content_length(),
        ctime: None,
        mtime: mtime.clone(),
        atime: None,
    }
}

/// Convert opendal's `Timestamp` metadata (a `jiff::Timestamp` in this
/// version of the crate) into a `prost_types::Timestamp`.
fn chrono_to_ts(ts: opendal_core::raw::Timestamp) -> prost_types::Timestamp {
    let inner = ts.into_inner();
    prost_types::Timestamp {
        seconds: inner.as_second(),
        nanos: inner.subsec_nanosecond(),
    }
}

// ── Server state ──────────────────────────────────────────────────────────────

/// A running job's cancel handle plus the username it runs on behalf of, so
/// `set_vfs` can cancel jobs whose user's VFS config just changed underneath
/// them.
struct JobHandle {
    token: CancelToken,
    user: String,
}

struct Inner<S, U>
where
    S: StorageSystem,
    U: UserSystem,
{
    storage: Arc<S>,
    users: Arc<U>,
    quota: QuotaState,
    jobs: DashMap<Uuid, JobHandle>,
    events: StdMutex<VecDeque<JobEvent>>,
    /// Open `Vfs_OpenWrite` handles, keyed by the id handed back to the
    /// caller. Entries live here for the lifetime of the handle and are
    /// removed by `Vfs_CloseWrite`.
    write_handles: DashMap<Uuid, ActiveWriteHandle>,
}

/// tonic service handle. Cheap to clone — all state lives behind `Arc`.
#[derive(Clone)]
pub struct GrpcServer<S, U>
where
    S: StorageSystem,
    U: UserSystem,
{
    inner: Arc<Inner<S, U>>,
}

impl<S, U> GrpcServer<S, U>
where
    S: StorageSystem,
    U: UserSystem,
{
    pub fn new(storage: Arc<S>, users: Arc<U>, quota: QuotaState) -> Self {
        Self {
            inner: Arc::new(Inner {
                storage,
                users,
                quota,
                jobs: DashMap::new(),
                events: StdMutex::new(VecDeque::new()),
                write_handles: DashMap::new(),
            }),
        }
    }

    /// Attempts to resolve the [`Operator`] and path.
    async fn get_operator(
        &self,
        user: &str,
        path: &Option<VfsPath>,
        is_dir: bool,
    ) -> Result<(Operator, String), Status> {
        let user = self.get_user(user).await?;
        let op = self.inner.storage.get_vfs(&user).await.map_err(map_vfs)?;
        let path = path
            .as_ref()
            .ok_or(Status::invalid_argument("path is blank"))?;
        Ok((op, fix_path(path.to_string(), is_dir)))
    }

    async fn get_user(&self, user: &str) -> Result<VfsUser, Status> {
        self.inner.users.get_user(&user).await.map_err(map_vfs)
    }

    /// Spawn a background job owned by `user` and return its ID immediately.
    ///
    /// The closure runs inside `spawn_blocking` so it may call blocking rustic
    /// APIs freely. For async `StorageSystem` methods (e.g. `get_repo_job`),
    /// use `Handle::current().block_on(...)` inside the closure — this is safe
    /// because `spawn_blocking` threads are not async-task threads.
    fn spawn_job<F>(&self, user: impl Into<String>, f: F) -> String
    where
        F: FnOnce(
                Uuid,
                chan::Sender<Data>,
                CancelToken,
            ) -> rustic_core::RusticResult<Option<String>>
            + Send
            + 'static,
    {
        let job_id = Uuid::new_v4();
        let token = CancelToken::new();
        let (tx, rx) = chan::unbounded::<Data>();
        self.inner.jobs.insert(
            job_id,
            JobHandle {
                token: token.clone(),
                user: user.into(),
            },
        );
        {
            // Bridge thread — drains crossbeam channel into the async-safe
            // event buffer using std::Mutex, never touching the tokio runtime.
            let inner = Arc::clone(&self.inner);
            std::thread::spawn(move || {
                while let Ok(data) = rx.recv() {
                    if let Ok(mut buf) = inner.events.lock() {
                        buf.push_back(JobEvent { data: Some(data) });
                    }
                }
            });
        }
        {
            let inner = Arc::clone(&self.inner);
            tokio::task::spawn_blocking(move || {
                let result = f(job_id, tx.clone(), token);

                if let Err(ref e) = result {
                    let _ = tx.send(Data::JobMessage(JobNewMessageEvent {
                        job_id: job_id.to_string(),
                        priority: Priority::Error as i32,
                        message: e.to_string(),
                        time: Some(utils::to_ts(Zoned::now())),
                    }));
                }

                let _ = tx.send(Data::JobFinished(JobFinishedEvent {
                    job_id: job_id.to_string(),
                    success: result.is_ok(),
                    snapshot: result.ok().flatten(),
                    time: Some(utils::to_ts(Zoned::now())),
                }));

                inner.jobs.remove(&job_id);
            });
        }

        job_id.to_string()
    }
}

// ── IpcService ────────────────────────────────────────────────────────────────

#[tonic::async_trait]
impl<S, U> IpcServiceTrait for GrpcServer<S, U>
where
    S: StorageSystem,
    U: UserSystem,
{
    async fn backup(&self, req: Request<BackupArgs>) -> Result<Response<JobStartResponse>, Status> {
        let args = req.into_inner();
        let user = self.get_user(&args.user).await?;

        // Backups write new snapshots into the repo, so reject up front if
        // the repo point was marked read-only instead of letting the job
        // fail once it's already running.
        let repo_point = utils::require_repo_point(&user, &args.repo_name)?;
        utils::require_writable(repo_point)?;

        let repo_src = utils::repo_source(repo_point)?;
        let data_point = utils::require_data_point(&user, &args.data_name)?;

        // Local/fs sources read straight off disk via `LocalSource`, bypassing
        // the OpenDAL layer (and its quota tracking) entirely. Everything else
        // still goes through `get_data_operator` as before.
        let local_path = if utils::is_local_scheme(&data_point.scheme) {
            Some(utils::local_source_path(data_point)?)
        } else {
            None
        };
        let source_op = if local_path.is_some() {
            None
        } else {
            Some(
                self.inner
                    .storage
                    .get_data_operator(&user, data_point)
                    .map_err(map_vfs)?,
            )
        };

        let repo_op = self
            .inner
            .storage
            .get_data_operator(&user, repo_point)
            .map_err(map_vfs)?;

        let tags = args.tags;
        let storage = Arc::clone(&self.inner.storage);
        let username = user.username.clone();

        let job_id = self.spawn_job(username, move |job_id, tx, token| {
            let handle = tokio::runtime::Handle::current();
            let tags = StringList::from_str(&tags.join(",")).unwrap();
            let snap = SnapshotOptions::default().tags(vec![tags]).to_snapshot()?;
            let repo = handle.block_on(storage.get_repo_job(&repo_src, repo_op, job_id, tx))?;
            let paths = PathList::from_string(&*args.source_path)?;

            let saved = if let Some(path) = local_path {
                let source = LocalSource::new(path);
                repo.backup(snap)
                    .add_multi(&source, paths.paths())
                    .with_token(token)
                    .run()?
            } else {
                let source = OpenDALSource::new(source_op.expect("non-local backup source"));
                repo.backup(snap)
                    .add_multi(&source, paths.paths())
                    .with_token(token)
                    .run()?
            };

            Ok(Some(saved.id.to_string()))
        });

        Ok(Response::new(JobStartResponse { job_id }))
    }

    async fn restore(
        &self,
        req: Request<RestoreArgs>,
    ) -> Result<Response<JobStartResponse>, Status> {
        let args = req.into_inner();
        let user = self.get_user(&args.user).await?;

        let repo_point = utils::require_repo_point(&user, &args.repo_name)?;
        let repo_src = utils::repo_source(repo_point)?;

        // Restores write into the destination data point, so it must be
        // writable — checked up front for the same reason as backup above.
        let dest_point = utils::require_data_point(&user, &args.data_name)?;
        utils::require_writable(dest_point)?;
        let dest_op = self
            .inner
            .storage
            .get_data_operator(&user, dest_point)
            .map_err(map_vfs)?;

        let repo_op = self
            .inner
            .storage
            .get_data_operator(&user, repo_point)
            .map_err(map_vfs)?;

        let snapshot_id = args.snapshot_id;
        let snapshot_path = args.snapshot_path;
        let delete = args.delete;
        let dry_run = args.dry_run;
        let storage = Arc::clone(&self.inner.storage);
        let username = user.username.clone();

        let job_id = self.spawn_job(username, move |job_id, tx, token| {
            let handle = tokio::runtime::Handle::current();
            let repo =
                Arc::new(handle.block_on(storage.get_repo_job(&repo_src, repo_op, job_id, tx))?);
            let dest = OpenDALSource::new(dest_op);
            let opts = RestoreOptions::default().delete(delete);
            let snap_path = format!("{}:{}", &snapshot_id, &snapshot_path);
            let node = repo.node_from_snapshot_path(&snap_path, |_| true)?;
            let streamer_opts = LsOptions::default();
            let ls = repo.ls(&node, &streamer_opts)?;
            let plan = repo.prepare_restore(
                &opts,
                ls.clone(),
                &dest,
                &args.output_path,
                dry_run,
                token.clone(),
            )?;
            if !dry_run {
                repo.restore(plan, &opts, ls.clone(), &dest, token)?;
            }
            Ok(None)
        });

        Ok(Response::new(JobStartResponse { job_id }))
    }

    async fn check(&self, req: Request<CheckArgs>) -> Result<Response<JobStartResponse>, Status> {
        let args = req.into_inner();
        let user = self.get_user(&args.user).await?;

        let repo_point = utils::require_repo_point(&user, &args.repo_name)?;
        let repo_src = utils::repo_source(repo_point)?;
        let repo_op = self
            .inner
            .storage
            .get_data_operator(&user, repo_point)
            .map_err(map_vfs)?;

        let storage = Arc::clone(&self.inner.storage);
        let username = user.username.clone();

        let job_id = self.spawn_job(username, move |job_id, tx, _token| {
            let handle = tokio::runtime::Handle::current();
            let repo = handle.block_on(storage.get_repo_job(&repo_src, repo_op, job_id, tx))?;
            repo.check(CheckOptions::default())?;
            Ok(None)
        });

        Ok(Response::new(JobStartResponse { job_id }))
    }

    async fn forget(&self, req: Request<ForgetArgs>) -> Result<Response<JobStartResponse>, Status> {
        let args = req.into_inner();
        let user = self.get_user(&args.user).await?;

        // Forget deletes snapshots from the repo, so it's write-bound like
        // backup — reject up front if the repo point is read-only.
        let repo_point = utils::require_repo_point(&user, &args.repo_name)?;
        utils::require_writable(repo_point)?;
        let repo_src = utils::repo_source(repo_point)?;
        let repo_op = self
            .inner
            .storage
            .get_data_operator(&user, repo_point)
            .map_err(map_vfs)?;

        let snap_ids: Vec<SnapshotId> = args
            .snapshots
            .iter()
            .map(|s| {
                SnapshotId::from_str(s).map_err(|e| {
                    Status::invalid_argument(format!("invalid snapshot id '{s}': {e}"))
                })
            })
            .collect::<Result<_, _>>()?;

        let storage = Arc::clone(&self.inner.storage);
        let username = user.username.clone();

        let job_id = self.spawn_job(username, move |job_id, tx, _token| {
            let handle = tokio::runtime::Handle::current();
            let repo = handle.block_on(storage.get_repo_job(&repo_src, repo_op, job_id, tx))?;
            repo.delete_snapshots(&snap_ids)?;
            Ok(None)
        });

        Ok(Response::new(JobStartResponse { job_id }))
    }

    async fn get_snapshots(
        &self,
        req: Request<GetSnapshotArgs>,
    ) -> Result<Response<SnapshotResponse>, Status> {
        let args = req.into_inner();
        let user = self.get_user(&args.user).await?;

        let repo_src = utils::repo_source(utils::require_repo_point(&user, &args.repo_src)?)?;

        // get_repo is async (spawn_blocking inside), so .await here is correct.
        // get_all_snapshots is blocking, so it gets its own spawn_blocking.
        let repo = self
            .inner
            .storage
            .get_repo(&repo_src)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;

        let snaps = tokio::task::spawn_blocking(move || repo.get_all_snapshots())
            .await
            .map_err(|e| Status::internal(format!("task join: {e}")))?
            .map_err(|err| Status::internal(err.to_string()))?;

        Ok(Response::new(SnapshotResponse {
            output: snaps.into_iter().map(Into::into).collect(),
        }))
    }

    // ── CancelJob ─────────────────────────────────────────────────────────────

    async fn cancel_job(
        &self,
        req: Request<CancelArgs>,
    ) -> Result<Response<JobCancelResponse>, Status> {
        let args = req.into_inner();
        let uuid = Uuid::parse_str(&args.job_id)
            .map_err(|e| Status::invalid_argument(format!("bad job_id: {e}")))?;

        match self.inner.jobs.get(&uuid) {
            Some(handle) => {
                handle.token.cancel();
                Ok(Response::new(JobCancelResponse {
                    job_id: args.job_id,
                }))
            }
            None => Err(Status::not_found(format!(
                "job '{}' not found or already finished",
                args.job_id
            ))),
        }
    }

    async fn poll(&self, _: Request<Empty>) -> Result<Response<PollResponse>, Status> {
        let events = self
            .inner
            .events
            .lock()
            .map_err(|e| Status::internal(format!("event buffer lock poisoned: {e}")))?
            .drain(..)
            .collect();
        Ok(Response::new(PollResponse { events }))
    }

    async fn set_vfs(&self, req: Request<SetVfsArgs>) -> Result<Response<Empty>, Status> {
        let users = req
            .into_inner()
            .users
            .into_iter()
            .map(VfsUser::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        let old_users = self.inner.users.get_users().await.map_err(map_vfs)?;
        let mut changed_users = Vec::new();
        let mut removed_quotas = Vec::new();

        for old_user in &old_users {
            let new_user = users.iter().find(|user| user.username == old_user.username);

            match new_user {
                Some(new_user) => {
                    if new_user != old_user {
                        changed_users.push(old_user.clone());

                        // Find points that were removed from this user.
                        for old_point in &old_user.points {
                            let still_exists = new_user
                                .points
                                .iter()
                                .any(|point| point.name == old_point.name);

                            if !still_exists && !old_point.is_repo {
                                removed_quotas
                                    .push(utils::quota_id(&old_user.username, &old_point.name));
                            }
                        }
                    }
                }

                None => {
                    // The entire user was removed.
                    changed_users.push(old_user.clone());

                    // Remove all quota state belonging to this user.
                    for point in &old_user.points {
                        if !point.is_repo {
                            removed_quotas.push(utils::quota_id(&old_user.username, &point.name));
                        }
                    }
                }
            }
        }

        // Update the database first. If this fails, don't cancel jobs,
        // invalidate caches, or remove quota state.
        self.inner.users.set_users(users).await.map_err(map_vfs)?;

        // Cancel any in-flight job belonging to a user whose VFS config just
        // changed (including removal, and read_only/max_bytes edits) before
        // touching caches, so no job keeps running against stale points.
        let changed_usernames: HashSet<&str> =
            changed_users.iter().map(|u| u.username.as_str()).collect();
        for job in self.inner.jobs.iter() {
            if changed_usernames.contains(job.user.as_str()) {
                job.token.cancel();
            }
        }

        for user in changed_users {
            warn!(
                "Detected change on user: {}. Invalidating...",
                &user.username
            );
            self.inner.storage.invalidate_vfs(&user);
        }

        // Remove quota state for points that were removed.
        for id in removed_quotas {
            self.inner
                .quota
                .clear(&id)
                .map_err(|_| Status::internal("Failed to clear quota."))?;
        }

        Ok(Response::new(Empty {}))
    }

    async fn get_vfs(&self, _request: Request<Empty>) -> Result<Response<InfoResponse>, Status> {
        let users = self.inner.users.get_users().await.map_err(map_vfs)?;

        let mut info = Vec::new();

        for user in users {
            for point in &user.points {
                let used_bytes = if !point.is_repo && utils::is_local_scheme(&point.scheme) {
                    // Local points aren't tracked by the counter-based
                    // `QuotaTracker` any more (see `store::point_operator`),
                    // so report their true on-disk footprint directly.
                    //
                    // A single misconfigured/unreadable local point (bad
                    // `root` config, permissions error, etc.) shouldn't take
                    // out visibility into every other user's/point's info,
                    // so failures here are logged and reported as 0 rather
                    // than propagated with `?`.
                    match utils::local_source_path(point) {
                        Ok(root) => {
                            let root = PathBuf::from(root);
                            match tokio::task::spawn_blocking(move || utils::dir_size(&root)).await
                            {
                                Ok(Ok(bytes)) => bytes,
                                Ok(Err(e)) => {
                                    warn!(
                                        "GetVfs: failed to measure usage for {}'s point '{}': {e}",
                                        user.username, point.name
                                    );
                                    0
                                }
                                Err(e) => {
                                    warn!(
                                        "GetVfs: quota scan panicked for {}'s point '{}': {e}",
                                        user.username, point.name
                                    );
                                    0
                                }
                            }
                        }
                        Err(e) => {
                            warn!(
                                "GetVfs: point '{}' for user '{}' has no usable local root: {e}",
                                point.name, user.username
                            );
                            0
                        }
                    }
                } else {
                    self.inner
                        .quota
                        .current_bytes(&utils::quota_id(&user.username, &point.name))
                        .await
                        .map_err(|err| Status::internal(err.to_string()))?
                };

                info.push(crate::ipc::VfsInfo {
                    user: user.username.clone(),
                    point: Some(point.into()),
                    used_bytes,
                });
            }
        }

        Ok(Response::new(InfoResponse { info }))
    }

    async fn vfs_read_file(
        &self,
        request: Request<ReadVfsArgs>,
    ) -> Result<Response<ReadVfsResponse>, Status> {
        let args = request.into_inner();
        let (op, path) = self.get_operator(&args.user, &args.path, false).await?;
        let buf = if args.length == 0 {
            op.read(&path).await.map_err(map_dal)?
        } else {
            op.read_with(&path)
                .range(args.offset..args.offset + args.length)
                .await
                .map_err(map_dal)?
        };

        Ok(Response::new(ReadVfsResponse {
            data: buf.to_bytes().to_vec(),
        }))
    }

    // ── Write handles (streaming + random access) ─────────────────

    async fn vfs_open_write(
        &self,
        request: Request<OpenWriteArgs>,
    ) -> Result<Response<OpenWriteResponse>, Status> {
        let args = request.into_inner();
        let user = self.get_user(&args.user).await?;
        let path_str = args
            .path
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("path is blank"))?
            .to_string();

        let (point, rest) = utils::resolve_data_path(&user, &path_str)?;
        utils::require_writable(point)?;

        let handle_id = Uuid::new_v4();

        if utils::is_local_scheme(&point.scheme) {
            let root = PathBuf::from(utils::local_source_path(point)?);
            let full_path = root.join(&rest);

            if let Some(parent) = full_path.parent() {
                tokio::fs::create_dir_all(parent).await.map_err(io_status)?;
            }

            let file = tokio::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .read(true)
                .truncate(true)
                .open(&full_path)
                .await
                .map_err(io_status)?;

            let root_for_scan = root.clone();
            let baseline_bytes =
                tokio::task::spawn_blocking(move || utils::dir_size(&root_for_scan))
                    .await
                    .map_err(|e| Status::internal(format!("quota check panicked: {e}")))?
                    .map_err(|e| Status::internal(format!("failed to measure quota usage: {e}")))?;

            if let Some(max) = point.max_bytes {
                if baseline_bytes > max {
                    return Err(Status::resource_exhausted(format!(
                        "point '{}' is already over its {max}-byte quota ({baseline_bytes} bytes used)",
                        point.name
                    )));
                }
            }

            self.inner.write_handles.insert(
                handle_id,
                ActiveWriteHandle {
                    state: TokioMutex::new(WriteState {
                        backend: WriteStateBackend::Local(file),
                        len: 0,
                    }),
                    max_bytes: point.max_bytes,
                    baseline_bytes,
                },
            );
        } else {
            let (op, file_path) = self.get_operator(&args.user, &args.path, false).await?;
            let writer = op.writer(&file_path).await.map_err(map_dal)?;

            self.inner.write_handles.insert(
                handle_id,
                ActiveWriteHandle {
                    state: TokioMutex::new(WriteState {
                        backend: WriteStateBackend::Remote(writer),
                        len: 0,
                    }),
                    max_bytes: None, // Remote quotas are handled downstream via OpenDAL quota layer
                    baseline_bytes: 0,
                },
            );
        }

        Ok(Response::new(OpenWriteResponse {
            handle_id: handle_id.to_string(),
        }))
    }

    async fn vfs_write_at(&self, request: Request<WriteAtArgs>) -> Result<Response<Empty>, Status> {
        let args = request.into_inner();
        let handle_id = parse_handle_id(&args.handle_id)?;

        let handle = self
            .inner
            .write_handles
            .get(&handle_id)
            .ok_or_else(|| Status::not_found("unknown or already-closed write handle"))?;

        let mut state = handle.state.lock().await;

        let WriteState { backend, len } = &mut *state;

        match backend {
            WriteStateBackend::Local(file) => {
                let new_len = (*len).max(args.offset + args.data.len() as u64);

                if let Some(max) = handle.max_bytes {
                    if handle.baseline_bytes + new_len > max {
                        return Err(Status::resource_exhausted(format!(
                            "write would exceed point quota ({} + {new_len} > {max} bytes)",
                            handle.baseline_bytes
                        )));
                    }
                }

                file.seek(SeekFrom::Start(args.offset))
                    .await
                    .map_err(io_status)?;

                file.write_all(&args.data).await.map_err(io_status)?;

                *len = new_len;
            }

            WriteStateBackend::Remote(writer) => {
                if args.offset != *len {
                    return Err(Status::invalid_argument(
                        "write_at (random access/seeking) is restricted to local backends. Non-local backends must write sequentially.",
                    ));
                }

                writer.write(args.data.clone()).await.map_err(map_dal)?;

                *len += args.data.len() as u64;
            }
        }

        Ok(Response::new(Empty {}))
    }

    async fn vfs_close_write(
        &self,
        request: Request<CloseHandleArgs>,
    ) -> Result<Response<Empty>, Status> {
        let args = request.into_inner();
        let handle_id = parse_handle_id(&args.handle_id)?;

        let (_, handle) = self
            .inner
            .write_handles
            .remove(&handle_id)
            .ok_or_else(|| Status::not_found("unknown or already-closed write handle"))?;

        let mut state = handle.state.lock().await;
        match &mut state.backend {
            WriteStateBackend::Local(file) => {
                file.flush().await.map_err(io_status)?;
                file.sync_all().await.map_err(io_status)?;
            }

            WriteStateBackend::Remote(writer) => {
                writer.close().await.map_err(map_dal)?;
            }
        }

        Ok(Response::new(Empty {}))
    }

    async fn vfs_touch_file(&self, request: Request<FilePath>) -> Result<Response<Empty>, Status> {
        let args = request.into_inner();
        let (op, path) = self.get_operator(&args.user, &args.path, false).await?;
        op.write(&path, Buffer::new()).await.map_err(map_dal)?;
        Ok(Response::new(Empty {}))
    }

    async fn vfs_list_dir(
        &self,
        request: Request<FilePath>,
    ) -> Result<Response<ListVfsResponse>, Status> {
        let args = request.into_inner();
        let (op, path) = self.get_operator(&args.user, &args.path, true).await?;
        let entries = op.list_with(&path).await.map_err(map_dal)?;

        Ok(Response::new(ListVfsResponse {
            nodes: entries.iter().map(entry_to_node).collect(),
        }))
    }

    async fn vfs_create_dir(&self, request: Request<FilePath>) -> Result<Response<Empty>, Status> {
        let args = request.into_inner();
        let (op, path) = self.get_operator(&args.user, &args.path, true).await?;
        op.create_dir(&path).await.map_err(map_dal)?;
        Ok(Response::new(Empty {}))
    }

    async fn vfs_remove_file(&self, request: Request<FilePath>) -> Result<Response<Empty>, Status> {
        let args = request.into_inner();
        let (op, path) = self.get_operator(&args.user, &args.path, false).await?;
        op.delete_with(&path).await.map_err(map_dal)?;
        Ok(Response::new(Empty {}))
    }

    async fn vfs_remove_dir(&self, request: Request<FilePath>) -> Result<Response<Empty>, Status> {
        let args = request.into_inner();
        let (op, path) = self.get_operator(&args.user, &args.path, true).await?;
        op.delete_with(&path)
            .recursive(true)
            .await
            .map_err(map_dal)?;
        Ok(Response::new(Empty {}))
    }

    async fn vfs_stat(&self, request: Request<FilePath>) -> Result<Response<StatResponse>, Status> {
        let args = request.into_inner();

        // We don't know up front whether the path is a file or a directory,
        // and `fix_path` normalizes each differently (trailing slash for
        // dirs). Try the file form first since that's the common case, and
        // fall back to the directory form on NotFound before giving up.
        let (op, file_path) = self.get_operator(&args.user, &args.path, false).await?;

        let meta = match op.stat(&file_path).await {
            Ok(meta) => meta,
            Err(e) if e.kind() == DalErrorKind::NotFound => {
                let dir_path = fix_path(&file_path, true);
                op.stat(&dir_path).await.map_err(map_dal)?
            }
            Err(e) => return Err(map_dal(e)),
        };

        let name = file_path
            .trim_end_matches('/')
            .rsplit('/')
            .next()
            .unwrap_or(&file_path)
            .to_string();

        let mtime = meta.last_modified().map(chrono_to_ts);

        Ok(Response::new(StatResponse {
            node: Some(VfsNode {
                name,
                is_dir: meta.is_dir(),
                bytes: meta.content_length(),
                ctime: None,
                mtime,
                atime: None,
            }),
        }))
    }

    async fn vfs_exists(
        &self,
        request: Request<FilePath>,
    ) -> Result<Response<ExistsResponse>, Status> {
        let args = request.into_inner();

        // Same file-then-dir probing strategy as `vfs_stat`, since we don't
        // know the path kind up front.
        let (op, file_path) = self.get_operator(&args.user, &args.path, false).await?;

        let exists = match op.stat(&file_path).await {
            Ok(_) => true,
            Err(e) if e.kind() == DalErrorKind::NotFound => {
                let dir_path = fix_path(&file_path, true);
                match op.stat(&dir_path).await {
                    Ok(_) => true,
                    Err(e2) if e2.kind() == DalErrorKind::NotFound => false,
                    Err(e2) => return Err(map_dal(e2)),
                }
            }
            Err(e) => return Err(map_dal(e)),
        };

        Ok(Response::new(ExistsResponse { exists }))
    }

    async fn vfs_transfer(
        &self,
        request: Request<TransferArgs>,
    ) -> Result<Response<Empty>, Status> {
        let args = request.into_inner();

        let (src_op, src_path) = self
            .get_operator(&args.old_user, &args.old_path, false)
            .await?;
        let (dst_op, dst_path) = self
            .get_operator(&args.new_user, &args.new_path, false)
            .await?;

        // Two operators are "the same backend" if their scheme, root, and
        // backend name all match. This is the closest thing OpenDAL exposes
        // to identity/equality on `Operator` (which doesn't impl PartialEq).
        let src_info = src_op.info();
        let dst_info = dst_op.info();
        let same_backend = src_info.scheme() == dst_info.scheme()
            && src_info.root() == dst_info.root()
            && src_info.name() == dst_info.name();

        if same_backend {
            // Same backend: let opendal do an intra-backend copy/rename,
            // which is typically far cheaper than a read+write round trip
            // (and atomic where the backend supports it).
            if args.copy {
                src_op.copy(&src_path, &dst_path).await.map_err(map_dal)?;
            } else {
                src_op.rename(&src_path, &dst_path).await.map_err(map_dal)?;
            }
        } else {
            // Different backends: no cross-backend copy/rename primitive
            // exists, so stream the bytes through manually. This only
            // handles single files, not recursive directory trees.
            let buf = src_op.read(&src_path).await.map_err(map_dal)?;

            // If the destination is a local point with a quota, check real
            // on-disk usage before writing — this write goes through the
            // OpenDAL operator like any other non-handle write, so it isn't
            // covered by `Vfs_OpenWrite`/`Vfs_WriteAt`'s per-write checks.
            if let Ok(dst_user) = self.get_user(&args.new_user).await {
                if let Ok((dst_point, dst_rest)) = utils::resolve_data_path(&dst_user, &dst_path) {
                    if utils::is_local_scheme(&dst_point.scheme) {
                        let root = PathBuf::from(utils::local_source_path(dst_point)?);
                        let full_path = root.join(&dst_rest);
                        let existing_len = tokio::fs::metadata(&full_path)
                            .await
                            .map(|m| m.len())
                            .unwrap_or(0);
                        let growth = (buf.len() as u64).saturating_sub(existing_len);
                        check_local_quota(&root, dst_point.max_bytes, growth).await?;
                    }
                }
            }

            dst_op.write(&dst_path, buf).await.map_err(map_dal)?;

            if !args.copy {
                src_op.delete(&src_path).await.map_err(map_dal)?;
            }
        }

        Ok(Response::new(Empty {}))
    }
}
