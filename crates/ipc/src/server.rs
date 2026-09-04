//! gRPC server implementing every method in `IpcService`.

use crossbeam_channel as chan;
use dashmap::DashMap;
use opendal_core::{Buffer, ErrorKind as DalErrorKind, Operator};
use rustic_backend::opendal::OpenDALSource;
use std::collections::VecDeque;
use std::str::FromStr;
use std::sync::{Arc, Mutex as StdMutex};
use log::warn;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::core::{UserSystem, VfsPoint, VfsUser};
use crate::ipc::ipc_service_server::IpcService as IpcServiceTrait;
use crate::ipc::job_event::Data;
use crate::ipc::vfs_path::Path;
use crate::ipc::vfs_point::Src as ProtoSrc;
use crate::ipc::{
    BackupArgs, CancelArgs, CheckArgs, Empty, ExistsResponse, FilePath, ForgetArgs,
    GetSnapshotArgs, InfoResponse, JobCancelResponse, JobEvent, JobFinishedEvent,
    JobNewMessageEvent, JobStartResponse, ListVfsResponse, PointSource as ProtoPoint, PollResponse,
    Priority, ReadVfsArgs, ReadVfsResponse, RepoSource as ProtoRepo, RestoreArgs, SetVfsArgs,
    Snapshot, SnapshotResponse, StatResponse, Summary, TransferArgs, VfsNode, VfsPath,
    VfsPoint as ProtoVfsPoint, VfsUser as ProtoVfsUser, WriteVfsArgs,
};
use crate::store::StorageSystem;
use crate::utils;
use crate::utils::{fix_path, map_dal, map_vfs};
use opendal_vfs::layers::quota::QuotaTracker;
use rustic_core::jiff::Zoned;
use rustic_core::repofile::{SnapshotFile, SnapshotId, SnapshotSummary};
use rustic_core::{
    BackupOptions, CancelToken, CheckOptions, LsOptions, PathList, RestoreOptions, SnapshotOptions,
    StringList,
};

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
            data_added_packed: s.data_added_packed,
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
                write!(f, "/points/{}/{}", path.point_name, path.point_path)
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

struct Inner<S, U, Q>
where
    S: StorageSystem,
    U: UserSystem,
    Q: QuotaTracker,
{
    storage: Arc<S>,
    users: Arc<U>,
    quota: Arc<Q>,
    jobs: DashMap<Uuid, CancelToken>,
    events: StdMutex<VecDeque<JobEvent>>,
}

/// tonic service handle. Cheap to clone — all state lives behind `Arc`.
#[derive(Clone)]
pub struct GrpcServer<S, U, Q>
where
    S: StorageSystem,
    U: UserSystem,
    Q: QuotaTracker,
{
    inner: Arc<Inner<S, U, Q>>,
}

impl<S, U, Q> GrpcServer<S, U, Q>
where
    S: StorageSystem,
    U: UserSystem,
    Q: QuotaTracker,
{
    pub fn new(storage: Arc<S>, users: Arc<U>, quota: Arc<Q>) -> Self {
        Self {
            inner: Arc::new(Inner {
                storage,
                users,
                quota,
                jobs: DashMap::new(),
                events: StdMutex::new(VecDeque::new()),
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

    /// Spawn a background job and return its ID immediately.
    ///
    /// The closure runs inside `spawn_blocking` so it may call blocking rustic
    /// APIs freely. For async `StorageSystem` methods (e.g. `get_repo_job`),
    /// use `Handle::current().block_on(...)` inside the closure — this is safe
    /// because `spawn_blocking` threads are not async-task threads.
    fn spawn_job<F>(&self, f: F) -> String
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
        self.inner.jobs.insert(job_id, token.clone());
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
impl<S, U, Q> IpcServiceTrait for GrpcServer<S, U, Q>
where
    S: StorageSystem,
    U: UserSystem,
    Q: QuotaTracker,
{
    async fn backup(&self, req: Request<BackupArgs>) -> Result<Response<JobStartResponse>, Status> {
        let args = req.into_inner();
        let user = self.get_user(&args.user).await?;

        let repo_src = utils::resolve_repo_point(&user, &args.repo_name)?;
        let data_config = utils::resolve_data_point(&user, &args.data_name, &args.source_path)?;
        let tags = args.tags;
        let storage = Arc::clone(&self.inner.storage);

        let job_id = self.spawn_job(move |job_id, tx, token| {
            let handle = tokio::runtime::Handle::current();
            let source = OpenDALSource::from_config(&data_config)?;
            let tags = StringList::from_str(&tags.join(",")).unwrap();
            let snap = SnapshotOptions::default().tags(vec![tags]).to_snapshot()?;
            let repo = handle.block_on(storage.get_repo_job(&repo_src, job_id, tx))?;
            let saved = repo.backup_with(
                &BackupOptions::default(),
                &source,
                snap,
                PathList::from(args.source_path),
                token,
            )?;
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

        let repo_src = utils::resolve_repo_point(&user, &args.repo_name)?;
        let dest_config = utils::resolve_data_point(&user, &args.data_name, &args.output_path)?;
        let snapshot_id = args.snapshot_id;
        let snapshot_path = args.snapshot_path;
        let delete = args.delete;
        let dry_run = args.dry_run;
        let storage = Arc::clone(&self.inner.storage);

        let job_id = self.spawn_job(move |job_id, tx, token| {
            let handle = tokio::runtime::Handle::current();
            let repo = Arc::new(handle.block_on(storage.get_repo_job(&repo_src, job_id, tx))?);
            let dest = OpenDALSource::from_config(&dest_config)?;
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

        let repo_src = utils::resolve_repo_point(&user, &args.repo_name)?;
        let storage = Arc::clone(&self.inner.storage);

        let job_id = self.spawn_job(move |job_id, tx, _token| {
            let handle = tokio::runtime::Handle::current();
            let repo = handle.block_on(storage.get_repo_job(&repo_src, job_id, tx))?;
            repo.check(CheckOptions::default())?;
            Ok(None)
        });

        Ok(Response::new(JobStartResponse { job_id }))
    }

    async fn forget(&self, req: Request<ForgetArgs>) -> Result<Response<JobStartResponse>, Status> {
        let args = req.into_inner();
        let user = self.get_user(&args.user).await?;

        let repo_src = utils::resolve_repo_point(&user, &args.repo_name)?;

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

        let job_id = self.spawn_job(move |job_id, tx, _token| {
            let handle = tokio::runtime::Handle::current();
            let repo = handle.block_on(storage.get_repo_job(&repo_src, job_id, tx))?;
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

        let repo_src = utils::resolve_repo_point(&user, &args.repo_src)?;

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
            Some(token) => {
                token.cancel();
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
                                    .push(format!("{}-{}", old_user.username, old_point.name));
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
                            removed_quotas.push(format!("{}-{}", old_user.username, point.name));
                        }
                    }
                }
            }
        }

        // Update the database first. If this fails, don't invalidate caches or
        // remove quota state.
        self.inner.users.set_users(users).await.map_err(map_vfs)?;

        // Invalidate only users whose VFS configuration changed.
        for user in changed_users {
            warn!("Detected change on user: {}. Invalidating...", &user.username);
            self.inner.storage.invalidate_vfs(&user);
        }

        // Remove quota state for points that were removed.
        for id in removed_quotas {
            self.inner.quota.clear(&id).await.map_err(|_|Status::internal("Failed to clear quota."))?;
        }

        Ok(Response::new(Empty {}))
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

    async fn vfs_write_file(
        &self,
        request: Request<WriteVfsArgs>,
    ) -> Result<Response<Empty>, Status> {
        let args = request.into_inner();
        let (op, path) = self.get_operator(&args.user, &args.path, false).await?;
        if args.append {
            op.write_with(&path, args.data)
                .append(true)
                .await
                .map_err(map_dal)?;
        } else {
            op.write(&path, args.data).await.map_err(map_dal)?;
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
            dst_op.write(&dst_path, buf).await.map_err(map_dal)?;

            if !args.copy {
                src_op.delete(&src_path).await.map_err(map_dal)?;
            }
        }

        Ok(Response::new(Empty {}))
    }

    async fn get_vfs(&self, _request: Request<Empty>) -> Result<Response<InfoResponse>, Status> {
        let users = self.inner.users.get_users().await.map_err(map_vfs)?;

        let mut info = Vec::new();

        for user in users {
            for point in &user.points {
                let used_bytes = if point.is_repo {
                    // Repository VFS points are read-only and don't have a quota.
                    0
                } else {
                    self.inner
                        .quota
                        .get_bytes_written(&format!("{}-{}", &user.username, &point.name))
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
}
