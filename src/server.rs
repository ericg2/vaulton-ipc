//! gRPC server implementing every method in `IpcService`.

use crossbeam_channel as chan;
use dashmap::DashMap;
use opendal_core::Buffer;
use prost_types::Timestamp;
use rustic_backend::opendal::{OpenDALConfig, OpenDALSource};
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex as StdMutex};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::ipc::ipc_service_server::IpcService as IpcServiceTrait;
use crate::ipc::job_event::Data;
use crate::ipc::vfs_point::Src as ProtoSrc;
use crate::ipc::{
    AppendVfsArgs, BackupArgs, CancelArgs, CheckArgs, Empty, ForgetArgs, GetSnapshotArgs,
    JobCancelResponse, JobEvent, JobFinishedEvent, JobNewMessageEvent, JobStartResponse,
    ListVfsArgs, ListVfsResponse, PollResponse, Priority, ReadVfsArgs, ReadVfsResponse,
    RepoSource as ProtoRepoSource, RestoreArgs, SetVfsArgs, Snapshot, SnapshotResponse, Summary,
    TouchVfsArgs, VfsNode, VfsPoint as ProtoVfsPoint, VfsUser as ProtoVfsUser, WriteVfsArgs,
};
use rustic_core::jiff::Zoned;
use rustic_core::repofile::{SnapshotFile, SnapshotId, SnapshotSummary};
use rustic_core::{
    BackupOptions, CancelToken, CheckOptions, LsOptions, PathList, RestoreOptions, SnapshotOptions,
    StringList,
};

use crate::core::{UserSystem, VfsError, VfsPoint, VfsUser};
use crate::store::{RepoSource, StorageSystem};

// ── Error helpers ─────────────────────────────────────────────────────────────

fn internal(e: impl std::fmt::Display) -> Status {
    Status::internal(e.to_string())
}

fn invalid(msg: impl Into<String>) -> Status {
    Status::invalid_argument(msg.into())
}

fn not_found(msg: impl Into<String>) -> Status {
    Status::not_found(msg.into())
}

fn map_vfs(e: VfsError) -> Status {
    match e {
        VfsError::UserNotFound => not_found("user not found"),
        e => internal(e),
    }
}

fn map_dal(e: opendal_core::Error) -> Status {
    match e.kind() {
        opendal_core::ErrorKind::NotFound => not_found(e.to_string()),
        opendal_core::ErrorKind::PermissionDenied => Status::permission_denied(e.to_string()),
        _ => internal(e),
    }
}

// ── Timestamp helpers ─────────────────────────────────────────────────────────

fn to_ts(dt: Zoned) -> Timestamp {
    Timestamp {
        seconds: dt.timestamp().as_second(),
        nanos: dt.timestamp().subsec_nanosecond(),
    }
}

fn opt_ts(dt: Option<Zoned>) -> Option<Timestamp> {
    dt.map(to_ts)
}

// ── Domain conversions ────────────────────────────────────────────────────────

/// Converts a [`Path`] into an OpenDAL-supported [`String`].
///
/// # Arguments
/// * `base` - The root [`Path`] to use.
/// * `p` - The [`Path`] to convert from.
/// * `is_dir` - If representing a directory or file.
///
/// # Returns
/// A valid [`String`] for OpenDAL use.
pub(crate) fn fix_path(p: impl AsRef<Path>, is_dir: bool) -> String {
    let mut r = p.as_ref().to_string_lossy().to_string();
    if !r.starts_with("/") {
        r = format!("/{r}")
    }
    if is_dir && !r.ends_with("/") {
        r += "/"
    } else if !is_dir && r.ends_with("/") {
        r = r.strip_suffix("/").unwrap_or(&r).to_string()
    }
    r.replace("\\", "/") // *** fix for windows-style directories
}

fn parse_repo_src(p: ProtoRepoSource) -> Result<RepoSource, Status> {
    let src = p.src.ok_or(invalid("missing repo src"))?;
    Ok(RepoSource {
        scheme: src.scheme,
        config: src.config.into_iter().collect(),
        password: p.password,
    })
}

fn require_repo_src(
    opt: Option<ProtoRepoSource>,
    field: &'static str,
) -> Result<RepoSource, Status> {
    parse_repo_src(opt.ok_or_else(|| invalid(format!("missing {field}")))?)
}

/// Resolves a `repo_name` against a loaded [`VfsUser`]'s mounted points,
/// producing the [`RepoSource`] needed to open the repository.
///
/// Only points mounted with `is_repo = true` qualify; the point must also
/// carry a `repo_password`, since that's required to open/decrypt it.
fn resolve_repo_point(user: &VfsUser, repo_name: &str) -> Result<RepoSource, Status> {
    let point = user
        .points
        .iter()
        .find(|p| p.name == repo_name)
        .ok_or_else(|| not_found(format!("repo point '{repo_name}' not found")))?;

    if !point.is_repo {
        return Err(invalid(format!(
            "point '{repo_name}' is a data point, not a repo"
        )));
    }

    let password = point
        .repo_password
        .clone()
        .ok_or_else(|| invalid(format!("repo point '{repo_name}' is missing a password")))?;

    Ok(RepoSource {
        scheme: point.scheme.clone(),
        config: point.config.clone().into_iter().collect(),
        password,
    })
}

/// Resolves a VFS-relative path (as exposed to VFS clients, e.g.
/// `/points/<name>/sub/dir`) against a loaded [`VfsUser`]'s mounted points.
///
/// Returns the [`OpenDALConfig`] for the underlying data point plus the
/// remaining path within that point. Only data points (`is_repo = false`)
/// are supported for backup/restore — repo-mounted paths are intentionally
/// rejected, since they're harder to parse reliably and more prone to
/// changing shape.
fn resolve_data_path(user: &VfsUser, vfs_path: &str) -> Result<(OpenDALConfig, PathBuf), Status> {
    let trimmed = vfs_path.trim_start_matches('/');
    let mut parts = trimmed.splitn(3, '/');

    let root = parts.next().unwrap_or("");
    if root != "points" {
        return Err(invalid(format!(
            "path '{vfs_path}' must be under /points/<name>/... (repo-mounted paths aren't supported for backup/restore)"
        )));
    }

    let point_name = parts
        .next()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| invalid(format!("path '{vfs_path}' is missing a point name")))?;
    let rest = parts.next().unwrap_or("");

    let point = user
        .points
        .iter()
        .find(|p| p.name == point_name)
        .ok_or_else(|| not_found(format!("data point '{point_name}' not found")))?;

    if point.is_repo {
        return Err(invalid(format!(
            "point '{point_name}' is a repo, not a data point"
        )));
    }

    let config = OpenDALConfig::default()
        .scheme(point.scheme.clone())
        .options(point.config.clone().into_iter().collect::<HashMap<_, _>>());

    Ok((config, PathBuf::from(rest)))
}

impl TryFrom<ProtoVfsPoint> for VfsPoint {
    type Error = Status;

    fn try_from(p: ProtoVfsPoint) -> Result<Self, Status> {
        let (scheme, config, is_repo, repo_password) = match p.src {
            Some(ProtoSrc::Data(ps)) => (ps.scheme, ps.config, false, None),
            Some(ProtoSrc::Repo(rs)) => match rs.src {
                None => return Err(invalid("VfsPoint[repo].src is required")),
                Some(x) => (x.scheme, x.config, true, Some(rs.password)),
            },
            None => return Err(invalid("VfsPoint.src is required")),
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

impl TryFrom<ProtoVfsUser> for VfsUser {
    type Error = Status;

    fn try_from(p: ProtoVfsUser) -> Result<Self, Status> {
        Ok(VfsUser {
            username: p.name,
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
            backup_start: Some(to_ts(s.backup_start)),
            backup_end: Some(to_ts(s.backup_end)),
        }
    }
}

impl From<SnapshotFile> for Snapshot {
    fn from(s: SnapshotFile) -> Self {
        Snapshot {
            id: s.id.to_string(),
            time: Some(to_ts(s.time)),
            summary: s.summary.map(Into::into),
            tags: s.tags.iter().map(|t| t.to_string()).collect(),
            paths: s.paths.iter().map(|p| p.to_string()).collect(),
            app_version: s.program_version,
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
    let ts = None;
    VfsNode {
        name,
        is_dir: meta.is_dir(),
        bytes: meta.content_length(),
        ctime: ts.clone(),
        mtime: ts.clone(),
        atime: ts,
    }
}

// ── Server state ──────────────────────────────────────────────────────────────

struct Inner<S, U>
where
    S: StorageSystem,
    U: UserSystem,
{
    storage: Arc<S>,
    users: Arc<U>,
    jobs: DashMap<Uuid, CancelToken>,
    events: StdMutex<VecDeque<JobEvent>>,
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
    pub fn new(storage: Arc<S>, users: Arc<U>) -> Self {
        Self {
            inner: Arc::new(Inner {
                storage,
                users,
                jobs: DashMap::new(),
                events: StdMutex::new(VecDeque::new()),
            }),
        }
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
                        time: Some(to_ts(Zoned::now())),
                    }));
                }

                let _ = tx.send(Data::JobFinished(JobFinishedEvent {
                    job_id: job_id.to_string(),
                    success: result.is_ok(),
                    snapshot: result.ok().flatten(),
                    time: Some(to_ts(Zoned::now())),
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
    // ── Backup ────────────────────────────────────────────────────────────────

    async fn backup(&self, req: Request<BackupArgs>) -> Result<Response<JobStartResponse>, Status> {
        let args = req.into_inner();
        let user = self
            .inner
            .users
            .load_user(&args.user)
            .await
            .map_err(map_vfs)?;

        let repo_src = resolve_repo_point(&user, &args.repo_name)?;
        let (data_config, backup_path) = resolve_data_path(&user, &args.source_path)?;
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
                PathList::from(backup_path),
                token,
            )?;
            Ok(Some(saved.id.to_string()))
        });

        Ok(Response::new(JobStartResponse { job_id }))
    }

    // ── Restore ───────────────────────────────────────────────────────────────

    async fn restore(
        &self,
        req: Request<RestoreArgs>,
    ) -> Result<Response<JobStartResponse>, Status> {
        let args = req.into_inner();
        let user = self
            .inner
            .users
            .load_user(&args.user)
            .await
            .map_err(map_vfs)?;

        let repo_src = resolve_repo_point(&user, &args.repo_name)?;
        let (dest_config, restore_path) = resolve_data_path(&user, &args.output_path)?;
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
                &restore_path,
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

    // ── Check ─────────────────────────────────────────────────────────────────

    async fn check(&self, req: Request<CheckArgs>) -> Result<Response<JobStartResponse>, Status> {
        let args = req.into_inner();
        let user = self
            .inner
            .users
            .load_user(&args.user)
            .await
            .map_err(map_vfs)?;

        let repo_src = resolve_repo_point(&user, &args.repo_name)?;
        let storage = Arc::clone(&self.inner.storage);

        let job_id = self.spawn_job(move |job_id, tx, _token| {
            let handle = tokio::runtime::Handle::current();
            let repo = handle.block_on(storage.get_repo_job(&repo_src, job_id, tx))?;
            repo.check(CheckOptions::default())?;
            Ok(None)
        });

        Ok(Response::new(JobStartResponse { job_id }))
    }

    // ── Forget ────────────────────────────────────────────────────────────────

    async fn forget(&self, req: Request<ForgetArgs>) -> Result<Response<JobStartResponse>, Status> {
        let args = req.into_inner();
        let user = self
            .inner
            .users
            .load_user(&args.user)
            .await
            .map_err(map_vfs)?;

        let repo_src = resolve_repo_point(&user, &args.repo_name)?;

        let snap_ids: Vec<SnapshotId> = args
            .snapshots
            .iter()
            .map(|s| {
                SnapshotId::from_str(s)
                    .map_err(|e| invalid(format!("invalid snapshot id '{s}': {e}")))
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

    // ── GetSnapshots ──────────────────────────────────────────────────────────

    async fn get_snapshots(
        &self,
        req: Request<GetSnapshotArgs>,
    ) -> Result<Response<SnapshotResponse>, Status> {
        let args = req.into_inner();
        let user = self
            .inner
            .users
            .load_user(&args.user)
            .await
            .map_err(map_vfs)?;

        let repo_src = resolve_repo_point(&user, &args.repo_src)?;

        // get_repo is async (spawn_blocking inside), so .await here is correct.
        // get_all_snapshots is blocking, so it gets its own spawn_blocking.
        let repo = self
            .inner
            .storage
            .get_repo(&repo_src)
            .await
            .map_err(internal)?;

        let snaps = tokio::task::spawn_blocking(move || repo.get_all_snapshots())
            .await
            .map_err(|e| internal(format!("task join: {e}")))?
            .map_err(internal)?;

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
        let uuid =
            Uuid::parse_str(&args.job_id).map_err(|e| invalid(format!("bad job_id: {e}")))?;

        match self.inner.jobs.get(&uuid) {
            Some(token) => {
                token.cancel();
                Ok(Response::new(JobCancelResponse {
                    job_id: args.job_id,
                }))
            }
            None => Err(not_found(format!(
                "job '{}' not found or already finished",
                args.job_id
            ))),
        }
    }

    // ── Poll ──────────────────────────────────────────────────────────────────

    async fn poll(&self, _: Request<Empty>) -> Result<Response<PollResponse>, Status> {
        let events = self
            .inner
            .events
            .lock()
            .map_err(|e| internal(format!("event buffer lock poisoned: {e}")))?
            .drain(..)
            .collect();
        Ok(Response::new(PollResponse { events }))
    }

    // ── SetVfs ────────────────────────────────────────────────────────────────

    async fn set_vfs(&self, req: Request<SetVfsArgs>) -> Result<Response<Empty>, Status> {
        let users = req
            .into_inner()
            .users
            .into_iter()
            .map(VfsUser::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        self.inner.users.set_users(users).await.map_err(map_vfs)?;
        Ok(Response::new(Empty {}))
    }

    // ── ListVfs ───────────────────────────────────────────────────────────────

    async fn list_vfs(
        &self,
        req: Request<ListVfsArgs>,
    ) -> Result<Response<ListVfsResponse>, Status> {
        let args = req.into_inner();
        let user = self
            .inner
            .users
            .load_user(&args.user)
            .await
            .map_err(map_vfs)?;
        let op = self.inner.storage.get_vfs(&user).await.map_err(map_vfs)?;
        let path = fix_path(args.path, true);

        let entries = op.list_with(&path).await.map_err(map_dal)?;

        Ok(Response::new(ListVfsResponse {
            nodes: entries.iter().map(entry_to_node).collect(),
        }))
    }

    // ── ReadVfs ───────────────────────────────────────────────────────────────

    async fn read_vfs(
        &self,
        req: Request<ReadVfsArgs>,
    ) -> Result<Response<ReadVfsResponse>, Status> {
        let args = req.into_inner();
        let user = self
            .inner
            .users
            .load_user(&args.user)
            .await
            .map_err(map_vfs)?;
        let op = self.inner.storage.get_vfs(&user).await.map_err(map_vfs)?;
        let path = fix_path(args.path, false);

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

    // ── AppendVfs ─────────────────────────────────────────────────────────────

    async fn append_vfs(&self, req: Request<AppendVfsArgs>) -> Result<Response<Empty>, Status> {
        let args = req.into_inner();
        let user = self
            .inner
            .users
            .load_user(&args.user)
            .await
            .map_err(map_vfs)?;
        let op = self.inner.storage.get_vfs(&user).await.map_err(map_vfs)?;
        let path = fix_path(args.path, false);

        op.write_with(&path, args.data)
            .append(true)
            .await
            .map_err(map_dal)?;

        Ok(Response::new(Empty {}))
    }

    // ── WriteVfs ──────────────────────────────────────────────────────────────

    async fn write_vfs(&self, req: Request<WriteVfsArgs>) -> Result<Response<Empty>, Status> {
        let args = req.into_inner();
        let user = self
            .inner
            .users
            .load_user(&args.user)
            .await
            .map_err(map_vfs)?;

        let op = self.inner.storage.get_vfs(&user).await.map_err(map_vfs)?;
        let path = fix_path(args.path, false);

        if args.offset == 0 {
            op.write(&path, args.data).await.map_err(map_dal)?;
        } else {
            // opendal has no native random-access write; fall back to
            // read-splice-write. Consider a chunked backend for large files.
            let mut body: Vec<u8> = op
                .read(&path)
                .await
                .map_err(map_dal)?
                .to_bytes()
                .to_vec();

            let start = args.offset as usize;
            let end = start + args.data.len();
            if body.len() < end {
                body.resize(end, 0);
            }
            body[start..end].copy_from_slice(&args.data);

            op.write(&path, body).await.map_err(map_dal)?;
        }

        Ok(Response::new(Empty {}))
    }

    // ── TouchVfs ──────────────────────────────────────────────────────────────

    async fn touch_vfs(&self, req: Request<TouchVfsArgs>) -> Result<Response<Empty>, Status> {
        let args = req.into_inner();
        let user = self
            .inner
            .users
            .load_user(&args.user)
            .await
            .map_err(map_vfs)?;

        let op = self.inner.storage.get_vfs(&user).await.map_err(map_vfs)?;
        let path = fix_path(args.path, false);
        op.write(&path, Buffer::new()).await.map_err(map_dal)?;
        Ok(Response::new(Empty {}))
    }
}
