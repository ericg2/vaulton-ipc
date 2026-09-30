//! gRPC server implementing every method in `IpcService`.

use crossbeam_channel as chan;
use dashmap::DashMap;
use log::{error, warn};
use opendal_core::{Buffer, ErrorKind as DalErrorKind, Operator};
use rustic_backend::local::LocalSource;
use rustic_backend::opendal::OpenDALSource;
use std::collections::{HashSet, VecDeque};
use std::io::SeekFrom;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{
    Arc, Mutex as StdMutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{Mutex as TokioMutex, OwnedMutexGuard};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::core::{PointHealth, UserSystem, VfsPoint, VfsUser};
use crate::event_bus::{is_critical, send as send_event};
use crate::ipc::file_path::Path;
use crate::ipc::ipc_event::Data;
use crate::ipc::ipc_service_server::IpcService as IpcServiceTrait;
use crate::ipc::vfs_point::Src as ProtoSrc;
use crate::ipc::{
    BackupArgs, CancelArgs, CheckArgs, CloseHandleArgs, Empty, ExistsResponse, FilePath,
    ForgetArgs, GetSnapshotArgs, InfoResponse, IpcEvent, JobCancelResponse, JobFinishedEvent,
    JobNewMessageEvent, JobStartResponse, ListVfsResponse, OpenWriteArgs, OpenWriteResponse,
    PointSource as ProtoPoint, PollResponse, Priority, ReadVfsArgs, ReadVfsResponse, ReloadArgs,
    RepoSource as ProtoRepo, RestoreArgs, SetVfsArgs, Snapshot, SnapshotResponse, StatResponse,
    Summary, TransferArgs, VfsNode, VfsPoint as ProtoVfsPoint, VfsUser as ProtoVfsUser,
    WriteAtArgs,
};
use crate::progress::RusticProgressBars;
use crate::store::{RepoSource, StorageSystem};
use crate::utils;
use crate::utils::{fix_path, map_dal, map_vfs};
use moka::sync::Cache;
use opendal_vfs::layers::quota::{QuotaState, QuotaTracker};
use rustic_core::jiff::Zoned;
use rustic_core::repofile::{SnapshotFile, SnapshotId, SnapshotSummary};
use rustic_core::{
    CancelToken, CheckOptions, ErrorKind, LsOptions, PathList, ProgressBars, ProgressType,
    RestoreOptions, RusticError, SnapshotOptions, StringList,
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

/// Max buffered events retained for Poll; telemetry is discarded first when saturated.
const MAX_EVENTS: usize = 10_000;
/// Max events returned by a single `Poll`.
const POLL_BATCH: usize = 2_000;
/// Write handles idle longer than this are closed by the reaper.
const HANDLE_IDLE_TTL: Duration = Duration::from_secs(600);
/// Largest single read served over gRPC (callers must chunk beyond this).
const MAX_READ: u64 = 32 * 1024 * 1024;

struct ActiveWriteHandle {
    last_used: StdMutex<Instant>,
    state: TokioMutex<WriteState>,
    max_bytes: Option<u64>,
    /// Total quota usage excluding the file currently owned by this handle.
    quota_base_bytes: u64,
    point_id: Uuid,
    username: String,
    /// When true, every `Vfs_WriteAt` on this handle ignores the caller's
    /// `offset` and instead writes at the current end of the file/stream.
    ///
    /// Always `true` for non-local (remote) backends: `Vfs_OpenWrite`
    /// always creates a brand-new remote writer for the handle (there's no
    /// "open an existing remote object for random-access editing"
    /// primitive), so writes are inherently sequential regardless of
    /// whether the backend advertises native append support — we're simply
    /// streaming bytes into a fresh object in call order, not resuming an
    /// existing one. `Vfs_OpenWrite` rejects `append == false` for remote
    /// destinations rather than silently ignoring `offset`.
    ///
    /// For local backends this mirrors whatever the caller requested, and
    /// combines with `overwrite` (truncate-on-open) the normal way: e.g.
    /// `overwrite=true, append=true` truncates on open and then appends
    /// everything written in this session from byte 0.
    append: bool,
    /// Held for the lifetime of local write handles so quota measurement,
    /// truncation, and writes are serialized per data point.
    local_point_guard: Option<OwnedMutexGuard<()>>,
    expired: AtomicBool,
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

/// Unwraps an optional `FilePath` message field, returning
/// `INVALID_ARGUMENT` if the caller left it unset.
fn require_path(path: &Option<FilePath>) -> Result<&FilePath, Status> {
    path.as_ref()
        .ok_or_else(|| Status::invalid_argument("path is blank"))
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
    let projected = used
        .checked_add(growth)
        .ok_or_else(|| Status::resource_exhausted("quota size overflow"))?;
    if projected > max {
        return Err(Status::resource_exhausted(format!(
            "write would exceed point quota ({projected} > {max} bytes)"
        )));
    }
    Ok(())
}

// ── Error helpers ─────────────────────────────────────────────────────────────
impl TryFrom<ProtoVfsPoint> for VfsPoint {
    type Error = Status;

    fn try_from(p: ProtoVfsPoint) -> Result<Self, Status> {
        let (scheme, root, mut config, is_repo, repo_password) = match p.src {
            Some(ProtoSrc::Data(ps)) => (ps.scheme, ps.root, ps.config, false, None),
            Some(ProtoSrc::Repo(rs)) => match rs.src {
                None => return Err(Status::invalid_argument("VfsPoint[repo].src is required")),
                Some(x) => (x.scheme, x.root, x.config, true, Some(rs.password)),
            },
            None => return Err(Status::invalid_argument("VfsPoint.src is required")),
        };

        // Attempt to add the ROOT from the point!
        if !root.is_empty() {
            config.remove("root");
            config.insert("root".into(), root);
        }

        if is_repo {
            utils::validate_repo_password(repo_password.as_deref().unwrap_or_default())?;
        }

        Ok(VfsPoint {
            id: Uuid::parse_str(&p.id)
                .map_err(|_| Status::invalid_argument("Failed to parse UUID"))?,
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
        let mut config = point.config.clone();
        let root = config.remove("root").unwrap_or(String::new());
        let p = ProtoPoint {
            scheme: point.scheme.clone(),
            config: config.into_iter().collect(),
            root,
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
            id: point.id.to_string(),
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

/// Validates stable point IDs and makes point names unique within each
/// user's VFS namespace.
///
/// Point IDs are globally unique across the complete configuration. Names are
/// only a presentation/mount concern, so a duplicate name is retained for the
/// first point and later points receive a deterministic ID-based suffix.
fn normalize_vfs_users(users: &mut [VfsUser]) -> Result<(), Status> {
    let mut point_ids = HashSet::new();

    let mut usernames = HashSet::new();

    for user in users.iter_mut() {
        utils::validate_username(&user.username)?;
        if !usernames.insert(user.username.clone()) {
            return Err(Status::invalid_argument(format!(
                "duplicate username '{}'",
                user.username
            )));
        }

        let mut names = HashSet::new();

        for point in &mut user.points {
            if point.id.is_nil() {
                return Err(Status::invalid_argument(format!(
                    "point '{}' for user '{}' has a nil ID",
                    point.name, user.username
                )));
            }

            if !point_ids.insert(point.id) {
                return Err(Status::invalid_argument(format!(
                    "duplicate VFS point ID '{}'",
                    point.id
                )));
            }

            if point.name.is_empty() {
                return Err(Status::invalid_argument(format!(
                    "point '{}' for user '{}' has an empty name",
                    point.id, user.username
                )));
            }

            if point.is_repo {
                let Some(password) = point.repo_password.as_ref() else {
                    return Err(Status::invalid_argument(format!(
                        "repo point '{}' is missing a repository password",
                        point.name
                    )));
                };
                if password.is_empty() {
                    return Err(Status::invalid_argument(format!(
                        "repo point '{}' has an empty repository password",
                        point.name
                    )));
                }
            }

            if point.name == "."
                || point.name == ".."
                || point.name.contains('/')
                || point.name.contains('\\')
                || point.name.chars().any(|c| c.is_control())
            {
                return Err(Status::invalid_argument(format!(
                    "invalid VFS point name '{}'",
                    point.name
                )));
            }

            let original_name = point.name.clone();
            if names.insert(original_name.clone()) {
                continue;
            }

            let id_text = point.id.simple().to_string();
            let mut assigned = None;

            for prefix_len in [8usize, 12, 16, 20, 24, 32] {
                let candidate = format!("{}-{}", original_name, &id_text[..prefix_len]);
                if names.insert(candidate.clone()) {
                    assigned = Some(candidate);
                    break;
                }
            }

            if assigned.is_none() {
                let mut suffix = 2u64;
                loop {
                    let candidate = format!("{original_name}-{id_text}-{suffix}");
                    if names.insert(candidate.clone()) {
                        assigned = Some(candidate);
                        break;
                    }
                    suffix += 1;
                }
            }

            point.name = assigned.ok_or_else(|| {
                Status::internal(format!(
                    "failed to assign a unique name for point '{}'",
                    original_name
                ))
            })?;
        }
    }

    Ok(())
}

/// Resolves a `FilePath` into the user's actual VFS mount path.
///
/// Indexed paths are ID-based so a point can be renamed without invalidating
/// clients that already have its stable ID. The resulting VFS path still uses
/// the point's current name because mount names are the user-facing namespace.
fn resolve_file_path(user: &VfsUser, path: &FilePath) -> Result<String, Status> {
    match &path.path {
        None => Err(Status::invalid_argument("path is blank")),
        Some(Path::Virtual(path)) => {
            // Virtual paths come from clients directly. Reject parent/prefix
            // components before handing the path to an OpenDAL VFS mount.
            let normalized = utils::validate_relative_point_path(path)?;
            if normalized.is_empty() {
                Ok("/".to_string())
            } else {
                Ok(format!("/{normalized}"))
            }
        }
        Some(Path::Indexed(path)) => {
            let point_id = Uuid::parse_str(&path.point_id)
                .map_err(|_| Status::invalid_argument("malformed point_id"))?;

            let point = user
                .points
                .iter()
                .find(|point| point.id == point_id)
                .ok_or_else(|| Status::not_found(format!("point '{}' not found", path.point_id)))?;

            if point.is_repo {
                return Err(Status::invalid_argument(format!(
                    "point '{}' is a repo point; indexed paths only support data points",
                    point.name
                )));
            }

            Ok(format!(
                "{}/{}",
                utils::data_mount_path(&point.name),
                path.point_path
            ))
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
        ctime: mtime.clone(),
        mtime: mtime.clone(),
        atime: Some(prost_types::Timestamp {
            seconds: 0,
            nanos: 0,
        }),
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
    events: StdMutex<VecDeque<IpcEvent>>,
    /// Open `Vfs_OpenWrite` handles, keyed by the id handed back to the
    /// caller. Entries live here for the lifetime of the handle and are
    /// removed by `Vfs_CloseWrite`.
    write_handles: DashMap<Uuid, Arc<ActiveWriteHandle>>,
    local_point_locks: DashMap<Uuid, Arc<TokioMutex<()>>>,
    snapshot_cache: Cache<RepoSource, Arc<Vec<Snapshot>>>,
    /// Global event sink: logs, point status and every job's progress.
    tx: chan::Sender<Data>,
}

/// tonic service handle. Cheap to clone — all state lives behind `Arc`.
pub struct GrpcServer<S, U>
where
    S: StorageSystem,
    U: UserSystem,
{
    inner: Arc<Inner<S, U>>,
}

impl<S, U> Clone for GrpcServer<S, U>
where
    S: StorageSystem,
    U: UserSystem,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<S, U> GrpcServer<S, U>
where
    S: StorageSystem,
    U: UserSystem,
{
    /// `tx`/`rx` are the two ends of the global event channel shared with the
    /// logger and `StorageManager`.
    pub fn new(
        storage: Arc<S>,
        users: Arc<U>,
        quota: QuotaState,
        tx: chan::Sender<Data>,
        rx: chan::Receiver<Data>,
    ) -> Self {
        let server = Self {
            inner: Arc::new(Inner {
                storage,
                users,
                quota,
                jobs: DashMap::new(),
                events: StdMutex::new(VecDeque::new()),
                write_handles: DashMap::new(),
                local_point_locks: DashMap::new(),
                snapshot_cache: Cache::builder()
                    .time_to_live(Duration::from_secs(5))
                    .max_capacity(128)
                    .build(),
                tx,
            }),
        };
        // Single bridge thread for *all* events (bounded buffer).
        let inner = Arc::clone(&server.inner);

        std::thread::spawn(move || {
            while let Ok(data) = rx.recv() {
                let incoming_critical = is_critical(&data);
                let mut pending = Some(data);

                loop {
                    let mut accepted = false;

                    if let Ok(mut buf) = inner.events.lock() {
                        if buf.len() < MAX_EVENTS {
                            buf.push_back(IpcEvent {
                                data: pending.take(),
                            });
                            accepted = true;
                        } else if let Some(index) = buf.iter().position(|event| {
                            event
                                .data
                                .as_ref()
                                .map(|event| !is_critical(event))
                                .unwrap_or(true)
                        }) {
                            // Prefer sacrificing telemetry/progress over a
                            // terminal job or point-state event.
                            buf.remove(index);

                            buf.push_back(IpcEvent {
                                data: pending.take(),
                            });

                            accepted = true;
                        }
                    }

                    if accepted {
                        break;
                    }

                    if !incoming_critical {
                        // The Poll buffer is saturated with critical state;
                        // dropping another progress/log event is intentional.
                        break;
                    }

                    // All buffered events are critical. Backpressure the
                    // bridge until Poll makes room instead of losing a
                    // terminal event. Producers will eventually back up on
                    // the bounded crossbeam queue, which is the desired
                    // failure mode under sustained overload.
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
        });

        // Reaper for abandoned write handles (client crashed / never closed).
        let weak = Arc::downgrade(&server.inner);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(60));
            loop {
                tick.tick().await;
                let Some(inner) = weak.upgrade() else { break };

                let expired: Vec<Uuid> = inner
                    .write_handles
                    .iter()
                    .filter_map(|entry| {
                        let idle = entry
                            .value()
                            .last_used
                            .lock()
                            .map(|t| t.elapsed())
                            .unwrap_or_default();
                        (idle > HANDLE_IDLE_TTL).then_some(*entry.key())
                    })
                    .collect();

                for id in expired {
                    let Some((_, handle)) = inner.write_handles.remove(&id) else {
                        continue;
                    };
                    warn!("closing idle write handle {id}");
                    handle.expired.store(true, Ordering::Release);
                    tokio::spawn(Self::close_write_handle(handle));
                }
            }
        });

        server
    }

    async fn close_write_handle(handle: Arc<ActiveWriteHandle>) {
        let mut state = handle.state.lock().await;
        match &mut state.backend {
            WriteStateBackend::Local(file) => {
                let _ = file.sync_all().await;
            }
            WriteStateBackend::Remote(writer) => {
                let _ = writer.close().await;
            }
        }
    }

    fn invalidate_user_write_handles(&self, username: &str) {
        let ids: Vec<Uuid> = self
            .inner
            .write_handles
            .iter()
            .filter_map(|entry| (entry.value().username == username).then_some(*entry.key()))
            .collect();

        for id in ids {
            if let Some((_, handle)) = self.inner.write_handles.remove(&id) {
                tokio::spawn(Self::close_write_handle(handle));
            }
        }
    }

    /// Cancels active jobs and closes all write handles during process shutdown.
    /// This prevents detached rustic work or open local files from surviving
    /// after the network listeners have stopped accepting new requests.
    pub async fn shutdown(&self) {
        for job in self.inner.jobs.iter() {
            job.token.cancel();
        }

        let handles: Vec<Arc<ActiveWriteHandle>> = self
            .inner
            .write_handles
            .iter()
            .map(|entry| Arc::clone(entry.value()))
            .collect();
        self.inner.write_handles.clear();

        for handle in handles {
            Self::close_write_handle(handle).await;
        }
    }

    /// Attempts to resolve the [`Operator`] and normalized path string for a
    /// [`FilePath`]. The owning user is taken from `path.user`.
    async fn get_operator(
        &self,
        path: &FilePath,
        is_dir: bool,
    ) -> Result<(Operator, String), Status> {
        let user = self.get_user(&path.user).await?;
        let vfs_path = resolve_file_path(&user, path)?;
        let op = self.inner.storage.get_vfs(&user).await.map_err(map_vfs)?;
        self.check_point_loaded(&user, &vfs_path)?;
        Ok((op, fix_path(vfs_path, is_dir)))
    }

    /// If `vfs_path` lies under a point that failed to load, return
    /// FAILED_PRECONDITION with the real reason instead of a bare NotFound.
    fn check_point_loaded(&self, user: &VfsUser, vfs_path: &str) -> Result<(), Status> {
        let mut parts = vfs_path.trim_start_matches('/').splitn(3, '/');
        let (root, name) = (parts.next().unwrap_or(""), parts.next().unwrap_or(""));
        if root != utils::POINTS_ROOT && root != utils::REPOS_ROOT {
            return Ok(());
        }
        let is_repo = root == utils::REPOS_ROOT;
        if let Some(p) = user
            .points
            .iter()
            .find(|p| p.name == name && p.is_repo == is_repo)
        {
            if let Some(PointHealth::Failed(e)) = self.inner.storage.point_health(&p.id) {
                return Err(Status::failed_precondition(format!(
                    "point '{}' failed to load: {e}",
                    p.name
                )));
            }
        }
        Ok(())
    }

    /// Per-point info (usage + health) for one user. Never fails as a whole:
    /// a point with a problem is reported with `FAILED` and an error string.
    async fn user_info(&self, user: &VfsUser) -> Vec<crate::ipc::VfsInfo> {
        // Force a (cached) build so health reflects reality, not "unknown".
        if let Err(e) = self.inner.storage.get_vfs(user).await {
            warn!("GetVfs: building VFS for '{}': {e}", user.username);
        }

        // Usage probes are independent. Run them concurrently rather than
        // making one slow filesystem walk hold up every other point.
        let mut probes = tokio::task::JoinSet::new();
        for (index, point) in user.points.iter().cloned().enumerate() {
            let quota = self.inner.quota.clone();
            let username = user.username.clone();
            probes.spawn(async move {
                let mut errors = Vec::new();
                let used_bytes = if !point.is_repo && utils::is_local_scheme(&point.scheme) {
                    match utils::local_source_path(&point) {
                        Ok(root) => {
                            let root = PathBuf::from(root);
                            match tokio::task::spawn_blocking(move || utils::dir_size(&root)).await
                            {
                                Ok(Ok(b)) => b,
                                Ok(Err(e)) => {
                                    errors.push(format!("failed to measure usage: {e}"));
                                    0
                                }
                                Err(e) => {
                                    errors.push(format!("usage scan panicked: {e}"));
                                    0
                                }
                            }
                        }
                        Err(e) => {
                            errors.push(e.message().to_string());
                            0
                        }
                    }
                } else {
                    match quota
                        .current_bytes(&utils::quota_id(&username, &point.id))
                        .await
                    {
                        Ok(b) => b,
                        Err(e) => {
                            errors.push(format!("quota lookup failed: {e}"));
                            0
                        }
                    }
                };
                (index, used_bytes, errors)
            });
        }

        let mut usage = vec![(0u64, Vec::<String>::new()); user.points.len()];
        while let Some(result) = probes.join_next().await {
            match result {
                Ok((index, used, errors)) => usage[index] = (used, errors),
                Err(e) => {
                    warn!("GetVfs: usage probe task failed: {e}");
                }
            }
        }

        let mut out = Vec::with_capacity(user.points.len());
        for (index, point) in user.points.iter().enumerate() {
            let (used_bytes, errors) = &usage[index];
            let (mut health, mut error) = match self.inner.storage.point_health(&point.id) {
                Some(PointHealth::Healthy) => (crate::ipc::PointHealth::Healthy, String::new()),
                Some(PointHealth::Failed(e)) => (crate::ipc::PointHealth::Failed, e),
                None => (crate::ipc::PointHealth::Unknown, String::new()),
            };
            if !errors.is_empty() {
                warn!(
                    "GetVfs: {}'s point '{}': {}",
                    user.username,
                    point.name,
                    errors.join("; ")
                );
                if health != crate::ipc::PointHealth::Failed {
                    health = crate::ipc::PointHealth::Failed;
                    error = errors.join("; ");
                }
            }

            out.push(crate::ipc::VfsInfo {
                user: user.username.clone(),
                point: Some(point.into()),
                used_bytes: *used_bytes,
                health: health as i32,
                error,
            });
        }
        out
    }

    async fn get_user(&self, user: &str) -> Result<VfsUser, Status> {
        self.inner.users.get_user(user).await.map_err(map_vfs)
    }

    /// Registers a new job (cancel token + owning `user`) and starts the
    /// bridge thread that drains its `crossbeam` progress/event channel
    /// into the async-safe `events` buffer. Shared setup for `spawn_job`
    /// and `spawn_async_job` — everything past this point differs only in
    /// how the job body actually runs (blocking-thread rustic calls vs. a
    /// plain tokio future).
    fn register_job(&self, user: impl Into<String>) -> (Uuid, CancelToken, chan::Sender<Data>) {
        let job_id = Uuid::new_v4();
        let token = CancelToken::new();
        self.inner.jobs.insert(
            job_id,
            JobHandle {
                token: token.clone(),
                user: user.into(),
            },
        );
        (job_id, token, self.inner.tx.clone())
    }

    /// Sends the terminal `JobMessage` (on error) and `JobFinished` events
    /// for a job and removes it from the active-jobs map. Shared tail end
    /// of `spawn_job` and `spawn_async_job`.
    fn finish_job(
        inner: &Arc<Inner<S, U>>,
        job_id: Uuid,
        tx: chan::Sender<Data>,
        result: Result<Option<String>, String>,
    ) {
        if let Err(ref e) = result {
            error!("job {job_id} failed: {e}");
            let _ = send_event(
                &tx,
                Data::JobMessage(JobNewMessageEvent {
                    job_id: job_id.to_string(),
                    priority: Priority::Error as i32,
                    message: e.clone(),
                    time: Some(utils::to_ts(Zoned::now())),
                }),
            );
        }

        let _ = send_event(
            &tx,
            Data::JobFinished(JobFinishedEvent {
                job_id: job_id.to_string(),
                success: result.is_ok(),
                error: result.as_ref().err().cloned(),
                snapshot: result.ok().flatten(),
                time: Some(utils::to_ts(Zoned::now())),
            }),
        );

        inner.jobs.remove(&job_id);
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
        let (job_id, token, tx) = self.register_job(user);
        let inner = Arc::clone(&self.inner);

        tokio::task::spawn_blocking(move || {
            // A panic must still terminate the job, or it leaks forever.
            let result = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                f(job_id, tx.clone(), token)
            })) {
                Ok(r) => r.map_err(|e| e.to_string()),
                Err(_) => Err("job panicked".to_string()),
            };
            Self::finish_job(&inner, job_id, tx, result);
        });

        job_id.to_string()
    }

    /// Like [`spawn_job`](Self::spawn_job), but for jobs that are plain
    /// `async` tokio work rather than blocking rustic calls — e.g.
    /// `Vfs_Transfer`'s OpenDAL copy/streaming. Runs `f` as a future on the
    /// tokio runtime instead of inside `spawn_blocking`, and is generic
    /// over any displayable error type so callers don't have to funnel
    /// non-rustic errors through `RusticError` just to reuse this plumbing.
    fn spawn_async_job<F, Fut, E>(&self, user: impl Into<String>, f: F) -> String
    where
        F: FnOnce(Uuid, chan::Sender<Data>, CancelToken) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<Option<String>, E>> + Send + 'static,
        E: std::fmt::Display + std::marker::Send + 'static,
    {
        let (job_id, token, tx) = self.register_job(user);
        let inner = Arc::clone(&self.inner);

        tokio::spawn(async move {
            // Run in its own task so a panic is observed instead of leaking the job.
            let result = match tokio::spawn(f(job_id, tx.clone(), token)).await {
                Ok(r) => r.map_err(|e| e.to_string()),
                Err(e) => Err(format!("job panicked: {e}")),
            };
            Self::finish_job(&inner, job_id, tx, result);
        });

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

        let src = require_path(&args.src)?.clone();
        let source_user = self.get_user(&src.user).await?;
        let (data_point, source_path) = utils::resolve_data_file_path(&source_user, &src)?;

        let repo_user = self.get_user(&args.repo_user).await?;
        let repo_point = utils::require_repo_point_id(&repo_user, &args.repo_id)?;
        utils::require_writable(repo_point)?;
        let repo_src = utils::repo_source(repo_point)?;

        let local_path = if utils::is_local_scheme(&data_point.scheme) {
            let root = PathBuf::from(utils::local_source_path(data_point)?);
            let full_source = utils::safe_join(&root, &source_path)?;
            if let Ok(meta) = std::fs::metadata(&full_source) {
                if meta.is_dir() {
                    tokio::task::spawn_blocking(move || utils::reject_symlinks_under(&full_source))
                        .await
                        .map_err(|e| Status::internal(format!("symlink scan panicked: {e}")))?
                        .map_err(|e| Status::permission_denied(e.to_string()))?;
                }
            }
            Some(root.to_string_lossy().to_string())
        } else {
            None
        };

        let source_op = if local_path.is_some() {
            None
        } else {
            Some(
                self.inner
                    .storage
                    .get_data_operator(&source_user, data_point)
                    .map_err(map_vfs)?,
            )
        };

        let repo_op = self
            .inner
            .storage
            .get_data_operator(&repo_user, repo_point)
            .map_err(map_vfs)?;

        let tags = args.tags;
        let storage = Arc::clone(&self.inner.storage);
        let job_user = repo_user.username.clone();
        let snapshot_repo_src = repo_src.clone();

        let job_id = self.spawn_job(job_user, move |job_id, tx, token| {
            let handle = tokio::runtime::Handle::current();
            let tags = StringList::from_str(&tags.join(",")).map_err(|err| {
                RusticError::with_source(ErrorKind::InvalidInput, "Failed to parse tags", err)
            })?;
            let snap = SnapshotOptions::default().tags(vec![tags]).to_snapshot()?;
            let repo =
                handle.block_on(storage.get_repo_job(&repo_src, repo_op, job_id, tx, true))?;
            let paths = PathList::from_string(&source_path)?;

            let saved = if let Some(path) = local_path {
                let source = LocalSource::new(path);
                repo.backup(snap)
                    .add_multi(&source, paths.paths())
                    .with_token(token)
                    .run()?
            } else {
                let source_op = source_op.ok_or_else(|| {
                    rustic_core::RusticError::with_source(
                        rustic_core::ErrorKind::Backend,
                        "non-local backup source was not initialized",
                        std::io::Error::other("missing backup source operator"),
                    )
                })?;
                let source = OpenDALSource::new(source_op);
                repo.backup(snap)
                    .add_multi(&source, paths.paths())
                    .with_token(token)
                    .run()?
            };

            Ok(Some(saved.id.to_string()))
        });

        self.inner.snapshot_cache.invalidate(&snapshot_repo_src);
        Ok(Response::new(JobStartResponse { job_id }))
    }

    async fn restore(
        &self,
        req: Request<RestoreArgs>,
    ) -> Result<Response<JobStartResponse>, Status> {
        let args = req.into_inner();

        let dest = require_path(&args.dest)?.clone();
        let dest_user = self.get_user(&dest.user).await?;
        let (dest_point, dest_path) = utils::resolve_data_file_path(&dest_user, &dest)?;
        utils::require_writable(dest_point)?;

        let repo_user = self.get_user(&args.repo_user).await?;
        let repo_point = utils::require_repo_point_id(&repo_user, &args.repo_id)?;
        let repo_src = utils::repo_source(repo_point)?;

        if utils::is_local_scheme(&dest_point.scheme) {
            let root = PathBuf::from(utils::local_source_path(dest_point)?);
            let full_dest = utils::safe_join(&root, &dest_path)?;
            if let Ok(meta) = std::fs::metadata(&full_dest) {
                if meta.is_dir() {
                    tokio::task::spawn_blocking(move || utils::reject_symlinks_under(&full_dest))
                        .await
                        .map_err(|e| Status::internal(format!("symlink scan panicked: {e}")))?
                        .map_err(|e| Status::permission_denied(e.to_string()))?;
                }
            }
            if dest_point.max_bytes.is_some() && !args.dry_run {
                return Err(Status::failed_precondition(
                    "quota-limited local restores are disabled because rustic restore can overwrite multiple files atomically without a pre-write filesystem quota reservation",
                ));
            }
        }

        let dest_op = self
            .inner
            .storage
            .get_data_operator(&dest_user, dest_point)
            .map_err(map_vfs)?;

        let repo_op = self
            .inner
            .storage
            .get_data_operator(&repo_user, repo_point)
            .map_err(map_vfs)?;

        let snapshot_id = args.snapshot_id;
        let snapshot_path = args.snapshot_path;
        let delete = args.delete;
        let dry_run = args.dry_run;
        let storage = Arc::clone(&self.inner.storage);
        let job_user = repo_user.username.clone();

        let job_id = self.spawn_job(job_user, move |job_id, tx, token| {
            let handle = tokio::runtime::Handle::current();
            let repo = Arc::new(
                handle.block_on(storage.get_repo_job(&repo_src, repo_op, job_id, tx, false))?,
            );
            let dest = OpenDALSource::new(dest_op);
            let opts = RestoreOptions::default().delete(delete);
            let snap_path = format!("{}:{}", &snapshot_id, &snapshot_path);
            let node = repo.node_from_snapshot_path(&snap_path, |_| true)?;
            let streamer_opts = LsOptions::default();
            let ls = repo.ls(&node, &streamer_opts)?;
            let plan =
                repo.prepare_restore(&opts, ls.clone(), &dest, &dest_path, dry_run, token.clone())?;
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

        let repo_point = utils::require_repo_point_id(&user, &args.repo_id)?;
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
            let repo =
                handle.block_on(storage.get_repo_job(&repo_src, repo_op, job_id, tx, false))?;
            repo.check(CheckOptions::default())?;
            Ok(None)
        });

        Ok(Response::new(JobStartResponse { job_id }))
    }

    async fn forget(&self, req: Request<ForgetArgs>) -> Result<Response<JobStartResponse>, Status> {
        let args = req.into_inner();
        let user = self.get_user(&args.user).await?;

        let repo_point = utils::require_repo_point_id(&user, &args.repo_id)?;
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
        let snapshot_repo_src = repo_src.clone();

        let job_id = self.spawn_job(username, move |job_id, tx, _token| {
            let handle = tokio::runtime::Handle::current();
            let repo =
                handle.block_on(storage.get_repo_job(&repo_src, repo_op, job_id, tx, false))?;
            repo.delete_snapshots(&snap_ids)?;
            Ok(None)
        });

        self.inner.snapshot_cache.invalidate(&snapshot_repo_src);
        Ok(Response::new(JobStartResponse { job_id }))
    }

    async fn get_snapshots(
        &self,
        req: Request<GetSnapshotArgs>,
    ) -> Result<Response<SnapshotResponse>, Status> {
        let args = req.into_inner();
        let user = self.get_user(&args.user).await?;
        let repo_src = utils::repo_source(utils::require_repo_point_id(&user, &args.repo_id)?)?;

        if let Some(cached) = self.inner.snapshot_cache.get(&repo_src) {
            return Ok(Response::new(SnapshotResponse {
                output: cached.as_ref().clone(),
            }));
        }

        let repo = self
            .inner
            .storage
            .get_repo(&repo_src)
            .await
            .map_err(|err| Status::internal(format!("failed to open repository: {err}")))?;

        let snaps = tokio::task::spawn_blocking(move || repo.get_all_snapshots())
            .await
            .map_err(|e| Status::internal(format!("snapshot listing task failed: {e}")))?
            .map_err(|err| Status::internal(format!("snapshot listing failed: {err}")))?;
        let output: Vec<Snapshot> = snaps.into_iter().map(Into::into).collect();
        let cached = Arc::new(output.clone());
        self.inner.snapshot_cache.insert(repo_src, cached);

        Ok(Response::new(SnapshotResponse { output }))
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
        let mut events = self
            .inner
            .events
            .lock()
            .map_err(|e| Status::internal(format!("event buffer lock poisoned: {e}")))?;
        let n = events.len().min(POLL_BATCH);
        let events = events.drain(..n).collect();
        Ok(Response::new(PollResponse { events }))
    }

    async fn set_vfs(&self, req: Request<SetVfsArgs>) -> Result<Response<Empty>, Status> {
        let mut users = req
            .into_inner()
            .users
            .into_iter()
            .map(VfsUser::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        normalize_vfs_users(&mut users)?;

        let old_users = self.inner.users.get_users().await.map_err(map_vfs)?;
        let mut changed_users = Vec::new();
        let mut removed_quotas = Vec::new();

        for old_user in &old_users {
            let new_user = users.iter().find(|user| user.username == old_user.username);

            match new_user {
                Some(new_user) => {
                    if new_user != old_user {
                        changed_users.push(old_user.clone());

                        // A point remains the same point when its stable ID
                        // remains present, even if its display/mount name
                        // changed.
                        for old_point in &old_user.points {
                            let new_point = new_user
                                .points
                                .iter()
                                .find(|point| point.id == old_point.id);

                            match new_point {
                                None => {
                                    removed_quotas
                                        .push(utils::quota_id(&old_user.username, &old_point.id));
                                }
                                Some(new_point)
                                    if old_point.scheme != new_point.scheme
                                        || old_point.config != new_point.config
                                        || (old_point.is_repo != new_point.is_repo) =>
                                {
                                    // The stable ID remains, but it now refers
                                    // to a different storage target. Never let
                                    // the old backend's quota accounting leak
                                    // into the new configuration.
                                    removed_quotas
                                        .push(utils::quota_id(&old_user.username, &old_point.id));
                                }
                                _ => {}
                            }
                        }
                    }
                }

                None => {
                    changed_users.push(old_user.clone());

                    for point in &old_user.points {
                        removed_quotas.push(utils::quota_id(&old_user.username, &point.id));
                    }
                }
            }
        }

        // Update the database first. If this fails, don't cancel jobs,
        // invalidate caches, or remove quota state.
        self.inner.users.set_users(users).await.map_err(map_vfs)?;

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
            self.invalidate_user_write_handles(&user.username);
            self.inner.storage.invalidate_vfs(&user);
        }

        for id in removed_quotas {
            // The new config is already committed; a failed cleanup must not
            // turn a successful SetVfs into an error.
            if self.inner.quota.clear(&id).is_err() {
                warn!("SetVfs: failed to clear quota '{id}'");
            }
        }

        Ok(Response::new(Empty {}))
    }

    async fn get_vfs(&self, _request: Request<Empty>) -> Result<Response<InfoResponse>, Status> {
        let users = self.inner.users.get_users().await.map_err(map_vfs)?;
        let mut info = Vec::new();
        for user in &users {
            info.extend(self.user_info(user).await);
        }
        Ok(Response::new(InfoResponse { info }))
    }

    async fn reload_vfs(&self, req: Request<ReloadArgs>) -> Result<Response<InfoResponse>, Status> {
        let user = self.get_user(&req.into_inner().user).await?;
        self.inner.storage.invalidate_vfs(&user);
        Ok(Response::new(InfoResponse {
            info: self.user_info(&user).await,
        }))
    }

    async fn vfs_read_file(
        &self,
        request: Request<ReadVfsArgs>,
    ) -> Result<Response<ReadVfsResponse>, Status> {
        let args = request.into_inner();
        let file = require_path(&args.path)?;
        let (op, path) = self.get_operator(file, false).await?;
        if args.length > MAX_READ {
            return Err(Status::invalid_argument(format!(
                "length exceeds {MAX_READ} bytes; read in chunks"
            )));
        }
        let buf = if args.length == 0 {
            let size = op.stat(&path).await.map_err(map_dal)?.content_length();
            if size > MAX_READ {
                return Err(Status::invalid_argument(format!(
                    "file is {size} bytes; use offset/length to read in chunks of <= {MAX_READ}"
                )));
            }
            op.read(&path).await.map_err(map_dal)?
        } else {
            let end = args
                .offset
                .checked_add(args.length)
                .ok_or_else(|| Status::invalid_argument("read range overflow"))?;
            op.read_with(&path)
                .range(args.offset..end)
                .await
                .map_err(map_dal)?
        };

        Ok(Response::new(ReadVfsResponse {
            data: buf.to_bytes().to_vec(),
        }))
    }

    // ── Write handles (streaming + random access) ─────────────────

    async fn vfs_touch_file(&self, request: Request<FilePath>) -> Result<Response<Empty>, Status> {
        let args = request.into_inner();
        let (op, path) = self.get_operator(&args, false).await?;
        op.write(&path, Buffer::new()).await.map_err(map_dal)?;
        Ok(Response::new(Empty {}))
    }

    async fn vfs_open_write(
        &self,
        request: Request<OpenWriteArgs>,
    ) -> Result<Response<OpenWriteResponse>, Status> {
        let args = request.into_inner();
        let file = require_path(&args.path)?;
        let user = self.get_user(&file.user).await?;
        let path_str = resolve_file_path(&user, file)?;

        let (point, rest) = utils::resolve_data_path(&user, &path_str)?;
        utils::require_writable(point)?;

        let handle_id = Uuid::new_v4();

        let local_point_guard = if utils::is_local_scheme(&point.scheme) {
            let lock = self
                .inner
                .local_point_locks
                .entry(point.id)
                .or_insert_with(|| Arc::new(TokioMutex::new(())))
                .clone();
            Some(lock.lock_owned().await)
        } else {
            None
        };

        if utils::is_local_scheme(&point.scheme) {
            let root = PathBuf::from(utils::local_source_path(point)?);
            let full_path = utils::safe_join(&root, &rest.to_string_lossy())?;

            if let Some(parent) = full_path.parent() {
                tokio::fs::create_dir_all(parent).await.map_err(io_status)?;
            }

            let existing_len = match tokio::fs::metadata(&full_path).await {
                Ok(meta) if meta.is_file() => meta.len(),
                Ok(_) => {
                    return Err(Status::failed_precondition(
                        "cannot open a directory as a write target",
                    ));
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
                Err(e) => return Err(io_status(e)),
            };

            let root_for_scan = root.clone();
            let total_used = tokio::task::spawn_blocking(move || utils::dir_size(&root_for_scan))
                .await
                .map_err(|e| Status::internal(format!("quota check panicked: {e}")))?
                .map_err(|e| Status::internal(format!("failed to measure quota usage: {e}")))?;
            let quota_base_bytes = total_used.saturating_sub(existing_len);
            let initial_len = if args.overwrite { 0 } else { existing_len };

            if let Some(max) = point.max_bytes {
                if quota_base_bytes
                    .checked_add(initial_len)
                    .ok_or_else(|| Status::resource_exhausted("quota size overflow"))?
                    > max
                {
                    return Err(Status::resource_exhausted(format!(
                        "point '{}' is already over its {max}-byte quota",
                        point.name
                    )));
                }
            }

            let file = tokio::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .read(true)
                .open(&full_path)
                .await
                .map_err(io_status)?;

            if args.overwrite {
                file.set_len(0).await.map_err(io_status)?;
            }

            self.inner.write_handles.insert(
                handle_id,
                Arc::new(ActiveWriteHandle {
                    last_used: StdMutex::new(Instant::now()),
                    state: TokioMutex::new(WriteState {
                        backend: WriteStateBackend::Local(file),
                        len: initial_len,
                    }),
                    max_bytes: point.max_bytes,
                    quota_base_bytes,
                    point_id: point.id,
                    username: user.username.clone(),
                    append: args.append,
                    local_point_guard,
                    expired: AtomicBool::new(false),
                }),
            );
        } else {
            // Remote destinations: `Vfs_OpenWrite` always opens a brand-new
            // writer (opendal has no "open an existing object for
            // random-access editing" primitive), so `Vfs_WriteAt` on this
            // handle is inherently sequential regardless of the backend's
            // native append capability — we're streaming into a fresh
            // object, not resuming one. Require the caller to acknowledge
            // this by setting `append = true` rather than silently ignoring
            // `offset` later; `overwrite` is implied by opening a fresh
            // writer and doesn't need separate handling here.
            if !args.append {
                return Err(Status::invalid_argument(
                    "non-local write destinations only support sequential (append) writes; set append=true",
                ));
            }

            let (op, file_path) = self.get_operator(file, false).await?;
            let writer = op.writer(&file_path).await.map_err(map_dal)?;

            self.inner.write_handles.insert(
                handle_id,
                Arc::new(ActiveWriteHandle {
                    last_used: StdMutex::new(Instant::now()),
                    state: TokioMutex::new(WriteState {
                        backend: WriteStateBackend::Remote(writer),
                        len: 0,
                    }),
                    max_bytes: None, // Remote quotas are handled downstream via OpenDAL quota layer
                    quota_base_bytes: 0,
                    point_id: point.id,
                    username: user.username.clone(),
                    append: true,
                    local_point_guard: None,
                    expired: AtomicBool::new(false),
                }),
            );
        }

        Ok(Response::new(OpenWriteResponse {
            handle_id: handle_id.to_string(),
        }))
    }

    async fn vfs_write_at(&self, request: Request<WriteAtArgs>) -> Result<Response<Empty>, Status> {
        let args = request.into_inner();
        let handle_id = parse_handle_id(&args.handle_id)?;

        // Clone the Arc out so no DashMap shard lock is held across `.await`.
        let handle = self
            .inner
            .write_handles
            .get(&handle_id)
            .map(|h| Arc::clone(&*h))
            .ok_or_else(|| Status::not_found("unknown or already-closed write handle"))?;
        if handle.expired.load(Ordering::Acquire) {
            return Err(Status::not_found("write handle expired"));
        }
        if let Ok(mut t) = handle.last_used.lock() {
            *t = Instant::now();
        }

        let mut state = handle.state.lock().await;

        let WriteState { backend, len } = &mut *state;

        match backend {
            WriteStateBackend::Local(file) => {
                // In append mode the caller's offset is ignored entirely —
                // always write at the current end of the file.
                let write_offset = if handle.append {
                    file.seek(SeekFrom::End(0)).await.map_err(io_status)?
                } else {
                    args.offset
                };

                let new_len = (*len).max(
                    write_offset
                        .checked_add(args.data.len() as u64)
                        .ok_or_else(|| Status::invalid_argument("offset overflow"))?,
                );

                if let Some(max) = handle.max_bytes {
                    let projected = handle
                        .quota_base_bytes
                        .checked_add(new_len)
                        .ok_or_else(|| Status::resource_exhausted("quota size overflow"))?;
                    if projected > max {
                        return Err(Status::resource_exhausted(format!(
                            "write would exceed point quota ({projected} > {max} bytes)"
                        )));
                    }
                }

                if !handle.append {
                    file.seek(SeekFrom::Start(write_offset))
                        .await
                        .map_err(io_status)?;
                }

                file.write_all(&args.data).await.map_err(io_status)?;

                *len = new_len;
            }

            WriteStateBackend::Remote(writer) => {
                if args.offset != *len {
                    return Err(Status::invalid_argument(format!(
                        "remote writes must be sequential: expected offset {}, got {}",
                        *len, args.offset
                    )));
                }

                writer.write(args.data.clone()).await.map_err(map_dal)?;
                *len = len
                    .checked_add(args.data.len() as u64)
                    .ok_or_else(|| Status::invalid_argument("write length overflow"))?;
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

        let handle = self
            .inner
            .write_handles
            .get(&handle_id)
            .map(|h| Arc::clone(&*h))
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

        self.inner.write_handles.remove(&handle_id);
        Ok(Response::new(Empty {}))
    }

    async fn vfs_list_dir(
        &self,
        request: Request<FilePath>,
    ) -> Result<Response<ListVfsResponse>, Status> {
        let args = request.into_inner();
        let (op, path) = self.get_operator(&args, true).await?;
        let entries = op.list_with(&path).await.map_err(map_dal)?;

        Ok(Response::new(ListVfsResponse {
            nodes: entries.iter().map(entry_to_node).collect(),
        }))
    }

    async fn vfs_create_dir(&self, request: Request<FilePath>) -> Result<Response<Empty>, Status> {
        let args = request.into_inner();
        let (op, path) = self.get_operator(&args, true).await?;
        op.create_dir(&path).await.map_err(map_dal)?;
        Ok(Response::new(Empty {}))
    }

    async fn vfs_remove_file(&self, request: Request<FilePath>) -> Result<Response<Empty>, Status> {
        let args = request.into_inner();
        let (op, path) = self.get_operator(&args, false).await?;
        op.delete_with(&path).await.map_err(map_dal)?;
        Ok(Response::new(Empty {}))
    }

    async fn vfs_remove_dir(&self, request: Request<FilePath>) -> Result<Response<Empty>, Status> {
        let args = request.into_inner();
        let (op, path) = self.get_operator(&args, true).await?;
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
        let (op, file_path) = self.get_operator(&args, false).await?;

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
        let (op, file_path) = self.get_operator(&args, false).await?;

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

    /// Transfers (copies or moves) a single file between two VFS paths as a
    /// background job, so callers get progress and can poll/cancel it the
    /// same way they do for `Backup`/`Restore`.
    ///
    /// Same-backend transfers use opendal's native `copy`/`rename` and
    /// report a single before/after tick — there's no meaningful partial
    /// progress for an atomic backend-side operation. Cross-backend
    /// transfers stream the file in chunks and report real byte progress
    /// per chunk, which also keeps a cancellation request from having to
    /// wait for the whole file to move first.
    async fn vfs_transfer(
        &self,
        request: Request<TransferArgs>,
    ) -> Result<Response<JobStartResponse>, Status> {
        let args = request.into_inner();

        let old_file = require_path(&args.old_path)?;
        let new_file = require_path(&args.new_path)?;

        if old_file.user == new_file.user {
            if let (Some(Path::Virtual(a)), Some(Path::Virtual(b))) =
                (&old_file.path, &new_file.path)
            {
                if a == b {
                    return Err(Status::invalid_argument(
                        "source and destination are identical",
                    ));
                }
            }
        }

        let dst_user = self.get_user(&new_file.user).await?;
        let dst_path_for_point = resolve_file_path(&dst_user, new_file)?;
        let (dst_point, _) = utils::resolve_data_path(&dst_user, &dst_path_for_point)?;
        utils::require_writable(dst_point)?;

        let (src_op, src_path) = self.get_operator(old_file, false).await?;
        let (dst_op, dst_path) = self.get_operator(new_file, false).await?;

        let src_meta = src_op.stat(&src_path).await.map_err(map_dal)?;
        if src_meta.is_dir() {
            return Err(Status::unimplemented(
                "directory transfers are not supported",
            ));
        }

        // Two operators are "the same backend" if their scheme, root, and
        // backend name all match. This is the closest thing OpenDAL exposes
        // to identity/equality on `Operator` (which doesn't impl PartialEq).
        let src_info = src_op.info();
        let dst_info = dst_op.info();
        let same_backend = src_info.scheme() == dst_info.scheme()
            && src_info.root() == dst_info.root()
            && src_info.name() == dst_info.name();

        let copy = args.copy;
        let username = old_file.user.clone();

        let src_user = self.get_user(&old_file.user).await?;
        let src_path_for_point = resolve_file_path(&src_user, old_file)?;
        let src_root = src_path_for_point.trim_start_matches('/').split('/').next();
        let src_point_name = src_path_for_point
            .trim_start_matches('/')
            .split('/')
            .nth(1)
            .unwrap_or_default();
        let src_point = match src_root {
            Some(root) if root == utils::POINTS_ROOT => {
                utils::require_data_point(&src_user, src_point_name)?
            }
            Some(root) if root == utils::REPOS_ROOT => {
                utils::require_repo_point(&src_user, src_point_name)?
            }
            _ => return Err(Status::invalid_argument("invalid source mount")),
        };
        let same_mount = src_point.id == dst_point.id;
        if same_mount && src_path == dst_path {
            return Err(Status::invalid_argument(
                "source and destination are identical",
            ));
        }

        if same_backend && same_mount {
            let inner = Arc::clone(&self.inner);
            let dst_local_quota = if copy && utils::is_local_scheme(&dst_point.scheme) {
                let root = PathBuf::from(utils::local_source_path(dst_point)?);
                let (_, rest) = utils::resolve_data_path(&dst_user, &dst_path)?;
                let full_path = utils::safe_join(&root, &rest.to_string_lossy())?;
                Some((root, full_path, dst_point.max_bytes))
            } else {
                None
            };
            let dst_point_id = dst_point.id;
            let dst_is_local = utils::is_local_scheme(&dst_point.scheme);
            // Same backend: let opendal do an intra-backend copy/rename,
            // which is typically far cheaper than a read+write round trip
            // (and atomic where the backend supports it). Nothing here can
            // report partial progress, so the bar just brackets the call.
            let job_id = self.spawn_async_job::<_, _, String>(
                username,
                move |job_id, tx, _token| async move {
                    let _local_guard = if dst_is_local {
                        let lock = inner
                            .local_point_locks
                            .entry(dst_point_id)
                            .or_insert_with(|| Arc::new(TokioMutex::new(())))
                            .clone();
                        Some(lock.lock_owned().await)
                    } else {
                        None
                    };
                    if let Some((root, full_path, max)) = dst_local_quota.as_ref() {
                        let existing_len = tokio::fs::metadata(full_path)
                            .await
                            .map(|m| m.len())
                            .unwrap_or(0);
                        let total = src_op
                            .stat(&src_path)
                            .await
                            .map_err(|e| e.to_string())?
                            .content_length();
                        let growth = total.saturating_sub(existing_len);
                        check_local_quota(root, *max, growth)
                            .await
                            .map_err(|e| e.to_string())?;
                    }

                    let bars = RusticProgressBars::new(job_id, tx);
                    let bar = bars.progress(ProgressType::Bytes, "transfer");
                    bar.set_length(1);

                    if copy {
                        src_op
                            .copy(&src_path, &dst_path)
                            .await
                            .map_err(|e| e.to_string())?;
                    } else {
                        src_op
                            .rename(&src_path, &dst_path)
                            .await
                            .map_err(|e| e.to_string())?;
                    }

                    bar.inc(1);
                    bar.finish();
                    Ok(None)
                },
            );

            return Ok(Response::new(JobStartResponse { job_id }));
        }

        // Different backends: no cross-backend copy/rename primitive
        // exists, so stream the bytes through manually, in chunks so
        // progress and cancellation are meaningful mid-transfer.
        //
        // `stat` and the quota check both happen up front, before the job
        // is even spawned — same as the read-only checks `backup`/`restore`
        // do — so a doomed transfer is rejected immediately instead of
        // being accepted and failing once it's already running.
        let meta = src_op.stat(&src_path).await.map_err(map_dal)?;
        let total = meta.content_length();

        let destination_exists = match dst_op.stat(&dst_path).await {
            Ok(meta) if !meta.is_dir() => true,
            Ok(_) => return Err(Status::failed_precondition("destination is a directory")),
            Err(e) if e.kind() == DalErrorKind::NotFound => false,
            Err(e) => return Err(map_dal(e)),
        };

        // Cross-backend streaming is not atomic. Refuse to overwrite an
        // existing destination so a failed transfer can always clean up its
        // partial output without destroying a pre-existing file.
        if destination_exists {
            return Err(Status::already_exists(
                "cross-backend transfers require a new destination",
            ));
        }

        let dst_local_quota = if utils::is_local_scheme(&dst_point.scheme) {
            let root = PathBuf::from(utils::local_source_path(dst_point)?);
            let (_, dst_rest) = utils::resolve_data_path(&dst_user, &dst_path)?;
            let full_path = utils::safe_join(&root, &dst_rest.to_string_lossy())?;
            Some((root, full_path, dst_point.max_bytes))
        } else {
            None
        };

        let inner = Arc::clone(&self.inner);
        let dst_point_id = dst_point.id;
        let dst_is_local = utils::is_local_scheme(&dst_point.scheme);
        let job_id = self.spawn_async_job(username, move |job_id, tx, token| async move {
            let _local_guard = if dst_is_local {
                let lock = inner
                    .local_point_locks
                    .entry(dst_point_id)
                    .or_insert_with(|| Arc::new(TokioMutex::new(())))
                    .clone();
                Some(lock.lock_owned().await)
            } else {
                None
            };
            if let Some((root, full_path, max)) = dst_local_quota.as_ref() {
                let existing_len = tokio::fs::metadata(full_path)
                    .await
                    .map(|m| m.len())
                    .unwrap_or(0);
                let growth = total.saturating_sub(existing_len);
                check_local_quota(root, *max, growth)
                    .await
                    .map_err(|e| e.to_string())?;
            }

            let bars = RusticProgressBars::new(job_id, tx);
            let bar = bars.progress(ProgressType::Bytes, "transfer");
            bar.set_length(total.max(1));

            // 8 MiB chunks: big enough to keep per-chunk overhead low,
            // small enough that progress updates stay meaningfully
            // granular and a cancellation is noticed promptly rather than
            // only between whole-file reads.
            const CHUNK: u64 = 8 * 1024 * 1024;

            let mut writer = dst_op.writer(&dst_path).await.map_err(|e| e.to_string())?;
            let mut offset = 0u64;

            let transfer_result: Result<(), String> = async {
                while offset < total {
                    if token.is_cancelled() {
                        return Err("transfer cancelled".to_string());
                    }

                    let end = (offset + CHUNK).min(total);
                    let buf = src_op
                        .read_with(&src_path)
                        .range(offset..end)
                        .await
                        .map_err(|e| e.to_string())?;
                    let n = buf.len() as u64;

                    writer.write(buf).await.map_err(|e| e.to_string())?;
                    bar.inc(n);
                    offset = end;
                }
                Ok(())
            }
            .await;

            if let Err(e) = transfer_result {
                // The destination was verified absent before the job started,
                // so cleanup cannot destroy a pre-existing file.
                let _ = dst_op.delete(&dst_path).await;
                return Err(e);
            }

            if let Err(e) = writer.close().await {
                let _ = dst_op.delete(&dst_path).await;
                return Err(e.to_string());
            }

            if !copy {
                if let Err(e) = src_op.delete(&src_path).await {
                    return Err(format!(
                        "destination transfer completed, but source deletion failed: {e}"
                    ));
                }
            }

            bar.finish();
            Ok(None)
        });

        Ok(Response::new(JobStartResponse { job_id }))
    }
}
