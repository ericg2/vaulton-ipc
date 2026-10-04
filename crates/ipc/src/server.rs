//! gRPC server implementing every method in `IpcService`.

use crossbeam_channel as chan;
use dashmap::DashMap;
use futures_lite::StreamExt;
use log::{error, info, warn};
use opendal_core::{Buffer, ErrorKind as DalErrorKind, Metadata, Operator, Reader};
use rustic_backend::local::LocalSource;
use rustic_backend::opendal::OpenDALSource;
use std::collections::{HashSet, VecDeque};
use std::io::SeekFrom;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{
    Arc, Mutex as StdMutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex as TokioMutex;
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
    BrowseSnapshotArgs, BrowseSnapshotResponse, ForgetArgs, GetJobArgs, GetSnapshotArgs, InfoResponse, IpcEvent, JobCancelResponse,
    JobFinishedEvent, JobNewMessageEvent, JobStartResponse, JobState, JobStatusResponse,
    ListVfsResponse, OpenWriteArgs, OpenWriteResponse, PointSource as ProtoPoint, PollResponse,
    Priority, ReadVfsArgs, ReadVfsResponse, ReloadArgs, RepoSource as ProtoRepo, RestoreArgs,
    ReadSnapshotFileArgs, ReadSnapshotFileResponse, RetentionArgs, SetLengthArgs, SetSnapshotLockArgs, SetSnapshotLockResponse, SetVfsArgs,
    Snapshot, SnapshotResponse, StatResponse, Summary, TransferArgs, VfsNode,
    VfsPoint as ProtoVfsPoint, VfsUser as ProtoVfsUser, WriteAtArgs,
};
use crate::browse;
use crate::progress::RusticProgressBars;
use crate::retention;
use crate::store::{RepoIndexed, RepoSource, StorageSystem};
use crate::utils;
use crate::utils::{fix_path, map_dal, map_vfs};
use moka::sync::Cache;
use opendal_vfs::layers::quota::{QuotaState, QuotaTracker};
use rustic_core::jiff::Zoned;
use rustic_core::repofile::{DeleteOption, Node, SnapshotFile, SnapshotId, SnapshotSummary};
use rustic_core::{
    CancelToken, CheckOptions, ErrorKind, LsOptions, PathList, ProgressBars, ProgressType,
    RestoreOptions, RusticError, SnapshotOptions, StringList, TreeId,
};

/// Resolves `comps` below the tree `root`, one component at a time, so a missing
/// name and a non-directory in the middle get distinct, precise errors.
/// `None` means the root itself (no components).
fn find_node(repo: &RepoIndexed, root: TreeId, comps: &[String]) -> Result<Option<Node>, Status> {
    let mut tree_id = root;
    let mut target: Option<Node> = None;
    for (i, comp) in comps.iter().enumerate() {
        let tree = repo
            .get_tree(&tree_id)
            .map_err(|e| Status::internal(format!("reading the snapshot tree failed: {e}")))?;
        let found = tree
            .nodes
            .into_iter()
            .find(|n| n.name().to_string_lossy().as_ref() == comp.as_str())
            .ok_or_else(|| {
                Status::not_found(format!(
                    "'{}' does not exist in this snapshot",
                    browse::join(&comps[..=i])
                ))
            })?;
        if i + 1 < comps.len() {
            match (found.is_dir(), found.subtree) {
                (true, Some(next)) => tree_id = next,
                _ => {
                    return Err(Status::failed_precondition(format!(
                        "'{}' is not a directory",
                        browse::join(&comps[..=i])
                    )));
                }
            }
        }
        target = Some(found);
    }
    Ok(target)
}

// ── Write handles ──────────────────────────────────────────────────────
//
// Backing state for `Vfs_OpenWrite`/`Vfs_WriteAt`/`Vfs_SetLength`/`Vfs_CloseWrite`.
//
// Handles exist only for writable local/fs-backed data points and are backed
// by the real destination file. Remote backends are not writable by users at
// all (see `utils::require_user_writable`), so there is no spooling/upload
// path: remote data is only ever written by backup/restore jobs.

struct WriteState {
    file: tokio::fs::File,
    len: u64,
}

/// Max buffered events retained for Poll; telemetry is discarded first when saturated.
const MAX_EVENTS: usize = 10_000;
/// Max events returned by a single `Poll`.
const POLL_BATCH: usize = 2_000;
/// Write handles idle longer than this are closed by the reaper.
const HANDLE_IDLE_TTL: Duration = Duration::from_secs(600);
/// Read handles are deliberately a little shorter-lived than write handles:
/// they retain an open local file descriptor or an OpenDAL Reader, but are
/// recreated transparently when idle.
const READ_HANDLE_IDLE_TTL: Duration = Duration::from_secs(120);
/// OpenDAL read-ahead chunk size for remote readers.
const READ_CHUNK: usize = 1024 * 1024;
/// Number of backend requests OpenDAL may issue concurrently for a read.
const READ_CONCURRENT: usize = 8;
/// Small-gap merge window for adjacent/random-access SMB reads.
const READ_GAP: usize = 256 * 1024;
/// Largest single read served over gRPC (callers must chunk beyond this).
const MAX_READ: u64 = 32 * 1024 * 1024;
/// Chunk size for cross-backend transfers.
const SPOOL_CHUNK: u64 = 8 * 1024 * 1024;
/// How long finished job results stay queryable through `GetJob`.
const FINISHED_JOB_TTL: Duration = Duration::from_secs(600);

struct ActiveWriteHandle {
    last_used: StdMutex<Instant>,
    state: TokioMutex<WriteState>,
    max_bytes: Option<u64>,
    /// Total quota usage excluding the file currently owned by this handle.
    /// A snapshot taken at open time.
    quota_base_bytes: u64,
    #[allow(dead_code)]
    point_id: Uuid,
    username: String,
    path: String,
    /// When true, every `Vfs_WriteAt` ignores the caller's `offset` and
    /// writes at the current end of the file.
    append: bool,
    expired: AtomicBool,
}

/// A persistent read handle for one VFS file.
///
/// Remote files keep an OpenDAL `Reader` alive.  OpenDAL's range-based Reader
/// is stateless at the public API, so multiple gRPC reads can use the same
/// reader concurrently without a seek/mutex bottleneck.  The reader is opened
/// with chunking/concurrency/gap-merging tuned for random-access clients such
/// as Windows Explorer.
///
/// Local files keep one opened Tokio file handle.  Each request clones the
/// underlying OS handle before seeking, so concurrent reads never race on a
/// shared file cursor.
enum ReadState {
    Local(tokio::fs::File),
    Remote(Reader),
}

struct ActiveReadHandle {
    last_used: StdMutex<Instant>,
    state: ReadState,
    username: String,
    #[allow(dead_code)]
    point_id: Uuid,
    #[allow(dead_code)]
    path: String,
    len: u64,
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
/// (`utils::dir_size`).
///
/// `growth` should already be the *net* increase in the file's length, so
/// in-place overwrites that don't extend the file aren't penalized.
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

/// Flushes a handle and syncs it to disk.
async fn finalize_write_state(state: &mut WriteState) -> Result<(), Status> {
    state.file.flush().await.map_err(io_status)?;
    state.file.sync_all().await.map_err(io_status)
}

/// Deletes a file or directory tree and verifies it is really gone.
/// OpenDAL treats deleting a missing path as success, so never trust Ok alone.
async fn remove_any(op: &Operator, path: &str) -> Result<(), Status> {
    // NotFound surfaces as an error here instead of a silent success.
    let (meta, p) = stat_any(op, path).await?;

    if meta.is_dir() {
        op.delete_with(&p).recursive(true).await.map_err(map_dal)?;

        // Fallback if the recursive delete was a no-op on this backend:
        // delete children deepest-first, then the directory itself.
        if op.stat(&p).await.is_ok() {
            let mut entries = op.list_with(&p).recursive(true).await.map_err(map_dal)?;
            entries.sort_by(|a, b| b.path().len().cmp(&a.path().len()));
            for e in &entries {
                if e.path() == p {
                    continue;
                }
                op.delete(e.path()).await.map_err(map_dal)?;
            }
            op.delete(&p).await.map_err(map_dal)?;
        }
    } else {
        // Plain delete: recursive(true) on a file path is what goes wrong.
        op.delete(&p).await.map_err(map_dal)?;
    }

    match op.stat(&p).await {
        Ok(_) => Err(Status::internal(format!(
            "delete of '{p}' reported success but it still exists"
        ))),
        Err(e) if e.kind() == DalErrorKind::NotFound => Ok(()),
        Err(e) => Err(map_dal(e)),
    }
}

/// Stats a path that may be a file or a directory (object stores need a
/// trailing slash for directories). Returns the metadata and the normalized path.
async fn stat_any(op: &Operator, path: &str) -> Result<(Metadata, String), Status> {
    match op.stat(path).await {
        Ok(m) => {
            let p = if m.is_dir() {
                fix_path(path, true)
            } else {
                path.to_string()
            };
            Ok((m, p))
        }
        Err(e) if e.kind() == DalErrorKind::NotFound => {
            let d = fix_path(path, true);
            let m = op.stat(&d).await.map_err(map_dal)?;
            Ok((m, d))
        }
        Err(e) => Err(map_dal(e)),
    }
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
            locked: matches!(s.delete, DeleteOption::Never),
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
    // IDs live in one namespace shared by data and repo points, across all
    // users. Validate that first, up front, before touching names.
    crate::core::validate_point_ids(users.iter()).map_err(map_vfs)?;

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
/// clients that already have its stable ID. Both data points (`/points/...`)
/// and repo points (`/repos/...`) can be addressed this way. The resulting VFS
/// path still uses the point's current name because mount names are the
/// user-facing namespace.
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
            // The ID alone selects the point (data and repo points share one
            // ID space); the point's kind then selects /points vs /repos.
            let point = utils::find_point_id(user, &path.point_id)?;

            // Same traversal protection as virtual paths: reject `..` and
            // prefix components so an indexed path can't escape its point.
            let rel = utils::validate_relative_point_path(&path.point_path)?;
            let mount = utils::point_mount_path(point);
            if rel.is_empty() {
                Ok(mount)
            } else {
                Ok(format!("{mount}/{rel}"))
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
        ctime: mtime.clone(),
        mtime: mtime.clone(),
        atime: mtime,
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

// ── Transfer engine ───────────────────────────────────────────────────────────

/// Live byte counters for a running job, readable through `GetJob`.
#[derive(Default)]
struct JobProgress {
    done: AtomicU64,
    total: AtomicU64,
}

/// Result of a finished job, retained briefly so callers can still query it.
struct FinishedJob {
    success: bool,
    error: String,
    done: u64,
    total: u64,
}

/// Progress callback: `Some(n)` = n more bytes done, `None` = finished.
type Report<'a> = &'a mut (dyn FnMut(Option<u64>) + Send);

struct TransferPlan {
    src_op: Operator,
    dst_op: Operator,
    /// Directories carry a trailing slash.
    src_path: String,
    dst_path: String,
    src_is_dir: bool,
    entries: Vec<opendal_core::Entry>,
    total_bytes: u64,
    copy: bool,
    /// Same backend *and* same mount: native copy/rename are usable.
    native: bool,
    /// (point root, destination file path, quota) for the (always local)
    /// destination; `None` when the transfer can't add bytes to it.
    local_quota: Option<(PathBuf, PathBuf, Option<u64>)>,
}

fn with_slash(path: &str) -> String {
    if path.ends_with('/') {
        path.to_string()
    } else {
        format!("{path}/")
    }
}

/// Streams one file through userspace in chunks.
async fn stream_copy(
    src_op: &Operator,
    src: &str,
    dst_op: &Operator,
    dst: &str,
    total: u64,
    token: &CancelToken,
    report: Report<'_>,
) -> Result<(), String> {
    let mut writer = dst_op
        .writer(dst)
        .await
        .map_err(|e| format!("failed to create destination file '{dst}': {e}"))?;

    let mut offset = 0u64;

    while offset < total {
        if token.is_cancelled() {
            let _ = writer.close().await;
            return Err("transfer cancelled".to_string());
        }

        let end = (offset + SPOOL_CHUNK).min(total);

        let buf = match src_op.read_with(src).range(offset..end).await {
            Ok(buf) => buf,
            Err(e) => {
                let _ = writer.close().await;
                return Err(format!("failed to read '{src}' at byte {offset}: {e}"));
            }
        };

        let n = buf.len() as u64;

        if n == 0 {
            let _ = writer.close().await;
            return Err(format!(
                "source '{src}' returned zero bytes before reaching its expected length"
            ));
        }

        if let Err(e) = writer.write(buf).await {
            let _ = writer.close().await;
            return Err(format!("failed to write '{dst}' at byte {offset}: {e}"));
        }

        offset = offset.saturating_add(n);
        report(Some(n));
    }

    writer
        .close()
        .await
        .map(|_| ())
        .map_err(|e| format!("failed to finalize destination file '{dst}': {e}"))
}

/// Copies one file using the native copy when possible, streaming otherwise.
async fn copy_one(
    plan: &TransferPlan,
    src: &str,
    dst: &str,
    len: u64,
    token: &CancelToken,
    report: Report<'_>,
) -> Result<(), String> {
    if plan.native {
        plan.src_op
            .copy(src, dst)
            .await
            .map_err(|e| format!("failed to copy '{src}' to '{dst}': {e}"))?;
        report(Some(len));
        Ok(())
    } else {
        stream_copy(&plan.src_op, src, &plan.dst_op, dst, len, token, report).await
    }
}

/// Copies the whole source (file or directory tree) to the destination.
async fn copy_tree(
    plan: &TransferPlan,
    token: &CancelToken,
    report: Report<'_>,
) -> Result<(), String> {
    if !plan.src_is_dir {
        return copy_one(
            plan,
            &plan.src_path,
            &plan.dst_path,
            plan.total_bytes,
            token,
            report,
        )
        .await;
    }

    let src_prefix = with_slash(&plan.src_path);
    let dst_prefix = with_slash(&plan.dst_path);

    plan.dst_op
        .create_dir(&plan.dst_path)
        .await
        .map_err(|e| format!("failed to create destination directory: {e}"))?;

    for entry in &plan.entries {
        if token.is_cancelled() {
            return Err("transfer cancelled".to_string());
        }

        let source = entry.path().to_string();

        if !source.starts_with(&src_prefix) {
            return Err(format!("source listing returned unexpected path: {source}"));
        }

        let relative = &source[src_prefix.len()..];

        // Some backends include the listed root itself.
        if relative.is_empty() {
            continue;
        }

        let destination = format!("{dst_prefix}{relative}");
        let meta = entry.metadata();

        // Preserve directories, including empty ones.
        if meta.is_dir() {
            plan.dst_op.create_dir(&destination).await.map_err(|e| {
                format!("failed to create destination directory '{destination}': {e}")
            })?;
            continue;
        }

        copy_one(
            plan,
            &source,
            &destination,
            meta.content_length(),
            token,
            report,
        )
        .await?;
    }

    Ok(())
}

/// The body of a `Vfs_Transfer` job.
async fn run_transfer(
    plan: TransferPlan,
    point_lock: Arc<TokioMutex<()>>,
    job_id: Uuid,
    tx: chan::Sender<Data>,
    token: CancelToken,
    progress: Arc<JobProgress>,
) -> Result<Option<String>, String> {
    // Re-check the destination quota under the point lock (another operation
    // may have changed the point between job creation and execution). The lock
    // is only held for the check so long transfers don't block new writes.
    if let Some((root, full_path, max)) = plan.local_quota.as_ref() {
        let _guard = point_lock.lock_owned().await;

        let existing_len = tokio::fs::metadata(full_path)
            .await
            .map(|m| m.len())
            .unwrap_or(0);

        let growth = plan.total_bytes.saturating_sub(existing_len);

        check_local_quota(root, *max, growth)
            .await
            .map_err(|e| e.to_string())?;
    }

    let total = plan.total_bytes.max(1);
    progress.total.store(total, Ordering::Relaxed);

    let bars = RusticProgressBars::new(job_id, tx);
    let bar = bars.progress(ProgressType::Bytes, "transfer");
    bar.set_length(total);

    let mut report: Box<dyn FnMut(Option<u64>) + Send> = {
        let progress = Arc::clone(&progress);
        Box::new(move |n| match n {
            Some(n) => {
                bar.inc(n);
                progress.done.fetch_add(n, Ordering::Relaxed);
            }
            None => bar.finish(),
        })
    };

    // MOVE on the same backend: try a native rename first.
    if plan.native && !plan.copy {
        match plan.src_op.rename(&plan.src_path, &plan.dst_path).await {
            Ok(()) => {
                report(Some(total));
                report(None);
                return Ok(None);
            }
            // Expected fallback: copy, then delete the source.
            Err(e) if e.kind() == DalErrorKind::Unsupported => {}
            Err(e) => return Err(format!("failed to rename source to destination: {e}")),
        }
    }

    if let Err(e) = copy_tree(&plan, &token, &mut *report).await {
        // The destination did not exist before this job, so we own it.
        let _ = plan
            .dst_op
            .delete_with(&plan.dst_path)
            .recursive(true)
            .await;
        return Err(e);
    }

    // A move never deletes the source until the destination is complete.
    if !plan.copy {
        if token.is_cancelled() {
            return Err(
                "transfer cancelled after destination copy completed; source was not deleted"
                    .to_string(),
            );
        }

        plan.src_op
            .delete_with(&plan.src_path)
            .recursive(true)
            .await
            .map_err(|e| {
                format!("destination transfer completed, but source deletion failed: {e}")
            })?;
    }

    if plan.total_bytes == 0 {
        report(Some(1));
    }
    report(None);

    Ok(None)
}

/// A pending repository password change detected by `set_vfs`.
struct RepoPasswordChange {
    user: String,
    point: String,
    /// Storage target plus the *old* password.
    old_src: RepoSource,
    new_password: String,
    /// Whether the repo point is writable (key rotation writes to the repo).
    allow_write: bool,
}

// ── Server state ──────────────────────────────────────────────────────────────

/// A running job's cancel handle plus the username it runs on behalf of, so
/// `set_vfs` can cancel jobs whose user's VFS config just changed underneath
/// them.
struct JobHandle {
    token: CancelToken,
    user: String,
    progress: Arc<JobProgress>,
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
    /// Recently finished jobs, queryable through `GetJob`.
    finished_jobs: Cache<Uuid, Arc<FinishedJob>>,
    events: StdMutex<VecDeque<IpcEvent>>,
    /// Open `Vfs_OpenWrite` handles, keyed by the id handed back to the
    /// caller. Entries live here for the lifetime of the handle and are
    /// removed by `Vfs_CloseWrite`.
    write_handles: DashMap<Uuid, Arc<ActiveWriteHandle>>,
    /// Read handles are keyed by `(username, normalized VFS path)`.  The public
    /// ReadVfs RPC remains path/offset/length based; the handle is an internal
    /// optimization so callers do not need a new RPC or handle ID.
    read_handles: DashMap<String, Arc<ActiveReadHandle>>,
    local_point_locks: DashMap<Uuid, Arc<TokioMutex<()>>>,
    /// Serializes snapshot lock/unlock per repository point (keyed by point id).
    repo_op_locks: DashMap<Uuid, Arc<TokioMutex<()>>>,
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
                finished_jobs: Cache::builder()
                    .time_to_live(FINISHED_JOB_TTL)
                    .max_capacity(4096)
                    .build(),
                events: StdMutex::new(VecDeque::new()),
                write_handles: DashMap::new(),
                read_handles: DashMap::new(),
                local_point_locks: DashMap::new(),
                repo_op_locks: DashMap::new(),
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
                    tokio::spawn(Self::close_write_handle(handle));
                }
            }
        });

        // Reaper for idle read handles.  Reads have no explicit close RPC, so
        // the TTL is what bounds open descriptors and remote reader state.
        let weak = Arc::downgrade(&server.inner);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(30));
            loop {
                tick.tick().await;
                let Some(inner) = weak.upgrade() else { break };

                let expired: Vec<String> = inner
                    .read_handles
                    .iter()
                    .filter_map(|entry| {
                        let idle = entry
                            .value()
                            .last_used
                            .lock()
                            .map(|t| t.elapsed())
                            .unwrap_or_default();
                        (idle > READ_HANDLE_IDLE_TTL).then_some(entry.key().clone())
                    })
                    .collect();

                for key in expired {
                    if let Some((_, handle)) = inner.read_handles.remove(&key) {
                        handle.expired.store(true, Ordering::Release);
                        // Dropping the Arc closes the local descriptor / reader.
                    }
                }
            }
        });

        server
    }

    async fn close_write_handle(handle: Arc<ActiveWriteHandle>) {
        handle.expired.store(true, Ordering::Release);
        let mut state = handle.state.lock().await;
        if let Err(e) = finalize_write_state(&mut state).await {
            warn!("failed to finalize write handle: {e}");
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

    fn invalidate_user_read_handles(&self, username: &str) {
        let keys: Vec<String> = self
            .inner
            .read_handles
            .iter()
            .filter_map(|entry| (entry.value().username == username).then_some(entry.key().clone()))
            .collect();

        for key in keys {
            if let Some((_, handle)) = self.inner.read_handles.remove(&key) {
                handle.expired.store(true, Ordering::Release);
            }
        }
    }

    fn invalidate_read_handle(&self, username: &str, vfs_path: &str) {
        let key = format!("{}\0{}", username, vfs_path);
        if let Some((_, handle)) = self.inner.read_handles.remove(&key) {
            handle.expired.store(true, Ordering::Release);
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

        for entry in self.inner.read_handles.iter() {
            entry.value().expired.store(true, Ordering::Release);
        }
        self.inner.read_handles.clear();
    }

    /// Gets or opens the internal read handle for `file`.
    ///
    /// This is intentionally transparent to the protobuf API: callers still
    /// provide only `path`, `offset`, and `length`.  The server reuses the
    /// reader while it is hot and drops it after the read-handle TTL.
    async fn get_read_handle(&self, file: &FilePath) -> Result<Arc<ActiveReadHandle>, Status> {
        let user = self.get_user(&file.user).await?;
        let vfs_path = resolve_file_path(&user, file)?;
        let (point, rest) = utils::resolve_point_path(&user, &vfs_path)?;
        let key = format!("{}\0{}", user.username, vfs_path);

        if let Some(existing) = self.inner.read_handles.get(&key) {
            let handle = Arc::clone(&*existing);
            drop(existing);

            if !handle.expired.load(Ordering::Acquire) {
                if let Ok(mut t) = handle.last_used.lock() {
                    *t = Instant::now();
                }
                return Ok(handle);
            }

            self.inner.read_handles.remove(&key);
        }

        let handle = if utils::is_local_scheme(&point.scheme) && !point.is_repo {
            let root = PathBuf::from(utils::local_source_path(point)?);
            let full_path = utils::safe_join(&root, &rest.to_string_lossy())?;
            let metadata = tokio::fs::metadata(&full_path).await.map_err(io_status)?;

            if !metadata.is_file() {
                return Err(Status::failed_precondition(
                    "cannot read a directory as a file",
                ));
            }

            let file = tokio::fs::OpenOptions::new()
                .read(true)
                .open(&full_path)
                .await
                .map_err(io_status)?;

            Arc::new(ActiveReadHandle {
                last_used: StdMutex::new(Instant::now()),
                state: ReadState::Local(file),
                username: user.username.clone(),
                point_id: point.id,
                path: vfs_path,
                len: metadata.len(),
                expired: AtomicBool::new(false),
            })
        } else {
            let op = self.inner.storage.get_vfs(&user).await.map_err(map_vfs)?;
            self.check_point_loaded(&user, &vfs_path)?;

            // OpenDAL's Reader is range based.  Keep it open between gRPC
            // reads and let OpenDAL split each requested range into 1 MiB
            // backend requests, with up to 8 in flight.  A small gap merge is
            // useful for SMB/Explorer's nearby 1 MiB reads without turning
            // unrelated random reads into giant requests.
            let path = fix_path(vfs_path.clone(), false);
            let reader = op
                .reader_with(&path)
                .chunk(READ_CHUNK)
                .concurrent(READ_CONCURRENT)
                .gap(READ_GAP)
                .await
                .map_err(map_dal)?;

            warn!("DOING STAT FOR HANDLE");
            let len = op.stat(&path).await.map_err(map_dal)?.content_length();

            Arc::new(ActiveReadHandle {
                last_used: StdMutex::new(Instant::now()),
                state: ReadState::Remote(reader),
                username: user.username.clone(),
                point_id: point.id,
                path: vfs_path,
                len,
                expired: AtomicBool::new(false),
            })
        };

        // Another request may have won the race while we opened the reader.
        // Prefer the already-live handle so only one backend reader is retained.
        if let Some(existing) = self
            .inner
            .read_handles
            .insert(key.clone(), Arc::clone(&handle))
        {
            if !existing.expired.load(Ordering::Acquire) {
                return Ok(existing);
            }
        }

        Ok(handle)
    }

    async fn read_from_handle(
        handle: &ActiveReadHandle,
        offset: u64,
        length: u64,
    ) -> Result<Buffer, Status> {
        if handle.expired.load(Ordering::Acquire) {
            return Err(Status::not_found("read handle expired"));
        }

        if offset >= handle.len || length == 0 {
            return Ok(Buffer::new());
        }

        let length = length.min(handle.len - offset);
        let end = offset
            .checked_add(length)
            .ok_or_else(|| Status::invalid_argument("read range overflow"))?;

        match &handle.state {
            ReadState::Remote(reader) => reader.read(offset..end).await.map_err(map_dal),
            ReadState::Local(file) => {
                // Clone the OS file handle rather than sharing a seek cursor.
                // Windows Explorer can issue overlapping reads concurrently.
                let mut f = file.try_clone().await.map_err(io_status)?;
                f.seek(SeekFrom::Start(offset)).await.map_err(io_status)?;

                let mut out = Vec::with_capacity(length as usize);
                let mut limited = f.take(length);
                limited.read_to_end(&mut out).await.map_err(io_status)?;

                Ok(Buffer::from(out))
            }
        }
    }

    /// Rejects `file` unless a user may modify it: only writable local/fs
    /// data points qualify (remote points and repos are read-only; see
    /// `utils::require_user_writable`).
    async fn require_user_write(&self, file: &FilePath) -> Result<(), Status> {
        let user = self.get_user(&file.user).await?;
        let vfs_path = resolve_file_path(&user, file)?;
        let (point, _) = utils::resolve_point_path(&user, &vfs_path)?;
        utils::require_user_writable(point)
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

    /// Registers a new job (cancel token + owning `user` + progress counters)
    /// and returns what the job body needs. Shared setup for `spawn_job` and
    /// `spawn_async_job`.
    fn register_job(
        &self,
        user: impl Into<String>,
    ) -> (Uuid, CancelToken, chan::Sender<Data>, Arc<JobProgress>) {
        let job_id = Uuid::new_v4();
        let token = CancelToken::new();
        let progress = Arc::new(JobProgress::default());
        self.inner.jobs.insert(
            job_id,
            JobHandle {
                token: token.clone(),
                user: user.into(),
                progress: Arc::clone(&progress),
            },
        );
        (job_id, token, self.inner.tx.clone(), progress)
    }

    /// Records the job's result for `GetJob`, sends the terminal `JobMessage`
    /// (on error) and `JobFinished` events, and removes it from the
    /// active-jobs map. Shared tail end of `spawn_job` and `spawn_async_job`.
    fn finish_job(
        inner: &Arc<Inner<S, U>>,
        job_id: Uuid,
        tx: chan::Sender<Data>,
        result: Result<Option<String>, String>,
    ) {
        // Publish the result *before* removing the job so `GetJob` (which
        // checks running jobs first, then finished ones) never sees a gap.
        let (done, total) = inner
            .jobs
            .get(&job_id)
            .map(|j| {
                (
                    j.progress.done.load(Ordering::Relaxed),
                    j.progress.total.load(Ordering::Relaxed),
                )
            })
            .unwrap_or((0, 0));

        inner.finished_jobs.insert(
            job_id,
            Arc::new(FinishedJob {
                success: result.is_ok(),
                error: result.as_ref().err().cloned().unwrap_or_default(),
                done,
                total,
            }),
        );

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
        let (job_id, token, tx, _progress) = self.register_job(user);
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
    /// `Vfs_Transfer`. The closure also receives the job's [`JobProgress`]
    /// so it can publish byte counters for `GetJob`.
    fn spawn_async_job<F, Fut, E>(&self, user: impl Into<String>, f: F) -> String
    where
        F: FnOnce(Uuid, chan::Sender<Data>, CancelToken, Arc<JobProgress>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<Option<String>, E>> + Send + 'static,
        E: std::fmt::Display + std::marker::Send + 'static,
    {
        let (job_id, token, tx, progress) = self.register_job(user);
        let inner = Arc::clone(&self.inner);

        tokio::spawn(async move {
            // Run in its own task so a panic is observed instead of leaking the job.
            let result = match tokio::spawn(f(job_id, tx.clone(), token, progress)).await {
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

        // Every snapshot is labelled with its data point. Retention groups
        // snapshots by host + label + paths, so two data points that are both
        // backed up from "/" stay separate series (and never thin each other
        // out); the label also keeps rustic's parent lookup per data point.
        let label = data_point.id.to_string();

        // Retention applies to real backups only.
        let retention_policy = if args.dry_run {
            None
        } else {
            args.retention.filter(retention::has_rules)
        };
        let repo_max_bytes = repo_point.max_bytes;
        let quota = self.inner.quota.clone();
        let quota_key = utils::quota_id(&repo_user.username, &repo_point.id);

        let job_id = self.spawn_job(job_user, move |job_id, tx, token| {
            let handle = tokio::runtime::Handle::current();
            let tags = StringList::from_str(&tags.join(",")).map_err(|err| {
                RusticError::with_source(ErrorKind::InvalidInput, "Failed to parse tags", err)
            })?;
            let snap = SnapshotOptions::default()
                .tags(vec![tags])
                .label(label)
                .to_snapshot()?;

            // Retention helpers. `open` hands out a fresh repository handle on
            // every call: a handle's in-memory index is stale after a prune, so
            // retention never shares one with the backup below.
            let retention_token = token.clone();
            let open = || {
                handle.block_on(storage.get_repo_job(
                    &repo_src,
                    repo_op.clone(),
                    job_id,
                    tx.clone(),
                    false,
                ))
            };
            let used_bytes = || {
                handle
                    .block_on(quota.current_bytes(&quota_key))
                    .map_err(|e| e.to_string())
            };
            let retention_run = retention::Retention {
                job_id,
                tx: &tx,
                token: &retention_token,
                open: &open,
                max_bytes: repo_max_bytes,
                used_bytes: &used_bytes,
                dry_run: false,
            };

            // Make room first: only when the free space is below the policy's
            // reserve (cheap check; a brand-new repository returns early).
            if let Some(policy) = retention_policy.as_ref() {
                if retention::has_space_rule(policy) {
                    if let Err(e) = retention_run.run(policy, retention::Phase::SpaceOnly) {
                        retention_run.say(
                            Priority::Warning,
                            format!("Could not make room before the backup: {e}"),
                        );
                    }
                }
            }

            let repo = handle.block_on(storage.get_repo_job(
                &repo_src,
                repo_op.clone(),
                job_id,
                tx.clone(),
                true,
            ))?;
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

            // The snapshot is safely stored. Whatever retention does from here on
            // can only produce warnings; it must never fail this backup.
            drop(repo);
            if let Some(policy) = retention_policy.as_ref() {
                match retention_run.run(policy, retention::Phase::Full) {
                    Ok(report) => {
                        if report.time_deleted + report.space_deleted > 0 {
                            retention_run.say(
                                Priority::Info,
                                format!(
                                    "Retention removed {} snapshot(s), reclaimed {}",
                                    report.time_deleted + report.space_deleted,
                                    retention::fmt_bytes(report.bytes_freed)
                                ),
                            );
                        }
                    }
                    Err(e) => retention_run.say(
                        Priority::Warning,
                        format!("Retention failed after the backup succeeded: {e}"),
                    ),
                }
            }

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
        let include_locked = args.include_locked;

        let job_id = self.spawn_job(username, move |job_id, tx, _token| {
            let handle = tokio::runtime::Handle::current();
            let repo =
                handle.block_on(storage.get_repo_job(&repo_src, repo_op, job_id, tx, false))?;

            // Authoritative lock check (the API pre-checks too, but a snapshot
            // can be locked between queueing and running).
            if !include_locked {
                let locked: Vec<String> = repo
                    .get_all_snapshots()?
                    .into_iter()
                    .filter(|s| matches!(s.delete, DeleteOption::Never) && snap_ids.contains(&s.id))
                    .map(|s| s.id.to_string())
                    .collect();
                if !locked.is_empty() {
                    return Err(RusticError::new(
                        ErrorKind::InvalidInput,
                        format!(
                            "refusing to delete locked snapshot(s): {}. Unlock them first.",
                            locked.join(", ")
                        ),
                    ));
                }
            }

            repo.delete_snapshots(&snap_ids)?;
            Ok(None)
        });

        self.inner.snapshot_cache.invalidate(&snapshot_repo_src);
        Ok(Response::new(JobStartResponse { job_id }))
    }

    /// Applies a retention policy on demand (time rules, then FIFO space rule,
    /// then prune), or previews it with `dry_run`.
    async fn apply_retention(
        &self,
        req: Request<RetentionArgs>,
    ) -> Result<Response<JobStartResponse>, Status> {
        let args = req.into_inner();
        let user = self.get_user(&args.user).await?;

        let repo_point = utils::require_repo_point_id(&user, &args.repo_id)?;
        utils::require_writable(repo_point)?;
        let repo_src = utils::repo_source(repo_point)?;
        let policy = args
            .policy
            .ok_or_else(|| Status::invalid_argument("a retention policy is required"))?;
        retention::validate(&policy).map_err(Status::invalid_argument)?;
        let repo_op = self
            .inner
            .storage
            .get_data_operator(&user, repo_point)
            .map_err(map_vfs)?;

        let storage = Arc::clone(&self.inner.storage);
        let quota = self.inner.quota.clone();
        let quota_key = utils::quota_id(&user.username, &repo_point.id);
        let repo_max_bytes = repo_point.max_bytes;
        let dry_run = args.dry_run;
        let username = user.username.clone();
        let snapshot_repo_src = repo_src.clone();

        let job_id = self.spawn_job(username, move |job_id, tx, token| {
            let handle = tokio::runtime::Handle::current();
            let open = || {
                handle.block_on(storage.get_repo_job(
                    &repo_src,
                    repo_op.clone(),
                    job_id,
                    tx.clone(),
                    false,
                ))
            };
            let used_bytes = || {
                handle
                    .block_on(quota.current_bytes(&quota_key))
                    .map_err(|e| e.to_string())
            };
            let run = retention::Retention {
                job_id,
                tx: &tx,
                token: &token,
                open: &open,
                max_bytes: repo_max_bytes,
                used_bytes: &used_bytes,
                dry_run,
            };

            if retention::has_space_rule(&policy) && repo_max_bytes.is_none() {
                run.say(
                    Priority::Warning,
                    "This repository has no quota, so the free-space rule is skipped",
                );
            }

            let report = run.run(&policy, retention::Phase::Full)?;
            run.say(
                Priority::Info,
                if dry_run {
                    "Dry run finished; nothing was changed".to_string()
                } else {
                    format!(
                        "Retention finished: {} snapshot(s) removed by time rules, {} by the space \
                         rule, {} reclaimed",
                        report.time_deleted,
                        report.space_deleted,
                        retention::fmt_bytes(report.bytes_freed)
                    )
                },
            );
            Ok(None)
        });

        self.inner.snapshot_cache.invalidate(&snapshot_repo_src);
        Ok(Response::new(JobStartResponse { job_id }))
    }

    /// Locks or unlocks one snapshot via rustic's delete protection.
    ///
    /// The snapshot file is content-addressed, so changing it means writing a
    /// modified copy and then removing the original; the copy has a new id.
    /// The copy is written first: a crash in between leaves a duplicate, never
    /// a lost snapshot.
    async fn set_snapshot_lock(
        &self,
        req: Request<SetSnapshotLockArgs>,
    ) -> Result<Response<SetSnapshotLockResponse>, Status> {
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
        let wanted = SnapshotId::from_str(&args.snapshot_id).map_err(|e| {
            Status::invalid_argument(format!("invalid snapshot id '{}': {e}", args.snapshot_id))
        })?;
        let want_locked = args.locked;

        let op_lock = self
            .inner
            .repo_op_locks
            .entry(repo_point.id)
            .or_insert_with(|| Arc::new(TokioMutex::new(())))
            .clone();
        let _guard = op_lock.lock().await;

        let storage = Arc::clone(&self.inner.storage);
        let src = repo_src.clone();
        let outcome = tokio::task::spawn_blocking(
            move || -> Result<(String, String, bool), Status> {
                let handle = tokio::runtime::Handle::current();
                // No job owns this handle, so its progress events go nowhere:
                // dropping the receiver makes every send fail fast.
                let (tx, rx) = chan::bounded::<Data>(1);
                drop(rx);

                let repo = handle
                    .block_on(storage.get_repo_job(&src, repo_op, Uuid::new_v4(), tx, false))
                    .map_err(|e| Status::internal(format!("failed to open repository: {e}")))?;
                let internal = |what: &str, e: Box<RusticError>| {
                    Status::internal(format!("{what}: {e}"))
                };

                let snaps = repo
                    .get_all_snapshots()
                    .map_err(|e| internal("snapshot listing failed", e))?;
                let old = snaps
                    .iter()
                    .find(|s| s.id == wanted)
                    .ok_or_else(|| Status::not_found("snapshot not found"))?;

                let is_locked = matches!(old.delete, DeleteOption::Never);
                if is_locked == want_locked {
                    let id = old.id.to_string();
                    return Ok((id.clone(), id, is_locked));
                }

                let mut modified = old.clone();
                modified.delete = if want_locked {
                    DeleteOption::Never
                } else {
                    DeleteOption::NotSet
                };
                repo.save_snapshots(vec![modified])
                    .map_err(|e| internal("saving the snapshot failed", e))?;

                // `save_snapshots` does not report the new id; find the copy.
                let after = repo
                    .get_all_snapshots()
                    .map_err(|e| internal("snapshot listing failed", e))?;
                let new_id = after
                    .iter()
                    .find(|s| {
                        s.id != old.id
                            && s.tree == old.tree
                            && s.time == old.time
                            && s.hostname == old.hostname
                            && s.label == old.label
                            && s.paths == old.paths
                            && s.tags == old.tags
                            && matches!(s.delete, DeleteOption::Never) == want_locked
                    })
                    .map(|s| s.id.clone())
                    .ok_or_else(|| Status::internal("the updated snapshot could not be found"))?;

                repo.delete_snapshots(&[old.id.clone()])
                    .map_err(|e| internal("removing the original snapshot failed", e))?;

                Ok((new_id.to_string(), old.id.to_string(), want_locked))
            },
        )
        .await
        .map_err(|e| Status::internal(format!("snapshot lock task failed: {e}")))??;

        self.inner.snapshot_cache.invalidate(&repo_src);
        let (snapshot_id, previous_snapshot_id, locked) = outcome;
        info!(
            "snapshot {previous_snapshot_id} of repo '{}' {} (new id {snapshot_id})",
            repo_point.name,
            if locked { "locked" } else { "unlocked" }
        );
        Ok(Response::new(SetSnapshotLockResponse {
            snapshot_id,
            previous_snapshot_id,
            locked,
        }))
    }

    /// Lists one directory of a snapshot (or describes a single file), so
    /// callers can navigate a snapshot and pick a path for restore without
    /// knowing the layout in advance.
    async fn browse_snapshot(
        &self,
        req: Request<BrowseSnapshotArgs>,
    ) -> Result<Response<BrowseSnapshotResponse>, Status> {
        let args = req.into_inner();
        let user = self.get_user(&args.user).await?;

        let repo_point = utils::require_repo_point_id(&user, &args.repo_id)?;
        let repo_src = utils::repo_source(repo_point)?;

        if args.snapshot_id.trim().is_empty() {
            return Err(Status::invalid_argument("snapshot_id is required"));
        }
        let comps = browse::split_path(&args.path).map_err(Status::invalid_argument)?;
        let canon = browse::join(&comps);
        let offset = args.offset as usize;
        let limit = browse::clamp_limit(args.limit);
        let snapshot_ref = args.snapshot_id;

        // The shared, cached handle (same one `get_snapshots` uses): no index
        // load per request.
        let repo = self
            .inner
            .storage
            .get_repo(&repo_src)
            .await
            .map_err(|err| Status::internal(format!("failed to open repository: {err}")))?;

        let response = tokio::task::spawn_blocking(
            move || -> Result<BrowseSnapshotResponse, Status> {
                let internal = |what: &str, e: Box<RusticError>| {
                    Status::internal(format!("{what}: {e}"))
                };

                let snap = repo
                    .get_snapshot_from_str(&snapshot_ref, |_| true)
                    .map_err(|e| {
                        Status::not_found(format!("snapshot '{snapshot_ref}' not found: {e}"))
                    })?;

                // Walk down from the snapshot root (None = the root itself).
                let target = find_node(&repo, snap.tree, &comps)?;

                let (node_entry, mut children) = match &target {
                    None => (
                        browse::root_entry(),
                        repo.get_tree(&snap.tree)
                            .map_err(|e| internal("reading the snapshot tree failed", e))?
                            .nodes,
                    ),
                    Some(n) if n.is_dir() => {
                        let kids = match n.subtree {
                            Some(t) => repo
                                .get_tree(&t)
                                .map_err(|e| internal("reading the directory failed", e))?
                                .nodes,
                            None => Vec::new(),
                        };
                        (browse::to_entry(n, canon.clone()), kids)
                    }
                    Some(n) => (browse::to_entry(n, canon.clone()), Vec::new()),
                };

                let total = children.len();
                browse::sort_nodes(&mut children);
                let entries = children
                    .iter()
                    .skip(offset)
                    .take(limit as usize)
                    .map(|n| browse::to_entry(n, browse::child_path(&canon, &browse::node_name(n))))
                    .collect();

                Ok(BrowseSnapshotResponse {
                    snapshot_id: snap.id.to_string(),
                    path: canon,
                    node: Some(node_entry),
                    entries,
                    total: u32::try_from(total).unwrap_or(u32::MAX),
                    offset: u32::try_from(offset).unwrap_or(u32::MAX),
                    limit,
                })
            },
        )
        .await
        .map_err(|e| Status::internal(format!("snapshot browse task failed: {e}")))??;

        Ok(Response::new(response))
    }

    /// Reads a chunk of a regular file inside a snapshot, so a client can
    /// download it (and resume/seek) the same way it reads VFS files.
    async fn read_snapshot_file(
        &self,
        req: Request<ReadSnapshotFileArgs>,
    ) -> Result<Response<ReadSnapshotFileResponse>, Status> {
        let args = req.into_inner();
        let user = self.get_user(&args.user).await?;

        let repo_point = utils::require_repo_point_id(&user, &args.repo_id)?;
        let repo_src = utils::repo_source(repo_point)?;

        if args.snapshot_id.trim().is_empty() {
            return Err(Status::invalid_argument("snapshot_id is required"));
        }
        if args.length == 0 || args.length > MAX_READ {
            return Err(Status::invalid_argument(format!(
                "length must be between 1 and {MAX_READ} bytes; read in chunks"
            )));
        }
        let comps = browse::split_path(&args.path).map_err(Status::invalid_argument)?;
        if comps.is_empty() {
            return Err(Status::invalid_argument("path must point to a file"));
        }
        let offset = args.offset;
        let length = args.length;
        let snapshot_ref = args.snapshot_id;

        // Same cached handle as browse/get_snapshots.
        let repo = self
            .inner
            .storage
            .get_repo(&repo_src)
            .await
            .map_err(|err| Status::internal(format!("failed to open repository: {err}")))?;

        let response = tokio::task::spawn_blocking(
            move || -> Result<ReadSnapshotFileResponse, Status> {
                let snap = repo
                    .get_snapshot_from_str(&snapshot_ref, |_| true)
                    .map_err(|e| {
                        Status::not_found(format!("snapshot '{snapshot_ref}' not found: {e}"))
                    })?;
                let node = find_node(&repo, snap.tree, &comps)?
                    .ok_or_else(|| Status::invalid_argument("path must point to a file"))?;
                if !node.is_file() {
                    return Err(Status::failed_precondition(format!(
                        "'{}' is not a regular file",
                        browse::join(&comps)
                    )));
                }

                let size = node.meta.size;
                if offset >= size {
                    return Ok(ReadSnapshotFileResponse { data: Vec::new(), size });
                }
                let want = length.min(size - offset);
                let (off, len) = match (usize::try_from(offset), usize::try_from(want)) {
                    (Ok(o), Ok(l)) => (o, l),
                    _ => return Err(Status::out_of_range("offset or length does not fit")),
                };

                let open = repo
                    .open_file(&node)
                    .map_err(|e| Status::internal(format!("opening the file failed: {e}")))?;
                let data = repo
                    .read_file_at(&open, off, len)
                    .map_err(|e| Status::internal(format!("reading the file failed: {e}")))?;
                Ok(ReadSnapshotFileResponse { data: data.to_vec(), size })
            },
        )
        .await
        .map_err(|e| Status::internal(format!("snapshot read task failed: {e}")))??;

        Ok(Response::new(response))
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

    // ── GetJob ────────────────────────────────────────────────────────────────

    /// Returns the live state of a running job, or the recorded result of a
    /// recently finished one. Lets synchronous callers (SMB) wait on jobs
    /// without consuming the shared `Poll` event stream.
    async fn get_job(
        &self,
        req: Request<GetJobArgs>,
    ) -> Result<Response<JobStatusResponse>, Status> {
        let args = req.into_inner();
        let uuid = Uuid::parse_str(&args.job_id)
            .map_err(|e| Status::invalid_argument(format!("bad job_id: {e}")))?;

        // Running jobs first; `finish_job` records the result before removing
        // the job, so one of the two lookups always hits.
        if let Some(job) = self.inner.jobs.get(&uuid) {
            return Ok(Response::new(JobStatusResponse {
                state: JobState::Running as i32,
                done_bytes: job.progress.done.load(Ordering::Relaxed),
                total_bytes: job.progress.total.load(Ordering::Relaxed),
                error: String::new(),
            }));
        }

        if let Some(finished) = self.inner.finished_jobs.get(&uuid) {
            return Ok(Response::new(JobStatusResponse {
                state: if finished.success {
                    JobState::Succeeded
                } else {
                    JobState::Failed
                } as i32,
                done_bytes: finished.done,
                total_bytes: finished.total,
                error: finished.error.clone(),
            }));
        }

        Err(Status::not_found(format!(
            "job '{}' not found (unknown or result expired)",
            args.job_id
        )))
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

        // Repo points that keep their ID and storage target but get a new
        // password: the password has to be changed *inside* the repository
        // too, or the point would fail to open with the new one.
        let mut password_changes: Vec<RepoPasswordChange> = Vec::new();
        for old_user in &old_users {
            let Some(new_user) = users.iter().find(|u| u.username == old_user.username) else {
                continue;
            };
            for old_point in old_user.points.iter().filter(|p| p.is_repo) {
                let Some(new_point) = new_user
                    .points
                    .iter()
                    .find(|p| p.id == old_point.id && p.is_repo)
                else {
                    continue;
                };
                let (Some(old_pw), Some(new_pw)) =
                    (&old_point.repo_password, &new_point.repo_password)
                else {
                    continue;
                };
                if old_pw == new_pw {
                    continue;
                }
                if old_point.scheme != new_point.scheme || old_point.config != new_point.config {
                    warn!(
                        "SetVfs: repo point '{}' of user '{}' changed password AND storage target; \
                         treating it as a different repository (no key rotation)",
                        new_point.name, new_user.username
                    );
                    continue;
                }
                warn!(
                    "SetVfs: repository password of point '{}' (user '{}') changed; \
                     rotating the key inside the repository",
                    new_point.name, new_user.username
                );
                password_changes.push(RepoPasswordChange {
                    user: new_user.username.clone(),
                    point: new_point.name.clone(),
                    old_src: RepoSource {
                        scheme: old_point.scheme.clone(),
                        config: old_point.config.clone(),
                        password: old_pw.clone(),
                    },
                    new_password: new_pw.clone(),
                    allow_write: !new_point.read_only,
                });
            }
        }

        // Phase 1: add the new key to every affected repo while the old one
        // still works. If anything fails, the request is rejected and the
        // stored configuration is untouched (a harmless extra key may remain
        // in repos that were already processed; the old password still works).
        for change in &password_changes {
            self.inner
                .storage
                .add_repo_key(&change.old_src, &change.new_password, change.allow_write)
                .await
                .map_err(|e| {
                    Status::failed_precondition(format!(
                        "failed to change the repository password of point '{}' (user '{}'): {e}",
                        change.point, change.user
                    ))
                })?;
        }

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
            self.invalidate_user_read_handles(&user.username);
            self.inner.storage.invalidate_vfs(&user);
        }

        for id in removed_quotas {
            // The new config is already committed; a failed cleanup must not
            // turn a successful SetVfs into an error.
            if self.inner.quota.clear(&id).is_err() {
                warn!("SetVfs: failed to clear quota '{id}'");
            }
        }

        // Phase 2: the new password is persisted, so drop the old key. A
        // failure here only leaves the old password valid in the repo.
        for change in &password_changes {
            if let Err(e) = self
                .inner
                .storage
                .remove_old_repo_key(&change.old_src, &change.new_password, change.allow_write)
                .await
            {
                warn!(
                    "SetVfs: new password for repo point '{}' (user '{}') is active, but removing \
                     the old key failed (the old password still works): {e}",
                    change.point, change.user
                );
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

        if args.length > MAX_READ {
            return Err(Status::invalid_argument(format!(
                "length exceeds {MAX_READ} bytes; read in chunks"
            )));
        }

        let file = require_path(&args.path)?;
        let handle = self.get_read_handle(file).await?;

        if handle.expired.load(Ordering::Acquire) {
            return Err(Status::not_found("read handle expired"));
        }

        if let Ok(mut t) = handle.last_used.lock() {
            *t = Instant::now();
        }

        // length == 0 preserves the existing public semantics: read the whole
        // file, provided it fits inside the RPC limit.
        let (read_offset, length) = if args.length == 0 {
            if handle.len > MAX_READ {
                return Err(Status::invalid_argument(format!(
                    "file is {} bytes; use offset/length to read in chunks of <= {MAX_READ}",
                    handle.len
                )));
            }
            // Preserve the existing RPC contract: length == 0 means the
            // complete file, and the legacy implementation ignored offset in
            // that mode.
            (0, handle.len)
        } else {
            (args.offset, args.length)
        };

        let buf = Self::read_from_handle(&handle, read_offset, length).await?;

        if let Ok(mut t) = handle.last_used.lock() {
            *t = Instant::now();
        }

        Ok(Response::new(ReadVfsResponse {
            data: buf.to_bytes().to_vec(),
        }))
    }

    // ── Write operations (writable local points only) ─────────────────────

    async fn vfs_touch_file(&self, request: Request<FilePath>) -> Result<Response<Empty>, Status> {
        let args = request.into_inner();
        self.require_user_write(&args).await?;
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

        let (point, rest) = utils::resolve_point_path(&user, &path_str)?;
        utils::require_user_writable(point)?;

        // A writer invalidates any cached reader for the same path so a
        // subsequent read cannot observe a stale length/object version.
        self.invalidate_read_handle(&user.username, &path_str);

        let handle_id = Uuid::new_v4();

        // Only held while opening; NOT for the life of the handle, or a
        // second open / set_length / transfer on this point would deadlock.
        let lock = self
            .inner
            .local_point_locks
            .entry(point.id)
            .or_insert_with(|| Arc::new(TokioMutex::new(())))
            .clone();
        let _guard = lock.lock_owned().await;

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
        let initial_len = if args.overwrite { 0 } else { existing_len };

        // Only walk the tree when a quota actually exists.
        let quota_base_bytes = if let Some(max) = point.max_bytes {
            let root_for_scan = root.clone();
            let total_used = tokio::task::spawn_blocking(move || utils::dir_size(&root_for_scan))
                .await
                .map_err(|e| Status::internal(format!("quota check panicked: {e}")))?
                .map_err(|e| Status::internal(format!("failed to measure quota usage: {e}")))?;
            let base = total_used.saturating_sub(existing_len);
            if base
                .checked_add(initial_len)
                .ok_or_else(|| Status::resource_exhausted("quota size overflow"))?
                > max
            {
                return Err(Status::resource_exhausted(format!(
                    "point '{}' is already over its {max}-byte quota",
                    point.name
                )));
            }
            base
        } else {
            0
        };

        let f = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&full_path)
            .await
            .map_err(io_status)?;

        if args.overwrite {
            f.set_len(0).await.map_err(io_status)?;
        }

        let handle = Arc::new(ActiveWriteHandle {
            last_used: StdMutex::new(Instant::now()),
            state: TokioMutex::new(WriteState {
                file: f,
                len: initial_len,
            }),
            max_bytes: point.max_bytes,
            quota_base_bytes,
            point_id: point.id,
            username: user.username.clone(),
            path: path_str.clone(),
            append: args.append,
            expired: AtomicBool::new(false),
        });

        self.inner.write_handles.insert(handle_id, handle);

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

        // Any write invalidates the cached read handle for this path. The next
        // read will reopen the local file/OpenDAL reader and observe new size
        // or object contents.
        self.invalidate_read_handle(&handle.username, &handle.path);

        if let Ok(mut t) = handle.last_used.lock() {
            *t = Instant::now();
        }

        let mut state = handle.state.lock().await;
        if handle.expired.load(Ordering::Acquire) {
            return Err(Status::not_found("write handle closed"));
        }
        let WriteState { file, len } = &mut *state;

        // In append mode the caller's offset is ignored entirely — always
        // write at the current end of the file.
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
        // tokio's File buffers; flush so stat/read on other fds see the data.
        file.flush().await.map_err(io_status)?;

        *len = new_len;

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

        handle.expired.store(true, Ordering::Release);
        let mut state = handle.state.lock().await;
        finalize_write_state(&mut state).await?;

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
        self.require_user_write(&args).await?;
        let (op, path) = self.get_operator(&args, true).await?;
        info!("Attempting to create {}", &path);
        op.create_dir(&path).await.map_err(map_dal)?;
        Ok(Response::new(Empty {}))
    }
    async fn vfs_remove_file(&self, request: Request<FilePath>) -> Result<Response<Empty>, Status> {
        let args = request.into_inner();
        self.require_user_write(&args).await?;
        let (op, path) = self.get_operator(&args, false).await?;
        info!("Attempting to remove {}", &path);
        remove_any(&op, &path).await?;
        Ok(Response::new(Empty {}))
    }

    async fn vfs_remove_dir(&self, request: Request<FilePath>) -> Result<Response<Empty>, Status> {
        let args = request.into_inner();
        self.require_user_write(&args).await?;
        let (op, path) = self.get_operator(&args, false).await?;
        info!("Attempting to remove {}", &path);
        remove_any(&op, &path).await?;
        Ok(Response::new(Empty {}))
    }

    async fn vfs_set_length(
        &self,
        request: Request<SetLengthArgs>,
    ) -> Result<Response<Empty>, Status> {
        let args = request.into_inner();

        // Preferred path: resize an open write handle. Never contends on the
        // point lock (the handle already owns its file).
        if !args.handle_id.is_empty() {
            let handle_id = parse_handle_id(&args.handle_id)?;
            let handle = self
                .inner
                .write_handles
                .get(&handle_id)
                .map(|h| Arc::clone(&*h))
                .ok_or_else(|| Status::not_found("unknown or already-closed write handle"))?;

            let mut state = handle.state.lock().await;
            if handle.expired.load(Ordering::Acquire) {
                return Err(Status::not_found("write handle closed"));
            }
            self.invalidate_read_handle(&handle.username, &handle.path);
            if let Some(max) = handle.max_bytes {
                if handle.quota_base_bytes.saturating_add(args.length) > max {
                    return Err(Status::resource_exhausted(
                        "set_length would exceed point quota",
                    ));
                }
            }
            state.file.set_len(args.length).await.map_err(io_status)?;
            state.len = args.length;
            if let Ok(mut t) = handle.last_used.lock() {
                *t = Instant::now();
            }
            return Ok(Response::new(Empty {}));
        }

        // Fallback: path-based resize (writable local points only).
        let file = require_path(&args.path)?;
        let user = self.get_user(&file.user).await?;
        let vfs_path = resolve_file_path(&user, file)?;

        let (point, rest) = utils::resolve_point_path(&user, &vfs_path)?;
        utils::require_user_writable(point)?;
        self.invalidate_read_handle(&user.username, &vfs_path);

        let lock = self
            .inner
            .local_point_locks
            .entry(point.id)
            .or_insert_with(|| Arc::new(TokioMutex::new(())))
            .clone();

        // Serialize with Vfs_OpenWrite and other local operations on this point.
        let _point_guard = lock.lock_owned().await;

        let root = PathBuf::from(utils::local_source_path(point)?);
        let full_path = utils::safe_join(&root, &rest.to_string_lossy())?;
        let metadata = tokio::fs::metadata(&full_path).await.map_err(io_status)?;

        if !metadata.is_file() {
            return Err(Status::failed_precondition(
                "cannot set the length of a directory",
            ));
        }

        let old_len = metadata.len();
        let new_len = args.length;

        // Only growing the file consumes additional quota. Shrinking it frees
        // space, so it never needs a quota reservation.
        if new_len > old_len {
            let growth = new_len - old_len;
            check_local_quota(&root, point.max_bytes, growth).await?;
        }

        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .open(&full_path)
            .await
            .map_err(io_status)?;

        file.set_len(new_len).await.map_err(io_status)?;

        Ok(Response::new(Empty {}))
    }

    async fn vfs_stat(&self, request: Request<FilePath>) -> Result<Response<StatResponse>, Status> {
        let args = request.into_inner();

        // We don't know up front whether the path is a file or a directory,
        // and `fix_path` normalizes each differently (trailing slash for
        // dirs). Try the file form first since that's the common case, and
        // fall back to the directory form on NotFound before giving up.
        let (op, file_path) = self.get_operator(&args, false).await?;

        error!("CALLING R-STAT FOR VFS");
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
                ctime: mtime.clone(),
                mtime: mtime.clone(),
                atime: mtime,
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

    /// Transfers (copies or moves) a file or directory between two VFS paths
    /// as a background job. Progress is readable through `GetJob`.
    ///
    /// Same-mount transfers use native backend operations where possible
    /// (native `copy`, native `rename` with a copy+delete fallback).
    /// Everything else is streamed through userspace in chunks so progress
    /// and cancellation remain meaningful.
    ///
    /// A move never deletes the source until the destination has been fully
    /// transferred successfully.
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

        let copy = args.copy;
        let username = old_file.user.clone();

        let src_user = self.get_user(&old_file.user).await?;
        let dst_user = self.get_user(&new_file.user).await?;

        let src_path_for_point = resolve_file_path(&src_user, old_file)?;
        let dst_path_for_point = resolve_file_path(&dst_user, new_file)?;

        // The source may be any data point (local or remote) or a repo point
        // (copying a file out of a repository snapshot is fine); the
        // destination must be a user-writable (local, writable) data point.
        let (src_point, _) = utils::resolve_point_path(&src_user, &src_path_for_point)?;
        let (dst_point, dst_rest) = utils::resolve_point_path(&dst_user, &dst_path_for_point)?;

        utils::require_user_writable(dst_point)?;
        if !args.copy {
            // A move deletes the source, so it needs a user-writable source.
            if src_point.is_repo {
                return Err(Status::permission_denied(
                    "cannot move out of a repository; use copy instead",
                ));
            }
            utils::require_user_writable(src_point).map_err(|e| {
                Status::permission_denied(format!(
                    "cannot move out of this point ({}); use copy instead",
                    e.message()
                ))
            })?;
        }

        let (src_op, src_path) = self.get_operator(old_file, false).await?;
        let (dst_op, dst_path) = self.get_operator(new_file, false).await?;

        // Inspect the source (file or directory).
        let (src_meta, src_path) = stat_any(&src_op, &src_path).await?;
        let src_is_dir = src_meta.is_dir();
        let dst_path = if src_is_dir {
            fix_path(&dst_path, true)
        } else {
            dst_path
        };

        // The destination must not already exist. This keeps cross-backend
        // transfers safe and gives a simple cleanup guarantee: if the job
        // creates the destination, it owns it and may remove it on failure.
        match stat_any(&dst_op, &dst_path).await {
            Ok((meta, _)) => {
                if src_is_dir && !meta.is_dir() {
                    return Err(Status::failed_precondition(
                        "destination exists and is not a directory",
                    ));
                }
                if !src_is_dir && meta.is_dir() {
                    return Err(Status::failed_precondition(
                        "destination exists and is a directory",
                    ));
                }
                return Err(Status::already_exists("destination already exists"));
            }
            Err(e) if e.code() == tonic::Code::NotFound => {}
            Err(e) => return Err(e),
        }

        // OpenDAL Operator does not implement PartialEq, so compare the
        // properties that identify the underlying operator.
        let src_info = src_op.info();
        let dst_info = dst_op.info();

        let same_backend = src_info.scheme() == dst_info.scheme()
            && src_info.root() == dst_info.root()
            && src_info.name() == dst_info.name();

        let same_mount = src_point.id == dst_point.id;

        if same_mount && src_path == dst_path {
            return Err(Status::invalid_argument(
                "source and destination are identical",
            ));
        }

        if src_is_dir && same_mount && dst_path.starts_with(&src_path) {
            return Err(Status::invalid_argument(
                "cannot transfer a directory into itself",
            ));
        }

        // Enumerate the source tree if this is a directory.
        let entries = if src_is_dir {
            src_op
                .list_with(&src_path)
                .recursive(true)
                .await
                .map_err(map_dal)?
        } else {
            Vec::new()
        };

        // Total bytes that will actually be copied. Directories themselves
        // have no useful byte progress, so progress is the sum of file sizes.
        let total_bytes = if src_is_dir {
            entries
                .iter()
                .map(|e| e.metadata())
                .filter(|m| !m.is_dir())
                .fold(0u64, |acc, m| acc.saturating_add(m.content_length()))
        } else {
            src_meta.content_length()
        };

        // Local destination quota information (re-checked inside the job).
        // A move within the same point doesn't change the point's total size,
        // so skip the check there. Copies, and moves between points, add bytes.
        let needs_quota_check = copy || !same_mount;

        // The destination is guaranteed local by `require_user_writable`.
        let root = PathBuf::from(utils::local_source_path(dst_point)?);
        let full_path = utils::safe_join(&root, &dst_rest.to_string_lossy())?;
        let point_lock = self
            .inner
            .local_point_locks
            .entry(dst_point.id)
            .or_insert_with(|| Arc::new(TokioMutex::new(())))
            .clone();
        let local_quota = needs_quota_check.then_some((root, full_path, dst_point.max_bytes));

        let plan = TransferPlan {
            src_op,
            dst_op,
            src_path,
            dst_path,
            src_is_dir,
            entries,
            total_bytes,
            copy,
            native: same_backend && same_mount,
            local_quota,
        };

        let job_id = self.spawn_async_job(username, move |job_id, tx, token, progress| {
            run_transfer(plan, point_lock, job_id, tx, token, progress)
        });

        Ok(Response::new(JobStartResponse { job_id }))
    }
}
