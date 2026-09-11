use crate::core::{VfsError, VfsPoint, VfsUser};
use crate::store::RepoSource;
use opendal_vfs::{Error, ErrorKind};
use prost_types::Timestamp;
use rustic_backend::opendal::OpenDALConfig;
use rustic_core::jiff::Zoned;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tonic::Status;

pub fn map_vfs(e: VfsError) -> Status {
    match e {
        VfsError::UserNotFound => Status::not_found("vfs user not found"),
        _ => Status::internal(e.to_string()),
    }
}

pub fn map_dal(e: Error) -> Status {
    match e.kind() {
        ErrorKind::NotFound => Status::not_found(e.to_string()),
        ErrorKind::PermissionDenied => Status::permission_denied(e.to_string()),
        _ => Status::internal(e.to_string()),
    }
}

// ── Timestamp helpers ─────────────────────────────────────────────────────────

pub fn to_ts(dt: Zoned) -> Timestamp {
    Timestamp {
        seconds: dt.timestamp().as_second(),
        nanos: dt.timestamp().subsec_nanosecond(),
    }
}

pub fn opt_ts(dt: Option<Zoned>) -> Option<Timestamp> {
    dt.map(to_ts)
}

// ── Domain conversions ────────────────────────────────────────────────────────

/// Normalizes `p` into an OpenDAL-style absolute path (leading `/`, and a
/// trailing `/` iff `is_dir`).
pub fn fix_path(p: impl AsRef<Path>, is_dir: bool) -> String {
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

pub fn parse_repo_src(p: crate::ipc::RepoSource) -> Result<RepoSource, Status> {
    let src = p.src.ok_or(Status::invalid_argument("missing repo src"))?;
    Ok(RepoSource {
        scheme: src.scheme,
        config: src.config.into_iter().collect(),
        password: p.password,
    })
}

pub fn require_repo_src(
    opt: Option<crate::ipc::RepoSource>,
    field: &'static str,
) -> Result<RepoSource, Status> {
    parse_repo_src(opt.ok_or_else(|| Status::invalid_argument(format!("missing {field}")))?)
}

// ── VFS mount layout ──────────────────────────────────────────────────────────
//
// Every user's composed [`Operator`](opendal_core::Operator) exposes two
// namespaces: `/points/<name>/**` for raw data-layer mounts and
// `/repos/<name>/**` for rustic-backed VFS mounts. These constants and
// helpers are the single source of truth for that layout so the prefix
// isn't hand-rolled at each call site.

pub const POINTS_ROOT: &str = "points";
pub const REPOS_ROOT: &str = "repos";

/// VFS-visible mount path for a data point, e.g. `/points/local`.
pub fn data_mount_path(point_name: &str) -> String {
    format!("/{POINTS_ROOT}/{point_name}")
}

/// VFS-visible mount path for a repo point, e.g. `/repos/backup`.
pub fn repo_mount_path(point_name: &str) -> String {
    format!("/{REPOS_ROOT}/{point_name}")
}

/// The quota-tracker id used for a user's mount point.
///
/// Shared by [`StorageManager`](crate::store::StorageManager) (when applying
/// the quota layer) and the `GetVfs`/`SetVfs` handlers in `server.rs` (when
/// reading or clearing quota usage), so the id format only lives here.
pub fn quota_id(username: &str, point_name: &str) -> String {
    format!("{username}-{point_name}")
}

// ── Point lookup & validation ─────────────────────────────────────────────────

fn find_point<'a>(user: &'a VfsUser, name: &str) -> Result<&'a VfsPoint, Status> {
    user.points
        .iter()
        .find(|p| p.name == name)
        .ok_or_else(|| Status::invalid_argument(format!("point '{name}' not found")))
}

/// Locates `repo_name` among `user`'s mounts and confirms it's a repo point.
pub fn require_repo_point<'a>(user: &'a VfsUser, repo_name: &str) -> Result<&'a VfsPoint, Status> {
    let point = find_point(user, repo_name)?;
    if !point.is_repo {
        return Err(Status::invalid_argument(format!(
            "point '{repo_name}' is a data point, not a repo"
        )));
    }
    Ok(point)
}

/// Locates `point_name` among `user`'s mounts and confirms it's a data point.
pub fn require_data_point<'a>(user: &'a VfsUser, point_name: &str) -> Result<&'a VfsPoint, Status> {
    let point = find_point(user, point_name)?;
    if point.is_repo {
        return Err(Status::invalid_argument(format!(
            "point '{point_name}' is a repo, not a data point"
        )));
    }
    Ok(point)
}

/// Rejects `point` if it's marked read-only.
///
/// Used to reject write-bound jobs (backup into a repo, restore into a data
/// point, forget on a repo) up front, so the caller gets an immediate
/// `PermissionDenied` instead of a job that's accepted and then fails once
/// polled.
pub fn require_writable(point: &VfsPoint) -> Result<(), Status> {
    if point.read_only {
        Err(Status::permission_denied(format!(
            "point '{}' is read-only",
            point.name
        )))
    } else {
        Ok(())
    }
}

/// Whether `scheme` should be backed up via [`LocalSource`](rustic_backend::local::LocalSource)
/// instead of an OpenDAL operator.
pub fn is_local_scheme(scheme: &str) -> bool {
    matches!(scheme.to_ascii_lowercase().as_str(), "local" | "fs")
}

/// Recursively sums the size, in bytes, of every regular file under `root`.
///
/// This is how quota usage is measured for local/fs-backed data points,
/// instead of the incremental write-counter [`QuotaTracker`](opendal_vfs::layers::quota::QuotaTracker)
/// uses for every other backend. A counter drifts for local points because
/// files can be overwritten in place or truncated — in particular by the
/// `Vfs_OpenWrite`/`Vfs_WriteAt` random-write handles, which write straight
/// to disk and never pass through an `Operator` a counting layer could
/// observe. Walking the real directory tree is slower but always correct.
///
/// Synchronous and blocking — callers on an async runtime should run this
/// inside `tokio::task::spawn_blocking`.
///
/// Symlinks are skipped (neither followed nor counted) to avoid cycles and
/// double-counting; a missing `root` is treated as zero bytes rather than
/// an error, since a point's directory may not have been created yet.
pub fn dir_size(root: &Path) -> std::io::Result<u64> {
    let mut total = 0u64;
    let mut stack = vec![root.to_path_buf()];

    while let Some(dir) = stack.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };

        for entry in entries {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                stack.push(entry.path());
            } else if file_type.is_file() {
                total += entry.metadata()?.len();
            }
        }
    }

    Ok(total)
}

/// Filesystem path for a local-scheme data point, read from its `root` or
/// `path` config option.
pub fn local_source_path(point: &VfsPoint) -> Result<String, Status> {
    point
        .config
        .get("root")
        .or_else(|| point.config.get("path"))
        .cloned()
        .ok_or_else(|| {
            Status::invalid_argument(format!(
                "point '{}' uses scheme '{}' but has no 'root' or 'path' option",
                point.name, point.scheme
            ))
        })
}

/// Builds the [`RepoSource`] needed to open a repo point's rustic backend.
pub fn repo_source(point: &VfsPoint) -> Result<RepoSource, Status> {
    let password = point.repo_password.clone().ok_or_else(|| {
        Status::invalid_argument(format!("repo point '{}' is missing a password", point.name))
    })?;

    Ok(RepoSource {
        scheme: point.scheme.clone(),
        config: point.config.clone().into_iter().collect(),
        password,
    })
}

/// Builds the [`OpenDALConfig`] for a data point's raw backend.
fn point_config(point: &VfsPoint) -> OpenDALConfig {
    OpenDALConfig::default()
        .scheme(point.scheme.clone())
        .options(point.config.clone().into_iter().collect::<HashMap<_, _>>())
}

/// Resolves a `repo_name` against a loaded [`VfsUser`]'s mounted points,
/// producing the [`RepoSource`] needed to open the repository.
///
/// Only points mounted with `is_repo = true` qualify; the point must also
/// carry a `repo_password`, since that's required to open/decrypt it.
pub fn resolve_repo_point(user: &VfsUser, repo_name: &str) -> Result<RepoSource, Status> {
    repo_source(require_repo_point(user, repo_name)?)
}

/// Resolves a data point by name against a loaded [`VfsUser`]'s mounted
/// points, producing the raw [`OpenDALConfig`] for its backend.
///
/// Only data points (`is_repo = false`) are supported for backup/restore —
/// repo-mounted paths are intentionally rejected.
///
/// Prefer [`require_data_point`] plus
/// [`StorageSystem::get_data_operator`](crate::store::StorageSystem::get_data_operator)
/// for new call sites — that path applies the same read-only/quota layering
/// as the rest of the VFS, whereas this raw config bypasses both.
pub fn resolve_data_point(
    user: &VfsUser,
    point_name: &str,
    _point_path: &str,
) -> Result<OpenDALConfig, Status> {
    Ok(point_config(require_data_point(user, point_name)?))
}

/// Resolves a VFS-relative path (as exposed to VFS clients, e.g.
/// `/points/<name>/sub/dir`) against a loaded [`VfsUser`]'s mounted points.
///
/// Returns the matching data point plus the remaining path within it. Only
/// data points (`is_repo = false`) are supported — repo-mounted paths are
/// intentionally rejected, since they're harder to parse reliably and more
/// prone to changing shape.
pub fn resolve_data_path<'a>(
    user: &'a VfsUser,
    vfs_path: &str,
) -> Result<(&'a VfsPoint, PathBuf), Status> {
    let trimmed = vfs_path.trim_start_matches('/');
    let mut parts = trimmed.splitn(3, '/');

    let root = parts.next().unwrap_or("");
    if root != POINTS_ROOT {
        return Err(Status::invalid_argument(format!(
            "path '{vfs_path}' must be under /{POINTS_ROOT}/<name>/... (repo-mounted paths aren't supported for backup/restore)"
        )));
    }

    let point_name = parts.next().filter(|s| !s.is_empty()).ok_or_else(|| {
        Status::invalid_argument(format!("path '{vfs_path}' is missing a point name"))
    })?;

    let rest = parts.next().unwrap_or("");
    let point = require_data_point(user, point_name)?;
    Ok((point, PathBuf::from(rest)))
}

pub fn vfs_permission_denied(path: &str) -> Error {
    Error::new(ErrorKind::PermissionDenied, "mount is read-only").with_context("path", path)
}

pub fn vfs_unsupported(op: &'static str) -> Error {
    Error::new(
        ErrorKind::Unsupported,
        format!("MountFs does not support `{op}`."),
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn data_point(name: &str, read_only: bool) -> VfsPoint {
        VfsPoint {
            name: name.to_string(),
            max_bytes: None,
            read_only,
            scheme: "s3".into(),
            config: BTreeMap::new(),
            is_repo: false,
            repo_password: None,
        }
    }

    fn repo_point(name: &str, read_only: bool, password: Option<&str>) -> VfsPoint {
        VfsPoint {
            name: name.to_string(),
            max_bytes: None,
            read_only,
            scheme: "s3".into(),
            config: BTreeMap::new(),
            is_repo: true,
            repo_password: password.map(str::to_string),
        }
    }

    fn user(points: Vec<VfsPoint>) -> VfsUser {
        VfsUser {
            username: "alice".into(),
            password: "pw".into(),
            points,
        }
    }

    #[test]
    fn mount_paths_are_namespaced() {
        assert_eq!(data_mount_path("local"), "/points/local");
        assert_eq!(repo_mount_path("backup"), "/repos/backup");
    }

    #[test]
    fn quota_id_combines_username_and_point() {
        assert_eq!(quota_id("alice", "local"), "alice-local");
    }

    #[test]
    fn require_repo_point_rejects_data_point() {
        let u = user(vec![data_point("d", false)]);
        assert!(require_repo_point(&u, "d").is_err());
    }

    #[test]
    fn require_repo_point_rejects_missing() {
        let u = user(vec![]);
        assert!(require_repo_point(&u, "ghost").is_err());
    }

    #[test]
    fn require_repo_point_accepts_repo() {
        let u = user(vec![repo_point("r", false, Some("pw"))]);
        assert!(require_repo_point(&u, "r").is_ok());
    }

    #[test]
    fn require_data_point_rejects_repo_point() {
        let u = user(vec![repo_point("r", false, Some("pw"))]);
        assert!(require_data_point(&u, "r").is_err());
    }

    #[test]
    fn require_writable_rejects_read_only() {
        let p = data_point("d", true);
        assert!(require_writable(&p).is_err());
    }

    #[test]
    fn require_writable_accepts_writable() {
        let p = data_point("d", false);
        assert!(require_writable(&p).is_ok());
    }

    #[test]
    fn repo_source_requires_password() {
        let p = repo_point("r", false, None);
        assert!(repo_source(&p).is_err());
    }

    #[test]
    fn repo_source_builds_from_point() {
        let p = repo_point("r", false, Some("secret"));
        let src = repo_source(&p).unwrap();
        assert_eq!(src.password, "secret");
        assert_eq!(src.scheme, "s3");
    }

    #[test]
    fn resolve_repo_point_matches_require_plus_source() {
        let u = user(vec![repo_point("r", false, Some("secret"))]);
        let src = resolve_repo_point(&u, "r").unwrap();
        assert_eq!(src.password, "secret");
    }

    #[test]
    fn resolve_data_point_returns_config_for_data_point() {
        let u = user(vec![data_point("d", false)]);
        let cfg = resolve_data_point(&u, "d", "unused");
        assert!(cfg.is_ok());
    }

    #[test]
    fn resolve_data_point_rejects_repo() {
        let u = user(vec![repo_point("r", false, Some("pw"))]);
        assert!(resolve_data_point(&u, "r", "unused").is_err());
    }

    #[test]
    fn resolve_data_path_requires_points_root() {
        let u = user(vec![data_point("local", false)]);
        assert!(resolve_data_path(&u, "/repos/local/x").is_err());
    }

    #[test]
    fn resolve_data_path_splits_point_and_rest() {
        let u = user(vec![data_point("local", false)]);
        let (point, rest) = resolve_data_path(&u, "/points/local/sub/dir").unwrap();
        assert_eq!(point.name, "local");
        assert_eq!(rest, PathBuf::from("sub/dir"));
    }

    #[test]
    fn resolve_data_path_rejects_unknown_point() {
        let u = user(vec![data_point("local", false)]);
        assert!(resolve_data_path(&u, "/points/ghost/x").is_err());
    }

    #[test]
    fn is_local_scheme_matches_local_and_fs() {
        assert!(is_local_scheme("local"));
        assert!(is_local_scheme("FS"));
        assert!(!is_local_scheme("s3"));
    }

    #[test]
    fn local_source_path_prefers_root_then_path() {
        let mut p = data_point("d", false);
        p.config.insert("path".into(), "/data".into());
        assert_eq!(local_source_path(&p).unwrap(), "/data");
        p.config.insert("root".into(), "/root-data".into());
        assert_eq!(local_source_path(&p).unwrap(), "/root-data");
    }

    #[test]
    fn local_source_path_requires_root_or_path() {
        let p = data_point("d", false);
        assert!(local_source_path(&p).is_err());
    }

    #[test]
    fn fix_path_normalizes_dirs_and_files() {
        assert_eq!(fix_path("a/b", true), "/a/b/");
        assert_eq!(fix_path("a/b/", false), "/a/b");
        assert_eq!(fix_path("a\\b", true), "/a/b/");
    }
}
