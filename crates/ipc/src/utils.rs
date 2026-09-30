use crate::core::{VfsError, VfsPoint, VfsUser};
use crate::store::RepoSource;
use opendal_vfs::{Error, ErrorKind};
use prost_types::Timestamp;
use rustic_backend::opendal::OpenDALConfig;
use rustic_core::jiff::Zoned;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tonic::Status;
use uuid::Uuid;
use crate::ipc::Priority;

pub fn map_vfs(e: VfsError) -> Status {
    match e {
        VfsError::UserNotFound => Status::not_found("vfs user not found"),
        VfsError::RepoPasswordMissing => Status::failed_precondition(e.to_string()),
        VfsError::PointFailed { .. } => Status::failed_precondition(e.to_string()),
        VfsError::OpenDal(e) => map_dal(e),
        _ => Status::internal(e.to_string()),
    }
}

pub fn proto_stamp(ts: rustic_core::jiff::Timestamp) -> Option<Timestamp> {
    Some(Timestamp {
        seconds: ts.as_second(),
        nanos: ts.subsec_nanosecond(),
    })
}


pub fn map_dal(e: Error) -> Status {
    let message = e.to_string();
    match e.kind() {
        ErrorKind::NotFound => Status::not_found(message),
        ErrorKind::PermissionDenied => Status::permission_denied(message),
        ErrorKind::RateLimited => Status::resource_exhausted(message),
        ErrorKind::ConfigInvalid => Status::invalid_argument(message),
        ErrorKind::Unsupported => Status::unimplemented(message),
        ErrorKind::AlreadyExists => Status::already_exists(message),
        _ => Status::internal(message),
    }
}

/// Validates the username constraints that are required by both VFS paths and
/// quota keys. Rejecting separators here prevents ambiguous identities later.
pub fn validate_username(username: &str) -> Result<(), Status> {
    if username.is_empty() || username == "." || username == ".." {
        return Err(Status::invalid_argument("username must not be empty or dot"));
    }
    if username
        .chars()
        .any(|c| c == '/' || c == '\\' || c.is_control())
    {
        return Err(Status::invalid_argument("username contains an illegal character"));
    }
    Ok(())
}

pub fn validate_repo_password(password: &str) -> Result<(), Status> {
    if password.is_empty() {
        return Err(Status::invalid_argument("repository password must not be empty"));
    }
    if password.chars().any(|c| c.is_control()) {
        return Err(Status::invalid_argument("repository password contains a control character"));
    }
    Ok(())
}

/// Joins `rest` onto `root`, rejecting `..`, absolute and prefix components so
/// a client can never escape a local point's root directory.
pub fn safe_join(root: &Path, rest: &str) -> Result<PathBuf, Status> {
    use std::path::Component;

    let mut out = root.to_path_buf();
    for c in Path::new(rest).components() {
        match c {
            Component::Normal(p) => {
                out.push(p);
                // Lexical `..` protection is not enough when a client can
                // address a symlink inside the point. Reject symlink
                // components so a configured local point cannot be escaped
                // through the filesystem namespace. Missing final components
                // are fine; they are created by the caller when appropriate.
                match std::fs::symlink_metadata(&out) {
                    Ok(meta) if meta.file_type().is_symlink() => {
                        return Err(Status::permission_denied(format!(
                            "path crosses a symlink: '{}'",
                            out.display()
                        )));
                    }
                    Ok(_) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(Status::internal(format!(
                        "failed to inspect local path '{}': {e}", out.display()
                    ))),
                }
            }
            Component::CurDir | Component::RootDir => {}
            _ => return Err(Status::invalid_argument(format!("illegal path '{rest}'"))),
        }
    }
    Ok(out)
}

pub fn validate_relative_point_path(rest: &str) -> Result<String, Status> {
    use std::path::Component;

    let normalized_separators = rest.replace('\\', "/");
    let mut out = PathBuf::new();
    for component in Path::new(&normalized_separators).components() {
        match component {
            Component::Normal(value) => out.push(value),
            Component::CurDir | Component::RootDir => {}
            Component::ParentDir | Component::Prefix(_) => {
                return Err(Status::invalid_argument(format!(
                    "illegal point-relative path '{rest}'"
                )));
            }
        }
    }

    Ok(out.to_string_lossy().replace('\\', "/"))
}

pub fn fix_level(item: log::Level) -> i32 {
    let ret = match item {
        log::Level::Error => Priority::Error,
        log::Level::Warn => Priority::Warning,
        log::Level::Info => Priority::Info,
        log::Level::Debug => Priority::Debug,
        log::Level::Trace => Priority::Debug
    };
    ret.into()
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
/// reading or clearing quota usage).
///
/// Point IDs are used instead of names so renaming a point does not create a
/// new quota bucket or orphan the old usage.
pub fn quota_id(username: &str, point_id: &Uuid) -> String {
    format!("{username}-{point_id}")
}

// ── Point lookup & validation ─────────────────────────────────────────────────

fn find_point<'a>(user: &'a VfsUser, name: &str) -> Result<&'a VfsPoint, Status> {
    user.points
        .iter()
        .find(|p| p.name == name)
        .ok_or_else(|| Status::not_found(format!("point '{name}' not found")))
}

fn find_point_id<'a>(user: &'a VfsUser, id: &str) -> Result<&'a VfsPoint, Status> {
    let id = uuid::Uuid::parse_str(id)
        .map_err(|_| Status::invalid_argument(format!("malformed point id '{id}'")))?;

    user.points
        .iter()
        .find(|p| p.id == id)
        .ok_or_else(|| Status::not_found(format!("point '{id}' not found")))
}

/// Locates a repo point by its stable ID.
pub fn require_repo_point_id<'a>(
    user: &'a VfsUser,
    point_id: &str,
) -> Result<&'a VfsPoint, Status> {
    let point = find_point_id(user, point_id)?;
    if !point.is_repo {
        return Err(Status::invalid_argument(format!(
            "point '{point_id}' is a data point, not a repo"
        )));
    }
    Ok(point)
}

/// Locates a data point by its stable ID.
pub fn require_data_point_id<'a>(
    user: &'a VfsUser,
    point_id: &str,
) -> Result<&'a VfsPoint, Status> {
    let point = find_point_id(user, point_id)?;
    if point.is_repo {
        return Err(Status::invalid_argument(format!(
            "point '{point_id}' is a repo, not a data point"
        )));
    }
    Ok(point)
}

/// Locates a repo point by its user-facing name.
///
/// Name lookup remains available for VFS-visible paths such as `/repos/name`.
pub fn require_repo_point<'a>(user: &'a VfsUser, name: &str) -> Result<&'a VfsPoint, Status> {
    let point = find_point(user, name)?;
    if !point.is_repo {
        return Err(Status::invalid_argument(format!(
            "point '{name}' is a data point, not a repo"
        )));
    }
    Ok(point)
}

/// Locates a data point by its user-facing name.
///
/// Name lookup remains available for VFS-visible paths such as `/points/name`.
pub fn require_data_point<'a>(user: &'a VfsUser, name: &str) -> Result<&'a VfsPoint, Status> {
    let point = find_point(user, name)?;
    if point.is_repo {
        return Err(Status::invalid_argument(format!(
            "point '{name}' is a repo, not a data point"
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
pub fn reject_symlinks_under(root: &Path) -> std::io::Result<()> {
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            };
            let ty = match entry.file_type() {
                Ok(ty) => ty,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            };
            if ty.is_symlink() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    format!("symlink is not allowed under '{}'", root.display()),
                ));
            }
            if ty.is_dir() {
                stack.push(entry.path());
            }
        }
    }
    Ok(())
}

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
            let entry = match entry {
                Ok(entry) => entry,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            };
            let file_type = match entry.file_type() {
                Ok(file_type) => file_type,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            };
            if file_type.is_dir() {
                stack.push(entry.path());
            } else if file_type.is_file() {
                match entry.metadata() {
                    Ok(meta) => {
                        total = total.checked_add(meta.len()).ok_or_else(|| {
                            std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "directory size exceeds u64::MAX",
                            )
                        })?;
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e),
                }
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
    validate_repo_password(&password)?;

    Ok(RepoSource {
        scheme: point.scheme.clone(),
        config: point.config.clone().into_iter().collect(),
        password,
    })
}

/// Resolves a VFS-relative path (as exposed to VFS clients, e.g.
/// `/points/<name>/sub/dir`) against a loaded [`VfsUser`]'s mounted points.
///
/// Returns the matching data point plus the remaining path within it. Only
/// data points (`is_repo = false`) are supported — repo-mounted paths are
/// intentionally rejected, since they're harder to parse reliably and more
/// prone to changing shape.
/// Resolves a backup/restore `FilePath` to a data point and a path relative
/// to that point. Indexed paths use the stable point ID. Virtual paths are
/// retained for callers that already have a VFS path, but only `/points/...`
/// paths are accepted here, so repository paths can never be used as backup
/// sources or restore destinations.
pub fn resolve_data_file_path<'a>(
    user: &'a VfsUser,
    path: &crate::ipc::FilePath,
) -> Result<(&'a VfsPoint, String), Status> {
    use crate::ipc::file_path::Path as FilePathKind;

    match path.path.as_ref() {
        Some(FilePathKind::Indexed(indexed)) => {
            let point = require_data_point_id(user, &indexed.point_id)?;
            let point_path = validate_relative_point_path(&indexed.point_path)?;
            Ok((point, point_path))
        }
        Some(FilePathKind::Virtual(virtual_path)) => {
            let trimmed = virtual_path.trim_start_matches('/');
            let mut parts = trimmed.splitn(3, '/');
            let root = parts.next().unwrap_or("");
            if root != POINTS_ROOT {
                return Err(Status::invalid_argument(format!(
                    "backup/restore paths must be under /{POINTS_ROOT}/; repository paths are not allowed"
                )));
            }

            let point_name = parts.next().filter(|s| !s.is_empty()).ok_or_else(|| {
                Status::invalid_argument(format!(
                    "path '{virtual_path}' is missing a data point name"
                ))
            })?;
            let rest = parts.next().unwrap_or("");
            let point = require_data_point(user, point_name)?;
            let rest = validate_relative_point_path(rest)?;
            Ok((point, rest))
        }
        None => Err(Status::invalid_argument("path is blank")),
    }
}

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
    let rest = validate_relative_point_path(rest)?;
    Ok((point, PathBuf::from(rest)))
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
            id: Uuid::new_v4(),
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
            id: Uuid::new_v4(),
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
    fn validate_username_rejects_path_separators() {
        assert!(validate_username("alice/bob").is_err());
        assert!(validate_username("alice\\bob").is_err());
        assert!(validate_username("alice").is_ok());
    }

    #[test]
    fn mount_paths_are_namespaced() {
        assert_eq!(data_mount_path("local"), "/points/local");
        assert_eq!(repo_mount_path("backup"), "/repos/backup");
    }

    #[test]
    fn quota_id_combines_username_and_point_id() {
        let id = Uuid::parse_str("01234567-89ab-cdef-0123-456789abcdef").unwrap();
        assert_eq!(
            quota_id("alice", &id),
            "alice-01234567-89ab-cdef-0123-456789abcdef"
        );
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
    fn require_repo_point_id_accepts_repo() {
        let p = repo_point("r", false, Some("pw"));
        let id = p.id.to_string();
        let u = user(vec![p]);
        assert!(require_repo_point_id(&u, &id).is_ok());
    }

    #[test]
    fn require_repo_point_id_rejects_data_point() {
        let p = data_point("d", false);
        let id = p.id.to_string();
        let u = user(vec![p]);
        assert!(require_repo_point_id(&u, &id).is_err());
    }

    #[test]
    fn require_data_point_id_rejects_repo_point() {
        let p = repo_point("r", false, Some("pw"));
        let id = p.id.to_string();
        let u = user(vec![p]);
        assert!(require_data_point_id(&u, &id).is_err());
    }

    #[test]
    fn resolve_data_file_path_rejects_repo_virtual_path() {
        let u = user(vec![repo_point("r", false, Some("pw"))]);
        let path = crate::ipc::FilePath {
            user: "alice".into(),
            path: Some(crate::ipc::file_path::Path::Virtual(
                "/repos/r/file.txt".into(),
            )),
        };
        assert!(resolve_data_file_path(&u, &path).is_err());
    }

    #[test]
    fn resolve_data_file_path_accepts_indexed_data_point() {
        let p = data_point("d", false);
        let id = p.id.to_string();
        let u = user(vec![p]);
        let path = crate::ipc::FilePath {
            user: "alice".into(),
            path: Some(crate::ipc::file_path::Path::Indexed(
                crate::ipc::IndexedPath {
                    point_id: id,
                    point_path: "sub/file.txt".into(),
                },
            )),
        };
        let (point, rest) = resolve_data_file_path(&u, &path).unwrap();
        assert_eq!(point.name, "d");
        assert_eq!(rest, "sub/file.txt");
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
