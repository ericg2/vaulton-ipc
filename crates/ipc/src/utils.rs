use crate::core::{VfsError, VfsUser};
use crate::store::RepoSource;
use opendal_vfs::{Error, ErrorKind};
use prost_types::Timestamp;
use rustic_backend::opendal::OpenDALConfig;
use rustic_core::jiff::Zoned;
use std::collections::{BTreeMap, HashMap};
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

/// Converts a [`Path`] into an OpenDAL-supported [`String`].
///
/// # Arguments
/// * `base` - The root [`Path`] to use.
/// * `p` - The [`Path`] to convert from.
/// * `is_dir` - If representing a directory or file.
///
/// # Returns
/// A valid [`String`] for OpenDAL use.
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

/// Resolves a `repo_name` against a loaded [`VfsUser`]'s mounted points,
/// producing the [`RepoSource`] needed to open the repository.
///
/// Only points mounted with `is_repo = true` qualify; the point must also
/// carry a `repo_password`, since that's required to open/decrypt it.
pub fn resolve_repo_point(user: &VfsUser, repo_name: &str) -> Result<RepoSource, Status> {
    let point = user
        .points
        .iter()
        .find(|p| p.name == repo_name)
        .ok_or_else(|| Status::invalid_argument(format!("repo point '{repo_name}' not found")))?;

    if !point.is_repo {
        return Err(Status::invalid_argument(format!(
            "point '{repo_name}' is a data point, not a repo"
        )));
    }

    let password = point.repo_password.clone().ok_or_else(|| {
        Status::invalid_argument(format!("repo point '{repo_name}' is missing a password"))
    })?;

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
pub fn resolve_data_path(
    user: &VfsUser,
    vfs_path: &str,
) -> Result<(OpenDALConfig, PathBuf), Status> {
    let trimmed = vfs_path.trim_start_matches('/');
    let mut parts = trimmed.splitn(3, '/');

    let root = parts.next().unwrap_or("");
    if root != "points" {
        return Err(Status::invalid_argument(format!(
            "path '{vfs_path}' must be under /points/<name>/... (repo-mounted paths aren't supported for backup/restore)"
        )));
    }

    let point_name = parts
        .next()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Status::invalid_argument(format!("path '{vfs_path}' is missing a point name")))?;
    let rest = parts.next().unwrap_or("");

    let point = user
        .points
        .iter()
        .find(|p| p.name == point_name)
        .ok_or_else(|| Status::invalid_argument(format!("data point '{point_name}' not found")))?;

    if point.is_repo {
        return Err(Status::invalid_argument(format!(
            "point '{point_name}' is a repo, not a data point"
        )));
    }

    let config = OpenDALConfig::default()
        .scheme(point.scheme.clone())
        .options(point.config.clone().into_iter().collect::<HashMap<_, _>>());

    Ok((config, PathBuf::from(rest)))
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
