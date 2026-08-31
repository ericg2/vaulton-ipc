use crate::vfs::sys::Mount;
use std::collections::BTreeMap;
use opendal_core::{Error, ErrorKind};

pub fn not_found(path: &str) -> Error {
    Error::new(ErrorKind::NotFound, "no mount covers this path").with_context("path", path)
}

pub fn permission_denied(path: &str) -> Error {
    Error::new(ErrorKind::PermissionDenied, "mount is read-only").with_context("path", path)
}

pub fn unsupported(op: &'static str) -> Error {
    Error::new(
        ErrorKind::Unsupported,
        format!("MountFs does not support `{op}`."),
    )
}

pub fn normalize(path: &str) -> String {
    let trimmed = path.trim_matches('/');
    if trimmed.is_empty() {
        "/".to_string()
    } else {
        format!("/{trimmed}")
    }
}

/// Find the mount (if any) that owns `path`, and the path made relative to
/// that mount's root. "Owns" means `path` equals the mount path or is nested
/// under it. If multiple configured mounts could match, the longest
/// (most specific) one wins.
pub fn resolve<'a>(
    mounts: &'a BTreeMap<String, Mount>,
    path: &str,
) -> Option<(&'a str, &'a Mount, String)> {
    let normalized = normalize(path);
    mounts
        .iter()
        .filter(|(mount_path, _)| {
            normalized == mount_path.as_str() || normalized.starts_with(&format!("{mount_path}/"))
        })
        .max_by_key(|(mount_path, _)| mount_path.len())
        .map(|(mount_path, mount)| {
            let rel = normalized
                .strip_prefix(mount_path.as_str())
                .unwrap()
                .trim_start_matches('/');

            let mut rel = rel.to_string();
            if path.ends_with('/') && !rel.is_empty() && !rel.ends_with('/') {
                rel.push('/');
            }

            (mount_path.as_str(), mount, rel)
        })
}
