//! Helpers for browsing the file tree inside a snapshot.
//!
//! Kept free of repository access so the path and paging rules are unit-testable.

use crate::ipc::{SnapshotEntry, SnapshotEntryKind};
use crate::utils::proto_stamp;
use rustic_core::repofile::{Node, NodeType};

pub const DEFAULT_LIMIT: u32 = 200;
pub const MAX_LIMIT: u32 = 1000;

/// Splits a user-supplied snapshot path into clean components.
///
/// Empty segments and `.` are dropped, so `"", "/", "//", "/./"` all mean the
/// root. `..` is refused instead of resolved: a path that climbs out of a
/// directory is almost certainly a mistake, and "never interpret `..`" is a
/// rule that is easy to trust.
pub fn split_path(path: &str) -> Result<Vec<String>, String> {
    if path.contains('\0') {
        return Err("path contains a NUL byte".into());
    }
    let mut out = Vec::new();
    for seg in path.split('/') {
        match seg {
            "" | "." => {}
            ".." => return Err("'..' is not allowed in snapshot paths".into()),
            s => out.push(s.to_string()),
        }
    }
    Ok(out)
}

/// Canonical absolute form of a component list: `[]` -> `/`, `[a, b]` -> `/a/b`.
pub fn join(components: &[String]) -> String {
    if components.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", components.join("/"))
    }
}

/// Path of a child named `name` inside the directory `parent`.
pub fn child_path(parent: &str, name: &str) -> String {
    if parent == "/" {
        format!("/{name}")
    } else {
        format!("{parent}/{name}")
    }
}

/// Page size to apply: default when 0, otherwise capped.
pub fn clamp_limit(limit: u32) -> u32 {
    match limit {
        0 => DEFAULT_LIMIT,
        n => n.min(MAX_LIMIT),
    }
}

/// Display name of a node (lossy for names that are not valid UTF-8).
pub fn node_name(node: &Node) -> String {
    node.name().to_string_lossy().into_owned()
}

/// Directories first, then by name ignoring case, then by exact name.
pub fn sort_nodes(nodes: &mut [Node]) {
    nodes.sort_by_cached_key(|n| {
        let name = node_name(n);
        (!n.is_dir(), name.to_lowercase(), name)
    });
}

/// Converts a node into its API form; `path` is the node's own path.
pub fn to_entry(node: &Node, path: String) -> SnapshotEntry {
    let (kind, link_target) = match &node.node_type {
        NodeType::File => (SnapshotEntryKind::File, String::new()),
        NodeType::Dir => (SnapshotEntryKind::Directory, String::new()),
        NodeType::Symlink { .. } => (
            SnapshotEntryKind::Symlink,
            node.node_type.to_link().to_string_lossy().into_owned(),
        ),
        _ => (SnapshotEntryKind::Other, String::new()),
    };
    SnapshotEntry {
        name: node_name(node),
        path,
        kind: kind as i32,
        size: if node.is_file() { node.meta.size } else { 0 },
        mtime: node.meta.mtime.and_then(proto_stamp),
        mode: node.meta.mode,
        user: node.meta.user.clone().unwrap_or_default(),
        group: node.meta.group.clone().unwrap_or_default(),
        link_target,
    }
}

/// The synthetic entry for the snapshot root, which has no node of its own.
pub fn root_entry() -> SnapshotEntry {
    SnapshotEntry {
        name: String::new(),
        path: "/".into(),
        kind: SnapshotEntryKind::Directory as i32,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustic_core::repofile::Metadata;
    use std::ffi::OsStr;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn root_spellings_are_all_empty() {
        for p in ["", "/", "//", "/./", "."] {
            assert!(split_path(p).unwrap().is_empty(), "{p:?}");
        }
    }

    #[test]
    fn splits_and_cleans_components() {
        assert_eq!(split_path("/a//b/./c/").unwrap(), s(&["a", "b", "c"]));
        assert_eq!(split_path("a/b").unwrap(), s(&["a", "b"]));
    }

    #[test]
    fn parent_traversal_and_nul_are_rejected() {
        assert!(split_path("/a/../b").is_err());
        assert!(split_path("..").is_err());
        assert!(split_path("/a\0b").is_err());
        // A name that merely contains dots is fine.
        assert_eq!(split_path("/..a/b..").unwrap(), s(&["..a", "b.."]));
    }

    #[test]
    fn join_and_child_path() {
        assert_eq!(join(&[]), "/");
        assert_eq!(join(&s(&["a", "b"])), "/a/b");
        assert_eq!(child_path("/", "x"), "/x");
        assert_eq!(child_path("/a/b", "x"), "/a/b/x");
    }

    #[test]
    fn limit_is_defaulted_and_capped() {
        assert_eq!(clamp_limit(0), DEFAULT_LIMIT);
        assert_eq!(clamp_limit(5), 5);
        assert_eq!(clamp_limit(1_000_000), MAX_LIMIT);
    }

    #[test]
    fn directories_sort_before_files_case_insensitively() {
        let mk = |n: &str, t: NodeType| Node::new_node(OsStr::new(n), t, Metadata::default());
        let mut nodes = vec![
            mk("b.txt", NodeType::File),
            mk("Zeta", NodeType::Dir),
            mk("A.txt", NodeType::File),
            mk("alpha", NodeType::Dir),
        ];
        sort_nodes(&mut nodes);
        let names: Vec<_> = nodes.iter().map(node_name).collect();
        assert_eq!(names, ["alpha", "Zeta", "A.txt", "b.txt"]);
    }

    #[test]
    fn entry_reports_kind_and_zero_size_for_dirs() {
        let mut meta = Metadata::default();
        meta.size = 42;
        let d = Node::new_node(OsStr::new("d"), NodeType::Dir, meta.clone());
        let f = Node::new_node(OsStr::new("f"), NodeType::File, meta);
        assert_eq!(to_entry(&d, "/d".into()).size, 0);
        assert_eq!(to_entry(&d, "/d".into()).kind, SnapshotEntryKind::Directory as i32);
        assert_eq!(to_entry(&f, "/f".into()).size, 42);
    }
}
