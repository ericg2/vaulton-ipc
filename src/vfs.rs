//! `MountFs` — a custom OpenDAL [`Service`] backend that mounts other
//! operators onto fixed virtual paths, the way you'd mount filesystems onto
//! directories on a Unix box.
//! ...

use std::collections::BTreeMap;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::sync::Arc;

use futures_lite::StreamExt;
use log::debug;
use tokio::sync::OnceCell;

use crate::quota::{MemoryTracker, QuotaLayer, QuotaTracker};
use crate::read_only::ReadOnlyLayer;
use opendal_core::raw::*;
use opendal_core::{Buffer, Builder, BytesRange, Capability, EntryMode, Error, ErrorKind, Lister, Metadata, OperationContext, Operator, Result, Writer};
use opendal_core::raw::oio::ReadStreamDyn;

fn normalize(path: &str) -> String {
    let trimmed = path.trim_matches('/');
    if trimmed.is_empty() {
        "/".to_string()
    } else {
        format!("/{trimmed}")
    }
}

// ---------------------------------------------------------------------------
// Builder internals
// ---------------------------------------------------------------------------

/// A not-yet-built mount, held inside [`VfsBuilder`].
struct PendingMount {
    path: String,
    operator: Operator,
    read_only: bool,
    quota: VfsQuota,
}

/// Configuration for a VFS Quota.
#[derive(Clone, Eq, PartialEq, Debug, Default)]
pub enum VfsQuota {
    #[default]
    Disabled,
    Enabled {
        id: String,
        bytes: u64,
    },
}

// ---------------------------------------------------------------------------
// Builder (public)
// ---------------------------------------------------------------------------

/// Builder for the `MountFs` backend.
pub struct VfsBuilder {
    tracker: Arc<dyn QuotaTracker>,
    pending: Vec<PendingMount>,
}

impl Default for VfsBuilder {
    fn default() -> Self {
        Self {
            tracker: Arc::new(MemoryTracker::default()),
            pending: Vec::new(),
        }
    }
}

impl Debug for VfsBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let paths: Vec<&str> = self.pending.iter().map(|m| m.path.as_str()).collect();
        f.debug_struct("VfsBuilder")
            .field("mounts", &paths)
            .finish()
    }
}

impl Builder for VfsBuilder {
    type Config = ();

    fn build(self) -> Result<impl Service> {
        let mut mounts = BTreeMap::new();
        for pending in self.pending {
            if pending.path == "/" {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "cannot mount at the virtual root '/'",
                ));
            }

            let mut op = pending.operator;

            if let VfsQuota::Enabled { id, bytes } = &pending.quota {
                op = op.layer(QuotaLayer::new(id.clone(), self.tracker.clone(), *bytes));
            }

            if pending.read_only {
                op = op.layer(ReadOnlyLayer);
            }

            if mounts
                .insert(
                    pending.path.clone(),
                    Mount {
                        operator: op,
                        read_only: pending.read_only,
                    },
                )
                .is_some()
            {
                return Err(Error::new(ErrorKind::ConfigInvalid, "duplicate mount path")
                    .with_context("path", pending.path));
            }
        }

        Ok(MountAccess {
            mounts: Arc::new(mounts),
        })
    }
}

impl VfsBuilder {
    /// Start an empty builder.
    pub fn new(tracker: Arc<impl QuotaTracker>) -> Self {
        Self {
            tracker,
            pending: Vec::new(),
        }
    }

    /// Override the [`QuotaTracker`].
    pub fn with_tracker(mut self, tracker: Arc<impl QuotaTracker>) -> Self {
        self.tracker = tracker;
        self
    }

    /// Mount a pre-built [`Operator`] at `path`.
    ///
    /// Chain `.read_only()` / `.quota(id, bytes)` immediately after to
    /// configure the mount just added.
    pub fn mount(mut self, path: impl Into<String>, operator: Operator) -> Self {
        self.pending.push(PendingMount {
            path: normalize(&path.into()),
            operator,
            read_only: false,
            quota: VfsQuota::Disabled,
        });
        self
    }

    /// Mark the most recently added mount as read-only.
    pub fn read_only(mut self) -> Self {
        if let Some(last) = self.pending.last_mut() {
            last.read_only = true;
        }
        self
    }

    /// Cap the most recently added mount's cumulative write quota.
    pub fn quota(mut self, id: impl AsRef<str>, bytes: u64) -> Self {
        if let Some(last) = self.pending.last_mut() {
            last.quota = VfsQuota::Enabled {
                id: id.as_ref().to_string(),
                bytes,
            };
        }
        self
    }
}

// ---------------------------------------------------------------------------
// Mount table
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct Mount {
    operator: Operator,
    read_only: bool,
}

/// Find the mount (if any) that owns `path`, and the path made relative to
/// that mount's root. "Owns" means `path` equals the mount path or is nested
/// under it. If multiple configured mounts could match, the longest
/// (most specific) one wins.
fn resolve<'a>(
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

/// True if `path` is a virtual ancestor directory of at least one mount.
fn virtual_children(mounts: &BTreeMap<String, Mount>, path: &str) -> Option<Vec<String>> {
    let normalized = normalize(path);

    let prefix = if normalized == "/" {
        "/".to_string()
    } else {
        format!("{normalized}/")
    };

    let mut children = std::collections::BTreeSet::new();

    for mount_path in mounts.keys() {
        let Some(rest) = mount_path.strip_prefix(&prefix) else {
            continue;
        };

        if rest.is_empty() {
            continue;
        }

        let next_segment = rest.split('/').next().unwrap_or(rest);
        let _ = children.insert(next_segment.to_string());
    }

    if children.is_empty() && normalized != "/" {
        None
    } else {
        Some(children.into_iter().collect())
    }
}

// ---------------------------------------------------------------------------
// Access impl
// ---------------------------------------------------------------------------

/// Accessor for VFS backend. Initialize via [`VfsBuilder`]!
pub struct MountAccess {
    mounts: Arc<BTreeMap<String, Mount>>,
}

impl MountAccess {
    async fn metadata(&self, path: &str) -> Result<Metadata> {
        if let Some((mount_path, mount, rel)) = resolve(&self.mounts, path) {
            if normalize(path) == mount_path && rel.is_empty() {
                return Ok(Metadata::new(EntryMode::DIR));
            }

            return mount.operator.stat(&rel).await;
        }

        if virtual_children(&self.mounts, path).is_some() {
            return Ok(Metadata::new(EntryMode::DIR));
        }

        Err(not_found(path))
    }
}

impl Debug for MountAccess {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("MountAccess")
            .field("mounts", &self.mounts.keys().collect::<Vec<_>>())
            .finish()
    }
}

fn not_found(path: &str) -> Error {
    Error::new(ErrorKind::NotFound, "no mount covers this path").with_context("path", path)
}

fn permission_denied(path: &str) -> Error {
    Error::new(ErrorKind::PermissionDenied, "mount is read-only").with_context("path", path)
}

fn unsupported(op: &'static str) -> Error {
    Error::new(
        ErrorKind::Unsupported,
        format!("MountFs does not support `{op}`."),
    )
}

impl Service for MountAccess {
    type Reader = MountReader;
    type Writer = MountWriter;
    type Lister = MountLister;
    type Deleter = MountDeleter;
    type Copier = ();

    fn info(&self) -> ServiceInfo {
        ServiceInfo::new("mount", "/", "mount")
    }

    fn capability(&self) -> Capability {
        Capability {
            stat: true,
            read: true,
            write: true,
            create_dir: true,
            delete: true,
            list: true,
            ..Default::default()
        }
    }

    async fn create_dir(
        &self,
        _ctx: &OperationContext,
        path: &str,
        _args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        match resolve(&self.mounts, path) {
            Some((_, mount, rel)) => {
                if mount.read_only {
                    return Err(permission_denied(path));
                }

                mount.operator.create_dir(&rel).await?;
                Ok(RpCreateDir::default())
            }
            None => Err(not_found(path)),
        }
    }

    async fn stat(&self, _ctx: &OperationContext, path: &str, _args: OpStat) -> Result<RpStat> {
        Ok(RpStat::new(self.metadata(path).await?))
    }

    /// Construct a [`MountReader`] for `path`.
    ///
    /// This is intentionally synchronous and does **no** I/O: it only
    /// resolves which mount owns `path` and clones that mount's `Operator`.
    /// The actual read (and the `stat` needed to build [`RpRead`]'s
    /// metadata) happens lazily, the first time [`oio::Read::read`] is
    /// called on the returned reader.
    fn read(&self, _ctx: &OperationContext, path: &str, _args: OpRead) -> Result<Self::Reader> {
        let Some((_, mount, rel)) = resolve(&self.mounts, path) else {
            return Err(not_found(path));
        };

        Ok(MountReader::new(mount.operator.clone(), rel))
    }

    /// Construct a [`MountWriter`] for `path`.
    ///
    /// Like `read`, this is synchronous and does no I/O — the underlying
    /// mount's `Operator::writer` is only opened lazily on the first call to
    /// [`oio::Write::write`]/`close`/`abort`.
    fn write(&self, _ctx: &OperationContext, path: &str, _args: OpWrite) -> Result<Self::Writer> {
        let Some((_, mount, rel)) = resolve(&self.mounts, path) else {
            return Err(not_found(path));
        };

        if mount.read_only {
            return Err(permission_denied(path));
        }

        Ok(MountWriter::new(mount.operator.clone(), rel))
    }

    fn delete(&self, _ctx: &OperationContext) -> Result<Self::Deleter> {
        Ok(MountDeleter {
            mounts: self.mounts.clone(),
        })
    }

    /// Construct a [`MountLister`] for `path`.
    ///
    /// Synchronous, no I/O: for a real mount it just resolves which
    /// `Operator` owns the path; `Operator::lister` is only called lazily on
    /// the first [`oio::List::next`]. For a virtual (unmounted ancestor)
    /// directory, the synthetic child entries are built up front since no
    /// I/O is required for those.
    fn list(&self, _ctx: &OperationContext, path: &str, _args: OpList) -> Result<Self::Lister> {
        if let Some((mount_path, mount, rel)) = resolve(&self.mounts, path) {
            debug!("LIST {path} to {}", &rel);

            return Ok(MountLister::Real {
                operator: mount.operator.clone(),
                rel,
                mount_path: mount_path.to_string(),
                inner: None,
            });
        }

        match virtual_children(&self.mounts, path) {
            Some(children) => {
                let base = normalize(path);

                let entries = children
                    .into_iter()
                    .map(|name| {
                        let full = if base == "/" {
                            format!("{name}/")
                        } else {
                            format!("{}/{name}/", base.trim_start_matches('/'))
                        };

                        oio::Entry::new(&full, Metadata::new(EntryMode::DIR))
                    })
                    .collect();

                Ok(MountLister::Virtual { entries })
            }
            None => Err(not_found(path)),
        }
    }

    fn copy(
        &self,
        _ctx: &OperationContext,
        _from: &str,
        _to: &str,
        _args: OpCopy,
        _opts: OpCopier,
    ) -> Result<Self::Copier> {
        Err(unsupported("copy"))
    }

    fn rename(
        &self,
        _ctx: &OperationContext,
        _from: &str,
        _to: &str,
        _args: OpRename,
    ) -> impl Future<Output = Result<RpRename>> + MaybeSend {
        async { Err(unsupported("rename")) }
    }

    fn presign(
        &self,
        _ctx: &OperationContext,
        _path: &str,
        _args: OpPresign,
    ) -> impl Future<Output = Result<RpPresign>> + MaybeSend {
        async { Err(unsupported("presign")) }
    }
}

// ---------------------------------------------------------------------------
// Reader / Writer / Lister / Deleter bridges
// ---------------------------------------------------------------------------

/// [`oio::Read`] implementation for a mounted path.
///
/// `MountReader` is fully lazy: it's constructed with just the owning
/// mount's `Operator` and the relative path, doing no I/O. The first call to
/// [`oio::Read::read`] resolves the entry's [`Metadata`] (via `stat`,
/// cached in a [`OnceCell`] so later calls reuse it) and issues a ranged
/// read against the mounted `Operator`.
#[allow(missing_debug_implementations)]
pub struct MountReader {
    operator: Operator,
    rel: String,
    meta: OnceCell<Metadata>,
}

impl MountReader {
    fn new(operator: Operator, rel: String) -> Self {
        Self {
            operator,
            rel,
            meta: OnceCell::new(),
        }
    }

    async fn metadata(&self) -> Result<Metadata> {
        let meta = self
            .meta
            .get_or_try_init(|| self.operator.stat(&self.rel))
            .await?;
        Ok(meta.clone())
    }
}
impl oio::Read for MountReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn ReadStreamDyn>)> {
        let meta = self.metadata().await?;

        let start = range.offset();
        let end = range.size().map(|size| start + size);

        let stream = MountReadStream {
            operator: self.operator.clone(),
            rel: self.rel.clone(),
            offset: start,
            end,
        };

        Ok((RpRead::new(meta), Box::new(stream)))
    }

    async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
        let meta = self.metadata().await?;

        let buf = self
            .operator
            .read_with(&self.rel)
            .range(range.to_range())
            .await
            .map_err(|err| {
                Error::new(ErrorKind::Unexpected, "mount reader: read failed")
                    .with_operation("MountReader::read")
                    .set_source(err)
            })?;

        Ok((RpRead::new(meta), buf))
    }
}

struct MountReadStream {
    operator: Operator,
    rel: String,
    offset: u64,
    end: Option<u64>,
}

const BUFFER_SIZE: u64 = 4_000_000;

impl oio::ReadStream for MountReadStream {
    async fn read(&mut self) -> Result<Buffer> {
        if let Some(end) = self.end {
            if self.offset >= end {
                return Ok(Buffer::new());
            }
        }

        let want = self
            .end
            .map(|end| (end - self.offset).min(BUFFER_SIZE as u64))
            .unwrap_or(BUFFER_SIZE as u64);

        let range = self.offset..self.offset + want;
        let buf = self.operator.read_with(&self.rel).range(range).await.map_err(|err| {
            Error::new(ErrorKind::Unexpected, "mount reader: stream read failed")
                .with_operation("MountReadStream::read")
                .set_source(err)
        })?;

        self.offset += buf.len() as u64;

        if (buf.len() as u64) < want {
            self.end = Some(self.offset);
        }

        Ok(buf)
    }
}
/// [`oio::Write`] implementation for a mounted path.
///
/// Like [`MountReader`], `MountWriter` is lazy: the mounted `Operator`'s
/// actual `Writer` is only opened (via `Operator::writer`) on the first call
/// to `write`, `close`, or `abort`, since opening a writer is itself async.
#[allow(missing_debug_implementations)]
pub struct MountWriter {
    operator: Operator,
    rel: String,
    inner: Option<Writer>,
}

impl MountWriter {
    fn new(operator: Operator, rel: String) -> Self {
        Self {
            operator,
            rel,
            inner: None,
        }
    }

    async fn writer(&mut self) -> Result<&mut Writer> {
        if self.inner.is_none() {
            let writer = self.operator.writer(&self.rel).await?;
            self.inner = Some(writer);
        }

        Ok(self.inner.as_mut().expect("just initialized above"))
    }
}

impl oio::Write for MountWriter {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        self.writer().await?.write(bs).await
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.writer().await?.close().await
    }

    async fn abort(&mut self) -> Result<()> {
        self.writer().await?.abort().await
    }
}

pub enum MountLister {
    Real {
        operator: Operator,
        rel: String,
        mount_path: String,
        /// Lazily opened on the first call to `next`, since
        /// `Operator::lister` is async.
        inner: Option<Lister>,
    },
    Virtual {
        entries: Vec<oio::Entry>,
    },
}

impl Debug for MountLister {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            MountLister::Real {
                mount_path, inner, ..
            } => f
                .debug_struct("Real")
                .field("mount_path", mount_path)
                .field("opened", &inner.is_some())
                .finish(),

            MountLister::Virtual { entries } => {
                f.debug_struct("Virtual").field("entries", entries).finish()
            }
        }
    }
}

impl oio::List for MountLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        match self {
            MountLister::Real {
                operator,
                rel,
                mount_path,
                inner,
            } => {
                if inner.is_none() {
                    *inner = Some(operator.lister(rel).await?);
                }

                let lister = inner.as_mut().expect("just initialized above");

                match lister.next().await {
                    Some(Ok(entry)) => {
                        let rebased = format!(
                            "{}/{}",
                            mount_path.trim_end_matches('/'),
                            entry.path().trim_start_matches('/')
                        );

                        Ok(Some(oio::Entry::new(&rebased, entry.metadata().clone())))
                    }
                    Some(Err(e)) => Err(e),
                    None => Ok(None),
                }
            }

            MountLister::Virtual { entries } => Ok(entries.pop()),
        }
    }
}

#[derive(Debug)]
pub struct MountDeleter {
    mounts: Arc<BTreeMap<String, Mount>>,
}

impl oio::Delete for MountDeleter {
    async fn delete(&mut self, path: &str, _args: OpDelete) -> Result<()> {
        match resolve(&self.mounts, path) {
            Some((_, mount, rel)) => {
                if mount.read_only {
                    return Err(permission_denied(path));
                }

                mount.operator.delete(&rel).await
            }
            None => Err(not_found(path)),
        }
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(unused_results)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use opendal_core::services::Memory;

    fn builder() -> VfsBuilder {
        VfsBuilder::new(Arc::new(MemoryTracker::default()))
    }

    fn memory() -> Operator {
        Operator::new(Memory::default())
            .unwrap()
    }

    #[tokio::test]
    async fn write_and_read_inside_a_mount_rebases_the_path() {
        let op = Operator::new(builder().mount("/repos/test", memory()))
            .unwrap();

        op.write("/repos/test/abc.txt", "hello").await.unwrap();

        let data = op.read("/repos/test/abc.txt").await.unwrap();
        assert_eq!(data.to_vec(), b"hello");
    }

    #[tokio::test]
    async fn create_dir_rebases_the_path() {
        let op = Operator::new(builder().mount("/repos/test", memory()))
            .unwrap();

        op.create_dir("/repos/test/abc/").await.unwrap();

        let meta = op.stat("/repos/test/abc/").await.unwrap();
        assert!(meta.is_dir());
    }

    #[tokio::test]
    async fn path_outside_any_mount_is_not_found() {
        let op = Operator::new(builder().mount("/repos/test", memory()))
            .unwrap();

        let err = op.write("/elsewhere/file.txt", "x").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn listing_an_unmounted_ancestor_shows_virtual_subfolders() {
        let op = Operator::new(
            builder()
                .mount("/repos/test", memory())
                .mount("/repos/other", memory())
                .mount("/images", memory()),
        )
            .unwrap();

        let mut names: Vec<String> = op
            .list("/")
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.name().trim_end_matches('/').to_string())
            .collect();

        names.sort();
        assert_eq!(names, vec!["images", "repos"]);

        let mut repos_children: Vec<String> = op
            .list("/repos/")
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.name().trim_end_matches('/').to_string())
            .collect();

        repos_children.sort();
        assert_eq!(repos_children, vec!["other", "test"]);
    }

    #[tokio::test]
    async fn listing_inside_a_mount_delegates_and_rebases_entries() {
        let op = Operator::new(builder().mount("/repos/test", memory()))
            .unwrap();

        op.write("/repos/test/a.txt", "1").await.unwrap();
        op.write("/repos/test/b.txt", "2").await.unwrap();

        let mut names: Vec<String> = op
            .list("/repos/test/")
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.path().to_string())
            .collect();

        names.sort();

        assert_eq!(names, vec!["/repos/test/a.txt", "/repos/test/b.txt"]);
    }

    #[tokio::test]
    async fn read_only_mount_rejects_writes_but_allows_reads() {
        let op = Operator::new(builder().mount("/repos/test", memory()).read_only())
            .unwrap();

        let err = op.write("/repos/test/a.txt", "x").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::PermissionDenied);

        let err = op.create_dir("/repos/test/dir/").await.unwrap_err();

        assert_eq!(err.kind(), ErrorKind::PermissionDenied);
    }

    #[tokio::test]
    async fn quota_is_enforced_per_mount() {
        let op = Operator::new(
            builder()
                .mount("/repos/test", memory())
                .quota("", 10)
                .mount("/scratch", memory()),
        )
            .unwrap();

        op.write("/repos/test/a.txt", "0123456789").await.unwrap();

        let err = op.write("/repos/test/b.txt", "x").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::RateLimited);

        op.write("/scratch/big.txt", "0123456789012345678901234567890")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn deleting_routes_per_path() {
        let op = Operator::new(
            builder()
                .mount("/repos/test", memory())
                .mount("/scratch", memory()),
        )
            .unwrap();

        op.write("/repos/test/a.txt", "x").await.unwrap();
        op.write("/scratch/b.txt", "y").await.unwrap();

        op.delete("/repos/test/a.txt").await.unwrap();
        op.delete("/scratch/b.txt").await.unwrap();

        assert_eq!(
            op.stat("/repos/test/a.txt").await.unwrap_err().kind(),
            ErrorKind::NotFound
        );

        assert_eq!(
            op.stat("/scratch/b.txt").await.unwrap_err().kind(),
            ErrorKind::NotFound
        );
    }

    #[tokio::test]
    async fn duplicate_mount_paths_are_rejected_at_build() {
        let err = builder()
            .mount("/repos/test", memory())
            .mount("/repos/test", memory())
            .build()
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
    }

    #[tokio::test]
    async fn mounting_root_is_rejected_at_build() {
        let err = builder().mount("/", memory()).build().unwrap_err();

        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
    }
}