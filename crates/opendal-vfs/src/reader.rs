use opendal_core::raw::oio;
use opendal_core::{Buffer, Metadata};

use crate::cache;
use crate::layers::vfs::Mount;

/// [`oio::PositionRead`] implementation for a mounted path.
///
/// `MountReader` is fully lazy: constructing it does no I/O. The first read
/// resolves the entry's [`Metadata`] through the mount's stat cache (so the
/// per-request reader that file servers create does **not** cost a backend
/// round trip), and every `read_at` is served through the mount's block
/// cache, which fetches large aligned blocks and prefetches on sequential
/// scans.
#[allow(missing_debug_implementations)]
pub struct MountReader {
    mount: Mount,
    path: String,
}

pub struct MountHandle {
    mount: Mount,
    path: String,
    content_length: u64,
    stamp: u64,
}

impl oio::PositionRead for MountReader {
    type Handle = MountHandle;

    async fn open(&self) -> opendal_core::Result<Self::Handle> {
        let metadata: Metadata = self.mount.stat(&self.path).await?;

        Ok(MountHandle {
            mount: self.mount.clone(),
            path: self.path.clone(),
            content_length: metadata.content_length(),
            stamp: cache::stamp(&metadata),
        })
    }

    /// Read up to `size` bytes starting at `offset`.
    ///
    /// The range is clamped to the entry's content length (some backends,
    /// e.g. `memory`, error on ranges past EOF instead of truncating).
    async fn read_at(
        handle: &Self::Handle,
        offset: u64,
        size: usize,
    ) -> opendal_core::Result<Buffer> {
        handle
            .mount
            .cache
            .read(
                &handle.mount.operator,
                &handle.path,
                handle.stamp,
                handle.content_length,
                offset,
                size,
            )
            .await
    }
}

impl MountReader {
    pub(crate) fn new(mount: Mount, path: String) -> Self {
        Self { mount, path }
    }
}
