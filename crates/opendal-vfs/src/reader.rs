use opendal_core::{Buffer, Metadata, Operator};
use opendal_core::raw::oio;
use tokio::sync::OnceCell;

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
    path: String,
    metadata: OnceCell<Metadata>,
}

pub struct MountHandle {
    reader: opendal_core::Reader,
    content_length: u64,
}

impl oio::PositionRead for MountReader {
    type Handle = MountHandle;

    async fn open(&self) -> opendal_core::Result<Self::Handle> {
        let metadata = self
            .metadata
            .get_or_try_init(|| self.operator.stat(&self.path))
            .await?;

        let reader = self.operator.reader(&self.path).await?;
        Ok(MountHandle {
            reader,
            content_length: metadata.content_length(),
        })
    }

    /// Read up to `size` bytes starting at `offset`.
    ///
    /// The requested range is clamped to the entry's actual content length
    /// (cached in [`MountHandle`] from `open`'s `stat`), since some backends
    /// (e.g. `memory`) treat a range that extends past EOF as an error
    /// (`RangeNotSatisfied`) rather than silently truncating it.
    async fn read_at(
        handle: &Self::Handle,
        offset: u64,
        size: usize,
    ) -> opendal_core::Result<Buffer> {
        if offset >= handle.content_length {
            return Ok(Buffer::new());
        }

        let end = (offset + size as u64).min(handle.content_length);
        handle.reader.read(offset..end).await
    }
}

impl MountReader {
    pub(crate) fn new(operator: Operator, path: String) -> Self {
        Self {
            operator,
            path,
            metadata: OnceCell::new(),
        }
    }
}