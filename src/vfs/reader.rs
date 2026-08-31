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
}

pub struct MountHandle {
    reader: opendal_core::Reader,
}

impl oio::PositionRead for MountReader {
    type Handle = MountHandle;

    async fn open(&self) -> opendal_core::Result<Self::Handle> {
        let reader = self.operator.reader(&self.path).await?;
        Ok(MountHandle { reader })
    }

    async fn read_at(
        handle: &Self::Handle,
        offset: u64,
        size: usize,
    ) -> opendal_core::Result<Buffer> {
        handle.reader.read(offset..offset + size as u64).await
    }
}

impl MountReader {
    pub(crate) fn new(operator: Operator, path: String) -> Self {
        Self {
            operator,
            path,
        }
    }
}
