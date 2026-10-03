use opendal_core::raw::oio;
use opendal_core::{Buffer, Metadata, Writer};

use crate::layers::vfs::Mount;

/// [`oio::Write`] implementation for a mounted path.
///
/// Like [`MountReader`](crate::reader::MountReader), `MountWriter` is lazy:
/// the mounted `Operator`'s actual `Writer` is only opened on the first call
/// to `write`, `close`, or `abort`. Multipart-capable backends (B2, S3, ...)
/// get a larger part size and concurrent part uploads. When the writer
/// finishes, the mount's stat/block caches are invalidated for the path.
#[allow(missing_debug_implementations)]
pub struct MountWriter {
    mount: Mount,
    rel: String,
    inner: Option<Writer>,
}

/// Open a [`Writer`] on the mount's operator, using a larger part size and
/// concurrent part uploads on multipart-capable backends.
pub(crate) async fn open_writer(mount: &Mount, rel: &str) -> opendal_core::Result<Writer> {
    let cfg = mount.cache.config();
    let cap = mount.operator.info().capability();

    let mut w = mount.operator.writer_with(rel);
    if cap.write_can_multi && cfg.write_concurrency > 1 {
        w = w.chunk(cfg.write_chunk).concurrent(cfg.write_concurrency);
    }

    w.await
}

impl MountWriter {
    pub(crate) fn new(mount: Mount, rel: String) -> Self {
        Self {
            mount,
            rel,
            inner: None,
        }
    }

    async fn writer(&mut self) -> opendal_core::Result<&mut Writer> {
        if self.inner.is_none() {
            self.inner = Some(open_writer(&self.mount, &self.rel).await?);
        }

        Ok(self.inner.as_mut().expect("just initialized above"))
    }
}

impl oio::Write for MountWriter {
    async fn write(&mut self, bs: Buffer) -> opendal_core::Result<()> {
        self.writer().await?.write(bs).await
    }

    async fn close(&mut self) -> opendal_core::Result<Metadata> {
        let res = match self.writer().await {
            Ok(w) => w.close().await,
            Err(e) => Err(e),
        };
        self.mount.cache.invalidate(&self.rel).await;
        res
    }

    async fn abort(&mut self) -> opendal_core::Result<()> {
        let res = match self.writer().await {
            Ok(w) => w.abort().await,
            Err(e) => Err(e),
        };
        self.mount.cache.invalidate(&self.rel).await;
        res
    }
}
