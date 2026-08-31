use opendal_core::raw::oio;
use opendal_core::{Buffer, Metadata, Operator, Writer};

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
    pub(crate) fn new(operator: Operator, rel: String) -> Self {
        Self {
            operator,
            rel,
            inner: None,
        }
    }

    async fn writer(&mut self) -> opendal_core::Result<&mut Writer> {
        if self.inner.is_none() {
            let writer = self.operator.writer(&self.rel).await?;
            self.inner = Some(writer);
        }

        Ok(self.inner.as_mut().expect("just initialized above"))
    }
}

impl oio::Write for MountWriter {
    async fn write(&mut self, bs: Buffer) -> opendal_core::Result<()> {
        self.writer().await?.write(bs).await
    }

    async fn close(&mut self) -> opendal_core::Result<Metadata> {
        self.writer().await?.close().await
    }

    async fn abort(&mut self) -> opendal_core::Result<()> {
        self.writer().await?.abort().await
    }
}
