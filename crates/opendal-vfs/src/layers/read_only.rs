use opendal_core::raw::*;
use opendal_core::{Capability, Error, ErrorKind, OperationContext, Result};
use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;

/// An OpenDAL layer that rejects all writes and deletes with
/// [`ErrorKind::PermissionDenied`].
#[derive(Debug)]
pub struct ReadOnlyLayer;

// Layer takes no generics and operates on type-erased service references
impl Layer for ReadOnlyLayer {
    fn apply_service(&self, inner: Arc<dyn ServiceDyn>) -> Arc<dyn ServiceDyn> {
        Arc::new(ReadOnlyService { inner })
    }

    fn apply_context(&self, _srv: Arc<dyn ServiceDyn>, ctx: OperationContext) -> OperationContext {
        ctx
    }
}

pub struct ReadOnlyService {
    inner: Arc<dyn ServiceDyn>,
}

impl Debug for ReadOnlyService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadOnlyService").finish_non_exhaustive()
    }
}

// Implement `Service` instead of `ServiceDyn`
impl Service for ReadOnlyService {
    type Reader = oio::Reader;
    type Writer = oio::Writer;
    type Lister = oio::Lister;
    type Deleter = oio::Deleter;
    type Copier = oio::Copier;
    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> Capability {
        self.inner.capability()
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        Err(
            Error::new(ErrorKind::PermissionDenied, "read-only mount point")
                .with_context("layer", "ReadOnlyLayer"),
        )
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner.stat(ctx, path, args).await
    }

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        self.inner.read(ctx, path, args)
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        Err(
            Error::new(ErrorKind::PermissionDenied, "read-only mount point")
                .with_context("layer", "ReadOnlyLayer"),
        )
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        Err(
            Error::new(ErrorKind::PermissionDenied, "read-only mount point")
                .with_context("layer", "ReadOnlyLayer"),
        )
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        self.inner.list(ctx, path, args)
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
        opts: OpCopier,
    ) -> Result<Self::Copier> {
        Err(
            Error::new(ErrorKind::PermissionDenied, "read-only mount point")
                .with_context("layer", "ReadOnlyLayer"),
        )
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        Err(
            Error::new(ErrorKind::PermissionDenied, "read-only mount point")
                .with_context("layer", "ReadOnlyLayer"),
        )
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        self.inner.presign(ctx, path, args).await
    }
}
