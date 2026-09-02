// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Quota layer implementation for Apache OpenDAL.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;

use opendal_core::raw::*;
use opendal_core::*;

/// Add a write-quota to the underlying services.
///
/// # Quota
///
/// A [`QuotaLayer`] limits the total number of bytes that may be written
/// through an operator. Usage is tracked per quota `id` and persisted via a
/// pluggable [`QuotaTracker`], so it can survive process restarts or be
/// shared across multiple operators.
///
/// # Note
///
/// The quota is enforced at write time: each call to `write` reserves its
/// byte length against the quota before the underlying write is attempted.
/// If the underlying write fails, the reservation is released so the quota
/// is not consumed by a failed operation.
///
/// # Examples
///
/// This example limits total writes to 1 KiB using an in-memory tracker.
///
/// ```no_run
/// # use std::sync::Arc;
/// # use opendal_core::services;
/// # use opendal_core::Operator;
/// # use opendal_core::Result;
/// # use opendal_layer_quota::{QuotaLayer, MemoryTracker};
/// #
/// # fn main() -> Result<()> {
/// let tracker = Arc::new(MemoryTracker::default());
/// let _ = Operator::new(services::Memory::default())
///     .expect("must init")
///     .layer(QuotaLayer::new("tenant-a", tracker, 1024));
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct QuotaLayer {
    state: Arc<QuotaState>,
}

impl QuotaLayer {
    /// Create a new `QuotaLayer` with a given quota id, tracker, and limit.
    ///
    /// - id: unique identifier for the quota bucket.
    /// - tracker: backend used to persist and retrieve quota usage.
    /// - limit_bytes: maximum number of bytes that may be written before
    ///   further writes are rejected.
    ///
    /// Usage is read from and written back to the [`QuotaTracker`] on every
    /// write; no local caching is performed.
    pub fn new(id: impl Into<String>, tracker: Arc<dyn QuotaTracker>, limit_bytes: u64) -> Self {
        Self {
            state: Arc::new(QuotaState {
                id: id.into(),
                tracker,
                limit: limit_bytes,
            }),
        }
    }
}

impl Layer for QuotaLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        Arc::new(self.layer(inner))
    }
}

impl QuotaLayer {
    fn layer(&self, inner: Servicer) -> QuotaAccessor {
        QuotaAccessor {
            inner,
            state: self.state.clone(),
        }
    }
}

/// Persistence backend for tracking bytes written under a quota.
///
/// A [`QuotaTracker`] stores the cumulative number of bytes written for each
/// quota identifier, allowing quota usage to survive process restarts or be
/// shared across multiple instances.
///
/// Implementations may store usage in memory, on disk, in a database, or any
/// other persistent backing store.
///
/// The `id` uniquely identifies a quota bucket. The exact meaning of the ID is
/// defined by the caller (for example, a user ID, tenant ID, filesystem path,
/// or mount identifier).
#[async_trait]
pub trait QuotaTracker: Send + Sync + 'static {
    /// Returns the total number of bytes recorded for the given quota ID.
    ///
    /// If no usage has been recorded yet, implementations should return `0`
    /// rather than an error.
    async fn get_bytes_written(&self, id: &str) -> Result<u64>;

    /// Stores the total number of bytes written for the given quota ID.
    ///
    /// This replaces the previously stored value rather than incrementing it.
    async fn set_bytes_written(&self, id: &str, bytes: u64) -> Result<()>;
}

/// Simple in-memory [`QuotaTracker`] implementation, primarily useful for
/// tests and single-process deployments.
#[derive(Default, Debug)]
pub struct MemoryTracker(Mutex<HashMap<String, u64>>);

#[async_trait]
impl QuotaTracker for MemoryTracker {
    async fn get_bytes_written(&self, id: &str) -> Result<u64> {
        Ok(*self.0.lock().unwrap().get(id).unwrap_or(&0))
    }

    async fn set_bytes_written(&self, id: &str, bytes: u64) -> Result<()> {
        let _ = self.0.lock().unwrap().insert(id.to_string(), bytes);
        Ok(())
    }
}

/// Shared quota state.
///
/// Cloning a [`QuotaLayer`] is inexpensive because all clones share the same
/// underlying `QuotaState` via `Arc`
struct QuotaState {
    /// Unique identifier for the quota bucket.
    id: String,
    /// Backend used to persist and retrieve quota usage.
    tracker: Arc<dyn QuotaTracker>,
    /// Maximum number of bytes that may be written before further writes
    /// are rejected.
    limit: u64,
}

impl Debug for QuotaState {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuotaState")
            .field("id", &self.id)
            .field("limit", &self.limit)
            .finish()
    }
}

impl QuotaState {
    /// Reserve `len` additional bytes against the quota.
    ///
    /// Returns an error without mutating state if the reservation would
    /// exceed the configured limit. Usage is read from and written back to
    /// the [`QuotaTracker`] on every call, so no local caching is involved.
    async fn reserve(&self, len: u64) -> Result<()> {
        if len == 0 {
            return Ok(());
        }

        let current = self.tracker.get_bytes_written(&self.id).await?;
        let new_total = current.saturating_add(len);
        if new_total > self.limit {
            return Err(Error::new(
                ErrorKind::RateLimited,
                format!(
                    "write quota exceeded for '{}': {} used, {} requested, {} limit",
                    self.id, current, len, self.limit
                ),
            )
                .with_context("quota_id", self.id.clone())
                .with_context("quota_limit", self.limit.to_string())
                .with_context("quota_used", current.to_string())
                .with_context("quota_requested", len.to_string()));
        }

        self.tracker.set_bytes_written(&self.id, new_total).await
    }

    /// Release `len` previously reserved bytes back to the quota, for
    /// example when an underlying write fails or is aborted.
    async fn release(&self, len: u64) {
        if len == 0 {
            return;
        }

        if let Ok(current) = self.tracker.get_bytes_written(&self.id).await {
            let new_total = current.saturating_sub(len);
            let _ = self.tracker.set_bytes_written(&self.id, new_total).await;
        }
    }
}

#[doc(hidden)]
#[derive(Debug)]
pub struct QuotaAccessor {
    inner: Servicer,
    state: Arc<QuotaState>,
}

impl Service for QuotaAccessor {
    type Reader = oio::Reader;
    type Writer = QuotaWriter<oio::Writer>;
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
        self.inner.create_dir(ctx, path, args).await
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner.stat(ctx, path, args).await
    }

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        self.inner.read(ctx, path, args)
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        let state = self.state.clone();
        self.inner
            .write(ctx, path, args)
            .map(|w| QuotaWriter::new(w, state))
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        self.inner.delete(ctx)
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
        self.inner.copy(ctx, from, to, args, opts)
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        self.inner.rename(ctx, from, to, args).await
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

#[doc(hidden)]
pub struct QuotaWriter<W> {
    inner: W,
    state: Arc<QuotaState>,
    reserved: u64,
}

impl<W> QuotaWriter<W> {
    fn new(inner: W, state: Arc<QuotaState>) -> Self {
        Self {
            inner,
            state,
            reserved: 0,
        }
    }
}

impl<W> Debug for QuotaWriter<W> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuotaWriter")
            .field("id", &self.state.id)
            .field("limit", &self.state.limit)
            .field("reserved", &self.reserved)
            .finish_non_exhaustive()
    }
}

impl<W: oio::Write> oio::Write for QuotaWriter<W> {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let len = bs.len() as u64;

        self.state.reserve(len).await?;
        self.reserved += len;

        if let Err(e) = self.inner.write(bs).await {
            self.reserved -= len;
            self.state.release(len).await;
            return Err(e);
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        let meta = self.inner.close().await?;
        self.reserved = 0;
        Ok(meta)
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await?;
        let to_release = self.reserved;
        self.reserved = 0;
        self.state.release(to_release).await;
        Ok(())
    }
}

#[cfg(test)]
#[allow(unused_results)]
mod tests {
    use super::*;
    use opendal_core::{Operator, services};
    use std::sync::Arc;

    const TENANT_ID: &'static str = "tenant-test";

    fn build_op(id: &str, tracker: Arc<MemoryTracker>, limit: u64) -> Operator {
        Operator::new(services::Memory::default())
            .unwrap()
            .layer(QuotaLayer::new(id, tracker, limit))
    }

    #[tokio::test]
    async fn writes_within_quota_succeed_and_are_tracked() {
        let tracker = Arc::new(MemoryTracker::default());
        let op = build_op(TENANT_ID, Arc::clone(&tracker), 1024);
        op.write("a.txt", "hello world").await.unwrap();

        assert_eq!(
            tracker.get_bytes_written(TENANT_ID).await.unwrap(),
            "hello world".len() as u64
        );
    }

    #[tokio::test]
    async fn write_exceeding_quota_is_rejected() {
        let tracker = Arc::new(MemoryTracker::default());
        let op = build_op(TENANT_ID, Arc::clone(&tracker), 10);

        let err = op
            .write("big.txt", "this is way too large")
            .await
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::RateLimited);
        assert_eq!(tracker.get_bytes_written(TENANT_ID).await.unwrap(), 0);
    }
}