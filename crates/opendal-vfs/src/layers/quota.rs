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
/// A [`QuotaLayer`] limits the total number of bytes currently occupied by
/// objects under an operator, tracked per quota `id` via a pluggable
/// [`QuotaTracker`]. The tracked total behaves like real filesystem usage:
///
/// - Writing a new object adds its size to the total.
/// - Overwriting an existing object replaces its old size with its new size
///   (the total only grows or shrinks by the *difference*).
/// - Deleting an object subtracts its size from the total.
/// - Copying an object adds the copied size to the total (as if a new file
///   were written), replacing the destination's old size if it existed.
/// - Renaming an object within the same quota id does not change the total,
///   since no bytes are gained or lost.
///
/// # Atomicity
///
/// The authoritative enforcement point is [`QuotaTracker::apply_delta`],
/// which every commit (write close, copy close, delete flush) goes through.
/// A correct implementation applies the delta and enforces `limit` as a
/// single atomic operation (a CAS loop, a DB transaction, a Lua script,
/// etc.), so concurrent commits against the same `id` can't race past the
/// limit the way a separate read-then-write ever could.
///
/// Writers additionally run an optimistic, non-atomic
/// [`QuotaTracker::get_bytes_written`] check on every streamed chunk purely
/// so an over-quota write fails fast instead of streaming megabytes before
/// being rejected. That check is advisory only — the final `apply_delta`
/// call at `close()` is what actually decides whether the operation is
/// allowed to stand, and it can still reject a write/copy that the
/// optimistic check let through.
///
/// # Examples
///
/// This example limits total usage to 1 KiB using an in-memory tracker.
///
/// `no_run
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
/// `
#[derive(Clone, Debug)]
pub struct QuotaLayer {
    state: Arc<QuotaState>,
}

impl QuotaLayer {
    /// Create a new `QuotaLayer` with a given quota id, tracker, and limit.
    ///
    /// - id: unique identifier for the quota bucket.
    /// - tracker: backend used to persist and atomically enforce quota usage.
    /// - limit_bytes: maximum number of bytes that may be occupied before
    ///   further writes/copies are rejected.
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
            inner: inner.clone(),
            state: self.state.clone(),
        }
    }
}

/// Persistence backend for tracking bytes currently used under a quota.
///
/// A [`QuotaTracker`] stores the cumulative number of bytes occupied by
/// objects for each quota identifier, allowing quota usage to survive
/// process restarts or be shared across multiple instances.
///
/// The `id` uniquely identifies a quota bucket. The exact meaning of the ID is
/// defined by the caller (for example, a user ID, tenant ID, filesystem path,
/// or mount identifier).
#[async_trait]
pub trait QuotaTracker: Send + Sync + 'static {
    /// Returns the total number of bytes recorded for the given quota ID.
    ///
    /// If no usage has been recorded yet, implementations should return `0`
    /// rather than an error. This is a plain read with no atomicity
    /// guarantees relative to concurrent [`apply_delta`](Self::apply_delta)
    /// calls — it's meant for inspection (stats, dashboards) and for the
    /// layer's optimistic fail-fast check, not for enforcement.
    async fn get_bytes_written(&self, id: &str) -> Result<u64>;

    /// Clears the quota for a specific point. Used on deletion.
    async fn clear(&self, id: &str) -> Result<()>;

    /// Atomically replace `old_size` bytes with `new_size` bytes in the
    /// running total for `id`, enforcing `limit` as part of the same
    /// operation, and return the resulting total.
    ///
    /// Implementations MUST perform the read, the limit check, and the
    /// write as a single atomic unit (e.g. a compare-and-swap loop, a
    /// database transaction, or an atomic script), so that concurrent
    /// callers against the same `id` cannot both observe a total that
    /// permits their delta and then both commit, overshooting `limit`.
    ///
    /// - Pass `old_size = 0` for a brand-new object (nothing being
    ///   replaced).
    /// - Pass `new_size = 0` for a pure deletion (freeing `old_size`
    ///   bytes).
    /// - Deletes should generally be called with `limit = u64::MAX` (or
    ///   otherwise guaranteed not to fail), since freeing space should
    ///   never be rejected by a quota.
    ///
    /// If applying the delta would push the total above `limit`, this
    /// returns a `RateLimited` error and MUST NOT mutate the stored value.
    /// The total must never be allowed to go negative; treat
    /// `old_size > current` as `current -> 0` (saturating), not as an
    /// underflow error.
    async fn apply_delta(
        &self,
        id: &str,
        old_size: u64,
        new_size: u64,
        limit: u64,
    ) -> Result<u64>;
}

/// Simple in-memory [`QuotaTracker`] implementation, primarily useful for
/// tests and single-process deployments.
///
/// The internal `Mutex` is held across the read-check-write sequence inside
/// `apply_delta`, which is what makes it atomic — unlike calling a separate
/// get/set pair, where the lock would be released (and re-acquired) between
/// the two calls.
#[derive(Default, Debug)]
pub struct MemoryTracker(Mutex<HashMap<String, u64>>);

#[async_trait]
impl QuotaTracker for MemoryTracker {
    async fn get_bytes_written(&self, id: &str) -> Result<u64> {
        Ok(*self.0.lock().unwrap().get(id).unwrap_or(&0))
    }

    async fn clear(&self, id: &str) -> Result<()> {
        Err(Error::new(ErrorKind::Unsupported, "clearing not supported"))
    }

    async fn apply_delta(
        &self,
        id: &str,
        old_size: u64,
        new_size: u64,
        limit: u64,
    ) -> Result<u64> {
        // Holding the lock across read + check + write is what makes this
        // atomic; a separate get_bytes_written()/set_bytes_written() pair
        // would release the lock in between and reopen the race.
        let mut map = self.0.lock().unwrap();
        let current = *map.get(id).unwrap_or(&0);
        let new_total = current.saturating_sub(old_size).saturating_add(new_size);

        if new_total > limit {
            return Err(quota_exceeded_error(id, current, new_total, limit));
        }

        map.insert(id.to_string(), new_total);
        Ok(new_total)
    }
}

fn quota_exceeded_error(id: &str, current: u64, hypothetical: u64, limit: u64) -> Error {
    Error::new(
        ErrorKind::RateLimited,
        format!(
            "write quota exceeded for '{id}': {current} used, {hypothetical} would be needed, {limit} limit"
        ),
    )
        .with_context("quota_id", id.to_string())
        .with_context("quota_limit", limit.to_string())
        .with_context("quota_used", current.to_string())
}

/// Shared quota state.
///
/// Cloning a [`QuotaLayer`] is inexpensive because all clones share the same
/// underlying `QuotaState` via `Arc`.
struct QuotaState {
    /// Unique identifier for the quota bucket.
    id: String,
    /// Backend used to persist and atomically enforce quota usage.
    tracker: Arc<dyn QuotaTracker>,
    /// Maximum number of bytes that may be occupied before further
    /// writes/copies are rejected.
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
    /// Optimistic, non-atomic fail-fast check used while a write/copy is
    /// still streaming, so an over-quota operation doesn't have to finish
    /// streaming before being rejected. This can race with concurrent
    /// commits and is NOT the source of truth — `commit` (via
    /// `apply_delta`) is.
    async fn optimistic_check(
        &self,
        old_size: u64,
        provisional: u64,
        additional: u64,
    ) -> Result<()> {
        let current = self.tracker.get_bytes_written(&self.id).await?;
        let hypothetical = current
            .saturating_sub(old_size)
            .saturating_add(provisional)
            .saturating_add(additional);


        if hypothetical > self.limit {
            return Err(quota_exceeded_error(
                &self.id,
                current,
                hypothetical,
                self.limit,
            ));
        }

        Ok(())
    }

    /// Atomically commit a completed write or copy: replace `old_size`
    /// bytes with `new_size` bytes, enforcing the quota limit as part of
    /// the same operation. This is the authoritative enforcement point.
    async fn commit_write(&self, old_size: u64, new_size: u64) -> Result<u64> {
        self.tracker
            .apply_delta(&self.id, old_size, new_size, self.limit)
            .await
    }

    /// Atomically commit a completed delete: subtract `size` bytes from the
    /// running total. Deletes are never rejected by the quota, so this
    /// passes `u64::MAX` as the limit and swallows tracker errors (freeing
    /// space should never fail the delete itself).
    async fn commit_delete(&self, size: u64) {
        let _ = self
            .tracker
            .apply_delta(&self.id, size, 0, u64::MAX)
            .await;
    }
}

/// Best-effort stat helper: returns `0` if the path doesn't exist or stat
/// fails, so callers can treat "no prior object" the same as "empty prior
/// object" without special-casing errors.
async fn size_of(accessor: &Servicer, ctx: &OperationContext, path: &str) -> u64 {
    accessor
        .stat(ctx, path, OpStat::default())
        .await
        .map(|rp| rp.into_metadata().content_length())
        .unwrap_or(0)
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
    type Deleter = QuotaDeleter;
    type Copier = QuotaCopier<oio::Copier>;


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
        // Directories don't occupy quota-tracked bytes.
        self.inner.create_dir(ctx, path, args).await
    }

    async fn stat(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpStat,
    ) -> Result<RpStat> {
        self.inner.stat(ctx, path, args).await
    }

    fn read(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRead,
    ) -> Result<Self::Reader> {
        self.inner.read(ctx, path, args)
    }

    fn write(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpWrite,
    ) -> Result<Self::Writer> {
        let state = self.state.clone();
        let accessor = self.inner.clone();
        let ctx = ctx.clone();
        let path = path.to_string();

        self.inner
            .write(&ctx, &path, args)
            .map(|w| QuotaWriter::new(w, state, accessor, ctx, path))
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        let inner = self.inner.delete(ctx)?;

        Ok(QuotaDeleter {
            inner,
            accessor: self.inner.clone(),
            ctx: ctx.clone(),
            state: self.state.clone(),
            pending_sizes: Vec::new(),
        })
    }

    fn list(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpList,
    ) -> Result<Self::Lister> {
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
        let inner = self.inner.copy(ctx, from, to, args, opts)?;

        Ok(QuotaCopier::new(
            inner,
            self.state.clone(),
            self.inner.clone(),
            ctx.clone(),
            from.to_string(),
            to.to_string(),
        ))
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        // Renaming within the same quota id is a net-zero change in total
        // bytes used (no bytes are created or destroyed), so no quota
        // accounting is needed here.
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
    accessor: Servicer,
    ctx: OperationContext,
    path: String,
    /// Size of the object at `path` before this write started, or `0` if it
    /// didn't exist. Looked up lazily on the first `write()` call.
    old_size: Option<u64>,
    /// Bytes written so far in this (not-yet-committed) write.
    written: u64,
}

impl<W> QuotaWriter<W> {
    fn new(
        inner: W,
        state: Arc<QuotaState>,
        accessor: Servicer,
        ctx: OperationContext,
        path: String,
    ) -> Self {
        Self {
            inner,
            state,
            accessor,
            ctx,
            path,
            old_size: None,
            written: 0,
        }
    }


    async fn old_size(&mut self) -> u64 {
        if let Some(sz) = self.old_size {
            return sz;
        }

        let sz = size_of(&self.accessor, &self.ctx, &self.path).await;
        self.old_size = Some(sz);
        sz
    }
}

impl<W> Debug for QuotaWriter<W> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuotaWriter")
            .field("id", &self.state.id)
            .field("limit", &self.state.limit)
            .field("path", &self.path)
            .field("written", &self.written)
            .finish_non_exhaustive()
    }
}

impl<W: oio::Write> oio::Write for QuotaWriter<W> {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let len = bs.len() as u64;
        let old_size = self.old_size().await;


        // Fail fast, optimistically, before streaming bytes downstream.
        // This is advisory only: `close()` is the real enforcement point.
        self.state
            .optimistic_check(old_size, self.written, len)
            .await?;

        self.inner.write(bs).await?;
        self.written += len;
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        let old_size = self.old_size().await;
        let meta = self.inner.close().await?;

        // Authoritative, atomic enforcement: this can still reject even if
        // every prior optimistic_check() passed, if a concurrent commit won
        // the race in between.
        self.state
            .commit_write(old_size, self.written)
            .await?;

        self.written = 0;
        Ok(meta)
    }

    async fn abort(&mut self) -> Result<()> {
        // Nothing was ever persisted to the tracker mid-write (accounting
        // only happens at close), so aborting only needs to abort the
        // underlying writer.
        self.inner.abort().await?;
        self.written = 0;
        Ok(())
    }
}

#[doc(hidden)]
pub struct QuotaDeleter {
    inner: oio::Deleter,
    accessor: Servicer,
    ctx: OperationContext,
    state: Arc<QuotaState>,
    /// Sizes of paths successfully queued via `delete()`, but not yet
    /// committed to the quota tracker.
    pending_sizes: Vec<u64>,
}

impl Debug for QuotaDeleter {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuotaDeleter")
            .field("id", &self.state.id)
            .field("pending", &self.pending_sizes.len())
            .finish_non_exhaustive()
    }
}

impl oio::Delete for QuotaDeleter {
    async fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        // Capture the size before forwarding the delete. Some backends apply
        // the deletion immediately from `delete()` rather than waiting until
        // `close()`, so statting during `close()` can already return 0.
        let size = size_of(&self.accessor, &self.ctx, path).await;


        self.inner.delete(path, args).await?;
        self.pending_sizes.push(size);

        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        // Only release quota after the underlying delete batch has committed.
        self.inner.close().await?;

        let sizes = std::mem::take(&mut self.pending_sizes);
        for size in sizes {
            self.state.commit_delete(size).await;
        }

        Ok(())
    }
}

/// Quota-aware wrapper around an [`oio::Copy`] implementation.
///
/// Like [`QuotaWriter`], accounting is not incremental: the source and
/// destination sizes are stat'd once (lazily, on the first `next()` call, or
/// eagerly in `close()` if `next()` was never driven), an optimistic check
/// runs against them, and the atomic quota delta is only committed once the
/// underlying copy's `close()` succeeds.
#[doc(hidden)]
pub struct QuotaCopier<C> {
    inner: C,
    state: Arc<QuotaState>,
    accessor: Servicer,
    ctx: OperationContext,
    from: String,
    to: String,
    /// Cached (from_size, to_size) once resolved, so `next()` and `close()`
    /// don't re-stat and so the optimistic check only ever runs once.
    sizes: Option<(u64, u64)>,
}

impl<C> QuotaCopier<C> {
    fn new(
        inner: C,
        state: Arc<QuotaState>,
        accessor: Servicer,
        ctx: OperationContext,
        from: String,
        to: String,
    ) -> Self {
        Self {
            inner,
            state,
            accessor,
            ctx,
            from,
            to,
            sizes: None,
        }
    }


    /// Resolve (and cache) the source size and the destination's current
    /// size, then run the optimistic check against them the first time this
    /// is called.
    async fn checked_sizes(&mut self) -> Result<(u64, u64)> {
        if let Some(sizes) = self.sizes {
            return Ok(sizes);
        }

        let from_size = size_of(&self.accessor, &self.ctx, &self.from).await;
        let to_size = size_of(&self.accessor, &self.ctx, &self.to).await;

        // Copying is accounted like a write of `from_size` bytes that
        // replaces whatever currently sits at `to` (if anything).
        self.state
            .optimistic_check(to_size, 0, from_size)
            .await?;

        self.sizes = Some((from_size, to_size));
        Ok((from_size, to_size))
    }
}

impl<C> Debug for QuotaCopier<C> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuotaCopier")
            .field("id", &self.state.id)
            .field("limit", &self.state.limit)
            .field("from", &self.from)
            .field("to", &self.to)
            .finish_non_exhaustive()
    }
}

impl<C: oio::Copy> oio::Copy for QuotaCopier<C> {
    async fn next(&mut self) -> Result<Option<usize>> {
        self.checked_sizes().await?;
        self.inner.next().await
    }


    async fn close(&mut self) -> Result<Metadata> {
        let (from_size, to_size) = self.checked_sizes().await?;
        let meta = self.inner.close().await?;

        // Authoritative, atomic commit — can still reject here even if the
        // optimistic check above passed.
        self.state
            .commit_write(to_size, from_size)
            .await?;

        Ok(meta)
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}

#[cfg(test)]
#[allow(unused_results)]
mod tests {
    use super::*;
    use opendal::{services, Operator};
    use std::sync::Arc;
    use tempfile::TempDir;


    const TENANT_ID: &'static str = "tenant-test";

    /// Build an `Operator` backed by a real filesystem in a fresh temp
    /// directory (rather than the in-memory service), so tests exercise the
    /// same stat/copy/delete code paths a real deployment would hit. The
    /// returned `TempDir` must be kept alive for as long as `op` is used —
    /// it deletes the directory on drop.
    fn build_op(id: &str, tracker: Arc<MemoryTracker>, limit: u64) -> (Operator, TempDir) {
        let dir = TempDir::new().expect("create temp dir");
        let op = Operator::new(
            services::Fs::default().root(dir.path().to_str().unwrap()),
        )
            .unwrap()
            .layer(QuotaLayer::new(id, tracker, limit));

        (op, dir)
    }

    #[tokio::test]
    async fn writes_within_quota_succeed_and_are_tracked() {
        let tracker = Arc::new(MemoryTracker::default());
        let (op, _dir) = build_op(TENANT_ID, Arc::clone(&tracker), 1024);

        op.write("a.txt", "hello world").await.unwrap();

        assert_eq!(
            tracker.get_bytes_written(TENANT_ID).await.unwrap(),
            "hello world".len() as u64
        );
    }

    #[tokio::test]
    async fn write_exceeding_quota_is_rejected() {
        let tracker = Arc::new(MemoryTracker::default());
        let (op, _dir) = build_op(TENANT_ID, Arc::clone(&tracker), 10);

        let err = op
            .write("big.txt", "this is way too large")
            .await
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::RateLimited);
        assert_eq!(tracker.get_bytes_written(TENANT_ID).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn overwrite_replaces_rather_than_adds() {
        let tracker = Arc::new(MemoryTracker::default());
        let (op, _dir) = build_op(TENANT_ID, Arc::clone(&tracker), 1024 * 1024);

        op.write("f.txt", vec![0u8; 1_000_000]).await.unwrap();
        assert_eq!(
            tracker.get_bytes_written(TENANT_ID).await.unwrap(),
            1_000_000
        );

        op.write("f.txt", vec![0u8; 500_000]).await.unwrap();
        assert_eq!(
            tracker.get_bytes_written(TENANT_ID).await.unwrap(),
            500_000
        );
    }

    #[tokio::test]
    async fn overwrite_that_would_exceed_quota_is_rejected_and_old_size_kept() {
        let tracker = Arc::new(MemoryTracker::default());
        let (op, _dir) =
            build_op(TENANT_ID, Arc::clone(&tracker), 1_000_000);

        op.write("f.txt", vec![0u8; 100_000]).await.unwrap();
        op.write("other.txt", vec![0u8; 850_000])
            .await
            .unwrap();

        assert_eq!(
            tracker.get_bytes_written(TENANT_ID).await.unwrap(),
            950_000
        );

        let err = op
            .write("f.txt", vec![0u8; 200_000])
            .await
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::RateLimited);

        assert_eq!(
            tracker.get_bytes_written(TENANT_ID).await.unwrap(),
            950_000
        );

        let meta = op.stat("f.txt").await.unwrap();
        assert_eq!(meta.content_length(), 100_000);
    }

    #[tokio::test]
    async fn delete_releases_exact_size() {
        let tracker = Arc::new(MemoryTracker::default());
        let (op, _dir) = build_op(TENANT_ID, Arc::clone(&tracker), 1024);

        op.write("a.txt", "hello world").await.unwrap();
        op.delete("a.txt").await.unwrap();

        assert_eq!(tracker.get_bytes_written(TENANT_ID).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn delete_then_rewrite_frees_room_for_new_writes() {
        let tracker = Arc::new(MemoryTracker::default());
        let (op, _dir) = build_op(TENANT_ID, Arc::clone(&tracker), 10);

        op.write("a.txt", "0123456789").await.unwrap();
        assert!(op.write("b.txt", "x").await.is_err());

        op.delete("a.txt").await.unwrap();

        assert_eq!(tracker.get_bytes_written(TENANT_ID).await.unwrap(), 0);

        op.write("b.txt", "0123456789").await.unwrap();

        assert_eq!(tracker.get_bytes_written(TENANT_ID).await.unwrap(), 10);
    }

    #[tokio::test]
    async fn deleting_nonexistent_path_is_a_noop_for_quota() {
        let tracker = Arc::new(MemoryTracker::default());
        let (op, _dir) = build_op(TENANT_ID, Arc::clone(&tracker), 1024);

        op.write("a.txt", "hello world").await.unwrap();
        op.delete("does-not-exist.txt").await.unwrap();

        assert_eq!(
            tracker.get_bytes_written(TENANT_ID).await.unwrap(),
            "hello world".len() as u64
        );
    }

    #[tokio::test]
    async fn rename_does_not_change_total_bytes_used() {
        let tracker = Arc::new(MemoryTracker::default());
        let (op, _dir) = build_op(TENANT_ID, Arc::clone(&tracker), 1024);

        op.write("a.txt", "hello world").await.unwrap();
        let before = tracker.get_bytes_written(TENANT_ID).await.unwrap();

        op.rename("a.txt", "b.txt").await.unwrap();

        let after = tracker.get_bytes_written(TENANT_ID).await.unwrap();
        assert_eq!(before, after);
    }

    #[tokio::test]
    async fn copy_of_new_object_adds_its_size_to_the_total() {
        let tracker = Arc::new(MemoryTracker::default());
        let (op, _dir) = build_op(TENANT_ID, Arc::clone(&tracker), 1024);

        op.write("a.txt", "hello world").await.unwrap();
        op.copy("a.txt", "b.txt").await.unwrap();

        assert_eq!(
            tracker.get_bytes_written(TENANT_ID).await.unwrap(),
            2 * "hello world".len() as u64
        );
    }

    #[tokio::test]
    async fn copy_exceeding_quota_is_rejected_and_source_untouched() {
        let tracker = Arc::new(MemoryTracker::default());
        let (op, _dir) = build_op(TENANT_ID, Arc::clone(&tracker), 15);

        op.write("a.txt", "hello world").await.unwrap();

        assert_eq!(tracker.get_bytes_written(TENANT_ID).await.unwrap(), 11);

        let err = op.copy("a.txt", "b.txt").await.unwrap_err();

        assert_eq!(err.kind(), ErrorKind::RateLimited);
        assert_eq!(tracker.get_bytes_written(TENANT_ID).await.unwrap(), 11);
        assert!(!op.exists("b.txt").await.unwrap());
    }

    #[tokio::test]
    async fn copy_overwriting_destination_replaces_rather_than_adds() {
        let tracker = Arc::new(MemoryTracker::default());
        let (op, _dir) =
            build_op(TENANT_ID, Arc::clone(&tracker), 1024 * 1024);

        op.write("a.txt", vec![0u8; 100]).await.unwrap();
        op.write("b.txt", vec![0u8; 900]).await.unwrap();

        assert_eq!(
            tracker.get_bytes_written(TENANT_ID).await.unwrap(),
            1000
        );

        op.copy("a.txt", "b.txt").await.unwrap();

        assert_eq!(
            tracker.get_bytes_written(TENANT_ID).await.unwrap(),
            200
        );
    }

    #[tokio::test]
    async fn copy_overwrite_that_would_exceed_quota_is_rejected_and_destination_kept() {
        let tracker = Arc::new(MemoryTracker::default());
        let (op, _dir) =
            build_op(TENANT_ID, Arc::clone(&tracker), 1_000_000);

        op.write("a.txt", vec![0u8; 950_000]).await.unwrap();
        op.write("b.txt", vec![0u8; 10_000]).await.unwrap();

        assert_eq!(
            tracker.get_bytes_written(TENANT_ID).await.unwrap(),
            960_000
        );

        let err = op.copy("a.txt", "b.txt").await.unwrap_err();

        assert_eq!(err.kind(), ErrorKind::RateLimited);

        assert_eq!(
            tracker.get_bytes_written(TENANT_ID).await.unwrap(),
            960_000
        );

        let meta = op.stat("b.txt").await.unwrap();
        assert_eq!(meta.content_length(), 10_000);
    }

    #[tokio::test]
    async fn multiple_deletes_in_one_batch_all_release() {
        let tracker = Arc::new(MemoryTracker::default());
        let (op, _dir) = build_op(TENANT_ID, Arc::clone(&tracker), 1024);

        op.write("a.txt", "hello").await.unwrap();
        op.write("b.txt", "world!").await.unwrap();

        let total_before = tracker.get_bytes_written(TENANT_ID).await.unwrap();
        assert_eq!(total_before, 11);

        let mut deleter = op.deleter().await.unwrap();
        deleter.delete("a.txt").await.unwrap();
        deleter.delete("b.txt").await.unwrap();
        deleter.close().await.unwrap();

        assert_eq!(tracker.get_bytes_written(TENANT_ID).await.unwrap(), 0);
    }

    // --- MemoryTracker::apply_delta atomicity ---

    #[tokio::test]
    async fn apply_delta_rejects_without_mutating_when_over_limit() {
        let tracker = MemoryTracker::default();

        tracker.apply_delta("id", 0, 100, 1000).await.unwrap();

        let err = tracker
            .apply_delta("id", 0, 950, 1000)
            .await
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::RateLimited);

        // Rejected delta must not have mutated the stored value.
        assert_eq!(tracker.get_bytes_written("id").await.unwrap(), 100);
    }

    #[tokio::test]
    async fn concurrent_apply_deltas_never_exceed_the_limit() {
        let tracker = Arc::new(MemoryTracker::default());
        let limit = 1000u64;

        // 20 concurrent "writes" of 100 bytes each as brand-new objects
        // (old_size = 0). If apply_delta were a naive get-then-set, more
        // than 10 of these could race past the limit; atomically, exactly
        // 10 should succeed and the rest should be rejected.
        let handles: Vec<_> = (0..20)
            .map(|_| {
                let t = Arc::clone(&tracker);
                tokio::spawn(async move {
                    t.apply_delta("shared", 0, 100, limit).await
                })
            })
            .collect();

        let mut succeeded = 0;

        for h in handles {
            if h.await.unwrap().is_ok() {
                succeeded += 1;
            }
        }

        assert_eq!(succeeded, 10);
        assert_eq!(
            tracker.get_bytes_written("shared").await.unwrap(),
            1000
        );
    }
}
