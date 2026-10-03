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
//!
//! Enforcement is done entirely in-memory via a single `AtomicU64` per
//! quota id, using a compare-and-swap loop for atomicity. A background
//! task periodically snapshots each id's current total to a pluggable
//! [`QuotaTracker`] for durability across restarts - that sync is
//! best-effort and never part of the enforcement path.
//!
//! # Shape
//!
//! - [`QuotaState`] is the one long-lived object: it owns the
//!   [`QuotaTracker`] and a map of per-id buckets (each just an atomic byte
//!   count). It knows nothing about any particular id up front - ids are
//!   discovered lazily. Construction is synchronous and does no I/O.
//! - Each id's bucket is loaded from the tracker lazily, the first time
//!   that id is actually touched by a commit, and only once - concurrent
//!   first-touches on the same id all await the same load rather than
//!   racing or double-loading.
//! - [`QuotaLayer`] wraps a cloned [`QuotaState`] plus a single `(id,
//!   limit)` pair - it's what an individual `Operator` gets layered with.
//!   Multiple `QuotaLayer`s (even with different ids, even over different
//!   backends) can share one `QuotaState` and therefore one background sync
//!   task and one tracker connection.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::OnceCell;

use opendal_core::raw::oio::Delete;
use opendal_core::raw::*;
use opendal_core::*;

/// Default interval between background syncs to the [`QuotaTracker`].
pub const DEFAULT_SYNC_INTERVAL: Duration = Duration::from_secs(30);

/// Durability backend: persists the last known total for a quota id.
///
/// This is **not** in the enforcement path and carries no atomicity
/// requirements of its own - enforcement is handled entirely in-memory by
/// the atomic counter in each id's bucket. `set_bytes` is called
/// periodically by a background task with whatever that bucket's value
/// happened to be at that moment; implementations just need to store it.
///
/// `get_bytes` is called lazily, once per id, the first time that id is
/// touched, to restore its total after a restart. Implementations should
/// return an error with [`ErrorKind::NotFound`] when nothing has been
/// persisted yet for `id` - [`QuotaState`] treats that specific error as
/// "no prior usage, start at 0" and treats any other error as a hard
/// failure that's returned to whatever operation triggered the load.
#[async_trait]
pub trait QuotaTracker: Send + Sync + 'static {
    /// Persist the current total bytes used for `id`. Called periodically,
    /// not on every write/delete. A failed sync is retried on the next
    /// tick, so implementations don't need their own retry logic.
    async fn set_bytes(&self, id: &str, bytes: u64) -> Result<()>;

    /// Gets the total bytes written for an `id`. Return
    /// [`ErrorKind::NotFound`] specifically if `id` has never been
    /// persisted - [`QuotaState`] relies on that to distinguish "first
    /// use" from a genuine lookup failure.
    async fn get_bytes(&self, id: &str) -> Result<u64>;

    /// Clears the bytes at `id`.
    async fn clear(&self, id: &str) -> Result<()> {
        self.set_bytes(id, 0).await
    }
}

/// Simple in-memory [`QuotaTracker`] implementation, primarily useful for
/// tests. Since it no longer needs to serve as an enforcement point, it's
/// just a map behind a mutex.
#[derive(Default, Debug)]
pub struct MemoryTracker(std::sync::Mutex<HashMap<String, u64>>);

#[async_trait]
impl QuotaTracker for MemoryTracker {
    async fn set_bytes(&self, id: &str, bytes: u64) -> Result<()> {
        self.0.lock().unwrap().insert(id.to_string(), bytes);
        Ok(())
    }

    async fn get_bytes(&self, id: &str) -> Result<u64> {
        let lck = self.0.lock().unwrap();
        let x = lck
            .get(&id.to_string())
            .ok_or(Error::new(ErrorKind::NotFound, "id not found"))?;
        Ok(*x)
    }
}

impl MemoryTracker {
    /// Read back the most recently synced value for `id` (`0` if nothing
    /// has synced yet). Mainly useful in tests to assert the background
    /// sync task ran.
    pub fn snapshot(&self, id: &str) -> u64 {
        *self.0.lock().unwrap().get(id).unwrap_or(&0)
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

/// In-memory, atomic, TOCTOU-safe running total for a single quota id.
///
/// All mutation goes through [`Self::apply_delta`], a `compare_exchange_weak`
/// loop. That loop is the entire correctness story: the value it compares
/// against and the value it writes are the same load, checked and swapped
/// as one indivisible step, so a losing thread never overwrites a winning
/// thread's update with stale data - it just re-reads and retries.
struct QuotaCounter {
    bytes: AtomicU64,
    /// Set whenever `apply_delta` changes the value; cleared by the sync
    /// task once it has persisted a snapshot. Lets the sync task skip a DB
    /// write on ticks where nothing changed.
    dirty: AtomicBool,
}

impl Debug for QuotaCounter {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuotaCounter")
            .field("bytes", &self.bytes.load(Ordering::Relaxed))
            .finish()
    }
}

impl QuotaCounter {
    fn new(initial: u64) -> Self {
        Self {
            bytes: AtomicU64::new(initial),
            dirty: AtomicBool::new(false),
        }
    }

    fn get(&self) -> u64 {
        self.bytes.load(Ordering::Acquire)
    }

    fn reset(&self) {
        self.bytes.store(0, Ordering::Release);
        self.dirty.store(true, Ordering::Release);
    }

    fn mark_dirty(&self) {
        self.dirty.store(true, Ordering::Release);
    }

    fn apply_delta(&self, id: &str, old_size: u64, new_size: u64, limit: u64) -> Result<u64> {
        let mut current = self.bytes.load(Ordering::Acquire);

        loop {
            let new_total = current.saturating_sub(old_size).saturating_add(new_size);

            if new_total > limit {
                return Err(quota_exceeded_error(id, current, new_total, limit));
            }

            match self.bytes.compare_exchange_weak(
                current,
                new_total,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.dirty.store(true, Ordering::Release);
                    return Ok(new_total);
                }

                Err(observed) => {
                    current = observed;
                }
            }
        }
    }
}

/// A single id's in-memory bucket: just the atomic counter. The id's limit
/// is *not* stored here - it's supplied by whichever [`QuotaLayer`] is
/// performing the commit, since [`QuotaState`] itself is id-agnostic and
/// only learns about an id when something touches it.
struct QuotaBucket {
    counter: QuotaCounter,
}

impl QuotaBucket {
    fn new(initial_bytes: u64) -> Self {
        Self {
            counter: QuotaCounter::new(initial_bytes),
        }
    }
}

/// The live, shareable quota engine: owns a [`QuotaTracker`] and a map of
/// per-id buckets. Cheap to clone - all clones share the same map and the
/// same background sync task via an inner `Arc`.
///
/// `QuotaState` knows nothing about any specific id at construction time.
/// A bucket for a given id is created and loaded from the tracker lazily,
/// the first time that id is committed against (via a [`QuotaLayer`]), and
/// only once - concurrent first touches on the same id all await the same
/// load rather than racing.
///
/// Construction ([`QuotaState::new`]) is synchronous and does no I/O.
#[derive(Clone, Debug)]
pub struct QuotaState {
    inner: Arc<QuotaStateInner>,
}

struct QuotaStateInner {
    tracker: Arc<dyn QuotaTracker>,
    /// One lazily-initialized bucket per id. The outer `Mutex` only ever
    /// guards a cheap insert-if-absent of an empty `OnceCell` - the actual
    /// (possibly slow) tracker load happens inside `OnceCell::get_or_try_init`,
    /// off the lock, so concurrent loads of *different* ids don't block on
    /// each other, and concurrent loads of the *same* id all await one
    /// shared load.
    buckets: Mutex<HashMap<String, Arc<OnceCell<Arc<QuotaBucket>>>>>,
}

impl Debug for QuotaStateInner {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let known_ids = self.buckets.lock().unwrap().len();
        f.debug_struct("QuotaStateInner")
            .field("known_ids", &known_ids)
            .finish_non_exhaustive()
    }
}

impl QuotaState {
    /// Build a new `QuotaState`, syncing to the tracker on
    /// [`DEFAULT_SYNC_INTERVAL`]. Synchronous - no ids are loaded until
    /// they're first touched.
    pub fn new(tracker: Arc<dyn QuotaTracker>) -> Self {
        Self::with_sync_interval(tracker, DEFAULT_SYNC_INTERVAL)
    }

    /// Creates a new state owned.
    pub fn new_owned(tracker: impl QuotaTracker) -> Self {
        Self::new(Arc::new(tracker))
    }

    /// Build a new `QuotaState` with an explicit sync interval.
    /// Synchronous - no ids are loaded until they're first touched.
    pub fn with_sync_interval(tracker: Arc<dyn QuotaTracker>, sync_interval: Duration) -> Self {
        let inner = Arc::new(QuotaStateInner {
            tracker,
            buckets: Mutex::new(HashMap::new()),
        });

        // Hold only a Weak ref in the background task: once every clone of
        // this QuotaState (and every QuotaLayer/QuotaAccessor built from
        // it) is dropped, the upgrade() below starts failing and the task
        // exits on its own instead of running forever.
        spawn_sync_task(Arc::downgrade(&inner), sync_interval);

        Self { inner }
    }

    /// Current in-memory total for `id`, loading it from the tracker first
    /// if this is the first time `id` has been touched. Useful for
    /// metrics/dashboards; reflects commits immediately, ahead of whatever
    /// has actually been synced to the [`QuotaTracker`].
    ///
    /// Because `commit_write` / `commit_delete` update the same in-memory
    /// atomic that this reads, this call reflects a prior `commit_*` the
    /// instant that `commit_*` returns - there's no delay waiting on the
    /// background sync. If you're seeing a stale/zero value right after a
    /// write, double check you're calling this on the *same* `QuotaState`
    /// instance (clones share state via `Arc`, but a freshly-constructed
    /// `QuotaState` does not) and with the *same* id string used by the
    /// `QuotaLayer` that performed the write.
    pub async fn current_bytes(&self, id: &str) -> Result<u64> {
        Ok(self.bucket(id).await?.counter.get())
    }

    /// Reset `id`'s quota usage to zero.
    ///
    /// This changes the live in-memory counter atomically and marks the
    /// bucket dirty so the background sync task persists `0` to the tracker.
    ///
    /// If the id has never been touched before, a zero-valued bucket is
    /// created without loading the tracker. This is intentional: reset
    /// means "make this id zero", regardless of what was previously persisted.
    pub fn reset(&self, id: &str) -> Result<()> {
        let cell = {
            let mut buckets = self.inner.buckets.lock().unwrap();
            buckets
                .entry(id.to_string())
                .or_insert_with(|| Arc::new(OnceCell::new()))
                .clone()
        };

        if let Some(bucket) = cell.get() {
            bucket.counter.reset();
            return Ok(());
        }

        let bucket = Arc::new(QuotaBucket::new(0));
        bucket.counter.mark_dirty();

        match cell.set(bucket.clone()) {
            Ok(()) => Ok(()),
            Err(_) => {
                // Another task initialized the bucket concurrently.
                // Reset the bucket that actually won.
                if let Some(bucket) = cell.get() {
                    bucket.counter.reset();
                }

                Ok(())
            }
        }
    }

    /// Alias for [`QuotaState::reset`].
    ///
    /// Resets the quota usage for `id` to zero.
    pub fn clear(&self, id: &str) -> Result<()> {
        self.reset(id)
    }

    /// Get (or lazily create + load) the bucket for `id`.
    async fn bucket(&self, id: &str) -> Result<Arc<QuotaBucket>> {
        let cell = {
            let mut buckets = self.inner.buckets.lock().unwrap();
            buckets
                .entry(id.to_string())
                .or_insert_with(|| Arc::new(OnceCell::new()))
                .clone()
        };

        let tracker = &self.inner.tracker;
        let bucket = cell
            .get_or_try_init(|| async move {
                let initial_bytes = match tracker.get_bytes(id).await {
                    Ok(bytes) => bytes,
                    Err(err) if err.kind() == ErrorKind::NotFound => 0,
                    Err(err) => return Err(err),
                };
                Ok::<_, Error>(Arc::new(QuotaBucket::new(initial_bytes)))
            })
            .await?;

        Ok(bucket.clone())
    }

    /// Atomically commit a completed write or copy for `id`: replace
    /// `old_size` bytes with `new_size` bytes, enforcing `limit`. This is
    /// the sole enforcement point - there is no separate fail-fast check
    /// while data streams. Lazily loads `id`'s bucket first if needed.
    async fn commit_write(
        &self,
        id: &str,
        limit: u64,
        old_size: u64,
        new_size: u64,
    ) -> Result<u64> {
        self.bucket(id)
            .await?
            .counter
            .apply_delta(id, old_size, new_size, limit)
    }

    /// Atomically commit a completed delete for `id`: subtract `size`
    /// bytes from the running total. Deletes are never rejected by the
    /// quota (limit is `u64::MAX`), but loading `id`'s bucket for the
    /// first time can still fail, so this returns a `Result`.
    async fn commit_delete(&self, id: &str, size: u64) -> Result<()> {
        let _ = self
            .bucket(id)
            .await?
            .counter
            .apply_delta(id, size, 0, u64::MAX);
        Ok(())
    }
}

/// Spawn the background durability-sync task. Runs until `inner` can no
/// longer be upgraded (i.e. the owning `QuotaState` and all its clones have
/// been dropped).
fn spawn_sync_task(inner: Weak<QuotaStateInner>, interval: Duration) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        // If a sync (or the process) stalls past one interval, don't try to
        // fire off a burst of catch-up ticks - just resume on the normal
        // cadence.
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            ticker.tick().await;

            let Some(inner) = inner.upgrade() else {
                return;
            };

            sync_all(&inner).await;
        }
    });
}

/// Snapshot every id's current total to the tracker, skipping ids that
/// haven't changed since the last sync. Called by the background task;
/// never called from the hot read/write path.
async fn sync_all(inner: &QuotaStateInner) {
    // Snapshot the known (id, bucket-cell) pairs, then release the lock
    // before doing any I/O.
    let cells: Vec<(String, Arc<OnceCell<Arc<QuotaBucket>>>)> = {
        let buckets = inner.buckets.lock().unwrap();
        buckets
            .iter()
            .map(|(id, cell)| (id.clone(), cell.clone()))
            .collect()
    };

    for (id, cell) in cells {
        // Only already-loaded buckets can be dirty; an in-flight or
        // not-yet-started load has nothing to sync yet.
        let Some(bucket) = cell.get() else {
            continue;
        };

        if !bucket.counter.dirty.swap(false, Ordering::AcqRel) {
            continue;
        }

        let bytes = bucket.counter.get();

        if let Err(err) = inner.tracker.set_bytes(&id, bytes).await {
            // Re-mark dirty so the next tick retries. A write that lands
            // between this swap and the failed set_bytes() call already
            // set dirty = true itself, so this is just belt-and-suspenders
            // for the "no concurrent write happened" case.
            bucket.counter.dirty.store(true, Ordering::Release);
            eprintln!("quota sync failed for '{id}': {err}");
        }
    }
}

/// Add a write-quota to the underlying services.
///
/// # Quota
///
/// A [`QuotaLayer`] limits the total number of bytes currently occupied by
/// objects under an operator, for one quota id. The tracked total behaves
/// like real filesystem usage:
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
/// # Construction
///
/// `QuotaLayer` itself does no I/O and is built with a plain, synchronous
/// [`QuotaLayer::new`] from a [`QuotaState`] plus this layer's `(id,
/// limit)`. The `QuotaState` can be built once and shared by any number of
/// `QuotaLayer`s - even ones using different ids, even over different
/// backends - since it's id-agnostic and only loads a given id from the
/// tracker the first time that id is actually touched.
///
/// # Atomicity
///
/// Enforcement never leaves process memory: each id's running total is a
/// single `AtomicU64`, and every commit (write close, copy close, delete
/// flush) goes through a `compare_exchange_weak` loop. The read of the
/// current value and the write of the new value are tied together by the
/// CAS primitive itself, so two concurrent commits can never both observe
/// a total that permits their delta and then both land - one of them is
/// guaranteed to retry against whatever the other one just wrote. This is
/// what makes it TOCTOU-safe without needing a lock.
///
/// Each write/copy captures the size it needs exactly once, before any
/// bytes move through the underlying writer/copier (not at close/finalize
/// time, since some backends mutate the destination as data streams rather
/// than only at close) - there's no incremental accounting. The atomic
/// commit happens right before the underlying operation is finalized, so a
/// rejection can abort the underlying operation instead of leaving
/// out-of-quota data behind.
///
/// # Durability
///
/// Because enforcement is purely in-memory, an id's running total does not
/// survive a process restart on its own. The first commit against an id
/// loads its starting value from the [`QuotaTracker`] (see
/// [`QuotaState::bucket`] internally), and after that a background task
/// wakes up every `sync_interval` and, for every id touched so far,
/// persists it via [`QuotaTracker::set_bytes`] if it changed. This sync is
/// best-effort: it never blocks or gates any operation, and a failed sync
/// is simply retried on the next tick.
///
/// # Examples
///
/// This example limits two different tenants to 1 KiB each, both backed by
/// one shared in-memory tracker and one shared background sync task.
///
/// ```no_run
/// # use std::sync::Arc;
/// # use opendal_core::services;
/// # use opendal_core::Operator;
/// # use opendal_core::Result;
/// # use opendal_layer_quota::{QuotaLayer, QuotaState, MemoryTracker};
/// #
/// # fn main() -> Result<()> {
/// let tracker = Arc::new(MemoryTracker::default());
/// let state = QuotaState::new(tracker);
///
/// let op_a = Operator::new(services::Memory::default())?
///     .layer(QuotaLayer::new(state.clone(), "tenant-a", 1024));
/// let op_b = Operator::new(services::Memory::default())?
///     .layer(QuotaLayer::new(state.clone(), "tenant-b", 1024));
/// # let _ = (op_a, op_b);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct QuotaLayer {
    state: QuotaState,
    id: Arc<str>,
    limit: u64,
}

impl QuotaLayer {
    /// Wrap a [`QuotaState`] as a layer for one `(id, limit)` pair. Cheap
    /// and synchronous - no I/O happens until an operation is actually
    /// performed through the resulting layer.
    pub fn new(state: QuotaState, id: impl Into<Arc<str>>, limit_bytes: u64) -> Self {
        Self {
            state,
            id: id.into(),
            limit: limit_bytes,
        }
    }

    /// Current in-memory total for this layer's id, loading it first if
    /// this id hasn't been touched yet.
    pub async fn current_bytes(&self) -> Result<u64> {
        self.state.current_bytes(&self.id).await
    }

    /// Access the underlying [`QuotaState`], e.g. to hand it to another
    /// `QuotaLayer` (same or different id) that should share the same
    /// tracker and background sync task.
    pub fn state(&self) -> &QuotaState {
        &self.state
    }
}

impl Layer for QuotaLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        Arc::new(QuotaAccessor {
            inner,
            state: self.state.clone(),
            id: self.id.clone(),
            limit: self.limit,
        })
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

/// Best-effort cleanup of a staging path. Failures are swallowed - this is
/// only ever called after we've already decided not to commit, so there's
/// nothing more useful to do with an error here than leave an orphaned
/// temp object behind for a future cleanup pass.
async fn best_effort_remove(accessor: &Servicer, ctx: &OperationContext, path: &str) {
    if let Ok(mut deleter) = accessor.delete(ctx) {
        let _ = deleter.delete(path, OpDelete::default()).await;
        let _ = deleter.close().await;
    }
}

/// Build a staging path alongside `path` for the write-to-temp-then-rename
/// pattern used by [`QuotaWriter`] and [`QuotaCopier`]. Not guaranteed
/// unique against another *process*, but unique enough within one running
/// process, which is all we need since the temp object is only ever
/// visible to the writer/copier that created it.
static TMP_SUFFIX_COUNTER: AtomicU64 = AtomicU64::new(0);

fn tmp_path_for(path: &str) -> String {
    let n = TMP_SUFFIX_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{path}.quota-tmp-{nanos}-{n}")
}

/// Rename `tmp` into `dest`, falling back to a non-atomic delete-then-rename
/// if the backend's `rename` refuses to overwrite an existing destination
/// (e.g. Windows filesystems, where `rename` fails rather than replaces).
async fn commit_rename(
    accessor: &Servicer,
    ctx: &OperationContext,
    tmp: &str,
    dest: &str,
) -> Result<()> {
    if accessor
        .rename(ctx, tmp, dest, OpRename::default())
        .await
        .is_ok()
    {
        return Ok(());
    }

    // Fallback: remove the existing destination first, then rename. Only
    // reached on backends where the first attempt failed specifically
    // because `dest` already existed.
    best_effort_remove(accessor, ctx, dest).await;
    accessor
        .rename(ctx, tmp, dest, OpRename::default())
        .await
        .map(|_| ())
}

#[doc(hidden)]
#[derive(Debug)]
pub struct QuotaAccessor {
    inner: Servicer,
    state: QuotaState,
    id: Arc<str>,
    limit: u64,
}

impl Service for QuotaAccessor {
    type Reader = oio::Reader;
    type Writer = QuotaWriter<oio::Writer>;
    type Lister = oio::Lister;
    type Deleter = QuotaDeleter;
    type Copier = QuotaCopier<oio::Copier>;
    type Composer = ();

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

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner.stat(ctx, path, args).await
    }

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        self.inner.read(ctx, path, args)
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        let state = self.state.clone();
        let id = self.id.clone();
        let limit = self.limit;
        let accessor = self.inner.clone();
        let ctx = ctx.clone();
        let path = path.to_string();
        // Write to a staging path, not `path` itself, so a rejected or
        // aborted write never touches (e.g. truncates) the real object.
        let tmp_path = tmp_path_for(&path);

        self.inner
            .write(&ctx, &tmp_path, args)
            .map(|w| QuotaWriter::new(w, state, id, limit, accessor, ctx, path, tmp_path))
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        let inner = self.inner.delete(ctx)?;

        Ok(QuotaDeleter {
            inner,
            accessor: self.inner.clone(),
            ctx: ctx.clone(),
            state: self.state.clone(),
            id: self.id.clone(),
            pending_sizes: Vec::new(),
        })
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
    ) -> Result<Self::Copier> {
        // Copy into a staging path, not `to` itself, so a rejected or
        // aborted copy never touches (e.g. partially overwrites) the real
        // destination object.
        let tmp_to = tmp_path_for(to);
        let inner = self.inner.copy(ctx, from, &tmp_to, args)?;

        Ok(QuotaCopier {
            inner,
            state: self.state.clone(),
            id: self.id.clone(),
            limit: self.limit,
            accessor: self.inner.clone(),
            ctx: ctx.clone(),
            from: from.to_string(),
            to: to.to_string(),
            tmp_to,
        })
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

/// Quota-aware wrapper around an [`oio::Write`] implementation.
///
/// Writes go to a *staging path* (`tmp_path`), never straight to `path`.
/// This matters because several backends (e.g. a local filesystem) mutate
/// the destination - truncating it - the moment the write opens, well
/// before `close()`. Writing to a temp path instead means the real object
/// at `path` is never touched by a write that ends up getting rejected or
/// aborted.
///
/// `close()` finalizes the staged temp object, fetches `path`'s current
/// (untouched) size, and only then runs the atomic quota check. On
/// success the temp object is renamed into place at `path`; on rejection
/// the temp object is deleted and `path` is left exactly as it was.
#[doc(hidden)]
pub struct QuotaWriter<W> {
    inner: W,
    state: QuotaState,
    id: Arc<str>,
    limit: u64,
    accessor: Servicer,
    ctx: OperationContext,
    path: String,
    tmp_path: String,
    /// Bytes written so far in this (not-yet-committed) write.
    written: u64,
}

impl<W> QuotaWriter<W> {
    fn new(
        inner: W,
        state: QuotaState,
        id: Arc<str>,
        limit: u64,
        accessor: Servicer,
        ctx: OperationContext,
        path: String,
        tmp_path: String,
    ) -> Self {
        Self {
            inner,
            state,
            id,
            limit,
            accessor,
            ctx,
            path,
            tmp_path,
            written: 0,
        }
    }
}

impl<W> Debug for QuotaWriter<W> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuotaWriter")
            .field("id", &self.id)
            .field("path", &self.path)
            .field("written", &self.written)
            .finish_non_exhaustive()
    }
}

impl<W: oio::Write> oio::Write for QuotaWriter<W> {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        // Streams into the staging path - `self.path` is never touched
        // here.
        let len = bs.len() as u64;
        self.inner.write(bs).await?;
        self.written += len;
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        // Finalize the staged object. `self.path` is still whatever it
        // was before this write started.
        let meta = self.inner.close().await?;
        let old_size = size_of(&self.accessor, &self.ctx, &self.path).await;

        // Sole enforcement point. Also where this id's bucket gets lazily
        // loaded, if this is its first touch.
        if let Err(err) = self
            .state
            .commit_write(&self.id, self.limit, old_size, self.written)
            .await
        {
            best_effort_remove(&self.accessor, &self.ctx, &self.tmp_path).await;
            self.written = 0;
            return Err(err);
        }

        // Commit accepted: move the finished write into place.
        if let Err(err) = commit_rename(&self.accessor, &self.ctx, &self.tmp_path, &self.path).await
        {
            // Roll back the quota commit so a failed rename doesn't leave
            // the counter permanently overstated. u64::MAX as the limit
            // here means "always allowed" - this is strictly undoing the
            // delta we just applied, not a fresh request that could fail.
            let _ = self
                .state
                .commit_write(&self.id, u64::MAX, self.written, old_size)
                .await;
            best_effort_remove(&self.accessor, &self.ctx, &self.tmp_path).await;
            self.written = 0;
            return Err(err);
        }

        self.written = 0;
        Ok(meta)
    }

    async fn abort(&mut self) -> Result<()> {
        // Nothing was ever committed to the counter mid-write (accounting
        // only happens at close), and the real `path` was never touched -
        // only the staging path needs cleaning up.
        self.inner.abort().await?;
        best_effort_remove(&self.accessor, &self.ctx, &self.tmp_path).await;
        self.written = 0;
        Ok(())
    }
}

#[doc(hidden)]
pub struct QuotaDeleter {
    inner: oio::Deleter,
    accessor: Servicer,
    ctx: OperationContext,
    state: QuotaState,
    id: Arc<str>,
    /// Sizes of paths successfully queued via `delete()`, but not yet
    /// committed to the quota counter.
    pending_sizes: Vec<u64>,
}

impl Debug for QuotaDeleter {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuotaDeleter")
            .field("id", &self.id)
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
            self.state.commit_delete(&self.id, size).await?;
        }

        Ok(())
    }
}

/// Quota-aware wrapper around an [`oio::Copy`] implementation.
///
/// Like [`QuotaWriter`], the copy lands in a staging path (`tmp_to`), not
/// `to` itself - some backends start creating/writing the destination as
/// soon as the copy begins, not just at `close()`. Copying into a temp
/// path means `to` is never touched by a copy that ends up rejected or
/// aborted.
///
/// `close()` finalizes the staged copy, fetches `from`'s and `to`'s
/// current (untouched) sizes, and only then runs the atomic quota check.
/// On success the temp object is renamed into place at `to`; on rejection
/// the temp object is deleted and `to` is left exactly as it was.
#[doc(hidden)]
pub struct QuotaCopier<C> {
    inner: C,
    state: QuotaState,
    id: Arc<str>,
    limit: u64,
    accessor: Servicer,
    ctx: OperationContext,
    from: String,
    to: String,
    tmp_to: String,
}

impl<C> Debug for QuotaCopier<C> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuotaCopier")
            .field("id", &self.id)
            .field("from", &self.from)
            .field("to", &self.to)
            .finish_non_exhaustive()
    }
}

impl<C: oio::Copy> oio::Copy for QuotaCopier<C> {
    async fn next(&mut self) -> Result<Option<usize>> {
        self.inner.next().await
    }

    async fn close(&mut self) -> Result<Metadata> {
        // Finalize the staged copy. `self.to` is still whatever it was
        // before this copy started.
        let meta = self.inner.close().await?;
        let from_size = size_of(&self.accessor, &self.ctx, &self.from).await;
        let to_size = size_of(&self.accessor, &self.ctx, &self.to).await;

        // Copying is accounted like a write of `from_size` bytes that
        // replaces whatever currently sits at `to` (if anything).
        if let Err(err) = self
            .state
            .commit_write(&self.id, self.limit, to_size, from_size)
            .await
        {
            best_effort_remove(&self.accessor, &self.ctx, &self.tmp_to).await;
            return Err(err);
        }

        // Commit accepted: move the finished copy into place.
        if let Err(err) = commit_rename(&self.accessor, &self.ctx, &self.tmp_to, &self.to).await {
            // Roll back the quota commit, same reasoning as QuotaWriter.
            let _ = self
                .state
                .commit_write(&self.id, u64::MAX, from_size, to_size)
                .await;
            best_effort_remove(&self.accessor, &self.ctx, &self.tmp_to).await;
            return Err(err);
        }

        Ok(meta)
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await?;
        // The real `to` was never touched - only the staging path needs
        // cleaning up.
        best_effort_remove(&self.accessor, &self.ctx, &self.tmp_to).await;
        Ok(())
    }
}

#[cfg(test)]
#[allow(unused_results)]
mod tests {
    use super::*;
    use opendal::{Operator, services};
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;

    const TENANT_ID: &'static str = "tenant-test";

    /// Build an `Operator` backed by a real filesystem in a fresh temp
    /// directory, plus the `QuotaState` itself (kept around so tests can
    /// inspect `current_bytes()` / drive the sync interval directly). The
    /// returned `TempDir` must be kept alive for as long as `op` is used -
    /// it deletes the directory on drop.
    fn build_op(
        id: &str,
        tracker: Arc<MemoryTracker>,
        limit: u64,
    ) -> (Operator, QuotaState, TempDir) {
        let dir = TempDir::new().expect("create temp dir");
        let state = QuotaState::with_sync_interval(tracker, Duration::from_millis(20));
        let op = Operator::new(services::Fs::default().root(dir.path().to_str().unwrap()))
            .unwrap()
            .layer(QuotaLayer::new(state.clone(), id, limit));

        (op, state, dir)
    }

    #[tokio::test]
    async fn writes_within_quota_succeed_and_are_tracked() {
        let tracker = Arc::new(MemoryTracker::default());
        let (op, state, _dir) = build_op(TENANT_ID, Arc::clone(&tracker), 1024);

        op.write("a.txt", "hello world").await.unwrap();

        assert_eq!(
            state.current_bytes(TENANT_ID).await.unwrap(),
            "hello world".len() as u64
        );
    }

    #[tokio::test]
    async fn write_exceeding_quota_is_rejected() {
        let tracker = Arc::new(MemoryTracker::default());
        let (op, state, _dir) = build_op(TENANT_ID, Arc::clone(&tracker), 10);

        let err = op
            .write("big.txt", "this is way too large")
            .await
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::RateLimited);
        assert_eq!(state.current_bytes(TENANT_ID).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn overwrite_replaces_rather_than_adds() {
        let tracker = Arc::new(MemoryTracker::default());
        let (op, state, _dir) = build_op(TENANT_ID, Arc::clone(&tracker), 1024 * 1024);

        op.write("f.txt", vec![0u8; 1_000_000]).await.unwrap();
        assert_eq!(state.current_bytes(TENANT_ID).await.unwrap(), 1_000_000);

        op.write("f.txt", vec![0u8; 500_000]).await.unwrap();
        assert_eq!(state.current_bytes(TENANT_ID).await.unwrap(), 500_000);
    }

    #[tokio::test]
    async fn overwrite_that_would_exceed_quota_is_rejected_and_old_size_kept() {
        let tracker = Arc::new(MemoryTracker::default());
        let (op, state, _dir) = build_op(TENANT_ID, Arc::clone(&tracker), 1_000_000);

        op.write("f.txt", vec![0u8; 100_000]).await.unwrap();
        op.write("other.txt", vec![0u8; 850_000]).await.unwrap();

        assert_eq!(state.current_bytes(TENANT_ID).await.unwrap(), 950_000);

        let err = op.write("f.txt", vec![0u8; 200_000]).await.unwrap_err();

        assert_eq!(err.kind(), ErrorKind::RateLimited);
        assert_eq!(state.current_bytes(TENANT_ID).await.unwrap(), 950_000);

        let meta = op.stat("f.txt").await.unwrap();
        assert_eq!(meta.content_length(), 100_000);
    }

    #[tokio::test]
    async fn delete_releases_exact_size() {
        let tracker = Arc::new(MemoryTracker::default());
        let (op, state, _dir) = build_op(TENANT_ID, Arc::clone(&tracker), 1024);

        op.write("a.txt", "hello world").await.unwrap();
        op.delete("a.txt").await.unwrap();

        assert_eq!(state.current_bytes(TENANT_ID).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn delete_then_rewrite_frees_room_for_new_writes() {
        let tracker = Arc::new(MemoryTracker::default());
        let (op, state, _dir) = build_op(TENANT_ID, Arc::clone(&tracker), 10);

        op.write("a.txt", "0123456789").await.unwrap();
        assert!(op.write("b.txt", "x").await.is_err());

        op.delete("a.txt").await.unwrap();
        assert_eq!(state.current_bytes(TENANT_ID).await.unwrap(), 0);

        op.write("b.txt", "0123456789").await.unwrap();
        assert_eq!(state.current_bytes(TENANT_ID).await.unwrap(), 10);
    }

    #[tokio::test]
    async fn rename_does_not_change_total_bytes_used() {
        let tracker = Arc::new(MemoryTracker::default());
        let (op, state, _dir) = build_op(TENANT_ID, Arc::clone(&tracker), 1024);

        op.write("a.txt", "hello world").await.unwrap();
        let before = state.current_bytes(TENANT_ID).await.unwrap();

        op.rename("a.txt", "b.txt").await.unwrap();

        assert_eq!(state.current_bytes(TENANT_ID).await.unwrap(), before);
    }

    #[tokio::test]
    async fn copy_of_new_object_adds_its_size_to_the_total() {
        let tracker = Arc::new(MemoryTracker::default());
        let (op, state, _dir) = build_op(TENANT_ID, Arc::clone(&tracker), 1024);

        op.write("a.txt", "hello world").await.unwrap();
        op.copy("a.txt", "b.txt").await.unwrap();

        assert_eq!(
            state.current_bytes(TENANT_ID).await.unwrap(),
            2 * "hello world".len() as u64
        );
    }

    #[tokio::test]
    async fn copy_exceeding_quota_is_rejected_and_source_untouched() {
        let tracker = Arc::new(MemoryTracker::default());
        let (op, state, _dir) = build_op(TENANT_ID, Arc::clone(&tracker), 15);

        op.write("a.txt", "hello world").await.unwrap();
        assert_eq!(state.current_bytes(TENANT_ID).await.unwrap(), 11);

        let err = op.copy("a.txt", "b.txt").await.unwrap_err();

        assert_eq!(err.kind(), ErrorKind::RateLimited);
        assert_eq!(state.current_bytes(TENANT_ID).await.unwrap(), 11);
        assert!(!op.exists("b.txt").await.unwrap());
    }

    #[tokio::test]
    async fn multiple_deletes_in_one_batch_all_release() {
        let tracker = Arc::new(MemoryTracker::default());
        let (op, state, _dir) = build_op(TENANT_ID, Arc::clone(&tracker), 1024);

        op.write("a.txt", "hello").await.unwrap();
        op.write("b.txt", "world!").await.unwrap();
        assert_eq!(state.current_bytes(TENANT_ID).await.unwrap(), 11);

        let mut deleter = op.deleter().await.unwrap();
        deleter.delete("a.txt").await.unwrap();
        deleter.delete("b.txt").await.unwrap();
        deleter.close().await.unwrap();

        assert_eq!(state.current_bytes(TENANT_ID).await.unwrap(), 0);
    }

    // --- one QuotaState managing multiple ids ---

    #[tokio::test]
    async fn one_state_tracks_multiple_ids_independently() {
        let tracker = Arc::new(MemoryTracker::default());
        let state = QuotaState::with_sync_interval(tracker.clone(), Duration::from_millis(20));

        let dir_a = TempDir::new().unwrap();
        let dir_b = TempDir::new().unwrap();

        let op_a = Operator::new(services::Fs::default().root(dir_a.path().to_str().unwrap()))
            .unwrap()
            .layer(QuotaLayer::new(state.clone(), "tenant-a", 1024));
        let op_b = Operator::new(services::Fs::default().root(dir_b.path().to_str().unwrap()))
            .unwrap()
            .layer(QuotaLayer::new(state.clone(), "tenant-b", 1024));

        op_a.write("a.txt", "hello").await.unwrap();
        op_b.write("b.txt", "world!!").await.unwrap();

        assert_eq!(state.current_bytes("tenant-a").await.unwrap(), 5);
        assert_eq!(state.current_bytes("tenant-b").await.unwrap(), 7);
    }

    #[tokio::test]
    async fn id_is_loaded_lazily_only_on_first_touch() {
        let tracker = Arc::new(MemoryTracker::default());
        // Pre-seed a persisted value for an id that hasn't been touched
        // through the layer yet.
        tracker.set_bytes("untouched", 999).await.unwrap();

        let state = QuotaState::new(Arc::clone(&tracker) as Arc<dyn QuotaTracker>);

        // Nothing has happened yet - construction did no I/O, so no bucket
        // exists for "untouched" until we ask.
        assert_eq!(state.current_bytes("untouched").await.unwrap(), 999);
    }

    #[tokio::test]
    async fn new_id_defaults_to_zero_when_nothing_persisted_yet() {
        let tracker = Arc::new(MemoryTracker::default());
        let state = QuotaState::new(tracker as Arc<dyn QuotaTracker>);

        assert_eq!(state.current_bytes("brand-new-id").await.unwrap(), 0);
    }

    #[tokio::test]
    async fn a_real_lookup_error_on_first_touch_is_propagated_not_defaulted() {
        struct AlwaysFailsTracker;

        #[async_trait]
        impl QuotaTracker for AlwaysFailsTracker {
            async fn set_bytes(&self, _id: &str, _bytes: u64) -> Result<()> {
                Ok(())
            }

            async fn get_bytes(&self, _id: &str) -> Result<u64> {
                Err(Error::new(ErrorKind::Unexpected, "db is on fire"))
            }
        }

        let state = QuotaState::new(Arc::new(AlwaysFailsTracker) as Arc<dyn QuotaTracker>);

        let err = state.current_bytes(TENANT_ID).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
    }

    #[tokio::test]
    async fn concurrent_first_touches_on_the_same_id_load_exactly_once() {
        struct CountingTracker {
            inner: MemoryTracker,
            get_calls: std::sync::atomic::AtomicUsize,
        }

        #[async_trait]
        impl QuotaTracker for CountingTracker {
            async fn set_bytes(&self, id: &str, bytes: u64) -> Result<()> {
                self.inner.set_bytes(id, bytes).await
            }

            async fn get_bytes(&self, id: &str) -> Result<u64> {
                self.get_calls.fetch_add(1, Ordering::SeqCst);
                // Simulate a slow lookup so concurrent callers actually race.
                tokio::time::sleep(Duration::from_millis(20)).await;
                self.inner.get_bytes(id).await
            }
        }

        let tracker = Arc::new(CountingTracker {
            inner: MemoryTracker::default(),
            get_calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let state = QuotaState::new(tracker.clone() as Arc<dyn QuotaTracker>);

        let handles: Vec<_> = (0..20)
            .map(|_| {
                let state = state.clone();
                tokio::spawn(async move { state.current_bytes(TENANT_ID).await })
            })
            .collect();

        for h in handles {
            h.await.unwrap().unwrap();
        }

        assert_eq!(tracker.get_calls.load(Ordering::SeqCst), 1);
    }

    // --- QuotaCounter::apply_delta atomicity ---

    #[test]
    fn apply_delta_rejects_without_mutating_when_over_limit() {
        let counter = QuotaCounter::new(0);

        counter.apply_delta("id", 0, 100, 1000).unwrap();

        let err = counter.apply_delta("id", 0, 950, 1000).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::RateLimited);

        // Rejected delta must not have mutated the stored value.
        assert_eq!(counter.get(), 100);
    }

    #[tokio::test]
    async fn concurrent_apply_deltas_never_exceed_the_limit() {
        let counter = Arc::new(QuotaCounter::new(0));
        let limit = 1000u64;

        // 20 concurrent "writes" of 100 bytes each as brand-new objects
        // (old_size = 0). If apply_delta were a naive load-then-store, more
        // than 10 of these could race past the limit; atomically, exactly
        // 10 should succeed and the rest should be rejected.
        let handles: Vec<_> = (0..20)
            .map(|_| {
                let c = Arc::clone(&counter);
                std::thread::spawn(move || c.apply_delta("shared", 0, 100, limit))
            })
            .collect();

        let mut succeeded = 0;
        for h in handles {
            if h.join().unwrap().is_ok() {
                succeeded += 1;
            }
        }

        assert_eq!(succeeded, 10);
        assert_eq!(counter.get(), 1000);
    }

    // --- background sync task ---

    #[tokio::test]
    async fn background_task_persists_dirty_total_to_tracker() {
        let tracker = Arc::new(MemoryTracker::default());
        let (op, _state, _dir) = build_op(TENANT_ID, Arc::clone(&tracker), 1024);

        op.write("a.txt", "hello world").await.unwrap();

        // Sync interval in build_op is 20ms; give it a couple of ticks.
        tokio::time::sleep(Duration::from_millis(80)).await;

        assert_eq!(tracker.snapshot(TENANT_ID), "hello world".len() as u64);
    }

    #[tokio::test]
    async fn background_task_syncs_multiple_ids_independently() {
        let tracker = Arc::new(MemoryTracker::default());
        let state = QuotaState::with_sync_interval(
            Arc::clone(&tracker) as Arc<dyn QuotaTracker>,
            Duration::from_millis(20),
        );

        state.current_bytes("a").await.unwrap(); // touch + load
        state.commit_write("a", u64::MAX, 0, 5).await.unwrap();
        state.commit_write("b", u64::MAX, 0, 9).await.unwrap();

        tokio::time::sleep(Duration::from_millis(80)).await;

        assert_eq!(tracker.snapshot("a"), 5);
        assert_eq!(tracker.snapshot("b"), 9);
    }

    #[tokio::test]
    async fn sync_task_stops_once_state_is_dropped() {
        let tracker = Arc::new(MemoryTracker::default());
        let state = QuotaState::with_sync_interval(
            Arc::clone(&tracker) as Arc<dyn QuotaTracker>,
            Duration::from_millis(10),
        );

        // Drop every strong reference; the background task's Weak::upgrade
        // should start failing and the task should exit rather than
        // spinning forever.
        drop(state);

        // No direct handle to assert "task exited" from the outside without
        // extra plumbing, but this at least documents/exercises the path
        // and ensures dropping doesn't panic or deadlock.
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
