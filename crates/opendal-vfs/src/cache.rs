//! Per-mount caching: metadata (`stat`) cache and a read block cache with
//! sequential read-ahead.
//!
//! Why this exists: network object stores (B2 in particular) have high
//! per-request latency, and a B2 `stat` is a `b2_list_file_names` call. A
//! file server (SMB/NFS/WebDAV) on top of a VFS issues *lots* of tiny
//! `stat`s and 1 MiB reads. Without caching, every 1 MiB read costs several
//! network round trips.
//!
//! * [`MountCache::stat`] caches positive *and* negative (`NotFound`) results
//!   for a short TTL, and coalesces concurrent identical lookups into one
//!   backend call.
//! * [`MountCache::read`] serves reads from large aligned blocks (default
//!   4 MiB) held in a byte-weighted LRU, and prefetches upcoming blocks when
//!   it detects a sequential read pattern.
//! * Every mutating operation routed through the VFS calls
//!   [`MountCache::invalidate`].

use std::collections::hash_map::DefaultHasher;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use moka::Expiry;
use moka::future::Cache;
use opendal_core::{Buffer, Error, ErrorKind, Metadata, Operator, Result};

/// Backends whose `list` returns the same *complete* metadata as `stat`, so
/// listing results can safely pre-populate the stat cache. (Backends like
/// `fs` return partial metadata from list, which must never be cached as if
/// it were a stat result.)
const SEEDABLE_SCHEMES: &[&str] = &["b2"];

/// Tunables for [`MountCache`]. See [`CacheConfig::default`] for values.
#[derive(Clone, Debug)]
pub struct CacheConfig {
    /// How long a successful `stat` is reused. `Duration::ZERO` disables the
    /// stat cache entirely.
    pub stat_ttl: Duration,
    /// How long a `NotFound` stat is reused.
    pub negative_stat_ttl: Duration,
    /// Max number of cached stat entries per mount.
    pub stat_capacity: u64,
    /// Size of one cached read block in bytes. Reads are served from aligned
    /// blocks of this size.
    pub block_size: u64,
    /// Total bytes of read blocks cached per mount. `0` disables the block
    /// cache (reads go straight to the backend).
    pub block_cache_bytes: u64,
    /// How many blocks beyond the current one to prefetch on sequential reads.
    pub readahead_blocks: u64,
    /// Upper bound on concurrently running prefetch tasks per mount.
    pub max_inflight_prefetch: usize,
    /// Pre-populate the stat cache from directory listings (only for
    /// backends in an internal allow-list, currently `b2`).
    pub seed_stat_from_list: bool,
    /// Part size used for multipart-capable writers (B2/S3/...).
    pub write_chunk: usize,
    /// Number of parts uploaded concurrently by multipart-capable writers.
    pub write_concurrency: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            stat_ttl: Duration::from_secs(10),
            negative_stat_ttl: Duration::from_secs(2),
            stat_capacity: 50_000,
            block_size: 4 * 1024 * 1024,
            block_cache_bytes: 256 * 1024 * 1024,
            readahead_blocks: 2,
            max_inflight_prefetch: 8,
            seed_stat_from_list: true,
            write_chunk: 8 * 1024 * 1024,
            write_concurrency: 4,
        }
    }
}

impl CacheConfig {
    /// Everything off: no stat cache, no block cache, no read-ahead, default
    /// writer settings. Useful for tests or debugging.
    pub fn disabled() -> Self {
        Self {
            stat_ttl: Duration::ZERO,
            block_cache_bytes: 0,
            readahead_blocks: 0,
            seed_stat_from_list: false,
            write_concurrency: 1,
            ..Self::default()
        }
    }
}

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
struct BlockKey {
    path: String,
    /// Fingerprint of the stat metadata the block was read under; a changed
    /// object naturally misses.
    stamp: u64,
    idx: u64,
}

struct StatExpiry {
    pos: Duration,
    neg: Duration,
}

impl Expiry<String, Option<Metadata>> for StatExpiry {
    fn expire_after_create(
        &self,
        _key: &String,
        value: &Option<Metadata>,
        _created_at: Instant,
    ) -> Option<Duration> {
        Some(if value.is_some() { self.pos } else { self.neg })
    }
}

/// The cache attached to a single mount. Cheap to clone (all `Arc`s inside).
#[derive(Clone, Debug)]
pub struct MountCache {
    cfg: Arc<CacheConfig>,
    stat: Cache<String, Option<Metadata>>,
    blocks: Cache<BlockKey, Buffer>,
    /// path -> high-water mark of the most recent read, for sequential
    /// detection.
    streams: Cache<String, u64>,
    inflight: Arc<AtomicUsize>,
    seed: bool,
}

/// Fingerprint of the facets of [`Metadata`] that identify one version of an
/// object.
pub(crate) fn stamp(meta: &Metadata) -> u64 {
    let mut h = DefaultHasher::new();
    meta.content_length().hash(&mut h);
    meta.etag().hash(&mut h);
    meta.content_md5().hash(&mut h);
    meta.last_modified().map(|t| t.to_string()).hash(&mut h);
    h.finish()
}

fn clone_error(e: &Error) -> Error {
    let out = Error::new(e.kind(), e.to_string());
    if e.is_temporary() {
        out.set_temporary()
    } else {
        out
    }
}

/// Retry transient failures (e.g. "connection closed before message
/// completed" from a stale pooled HTTP connection) with a short backoff.
pub(crate) async fn with_retry<T, F, Fut>(mut f: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    const ATTEMPTS: usize = 4;
    let mut delay = Duration::from_millis(100);
    let mut attempt = 1;
    loop {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) if e.is_temporary() && attempt < ATTEMPTS => {
                log::debug!("transient error (attempt {attempt}/{ATTEMPTS}), retrying: {e}");
                tokio::time::sleep(delay).await;
                delay *= 3;
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

impl MountCache {
    pub(crate) fn new(cfg: CacheConfig, scheme: &str) -> Self {
        let stat = Cache::builder()
            .max_capacity(cfg.stat_capacity.max(1))
            .expire_after(StatExpiry {
                pos: cfg.stat_ttl,
                neg: cfg.negative_stat_ttl,
            })
            .support_invalidation_closures()
            .build();

        let blocks = Cache::builder()
            .max_capacity(cfg.block_cache_bytes.max(1))
            .weigher(|_k: &BlockKey, v: &Buffer| v.len().min(u32::MAX as usize) as u32)
            .time_to_idle(Duration::from_secs(120))
            .support_invalidation_closures()
            .build();

        let streams = Cache::builder()
            .max_capacity(4096)
            .time_to_live(Duration::from_secs(30))
            .build();

        let seed = cfg.seed_stat_from_list && SEEDABLE_SCHEMES.contains(&scheme);

        Self {
            cfg: Arc::new(cfg),
            stat,
            blocks,
            streams,
            inflight: Arc::new(AtomicUsize::new(0)),
            seed,
        }
    }

    fn stat_enabled(&self) -> bool {
        !self.cfg.stat_ttl.is_zero()
    }

    fn blocks_enabled(&self) -> bool {
        self.cfg.block_cache_bytes > 0 && self.cfg.block_size > 0
    }

    pub(crate) fn config(&self) -> &CacheConfig {
        &self.cfg
    }

    // ------------------------------------------------------------------
    // stat
    // ------------------------------------------------------------------

    /// Cached, coalesced, retried `stat`.
    pub(crate) async fn stat(&self, op: &Operator, rel: &str) -> Result<Metadata> {
        if !self.stat_enabled() {
            return with_retry(|| op.stat(rel)).await;
        }

        let init_op = op.clone();
        let init_rel = rel.to_string();
        let res = self
            .stat
            .try_get_with(rel.to_string(), async move {
                match with_retry(|| init_op.stat(&init_rel)).await {
                    Ok(m) => Ok(Some(m)),
                    Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
                    Err(e) => Err(e),
                }
            })
            .await;

        match res {
            Ok(Some(m)) => Ok(m),
            Ok(None) => Err(Error::new(ErrorKind::NotFound, "path not found")),
            Err(e) => Err(clone_error(&e)),
        }
    }

    /// Record a stat result learned for free (from a directory listing).
    pub(crate) async fn seed(&self, rel: &str, meta: Metadata) {
        if self.seed && self.stat_enabled() {
            self.stat.insert(rel.to_string(), Some(meta)).await;
        }
    }

    pub(crate) fn seeding(&self) -> bool {
        self.seed && self.stat_enabled()
    }

    // ------------------------------------------------------------------
    // invalidation
    // ------------------------------------------------------------------

    /// Drop everything cached about `rel`: its own stat entry (file and dir
    /// spellings), the entries of all ancestor directories (a write can make
    /// a "missing" directory exist, and a delete can make one vanish), its
    /// cached blocks, and, for a directory path, everything below it.
    pub(crate) async fn invalidate(&self, rel: &str) {
        let is_dir = rel.ends_with('/');
        let mut p = rel.trim_end_matches('/');

        while !p.is_empty() {
            self.stat.invalidate(p).await;
            self.stat.invalidate(&format!("{p}/")).await;
            match p.rfind('/') {
                Some(i) => p = &p[..i],
                None => break,
            }
        }

        if is_dir {
            let prefix = rel.to_string();
            let sp = prefix.clone();
            let _ = self
                .stat
                .invalidate_entries_if(move |k, _| k.starts_with(&sp));
            let _ = self
                .blocks
                .invalidate_entries_if(move |k, _| k.path.starts_with(&prefix));
        } else {
            let path = rel.to_string();
            let _ = self.blocks.invalidate_entries_if(move |k, _| k.path == path);
        }
        self.streams.invalidate(rel).await;
    }

    // ------------------------------------------------------------------
    // reads
    // ------------------------------------------------------------------

    /// Read `size` bytes at `offset` of an object of known `len` / `stamp`.
    pub(crate) async fn read(
        &self,
        op: &Operator,
        rel: &str,
        stamp: u64,
        len: u64,
        offset: u64,
        size: usize,
    ) -> Result<Buffer> {
        if offset >= len || size == 0 {
            return Ok(Buffer::new());
        }
        let end = (offset + size as u64).min(len);

        if !self.blocks_enabled() {
            return with_retry(|| async { op.reader(rel).await?.read(offset..end).await }).await;
        }

        let bs = self.cfg.block_size;
        let first = offset / bs;
        let last = (end - 1) / bs;

        // Start prefetching *before* waiting on the current block so the
        // fetches overlap.
        if self.note_access(rel, offset, end).await {
            self.prefetch(op, rel, stamp, len, last + 1);
        }

        let mut parts = Vec::with_capacity((last - first + 1) as usize);
        for idx in first..=last {
            let buf = self.block(op, rel, stamp, idx, len).await?;
            let bstart = idx * bs;
            let a = (offset.max(bstart) - bstart) as usize;
            let b = (end.min(bstart + bs) - bstart) as usize;
            parts.push(buf.slice(a..b));
        }

        if parts.len() == 1 {
            Ok(parts.pop().expect("one part"))
        } else {
            Ok(parts.into_iter().flatten().collect())
        }
    }

    /// Get one block, from cache or the backend. Concurrent requests for the
    /// same block (including a running prefetch) share a single fetch.
    async fn block(
        &self,
        op: &Operator,
        rel: &str,
        stamp: u64,
        idx: u64,
        len: u64,
    ) -> Result<Buffer> {
        let bs = self.cfg.block_size;
        let start = idx * bs;
        let end = (start + bs).min(len);
        let key = BlockKey {
            path: rel.to_string(),
            stamp,
            idx,
        };

        let op = op.clone();
        let rel = rel.to_string();
        self.blocks
            .try_get_with(key, async move {
                let buf =
                    with_retry(|| async { op.reader(&rel).await?.read(start..end).await }).await?;
                if buf.len() as u64 != end - start {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        "backend returned a short block",
                    )
                    .with_context("expected", (end - start).to_string())
                    .with_context("got", buf.len().to_string()));
                }
                Ok(buf)
            })
            .await
            .map_err(|e| clone_error(&e))
    }

    /// Track the per-path read high-water mark; returns true if this access
    /// looks like part of a sequential scan.
    async fn note_access(&self, rel: &str, offset: u64, end: u64) -> bool {
        let bs = self.cfg.block_size;
        let hw = self.streams.get(rel).await;
        let sequential = match hw {
            // First read of a stream: treat reading from the start as a scan.
            None => offset == 0,
            // Near the high-water mark on either side (clients issue several
            // reads in flight, so they arrive slightly out of order).
            Some(hw) => offset <= hw.saturating_add(bs) && offset.saturating_add(2 * bs) >= hw,
        };
        let new = if sequential {
            hw.unwrap_or(0).max(end)
        } else {
            end
        };
        self.streams.insert(rel.to_string(), new).await;
        sequential
    }

    fn prefetch(&self, op: &Operator, rel: &str, stamp: u64, len: u64, from: u64) {
        let bs = self.cfg.block_size;
        for idx in from..from.saturating_add(self.cfg.readahead_blocks) {
            if idx * bs >= len {
                break;
            }
            let key = BlockKey {
                path: rel.to_string(),
                stamp,
                idx,
            };
            if self.blocks.contains_key(&key) {
                continue;
            }
            if self.inflight.load(Ordering::Relaxed) >= self.cfg.max_inflight_prefetch {
                break;
            }
            let _ = self.inflight.fetch_add(1, Ordering::Relaxed);

            let this = self.clone();
            let op = op.clone();
            let rel = rel.to_string();
            drop(tokio::spawn(async move {
                if let Err(e) = this.block(&op, &rel, stamp, idx, len).await {
                    log::debug!("prefetch of block {idx} of '{rel}' failed: {e}");
                }
                let _ = this.inflight.fetch_sub(1, Ordering::Relaxed);
            }));
        }
    }
}
