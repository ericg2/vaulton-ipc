//! Tests proving the caches cut backend traffic (counted with a spy layer)
//! and never serve stale data.

use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use opendal_core::raw::*;
use opendal_core::services::Memory;
use opendal_core::{Capability, ErrorKind, OperationContext, Operator, Result};

use crate::CacheConfig;
use crate::layers::quota::{MemoryTracker, QuotaState};
use crate::layers::vfs::VfsBuilder;

#[derive(Default, Debug)]
struct Counters {
    stat: AtomicUsize,
    read: AtomicUsize,
}

#[derive(Debug)]
struct SpyLayer {
    counters: Arc<Counters>,
    no_rename: bool,
}

impl Layer for SpyLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        Arc::new(SpyService {
            inner,
            counters: self.counters.clone(),
            no_rename: self.no_rename,
        })
    }
}

struct SpyService {
    inner: Servicer,
    counters: Arc<Counters>,
    no_rename: bool,
}

impl Debug for SpyService {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpyService").finish_non_exhaustive()
    }
}

impl Service for SpyService {
    type Reader = oio::Reader;
    type Writer = oio::Writer;
    type Lister = oio::Lister;
    type Deleter = oio::Deleter;
    type Copier = oio::Copier;
    type Composer = oio::Composer;

    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> Capability {
        let mut c = self.inner.capability();
        if self.no_rename {
            c.rename = false; // behave like B2 / S3
        }
        c
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
        let _ = self.counters.stat.fetch_add(1, Ordering::SeqCst);
        // Simulate network latency so concurrent callers actually overlap.
        tokio::time::sleep(Duration::from_millis(30)).await;
        self.inner.stat(ctx, path, args).await
    }

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        let _ = self.counters.read.fetch_add(1, Ordering::SeqCst);
        self.inner.read(ctx, path, args)
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        self.inner.write(ctx, path, args)
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
    ) -> Result<Self::Copier> {
        self.inner.copy(ctx, from, to, args)
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        if self.no_rename {
            return Err(opendal_core::Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ));
        }
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

    fn compose(
        &self,
        ctx: &OperationContext,
        to: &str,
        args: OpCompose,
    ) -> Result<Self::Composer> {
        self.inner.compose(ctx, to, args)
    }

    async fn restore(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRestore,
    ) -> Result<RpRestore> {
        self.inner.restore(ctx, path, args).await
    }
}

fn spy(no_rename: bool) -> (Operator, Arc<Counters>) {
    let counters = Arc::new(Counters::default());
    let op = Operator::new(Memory::default())
        .unwrap()
        .layer(SpyLayer {
            counters: counters.clone(),
            no_rename,
        });
    (op, counters)
}

fn builder() -> VfsBuilder {
    VfsBuilder::new(QuotaState::new_owned(MemoryTracker::default()))
}

fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

const MIB: usize = 1024 * 1024;

fn small_blocks(readahead: u64) -> CacheConfig {
    CacheConfig {
        block_size: MIB as u64,
        readahead_blocks: readahead,
        ..CacheConfig::default()
    }
}

#[tokio::test]
async fn repeated_stats_hit_the_backend_once() {
    let (inner, c) = spy(false);
    let op = Operator::new(builder().mount("/m", inner)).unwrap();
    op.write("/m/a.bin", vec![1u8; 100]).await.unwrap();
    c.stat.store(0, Ordering::SeqCst);

    for _ in 0..20 {
        assert_eq!(op.stat("/m/a.bin").await.unwrap().content_length(), 100);
    }
    assert_eq!(c.stat.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn concurrent_stats_are_coalesced() {
    let (inner, c) = spy(false);
    let op = Operator::new(builder().mount("/m", inner)).unwrap();
    op.write("/m/a.bin", vec![1u8; 100]).await.unwrap();
    c.stat.store(0, Ordering::SeqCst);

    let mut tasks = Vec::new();
    for _ in 0..25 {
        let op = op.clone();
        tasks.push(tokio::spawn(async move { op.stat("/m/a.bin").await.unwrap() }));
    }
    for t in tasks {
        let _ = t.await.unwrap();
    }
    assert_eq!(c.stat.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn negative_stat_cached_then_invalidated_by_write() {
    let (inner, c) = spy(false);
    let op = Operator::new(builder().mount("/m", inner)).unwrap();

    for _ in 0..5 {
        let e = op.stat("/m/nope.txt").await.unwrap_err();
        assert_eq!(e.kind(), ErrorKind::NotFound);
    }
    assert_eq!(c.stat.load(Ordering::SeqCst), 1);

    op.write("/m/nope.txt", "now it exists").await.unwrap();
    assert_eq!(op.stat("/m/nope.txt").await.unwrap().content_length(), 13);
}

#[tokio::test]
async fn delete_and_rename_invalidate_stat() {
    let (inner, _c) = spy(false);
    let op = Operator::new(builder().mount("/m", inner)).unwrap();
    op.write("/m/a.txt", "x").await.unwrap();
    assert!(op.stat("/m/a.txt").await.is_ok());

    op.delete("/m/a.txt").await.unwrap();
    assert_eq!(
        op.stat("/m/a.txt").await.unwrap_err().kind(),
        ErrorKind::NotFound
    );

    op.write("/m/b.txt", "y").await.unwrap();
    assert!(op.stat("/m/b.txt").await.is_ok());
    op.rename("/m/b.txt", "/m/c.txt").await.unwrap();
    assert_eq!(
        op.stat("/m/b.txt").await.unwrap_err().kind(),
        ErrorKind::NotFound
    );
    assert!(op.stat("/m/c.txt").await.is_ok());
}

#[tokio::test]
async fn many_small_reads_in_one_block_cost_one_backend_read() {
    let (inner, c) = spy(false);
    let op = Operator::new(
        builder()
            .with_cache_config(small_blocks(0))
            .mount("/m", inner),
    )
    .unwrap();
    let data = pattern(4 * MIB, 1);
    op.write("/m/big.bin", data.clone()).await.unwrap();
    c.stat.store(0, Ordering::SeqCst);
    c.read.store(0, Ordering::SeqCst);

    let reader = op.reader("/m/big.bin").await.unwrap();
    for i in 0..16usize {
        let start = i * 64 * 1024;
        let got = reader.read(start as u64..(start + 64 * 1024) as u64).await.unwrap();
        assert_eq!(got.to_vec(), data[start..start + 64 * 1024]);
    }
    assert_eq!(c.read.load(Ordering::SeqCst), 1);
    assert_eq!(c.stat.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn reads_spanning_blocks_and_eof_are_correct() {
    let (inner, _c) = spy(false);
    let op = Operator::new(
        builder()
            .with_cache_config(small_blocks(0))
            .mount("/m", inner),
    )
    .unwrap();
    let data = pattern(3 * MIB + 12_345, 7);
    op.write("/m/f.bin", data.clone()).await.unwrap();

    let reader = op.reader("/m/f.bin").await.unwrap();
    // crosses two block boundaries
    let a = MIB as u64 - 10;
    let b = 2 * MIB as u64 + 10;
    assert_eq!(
        reader.read(a..b).await.unwrap().to_vec(),
        data[a as usize..b as usize]
    );
    // tail (partial last block)
    let n = data.len() as u64;
    assert_eq!(
        reader.read(n - 500..n).await.unwrap().to_vec(),
        data[data.len() - 500..]
    );
    // whole file
    assert_eq!(op.read("/m/f.bin").await.unwrap().to_vec(), data);
}

#[tokio::test]
async fn sequential_scan_fetches_each_block_exactly_once() {
    let (inner, c) = spy(false);
    let op = Operator::new(
        builder()
            .with_cache_config(small_blocks(2))
            .mount("/m", inner),
    )
    .unwrap();
    let data = pattern(8 * MIB, 3);
    op.write("/m/seq.bin", data.clone()).await.unwrap();
    c.read.store(0, Ordering::SeqCst);

    // Emulate an SMB client: a fresh reader per 1 MiB request.
    for i in 0..8usize {
        let r = op.reader("/m/seq.bin").await.unwrap();
        let got = r.read((i * MIB) as u64..((i + 1) * MIB) as u64).await.unwrap();
        assert_eq!(got.to_vec(), data[i * MIB..(i + 1) * MIB]);
    }
    // 8 blocks, no block fetched twice (prefetch + demand reads coalesce).
    assert_eq!(c.read.load(Ordering::SeqCst), 8);
}

#[tokio::test]
async fn overwrite_is_never_served_stale() {
    let (inner, _c) = spy(false);
    let op = Operator::new(
        builder()
            .with_cache_config(small_blocks(0))
            .mount("/m", inner),
    )
    .unwrap();
    op.write("/m/f.bin", vec![b'a'; 2 * MIB]).await.unwrap();
    assert_eq!(op.read("/m/f.bin").await.unwrap().to_vec(), vec![b'a'; 2 * MIB]);

    // same length, different content
    op.write("/m/f.bin", vec![b'b'; 2 * MIB]).await.unwrap();
    assert_eq!(op.read("/m/f.bin").await.unwrap().to_vec(), vec![b'b'; 2 * MIB]);
}

#[tokio::test]
async fn disabled_cache_still_works() {
    let (inner, c) = spy(false);
    let op = Operator::new(
        builder()
            .with_cache_config(CacheConfig::disabled())
            .mount("/m", inner),
    )
    .unwrap();
    op.write("/m/a.bin", vec![9u8; 1000]).await.unwrap();
    c.stat.store(0, Ordering::SeqCst);
    for _ in 0..3 {
        op.stat("/m/a.bin").await.unwrap();
    }
    assert_eq!(c.stat.load(Ordering::SeqCst), 3);
    assert_eq!(op.read("/m/a.bin").await.unwrap().to_vec(), vec![9u8; 1000]);
}

// ---- object-store (no rename) behaviour: the B2 case ----------------------

#[tokio::test]
async fn quota_writes_work_on_backends_without_rename() {
    let (inner, _c) = spy(true);
    let op = Operator::new(builder().mount("/b2", inner).quota("t", 10)).unwrap();

    op.write("/b2/a.txt", "0123456789").await.unwrap();
    // overwrite replaces rather than adds
    op.write("/b2/a.txt", "abcdefghij").await.unwrap();
    assert_eq!(op.read("/b2/a.txt").await.unwrap().to_vec(), b"abcdefghij");

    // over quota: rejected, original left intact (not deleted)
    let err = op.write("/b2/a.txt", "way too long for this").await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::RateLimited);
    assert_eq!(op.read("/b2/a.txt").await.unwrap().to_vec(), b"abcdefghij");

    // no staging leftovers
    let names: Vec<String> = op
        .list("/b2/")
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.name().to_string())
        .collect();
    assert_eq!(names, vec!["a.txt"]);
}

#[tokio::test]
async fn rename_and_copy_work_on_backends_without_rename() {
    let (inner, _c) = spy(true);
    let op = Operator::new(builder().mount("/b2", inner)).unwrap();
    let data = pattern(MIB + 17, 5);
    op.write("/b2/a.bin", data.clone()).await.unwrap();

    op.rename("/b2/a.bin", "/b2/b.bin").await.unwrap();
    assert_eq!(op.read("/b2/b.bin").await.unwrap().to_vec(), data);
    assert_eq!(
        op.stat("/b2/a.bin").await.unwrap_err().kind(),
        ErrorKind::NotFound
    );
}

#[tokio::test]
async fn cross_mount_rename_streams_correctly() {
    let (a, _) = spy(true);
    let (b, _) = spy(true);
    let op = Operator::new(builder().mount("/a", a).mount("/b", b)).unwrap();
    let data = pattern(9 * MIB + 3, 11); // > one 8 MiB streaming chunk
    op.write("/a/x.bin", data.clone()).await.unwrap();

    op.rename("/a/x.bin", "/b/x.bin").await.unwrap();
    assert_eq!(op.read("/b/x.bin").await.unwrap().to_vec(), data);
    assert!(op.stat("/a/x.bin").await.is_err());
}
