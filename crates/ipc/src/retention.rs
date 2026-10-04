//! Repository retention: time-based rules plus FIFO space reclaim.
//!
//! # Why this is more than "delete the oldest snapshot"
//!
//! A rustic repository is deduplicated: a snapshot is only a small file that
//! points at shared blobs. Deleting it frees *nothing* by itself, and deleting
//! the oldest snapshot frees only the data that no newer snapshot still uses.
//! Space only comes back after a **prune**, which has two more catches:
//!
//! * by default prune keeps packs it has marked for deletion for 23 hours
//!   (`keep_delete`), so the quota counter would not move. Space reclaim
//!   therefore prunes with `instant_delete`;
//! * repacking writes new packs *before* removing the old ones, which needs
//!   headroom that a quota-limited repository may not have, so the repack
//!   budget is capped to what is actually free.
//!
//! After every prune the in-memory index of the repository handle is stale, so
//! each phase opens a **fresh** handle (`Retention::open`) instead of reusing
//! one across a prune.
//!
//! # Rules
//!
//! * Time rules: rustic `KeepOptions`, evaluated per group of snapshots with
//!   the same host + label + paths (the backup job labels every snapshot with
//!   its data point id, so each data point is its own series).
//! * Space rule: keep `reserve_bytes` / `reserve_percent` of the quota free by
//!   deleting the oldest unlocked snapshots first and pruning, in batches sized
//!   from each snapshot's recorded added bytes. Never deleted by this rule: any
//!   locked snapshot and the newest snapshot of every group.
//! * Locked snapshots (`DeleteOption::Never`) are never deleted by any rule.

use crate::event_bus::send as send_event;
use crate::ipc::ipc_event::Data;
use crate::ipc::{JobNewMessageEvent, Priority, RetentionPolicy};
use crate::store::RepoIndexed;
use crate::utils;
use crossbeam_channel::Sender;
use log::{info, warn};
use rustic_core::jiff::{Span, Zoned};
use rustic_core::repofile::{DeleteOption, SnapshotFile, SnapshotId};
use rustic_core::{
    CancelToken, ErrorKind, ForgetGroups, Grouped, KeepOptions, LimitOption, PruneOptions,
    RusticError, RusticResult, SnapshotGroupCriterion,
};
use std::str::FromStr;
use uuid::Uuid;

/// Upper bound on delete+prune rounds of one space reclaim.
const MAX_SPACE_ROUNDS: usize = 64;

/// The space rule never asks for more than this share of the quota to be
/// free, so a misconfigured policy cannot wipe a whole repository.
const MAX_RESERVE_DIVISOR: u64 = 2;

// ── Policy helpers ────────────────────────────────────────────────────────────

pub fn has_time_rules(p: &RetentionPolicy) -> bool {
    p.keep_last > 0
        || p.keep_hourly > 0
        || p.keep_daily > 0
        || p.keep_weekly > 0
        || p.keep_monthly > 0
        || p.keep_yearly > 0
        || p.keep_within_days > 0
}

pub fn has_space_rule(p: &RetentionPolicy) -> bool {
    p.reserve_bytes > 0 || p.reserve_percent > 0
}

pub fn has_rules(p: &RetentionPolicy) -> bool {
    has_time_rules(p) || has_space_rule(p)
}

/// Rejects policies that cannot be applied. The message is shown to the caller.
pub fn validate(p: &RetentionPolicy) -> Result<(), String> {
    if !has_rules(p) {
        return Err("the retention policy has no rules".into());
    }
    if p.reserve_percent > 50 {
        return Err("reserve_percent must be at most 50".into());
    }
    if p.keep_within_days > 36_500 {
        return Err("keep_within_days must be at most 36500".into());
    }
    let counts = [
        p.keep_last,
        p.keep_hourly,
        p.keep_daily,
        p.keep_weekly,
        p.keep_monthly,
        p.keep_yearly,
    ];
    if counts.iter().any(|c| *c > i32::MAX as u32) {
        return Err("keep counts are too large".into());
    }
    Ok(())
}

/// How many bytes of the quota the space rule wants to keep free.
pub fn target_free(max_bytes: u64, p: &RetentionPolicy) -> u64 {
    let by_percent = (u128::from(max_bytes) * u128::from(p.reserve_percent) / 100) as u64;
    p.reserve_bytes
        .max(by_percent)
        .min(max_bytes / MAX_RESERVE_DIVISOR)
}

fn criterion() -> SnapshotGroupCriterion {
    SnapshotGroupCriterion::new()
        .hostname(true)
        .label(true)
        .paths(true)
}

fn build_keep(p: &RetentionPolicy) -> RusticResult<Option<KeepOptions>> {
    if !has_time_rules(p) {
        return Ok(None);
    }
    let mut keep = KeepOptions::default();
    if p.keep_last > 0 {
        keep = keep.keep_last(p.keep_last as i32);
    }
    if p.keep_hourly > 0 {
        keep = keep.keep_hourly(p.keep_hourly as i32);
    }
    if p.keep_daily > 0 {
        keep = keep.keep_daily(p.keep_daily as i32);
    }
    if p.keep_weekly > 0 {
        keep = keep.keep_weekly(p.keep_weekly as i32);
    }
    if p.keep_monthly > 0 {
        keep = keep.keep_monthly(p.keep_monthly as i32);
    }
    if p.keep_yearly > 0 {
        keep = keep.keep_yearly(p.keep_yearly as i32);
    }
    if p.keep_within_days > 0 {
        let span = Span::new()
            .try_days(i64::from(p.keep_within_days))
            .map_err(|e| {
                RusticError::with_source(ErrorKind::InvalidInput, "invalid keep_within_days", e)
            })?;
        keep = keep.keep_within(span);
    }
    Ok(Some(keep))
}

fn is_locked(sn: &SnapshotFile) -> bool {
    matches!(sn.delete, DeleteOption::Never)
}

fn short(id: &SnapshotId) -> String {
    id.to_string().chars().take(8).collect()
}

fn when(sn: &SnapshotFile) -> String {
    sn.time.strftime("%Y-%m-%d %H:%M").to_string()
}

fn added_bytes(sn: &SnapshotFile) -> u64 {
    sn.summary
        .as_ref()
        .map(|s| s.data_added_packed)
        .unwrap_or(0)
}

pub fn fmt_bytes(b: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = b as f64;
    let mut i = 0;
    while v >= 1024.0 && i < UNITS.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{b} B")
    } else {
        format!("{v:.1} {}", UNITS[i])
    }
}

fn err(msg: impl Into<String>) -> Box<RusticError> {
    let msg = msg.into();
    RusticError::with_source(ErrorKind::Backend, msg.clone(), std::io::Error::other(msg))
}

/// Snapshots the space rule may delete, oldest first: everything unlocked
/// except the newest snapshot of each group.
fn fifo_candidates(all: Vec<SnapshotFile>, now: &Zoned) -> Vec<SnapshotFile> {
    let mut out = Vec::new();
    for group in Grouped::from_items(all, criterion()).groups {
        let mut items = group.items;
        items.sort_by(|a, b| a.time.cmp(&b.time));
        items.pop(); // the newest snapshot of the group always stays
        out.extend(items.into_iter().filter(|s| !s.must_keep(now)));
    }
    out.sort_by(|a, b| a.time.cmp(&b.time));
    out
}

// ── Runner ────────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Phase {
    /// Only the space rule (used right before a backup to make room).
    SpaceOnly,
    /// Time rules, then the space rule (after a backup, or on demand).
    Full,
}

#[derive(Default, Debug)]
pub struct Report {
    pub time_deleted: usize,
    pub space_deleted: usize,
    pub bytes_freed: u64,
}

/// Everything one retention run needs. Borrowed so it works from inside a job
/// closure without extra cloning.
pub struct Retention<'a> {
    pub job_id: Uuid,
    pub tx: &'a Sender<Data>,
    pub token: &'a CancelToken,
    /// Opens a fresh, indexed handle on the repository (see module docs).
    pub open: &'a dyn Fn() -> RusticResult<RepoIndexed>,
    /// Quota of the repository point; `None` = unlimited (space rule inert).
    pub max_bytes: Option<u64>,
    /// Currently tracked usage in bytes: the same number `GetVfs` reports.
    pub used_bytes: &'a dyn Fn() -> Result<u64, String>,
    pub dry_run: bool,
}

impl Retention<'_> {
    /// Logs and forwards a message to the job's log.
    pub fn say(&self, priority: Priority, message: impl Into<String>) {
        let message = message.into();
        match priority {
            Priority::Warning | Priority::Error | Priority::Critical => {
                warn!("retention (job {}): {message}", self.job_id)
            }
            _ => info!("retention (job {}): {message}", self.job_id),
        }
        let _ = send_event(
            self.tx,
            Data::JobMessage(JobNewMessageEvent {
                job_id: self.job_id.to_string(),
                priority: priority as i32,
                message,
                time: Some(utils::to_ts(Zoned::now())),
            }),
        );
    }

    fn used(&self) -> RusticResult<u64> {
        (self.used_bytes)().map_err(|e| err(format!("could not read repository usage: {e}")))
    }

    fn cancelled(&self) -> bool {
        self.token.is_cancelled()
    }

    pub fn run(&self, policy: &RetentionPolicy, phase: Phase) -> RusticResult<Report> {
        let mut report = Report::default();
        let space = has_space_rule(policy) && self.max_bytes.is_some();

        if phase == Phase::Full {
            if let Some(keep) = build_keep(policy)? {
                // With a space rule, free space must be real before it is
                // measured, so the prune after the time rules is instant.
                self.time_step(&keep, space, &mut report)?;
            }
        }

        if space {
            if let Some(max) = self.max_bytes {
                self.space_step(policy, max, &mut report)?;
            }
        }
        Ok(report)
    }

    // ── Time rules ────────────────────────────────────────────────────────

    fn time_step(&self, keep: &KeepOptions, instant: bool, report: &mut Report) -> RusticResult<()> {
        if self.cancelled() {
            return Ok(());
        }
        let repo = (self.open)()?;
        let now = Zoned::now();
        let all = repo.get_all_snapshots()?;
        let total = all.len();
        let locked = all.iter().filter(|s| is_locked(s)).count();
        if locked > 0 {
            self.say(
                Priority::Info,
                format!("{locked} locked snapshot(s) are never removed by retention"),
            );
        }

        let groups = ForgetGroups::from_grouped_snapshots_with_retention(
            Grouped::from_items(all, criterion()),
            keep,
            &now,
        )?;

        let mut doomed: Vec<SnapshotFile> = Vec::new();
        for group in groups.0 {
            for item in group.items {
                // `keep` already honours delete protection; the extra check is
                // a second line of defence for locked snapshots.
                if !item.keep && !item.snapshot.must_keep(&now) {
                    doomed.push(item.snapshot);
                }
            }
        }
        doomed.sort_by(|a, b| a.time.cmp(&b.time));

        if doomed.is_empty() {
            self.say(
                Priority::Info,
                format!("Time-based retention: nothing to remove ({total} snapshot(s))"),
            );
            return Ok(());
        }

        if self.dry_run {
            self.say(
                Priority::Info,
                format!(
                    "Dry run - time-based retention would remove {} of {total} snapshot(s):",
                    doomed.len()
                ),
            );
            for sn in &doomed {
                self.say(
                    Priority::Info,
                    format!("  would remove {} from {}", short(&sn.id), when(sn)),
                );
            }
            report.time_deleted += doomed.len();
            return Ok(());
        }

        let before = self.used().unwrap_or(0);
        let ids: Vec<SnapshotId> = doomed.iter().map(|s| s.id.clone()).collect();
        repo.delete_snapshots(&ids)?;
        drop(repo);
        self.say(
            Priority::Info,
            format!(
                "Time-based retention: removed {} of {total} snapshot(s); pruning",
                ids.len()
            ),
        );
        report.time_deleted += ids.len();

        self.prune(instant)?;
        let freed = before.saturating_sub(self.used().unwrap_or(before));
        report.bytes_freed += freed;
        self.say(
            Priority::Info,
            format!("Time-based retention reclaimed {}", fmt_bytes(freed)),
        );
        Ok(())
    }

    // ── Space rule (FIFO) ─────────────────────────────────────────────────

    fn space_step(&self, policy: &RetentionPolicy, max: u64, report: &mut Report) -> RusticResult<()> {
        let target = target_free(max, policy);
        if target == 0 {
            return Ok(());
        }

        // Each round that falls short doubles the minimum batch, so a
        // repository whose old snapshots share most of their data needs
        // O(log n) prunes instead of one per snapshot.
        let mut min_batch = 1usize;
        // Circuit breaker: if pruning keeps reclaiming nothing, either the data is
        // fully shared or the usage counter is not being credited. Deleting more
        // snapshots would then only destroy history, so stop.
        let mut empty_rounds = 0u32;

        for round in 0..MAX_SPACE_ROUNDS {
            if self.cancelled() {
                self.say(Priority::Warning, "Space retention cancelled");
                return Ok(());
            }

            let used = self.used()?;
            let free = max.saturating_sub(used);
            if free >= target {
                if round > 0 || self.dry_run {
                    self.say(
                        Priority::Info,
                        format!(
                            "Free space is {} (wanted at least {})",
                            fmt_bytes(free),
                            fmt_bytes(target)
                        ),
                    );
                }
                return Ok(());
            }
            let deficit = target - free;

            let repo = (self.open)()?;
            let now = Zoned::now();
            let candidates = fifo_candidates(repo.get_all_snapshots()?, &now);
            if candidates.is_empty() {
                self.say(
                    Priority::Warning,
                    format!(
                        "Free space is {} but {} is wanted. Nothing more can be removed: only \
                         locked snapshots and the newest snapshot of each backup remain. Free \
                         space manually, raise the quota, or lower the reserve.",
                        fmt_bytes(free),
                        fmt_bytes(target)
                    ),
                );
                return Ok(());
            }

            let mut batch: Vec<SnapshotFile> = Vec::new();
            let mut estimate = 0u64;
            for sn in candidates {
                estimate = estimate.saturating_add(added_bytes(&sn));
                batch.push(sn);
                if estimate >= deficit && batch.len() >= min_batch {
                    break;
                }
            }

            if self.dry_run {
                self.say(
                    Priority::Info,
                    format!(
                        "Dry run - free space is {} but {} is wanted. Oldest-first, space \
                         retention would remove (estimate, shared data may free less, so more \
                         rounds can follow):",
                        fmt_bytes(free),
                        fmt_bytes(target)
                    ),
                );
                for sn in &batch {
                    self.say(
                        Priority::Info,
                        format!(
                            "  would remove {} from {} (added {})",
                            short(&sn.id),
                            when(sn),
                            fmt_bytes(added_bytes(sn))
                        ),
                    );
                }
                report.space_deleted += batch.len();
                return Ok(());
            }

            let ids: Vec<SnapshotId> = batch.iter().map(|s| s.id.clone()).collect();
            repo.delete_snapshots(&ids)?;
            drop(repo);
            report.space_deleted += ids.len();

            self.prune(true)?;

            let after = self.used()?;
            let freed = used.saturating_sub(after);
            report.bytes_freed += freed;
            self.say(
                Priority::Info,
                format!(
                    "Space retention: removed the {} oldest snapshot(s) (up to {}), reclaimed {}; \
                     free space is now {}",
                    ids.len(),
                    when(batch.last().unwrap_or(&batch[0])),
                    fmt_bytes(freed),
                    fmt_bytes(max.saturating_sub(after))
                ),
            );
            if freed == 0 {
                empty_rounds += 1;
                self.say(
                    Priority::Warning,
                    "Pruning did not lower the tracked usage. Old snapshots only free space once \
                     no remaining snapshot shares their data. If this repeats for a repository \
                     that clearly shrank, the quota counter may not be crediting deleted files.",
                );
                if empty_rounds >= 2 {
                    self.say(
                        Priority::Warning,
                        "Stopping space retention: two rounds in a row reclaimed nothing, so \
                         deleting more snapshots would not help.",
                    );
                    return Ok(());
                }
            } else {
                empty_rounds = 0;
            }
            if freed < deficit {
                min_batch = min_batch.saturating_mul(2).max(ids.len());
            }
        }

        self.say(
            Priority::Warning,
            format!(
                "Stopped after {MAX_SPACE_ROUNDS} rounds without reaching {} of free space",
                fmt_bytes(target)
            ),
        );
        Ok(())
    }

    // ── Prune ─────────────────────────────────────────────────────────────

    /// Prunes on a fresh handle. `instant` removes packs immediately instead of
    /// keeping them marked for 23 hours, which is required for the quota
    /// counter to actually drop.
    fn prune(&self, instant: bool) -> RusticResult<()> {
        if self.cancelled() {
            return Ok(());
        }
        let repo = (self.open)()?;

        let mut opts = PruneOptions::default();
        if let Some(max) = self.max_bytes {
            // Repacking writes before it deletes: never plan more than the
            // free space (halved, for safety) and the usual 10 % of the repo.
            let used = self.used().unwrap_or(0);
            let free = max.saturating_sub(used);
            let budget = (free / 2).min(used / 10);
            opts = opts.max_repack(LimitOption::from_str(&format!("{budget}B"))?);
        }
        if instant {
            opts = opts
                .max_unused(LimitOption::Percentage(0))
                .keep_delete(Span::new())
                .instant_delete(true);
        }

        let plan = repo.prune_plan(&opts)?;
        repo.prune(&opts, plan)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> RetentionPolicy {
        RetentionPolicy::default()
    }

    #[test]
    fn empty_policy_is_invalid() {
        assert!(validate(&policy()).is_err());
    }

    #[test]
    fn any_single_rule_is_valid() {
        let mut p = policy();
        p.keep_daily = 7;
        assert!(validate(&p).is_ok());
        let mut p = policy();
        p.reserve_percent = 10;
        assert!(validate(&p).is_ok());
    }

    #[test]
    fn reserve_percent_is_capped() {
        let mut p = policy();
        p.reserve_percent = 51;
        assert!(validate(&p).is_err());
    }

    #[test]
    fn target_uses_larger_of_bytes_and_percent() {
        let mut p = policy();
        p.reserve_bytes = 100;
        p.reserve_percent = 10;
        assert_eq!(target_free(10_000, &p), 1_000);
        p.reserve_bytes = 2_000;
        assert_eq!(target_free(10_000, &p), 2_000);
    }

    #[test]
    fn target_never_exceeds_half_the_quota() {
        let mut p = policy();
        p.reserve_bytes = 9_000;
        assert_eq!(target_free(10_000, &p), 5_000);
    }

    #[test]
    fn target_is_zero_without_a_space_rule() {
        let mut p = policy();
        p.keep_last = 3;
        assert_eq!(target_free(10_000, &p), 0);
    }

    #[test]
    fn time_rules_build_keep_options() {
        let mut p = policy();
        p.keep_last = 3;
        p.keep_within_days = 30;
        assert!(build_keep(&p).unwrap().is_some());
        assert!(build_keep(&policy()).unwrap().is_none());
    }

    #[test]
    fn byte_formatting() {
        assert_eq!(fmt_bytes(512), "512 B");
        assert_eq!(fmt_bytes(1536), "1.5 KiB");
    }
}
