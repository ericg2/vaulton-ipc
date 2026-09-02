use futures_lite::StreamExt;
use opendal_core::raw::oio;
use opendal_core::{Lister, Operator};
use std::fmt;
use std::fmt::{Debug, Formatter};

pub enum MountLister {
    Real {
        operator: Operator,
        rel: String,
        mount_path: String,
        /// Lazily opened on the first call to `next`, since
        /// `Operator::lister` is async.
        inner: Option<Lister>,
    },
    Virtual {
        entries: Vec<oio::Entry>,
    },
}

impl Debug for MountLister {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            MountLister::Real {
                mount_path, inner, ..
            } => f
                .debug_struct("Real")
                .field("mount_path", mount_path)
                .field("opened", &inner.is_some())
                .finish(),

            MountLister::Virtual { entries } => {
                f.debug_struct("Virtual").field("entries", entries).finish()
            }
        }
    }
}

impl oio::List for MountLister {
    async fn next(&mut self) -> opendal_core::Result<Option<oio::Entry>> {
        match self {
            MountLister::Real {
                operator,
                rel,
                mount_path,
                inner,
            } => {
                if inner.is_none() {
                    *inner = Some(operator.lister(rel).await?);
                }

                let lister = inner.as_mut().expect("just initialized above");
                loop {
                    match lister.next().await {
                        Some(Ok(entry)) => {
                            // Some backends (notably fs-based ones) yield the
                            // queried directory itself as the first entry
                            // when listing. Skip it — callers only want
                            // children, not a self-referential entry.
                            let entry_path = entry.path().trim_matches('/');
                            let rel_trimmed = rel.trim_matches('/');
                            if entry_path == rel_trimmed {
                                continue;
                            }

                            let rebased = format!(
                                "{}/{}",
                                mount_path.trim_end_matches('/'),
                                entry.path().trim_start_matches('/')
                            );

                            return Ok(Some(oio::Entry::new(&rebased, entry.metadata().clone())));
                        }
                        Some(Err(e)) => return Err(e),
                        None => return Ok(None),
                    }
                }
            }

            MountLister::Virtual { entries } => Ok(entries.pop()),
        }
    }
}