use crate::vfs::sys::Mount;
use opendal_core::raw::{OpDelete, oio};
use std::collections::BTreeMap;
use std::sync::Arc;
use crate::vfs::util;

#[derive(Debug)]
pub struct MountDeleter(pub Arc<BTreeMap<String, Mount>>);

impl oio::Delete for MountDeleter {
    async fn delete(&mut self, path: &str, _args: OpDelete) -> opendal_core::Result<()> {
        match util::resolve(&self.0, path) {
            Some((_, mount, rel)) => {
                if mount.read_only {
                    return Err(util::permission_denied(path));
                }

                mount.operator.delete(&rel).await
            }
            None => Err(util::not_found(path)),
        }
    }

    async fn close(&mut self) -> opendal_core::Result<()> {
        Ok(())
    }
}
