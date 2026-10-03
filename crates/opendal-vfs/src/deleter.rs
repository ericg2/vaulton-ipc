use crate::Mount;
use opendal_core::raw::{OpDelete, oio};
use opendal_core::{Error, ErrorKind};
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Debug)]
pub struct MountDeleter(pub Arc<BTreeMap<String, Mount>>);

impl oio::Delete for MountDeleter {
    async fn delete(&mut self, path: &str, _args: OpDelete) -> opendal_core::Result<()> {
        match crate::resolve_path(&self.0, path) {
            Some((_, mount, rel)) => {
                if mount.read_only {
                    return Err(Error::new(
                        ErrorKind::PermissionDenied,
                        "mount is read only",
                    ));
                }

                mount.operator.delete(&rel).await
            }
            None => Err(Error::new(ErrorKind::NotFound, "path not found")),
        }
    }

    async fn close(&mut self) -> opendal_core::Result<()> {
        Ok(())
    }
}
