use crate::core::{UserSystem, VfsUser};
use crate::store::StorageSystem;
use crate::utils;

use async_trait::async_trait;

use std::fmt::{Debug, Formatter};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};

use unftp_core::auth::{
    AuthenticationError, Authenticator, Credentials, Principal, UserDetailError, UserDetailProvider,
};

use unftp_core::storage::{Error, ErrorKind, Fileinfo, Metadata, Result, StorageBackend};

use unftp_sbe_opendal::OpendalStorage;

pub struct FtpServer<S, U>
where
    S: StorageSystem,
    U: UserSystem,
{
    pub storage: Arc<S>,
    pub users: Arc<U>,
}

impl<S, U> Clone for FtpServer<S, U>
where
    S: StorageSystem,
    U: UserSystem,
{
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            users: self.users.clone(),
        }
    }
}

impl<S, U> Debug for FtpServer<S, U>
where
    S: StorageSystem,
    U: UserSystem,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ftp")
    }
}

impl<S, U> FtpServer<S, U>
where
    S: StorageSystem,
    U: UserSystem,
{
    pub fn new(storage: Arc<S>, users: Arc<U>) -> Self {
        Self { storage, users }
    }

    /// Create the unFTP OpenDAL backend for a specific user.
    async fn backend(&self, user: &VfsUser) -> Result<OpendalStorage> {
        let op = self
            .storage
            .get_vfs(user)
            .await
            .map_err(|err| Error::new(ErrorKind::LocalError, err))?;
        Ok(OpendalStorage::new(op))
    }
}

#[async_trait]
impl<S, U> Authenticator for FtpServer<S, U>
where
    S: StorageSystem + Send + Sync,
    U: UserSystem + Send + Sync,
{
    async fn authenticate(
        &self,
        username: &str,
        creds: &Credentials,
    ) -> std::result::Result<Principal, AuthenticationError> {
        let user = self
            .users
            .get_user(username)
            .await
            .map_err(|_| AuthenticationError::BadUser)?;

        // Temporary authentication while developing.
        //
        // Replace this with your actual password verification later.
        if user.password != *creds.password.as_ref().unwrap_or(&String::new()) {
            return Err(AuthenticationError::BadPassword);
        }

        Ok(Principal {
            username: username.to_string(),
        })
    }
}

#[async_trait]
impl<S, U> UserDetailProvider for FtpServer<S, U>
where
    S: StorageSystem + Send + Sync,
    U: UserSystem + Send + Sync,
{
    type User = VfsUser;

    async fn provide_user_detail(
        &self,
        principal: &Principal,
    ) -> std::result::Result<Self::User, UserDetailError> {
        self.users
            .get_user(&principal.username)
            .await
            .map_err(|_| UserDetailError::UserNotFound {
                username: principal.username.clone(),
            })
    }
}

#[async_trait]
impl<S, U> StorageBackend<VfsUser> for FtpServer<S, U>
where
    S: StorageSystem + Send + Sync,
    U: UserSystem + Send + Sync,
{
    type Metadata = <OpendalStorage as StorageBackend<VfsUser>>::Metadata;

    fn name(&self) -> &str {
        "opendal"
    }

    async fn metadata<P>(&self, user: &VfsUser, path: P) -> Result<Self::Metadata>
    where
        P: AsRef<Path> + Send + Debug,
    {
        self.backend(user).await?.metadata(user, path).await
    }

    async fn list<P>(
        &self,
        user: &VfsUser,
        path: P,
    ) -> Result<Vec<Fileinfo<PathBuf, Self::Metadata>>>
    where
        P: AsRef<Path> + Send + Debug,
        Self::Metadata: Metadata,
    {
        self.backend(user).await?.list(user, path).await
    }

    async fn get_into<'a, P, W: ?Sized>(
        &self,
        user: &VfsUser,
        path: P,
        start_pos: u64,
        output: &'a mut W,
    ) -> Result<u64>
    where
        P: AsRef<Path> + Send + Debug,
        W: AsyncWrite + Unpin + Sync + Send,
    {
        self.backend(user)
            .await?
            .get_into(user, path, start_pos, output)
            .await
    }

    async fn get<P>(
        &self,
        user: &VfsUser,
        path: P,
        start_pos: u64,
    ) -> Result<Box<dyn AsyncRead + Send + Sync + Unpin>>
    where
        P: AsRef<Path> + Send + Debug,
    {
        self.backend(user).await?.get(user, path, start_pos).await
    }

    async fn put<P, R>(&self, user: &VfsUser, input: R, path: P, start_pos: u64) -> Result<u64>
    where
        P: AsRef<Path> + Send + Debug,
        R: AsyncRead + Send + Sync + Unpin + 'static,
    {
        // Local/fs-backed data points with a hard quota cannot safely be
        // streamed through the FTP backend because its generic AsyncRead API
        // has no atomic reservation hook. Refuse those uploads rather than
        // bypassing the quota.
        if let Ok((point, _rest)) = utils::resolve_data_path(user, &path.as_ref().to_string_lossy())
        {
            if utils::is_local_scheme(&point.scheme) {
                if point.max_bytes.is_some() {
                    // FTP's StorageBackend::put receives an arbitrary AsyncRead
                    // and does not expose a pre-write byte reservation hook.
                    // Refuse this unsafe combination rather than allowing a
                    // large upload to bypass the filesystem quota. Clients can
                    // use the IPC write-handle API, which checks each write
                    // before it reaches disk.
                    return Err(Error::new(
                        ErrorKind::PermissionDenied,
                        format!(
                            "quota-limited local point '{}' requires IPC write handles",
                            point.name
                        ),
                    ));
                }
            }
        }

        self.backend(user)
            .await?
            .put(user, input, path, start_pos)
            .await
    }

    async fn del<P>(&self, user: &VfsUser, path: P) -> Result<()>
    where
        P: AsRef<Path> + Send + Debug,
    {
        self.backend(user).await?.del(user, path).await
    }

    async fn mkd<P>(&self, user: &VfsUser, path: P) -> Result<()>
    where
        P: AsRef<Path> + Send + Debug,
    {
        self.backend(user).await?.mkd(user, path).await
    }

    async fn rename<P>(&self, user: &VfsUser, from: P, to: P) -> Result<()>
    where
        P: AsRef<Path> + Send + Debug,
    {
        self.backend(user).await?.rename(user, from, to).await
    }

    async fn rmd<P>(&self, user: &VfsUser, path: P) -> Result<()>
    where
        P: AsRef<Path> + Send + Debug,
    {
        self.backend(user).await?.rmd(user, path).await
    }

    async fn cwd<P>(&self, user: &VfsUser, path: P) -> Result<()>
    where
        P: AsRef<Path> + Send + Debug,
    {
        self.backend(user).await?.cwd(user, path).await
    }
}
