use crate::db::DbManager;
use crate::ipc::ipc_service_server::IpcServiceServer;
use crate::server::GrpcServer;
use crate::store::StorageManager;
use log::LevelFilter;
use prost_types::Timestamp;
use simplelog::{Config, SimpleLogger};
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Server;

pub mod ipc {
    tonic::include_proto!("ipc");
}

mod core;
mod db;
mod ftp;
mod progress;
mod server;
mod store;
mod utils;

pub(crate) fn proto_stamp(ts: rustic_core::jiff::Timestamp) -> Option<Timestamp> {
    Some(Timestamp {
        seconds: ts.as_second(),
        nanos: ts.subsec_nanosecond() as i32,
    })
}

const TTL: Duration = Duration::from_mins(3);

#[tokio::main]
async fn main() {
    use rustic_core::*;

    let _ = SimpleLogger::init(LevelFilter::Debug, Config::default());

    let db = Arc::new(DbManager::open("poop.sqlite").await.unwrap());
    let store = Arc::new(StorageManager::new(db.clone(), TTL));
    let serv = GrpcServer::new(store.clone(), db.clone(), db.clone());
    let ftp = Arc::new(ftp::FtpServer::new(store.clone(), db.clone()));

    let ftp_server = libunftp::ServerBuilder::with_user_detail_provider(
        Box::new({
            let ftp = ftp.clone();
            move || (*ftp).clone()
        }),
        ftp.clone(),
    )
    .authenticator(ftp)
    .build()
    .unwrap();

    tokio::try_join!(
        async {
            Server::builder()
                .add_service(IpcServiceServer::new(serv))
                .serve("127.0.0.1:8080".parse().unwrap())
                .await
                .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
        },
        async {
            ftp_server
                .listen("127.0.0.1:2121")
                .await
                .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
        },
    )
    .unwrap();
}
