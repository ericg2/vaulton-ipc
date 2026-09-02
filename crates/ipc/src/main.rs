use crate::db::DbManager;
use crate::ipc::ipc_service_server::IpcServiceServer;
use crate::server::GrpcServer;
use crate::store::StorageManager;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use log::LevelFilter;
use prost_types::Timestamp;
use rustic_core::jiff::Zoned;
use serde::{Deserialize, Serialize};
use simplelog::{Config, SimpleLogger};
use std::error::Error;
use std::io::{BufRead, Write};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::{Handle, Runtime};
use tonic::transport::Server;

pub mod ipc {
    tonic::include_proto!("ipc");
}

mod db;
mod progress;
mod server;
mod store;
mod core;
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
    let ip = "127.0.0.1:8080".parse().expect("Failed to parse IP!");
    let db = Arc::new(DbManager::open("poop.sqlite").await.unwrap());
    let store = Arc::new(StorageManager::new(db.clone(), TTL));
    let serv = GrpcServer::new(store.clone(), db.clone());

    Server::builder()
        .add_service(IpcServiceServer::new(serv))
        .serve(ip)
        .await
        .unwrap()
}
