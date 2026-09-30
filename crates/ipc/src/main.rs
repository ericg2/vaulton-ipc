use crate::db::DbManager;
use crate::ipc::LogEvent;
use crate::ipc::ipc_event::Data;
use crate::ipc::ipc_service_server::IpcServiceServer;
use crate::server::GrpcServer;
use crate::store::StorageManager;
use crate::utils::fix_level;
use crossbeam_channel::Sender;
use log::{LevelFilter, Log, Metadata, Record};
use prost_types::Timestamp;
use rustic_core::jiff::Zoned;
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

struct ChannelLogger {
    logger: Box<SimpleLogger>,
    tx: Sender<Data>,
}

impl Log for ChannelLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        self.logger.enabled(metadata)
    }

    fn log(&self, record: &Record) {
        if !self.enabled(record.metadata()) {
            return;
        }

        self.logger.log(record);
        let _ = self.tx.send(Data::LogEvent(LogEvent {
            priority: fix_level(record.level()),
            message: record.args().to_string(),
            time: proto_stamp(rustic_core::jiff::Timestamp::now()),
        }));
    }

    fn flush(&self) {
        self.logger.flush();
    }
}

#[tokio::main]
async fn main() {
    use rustic_core::*;

    let logger = ChannelLogger {
        logger: SimpleLogger::new(LevelFilter::Debug, Config::default()),
        tx: log_tx,
    };

    log::set_boxed_logger(Box::new(logger)).unwrap();
    log::set_max_level(LevelFilter::Debug);
    
    let db = Arc::new(DbManager::open("poop.sqlite").await.unwrap());
    let store = Arc::new(StorageManager::new(db.clone(), TTL));
    let serv = GrpcServer::new(store.clone(), db.clone(), store.state.clone());
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
