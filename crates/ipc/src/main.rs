use crate::db::DbManager;
use crate::event_bus::send as send_event;
use crate::ipc::LogEvent;
use crate::ipc::ipc_event::Data;
use crate::ipc::ipc_service_server::IpcServiceServer;
use crate::server::GrpcServer;
use crate::store::StorageManager;
use crate::utils::fix_level;
use crossbeam_channel::Sender;
use log::{LevelFilter, Log, Metadata, Record};
use log::{error, info};
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
mod event_bus;
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

        // Forward our own crate at any level, but only warnings+ from
        // dependencies (hyper/h2/tonic debug logs would flood the event
        // queue, and every Poll would generate more of them).
        let ours = record.target().starts_with(env!("CARGO_CRATE_NAME"));
        if !ours && record.level() > log::Level::Warn {
            return;
        }
        let _ = send_event(
            &self.tx,
            Data::LogEvent(LogEvent {
                priority: fix_level(record.level()),
                message: record.args().to_string(),
                time: proto_stamp(rustic_core::jiff::Timestamp::now()),
            }),
        );
    }

    fn flush(&self) {
        self.logger.flush();
    }
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    info!("shutdown signal received");
}

#[tokio::main]
async fn main() {
    // One global event channel: logger, StorageManager and jobs -> Poll.
    let (tx, rx) = crossbeam_channel::bounded::<Data>(crate::event_bus::CAPACITY);

    let logger = ChannelLogger {
        logger: SimpleLogger::new(LevelFilter::Debug, Config::default()),
        tx: tx.clone(),
    };
    if let Err(e) = log::set_boxed_logger(Box::new(logger)) {
        eprintln!("failed to install logger: {e}");
        return;
    }
    log::set_max_level(LevelFilter::Debug);

    if let Err(e) = run(tx, rx).await {
        error!("fatal: {e}");
        std::process::exit(1);
    }
}
async fn run(
    tx: Sender<Data>,
    rx: crossbeam_channel::Receiver<Data>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let db_path = std::env::var("VFS_DB").unwrap_or_else(|_| "poop.sqlite".into());

    let db = Arc::new(DbManager::open(&db_path).await?);

    let store = Arc::new(StorageManager::new(db.clone(), TTL, tx.clone()));

    let serv = GrpcServer::new(store.clone(), db.clone(), store.state.clone(), tx, rx);

    let ftp = Arc::new(ftp::FtpServer::new(store.clone(), db.clone()));

    let ftp_server = libunftp::ServerBuilder::with_user_detail_provider(
        Box::new({
            let ftp = ftp.clone();
            move || (*ftp).clone()
        }),
        ftp.clone(),
    )
    .authenticator(ftp)
    .build()?;

    // WriteAt chunks can exceed tonic's 4 MiB default decode limit.
    let svc = IpcServiceServer::new(serv.clone())
        .max_decoding_message_size(48 * 1024 * 1024)
        .max_encoding_message_size(48 * 1024 * 1024);

    info!("listening: grpc 127.0.0.1:8080, ftp 127.0.0.1:2121");

    // One broadcast channel is used to coordinate shutdown between
    // the gRPC and FTP server tasks.
    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);

    let signal_tx = shutdown_tx.clone();

    tokio::spawn(async move {
        shutdown_signal().await;
        let _ = signal_tx.send(());
    });

    let mut grpc_shutdown = shutdown_tx.subscribe();

    let mut grpc = tokio::spawn(async move {
        Server::builder()
            .tcp_nodelay(true)
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .http2_keepalive_interval(Some(Duration::from_secs(30)))
            .add_service(svc)
            .serve_with_shutdown("127.0.0.1:8080".parse()?, async move {
                let _ = grpc_shutdown.recv().await;
            })
            .await
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
    });

    let mut ftp = tokio::spawn(async move {
        ftp_server
            .listen("127.0.0.1:2121")
            .await
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
    });

    let mut main_shutdown = shutdown_tx.subscribe();

    tokio::select! {
        result = &mut grpc => {
            match result {
                Ok(Ok(())) => {
                    info!("gRPC server stopped");
                }
                Ok(Err(e)) => {
                    error!("gRPC server stopped with error: {e}");
                }
                Err(e) => {
                    error!("gRPC server task panicked: {e}");
                }
            }

            tokio::select! {
                result = &mut ftp => {
                    match result {
                        Ok(Ok(())) => {
                            info!("FTP server stopped");
                        }
                        Ok(Err(e)) => {
                            error!("FTP server stopped with error: {e}");
                        }
                        Err(e) => {
                            error!("FTP server task panicked: {e}");
                        }
                    }
                }

                _ = main_shutdown.recv() => {
                    // FTP has no shutdown future in this API, so abort
                    // the listener when coordinated shutdown occurs.
                    ftp.abort();
                    let _ = ftp.await;
                }
            }
        }

        result = &mut ftp => {
            match result {
                Ok(Ok(())) => {
                    info!("FTP server stopped");
                }
                Ok(Err(e)) => {
                    error!("FTP server stopped with error: {e}");
                }
                Err(e) => {
                    error!("FTP server task panicked: {e}");
                }
            }

            tokio::select! {
                result = &mut grpc => {
                    match result {
                        Ok(Ok(())) => {
                            info!("gRPC server stopped");
                        }
                        Ok(Err(e)) => {
                            error!("gRPC server stopped with error: {e}");
                        }
                        Err(e) => {
                            error!("gRPC server task panicked: {e}");
                        }
                    }
                }

                _ = main_shutdown.recv() => {
                    grpc.abort();
                    let _ = grpc.await;
                }
            }
        }

        _ = main_shutdown.recv() => {
            // Ctrl-C: terminate both listeners.
            grpc.abort();
            ftp.abort();
            let _ = grpc.await;
            let _ = ftp.await;
        }
    }

    serv.shutdown().await;

    Ok(())
}
