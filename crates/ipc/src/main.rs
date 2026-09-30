use crate::db::DbManager;
use crate::event_bus::send as send_event;
use crate::ipc::LogEvent;
use crate::ipc::ipc_event::Data;
use crate::ipc::ipc_service_server::IpcServiceServer;
use crate::server::GrpcServer;
use crate::store::StorageManager;
use crate::utils::fix_level;

use clap::{ArgAction, Parser, ValueEnum};
use crossbeam_channel::Sender;
use log::{LevelFilter, Log, Metadata, Record, error, info, warn};
use prost_types::Timestamp;
use simplelog::{
    ColorChoice, CombinedLogger, Config, SharedLogger, SimpleLogger, TermLogger, TerminalMode,
    WriteLogger,
};
use std::error::Error;
use std::fs::{OpenOptions, create_dir_all};
use std::path::{Path, PathBuf};
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

const TTL: Duration = Duration::from_mins(3);

const DEFAULT_DB_PATH: &str = "poop.sqlite";
const DEFAULT_GRPC_ADDR: &str = "127.0.0.1:8080";
const DEFAULT_FTP_ADDR: &str = "127.0.0.1:2121";
const DEFAULT_LOG_FILE: &str = "vfs-server.log";

/// Log level exposed through the command line.
#[derive(Debug, Clone, Copy, ValueEnum)]
enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl From<LogLevel> for LevelFilter {
    fn from(level: LogLevel) -> Self {
        match level {
            LogLevel::Error => LevelFilter::Error,
            LogLevel::Warn => LevelFilter::Warn,
            LogLevel::Info => LevelFilter::Info,
            LogLevel::Debug => LevelFilter::Debug,
            LogLevel::Trace => LevelFilter::Trace,
        }
    }
}

/// VFS server command-line configuration.
///
/// All options have sensible defaults so the server can simply be started
/// with no arguments.
#[derive(Debug, Parser)]
#[command(
    name = "vfs-server",
    version,
    about = "VFS gRPC/FTP server",
    long_about = "Runs the VFS gRPC and FTP servers and persists metadata in SQLite."
)]
struct Args {
    /// Path to the SQLite database.
    #[arg(
        long,
        env = "VFS_DB",
        default_value = DEFAULT_DB_PATH,
        value_name = "PATH"
    )]
    db_path: PathBuf,

    /// Address for the gRPC server.
    #[arg(
        long,
        env = "VFS_GRPC_ADDR",
        default_value = DEFAULT_GRPC_ADDR,
        value_name = "ADDR"
    )]
    grpc_addr: String,

    /// Address for the FTP server.
    #[arg(
        long,
        env = "VFS_FTP_ADDR",
        default_value = DEFAULT_FTP_ADDR,
        value_name = "ADDR"
    )]
    ftp_addr: String,

    /// Minimum log level.
    ///
    /// Can be overridden by -v/-vv/-vvv.
    #[arg(
        long,
        env = "VFS_LOG_LEVEL",
        value_enum,
        default_value_t = LogLevel::Info,
        value_name = "LEVEL"
    )]
    log_level: LogLevel,

    /// Increase logging verbosity.
    ///
    /// -v   = debug
    /// -vv  = trace
    /// -vvv = trace
    #[arg(short = 'v', long = "verbose", action = ArgAction::Count)]
    verbose: u8,

    /// Path to the log file.
    ///
    /// Logs are appended to this file.
    #[arg(
        long,
        env = "VFS_LOG_FILE",
        default_value = DEFAULT_LOG_FILE,
        value_name = "PATH"
    )]
    log_file: PathBuf,

    /// Disable logging to a file.
    #[arg(long)]
    no_log_file: bool,

    /// Enable Rust backtraces.
    #[arg(long, default_value_t = true)]
    backtrace: bool,

    /// Disable Rust backtraces.
    ///
    /// This takes precedence over --backtrace.
    #[arg(long)]
    no_backtrace: bool,
}

impl Args {
    fn effective_log_level(&self) -> LevelFilter {
        // Explicit verbosity takes precedence over --log-level.
        match self.verbose {
            0 => self.log_level.into(),
            1 => LevelFilter::Debug,
            _ => LevelFilter::Trace,
        }
    }

    fn backtrace_enabled(&self) -> bool {
        self.backtrace && !self.no_backtrace
    }
}

struct ChannelLogger {
    logger: Box<dyn Log + Send + Sync>,
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

        // libunftp is extremely noisy at INFO/DEBUG/TRACE. Keep its
        // warnings and errors because authentication failures and other
        // operational problems are useful.
        if record.target().starts_with("libunftp") && record.level() < log::Level::Warn {
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
                time: utils::proto_stamp(rustic_core::jiff::Timestamp::now()),
            }),
        );
    }

    fn flush(&self) {
        self.logger.flush();
    }
}
fn initialize_logging(
    level: LevelFilter,
    log_file: Option<&Path>,
    tx: Sender<Data>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut loggers: Vec<Box<dyn SharedLogger>> = Vec::new();

    // Change SimpleLogger to TermLogger to get colored terminal output
    loggers.push(TermLogger::new(
        level,
        Config::default(),
        TerminalMode::Stderr,
        ColorChoice::Auto, // Automatically checks if terminal supports color
    ));

    // Keep file logger clean without colors
    if let Some(path) = log_file {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                create_dir_all(parent)?;
            }
        }

        let file = OpenOptions::new().create(true).append(true).open(path)?;
        loggers.push(WriteLogger::new(level, Config::default(), file));
    }

    let combined = CombinedLogger::new(loggers);
    let logger = ChannelLogger {
        logger: Box::new(combined),
        tx,
    };

    log::set_boxed_logger(Box::new(logger))?;
    log::set_max_level(level);
    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    info!("shutdown signal received");
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    if args.backtrace_enabled() {
        // SAFETY:
        // This is performed before the Tokio runtime starts doing meaningful
        // application work and is intentionally controlled by the CLI.
        unsafe {
            std::env::set_var("RUST_BACKTRACE", "1");
        }
    } else {
        // SAFETY:
        // Same reasoning as above. We explicitly disable application-level
        // backtraces when requested.
        unsafe {
            std::env::set_var("RUST_BACKTRACE", "0");
        }
    }

    // One global event channel: logger, StorageManager and jobs -> Poll.
    let (tx, rx) = crossbeam_channel::bounded::<Data>(crate::event_bus::CAPACITY);

    let log_level = args.effective_log_level();

    let log_file = if args.no_log_file {
        None
    } else {
        Some(args.log_file.as_path())
    };

    if let Err(e) = initialize_logging(log_level, log_file, tx.clone()) {
        eprintln!("failed to initialize logging: {e}");
        std::process::exit(1);
    }

    info!("VFS server starting");
    info!("log level: {log_level:?}");

    if let Some(path) = log_file {
        info!("log file: {}", path.display());
    } else {
        info!("file logging disabled");
    }

    info!("database: {}", args.db_path.display());
    info!("gRPC address: {}", args.grpc_addr);
    info!("FTP address: {}", args.ftp_addr);
    if let Err(e) = run(args, tx, rx).await {
        error!("fatal: {e}");
        std::process::exit(1);
    }

    // Attempt to finish all running jobs.
    info!("VFS server stopped");
}

async fn run(
    args: Args,
    tx: Sender<Data>,
    rx: crossbeam_channel::Receiver<Data>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let db_path = args.db_path;
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

    info!("listening: grpc {}, ftp {}", args.grpc_addr, args.ftp_addr);

    // One broadcast channel is used to coordinate shutdown between
    // the gRPC and FTP server tasks.
    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);

    let signal_tx = shutdown_tx.clone();

    tokio::spawn(async move {
        shutdown_signal().await;
        let _ = signal_tx.send(());
    });

    let mut grpc_shutdown = shutdown_tx.subscribe();

    let grpc_addr = args.grpc_addr.parse()?;

    let mut grpc = tokio::spawn(async move {
        Server::builder()
            .tcp_nodelay(true)
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .http2_keepalive_interval(Some(Duration::from_secs(30)))
            .add_service(svc)
            .serve_with_shutdown(grpc_addr, async move {
                let _ = grpc_shutdown.recv().await;
            })
            .await
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
    });

    let ftp_addr = args.ftp_addr;
    let mut ftp = tokio::spawn(async move {
        ftp_server
            .listen(&ftp_addr)
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
