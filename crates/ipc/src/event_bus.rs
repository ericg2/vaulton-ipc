use crate::ipc::ipc_event::Data;
use crossbeam_channel::{Sender, TrySendError};

/// Bounded producer-side event capacity. Non-critical telemetry is dropped
/// when this fills; terminal job/point-state events apply backpressure.
pub const CAPACITY: usize = 8_192;

pub fn is_critical(event: &Data) -> bool {
    match event {
        Data::JobFinished(_) | Data::PointStatus(_) => true,
        Data::JobMessage(e) => e.priority >= crate::ipc::Priority::Error as i32,
        Data::LogEvent(e) => e.priority >= crate::ipc::Priority::Error as i32,
        _ => false,
    }
}

/// Sends an event without allowing high-frequency progress/log traffic to grow
/// memory without bound. Critical state transitions use the blocking path so
/// a full queue cannot silently lose them.
pub fn send(tx: &Sender<Data>, event: Data) -> bool {
    if is_critical(&event) {
        tx.send(event).is_ok()
    } else {
        match tx.try_send(event) {
            Ok(()) => true,
            Err(TrySendError::Disconnected(_)) | Err(TrySendError::Full(_)) => false,
        }
    }
}
