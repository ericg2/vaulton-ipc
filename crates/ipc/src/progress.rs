use crate::ipc::job_event::Data;
use crate::ipc::{JobBarFinished, JobBarIncrement, JobBarLengthSet, JobBarTitleSet};
use crate::proto_stamp;
use crossbeam_channel::Sender;
use rustic_core::{Progress, ProgressBars, ProgressType, RusticProgress};
use rustic_core::jiff::Timestamp;
use uuid::Uuid;

#[derive(Clone, Copy, Debug)]
pub struct RepoBar {
    pub job_id: Uuid,
    pub bar_id: Uuid,
    pub mode: ProgressType,
}

#[derive(Clone, Debug)]
pub struct TxProgress {
    bar: RepoBar,
    tx: Sender<Data>,
    prefix: String,
}

impl RusticProgress for TxProgress {
    fn is_hidden(&self) -> bool {
        false
    }

    fn set_length(&self, length: u64) {
        self.tx
            .send(Data::JobStepLengthSet(JobBarLengthSet {
                job_id: self.bar.job_id.to_string(),
                bar_id: self.bar.bar_id.to_string(),
                time: proto_stamp(Timestamp::now()),
                length,
            }))
            .unwrap();
    }

    fn set_title(&self, title: &str) {
        self.tx
            .send(Data::JobStepTitleSet(JobBarTitleSet {
                job_id: self.bar.job_id.to_string(),
                bar_id: self.bar.bar_id.to_string(),
                time: proto_stamp(Timestamp::now()),
                title: format!("[{}] {}", &self.prefix, title),
            }))
            .unwrap();
    }

    fn inc(&self, inc: u64) {
        self.tx
            .send(Data::JobStepIncrement(JobBarIncrement {
                job_id: self.bar.job_id.to_string(),
                bar_id: self.bar.bar_id.to_string(),
                increment: inc,
                time: proto_stamp(Timestamp::now()),
            }))
            .unwrap();
    }

    fn finish(&self) {
        self.tx
            .send(Data::JobStepFinished(JobBarFinished {
                job_id: self.bar.job_id.to_string(),
                bar_id: self.bar.bar_id.to_string(),
                time: proto_stamp(Timestamp::now()),
            }))
            .unwrap();
    }
}

#[derive(Clone, Debug)]
pub struct RusticProgressBars {
    job_id: Uuid,
    tx: Sender<Data>,
}

impl RusticProgressBars {
    fn create_new(&self, mode: ProgressType, prefix: String) -> TxProgress {
        let tx = self.tx.clone();
        let bar = RepoBar {
            job_id: self.job_id,
            bar_id: Uuid::new_v4(),
            mode,
        };
        TxProgress { bar, tx, prefix }
    }
    pub fn new(job_id: Uuid, tx: Sender<Data>) -> Self {
        Self { job_id, tx }
    }
}

impl ProgressBars for RusticProgressBars {
    fn progress(&self, progress_type: ProgressType, prefix: &str) -> Progress {
        Progress::new(self.create_new(progress_type, prefix.to_string()))
    }
}
