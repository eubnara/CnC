use std::cmp;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use chrono::{DateTime, Local};
use log::error;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, RwLock};

use crate::model::cnc::CommandResult;

#[derive(Deserialize, Serialize, Debug)]
pub struct AlertMessage {
    pub hostname: String,
    pub host_group_name: String,
    pub description: String,
    pub status: Status,
    pub msg: String,
}

#[derive(PartialEq)]
enum AlertLevel {
    INFO,
    WARING,
    CRITICAL,
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub enum Status {
    Ok,
    Caution,
    Failed,
    Cleared,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CollectorState {
    pub hostname: String,
    pub description: String,
    pub host_group_name: String,
    pub retry_interval_s: u32,
    pub max_retries: u32,
    pub notification_interval_s: u32,
    pub last_notified_time: SystemTime,
    pub last_updated_time: SystemTime,
    pub failed_count: u32,
    pub status: Status,
    pub last_msg: String,
}

impl CollectorState {
    fn is_time_to_notify(&self) -> bool {
        let elapsed = match self.last_notified_time.elapsed() {
            Ok(elapsed) => elapsed,
            Err(e) => {
                error!("Error when getting duration between last_notification_time and now.: {}", e);
                return true;
            }
        };
        elapsed.as_secs() > self.notification_interval_s as u64
    }

    fn not_updated(&self) -> bool {
        let elapsed = match self.last_updated_time.elapsed() {
            Ok(elapsed) => elapsed,
            Err(e) => {
                error!("Error when getting duration between last_updated_time and now.: {}", e);
                return true;
            }
        };
        elapsed.as_secs() > (self.retry_interval_s * (self.max_retries + 1)) as u64
    }

    pub fn process_result(&mut self, result: CommandResult) {
        let failed = result.return_code != 0;
        if failed {
            self.failed_count = cmp::min(self.failed_count + 1, self.max_retries);
            if self.failed_count == self.max_retries {
                self.status = Status::Failed;
            } else {
                self.status = Status::Caution;
            }
        } else {
            if self.failed_count > 0 {
                self.failed_count -= 1;
                if self.failed_count == 0 {
                    self.status = Status::Cleared;
                }
            }
        }
        self.last_msg = format!("stdout: {}\nstderr: {}", result.stdout, result.stderr);
        self.last_updated_time = SystemTime::now();
    }

    pub async fn check_alert(&mut self, sender_channel: &mpsc::Sender<AlertMessage>) {

            if !self.is_time_to_notify() {
                return;
            }

            if self.not_updated() {
                self.status = Status::Failed;
                let datetime: DateTime<Local> = self.last_updated_time.into();
                self.last_msg = format!("No update since {}", datetime.format("%Y-%m-%d %H:%M:%S"));
            }

            match self.status {
                Status::Ok | Status::Caution => {
                    return;
                }
                _ => {}
            }

            let alert_message = AlertMessage {
                hostname: self.hostname.clone(),
                host_group_name: self.host_group_name.clone(),
                description: self.description.clone(),
                status: self.status.clone(),
                msg: self.last_msg.clone(),
            };

            match sender_channel.send(alert_message).await {
                Err(send_error) => {
                    error!("Failed to send alert message");
                    error!("{}", send_error);
                },
                Ok(_) => {
                    if self.status == Status::Cleared {
                        self.status = Status::Ok;
                    }
                    self.last_notified_time = SystemTime::now();
                },
            }
    }
}

#[derive(Debug)]
pub struct Host {
    pub collector_states: Arc<RwLock<HashMap<String, RwLock<CollectorState>>>>,
    pub sender_channel: mpsc::Sender<AlertMessage>,
}

impl Host {
    pub async fn run(&self) {
        for (key, collector_state) in self.collector_states.read().await.iter() {
            let mut collector_state = collector_state.write().await;
            // to check collector state not updated for long time.
            collector_state.check_alert(&self.sender_channel).await;
        }
    }
}
