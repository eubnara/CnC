use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use leon::Template;

use log::{debug, error};
use serde_json::json;
use subprocess::{ExitStatus, Popen, PopenConfig, Redirection};
use tokio::sync::{mpsc, RwLock};
use tokio::time::sleep;

use crate::common::config::{CollectorInfo, HarvesterConfig};

pub struct Collector {
    pub sender_channel: mpsc::Sender<String>,
    pub collector_info: Arc<CollectorInfo>,
    pub harvester_config: Arc<RwLock<HarvesterConfig>>,
    pub last_notification_time: Arc<RwLock<SystemTime>>,
}

impl Collector {
    async fn resolve_command(&self) -> Option<String> {
        let command_name = &self.collector_info.command.name;

        //     TODO: params 에 있는 변수와 global 변수도 명령에 주입시킬 수 있도록 하기
        match self
            .harvester_config
            .read()
            .await
            .get_commands()
            .get(command_name)
        {
            Some(cmd) => Some(String::from(&cmd.command_line)),
            None => None,
        }
    }

    pub async fn run(&mut self) {
        let cmd = match self.resolve_command().await {
            None => {
                log::error!(
                    "Unknown command name: {}",
                    &self.collector_info.command.name
                );
                return;
            }
            Some(cmd) => cmd,
        };

        let cmd = Template::parse(&cmd).unwrap();
        let mut values: HashMap<String, String> = HashMap::new();
        if let Some(param) = self.collector_info.command.param.as_ref() {
            for (k, v) in param.iter() {
                values.insert(k.clone(), v.to_string());
            }
        }

        let cmd = cmd.render(&values).unwrap();

        let max_retries = self.collector_info.max_retries;
        let retry_interval_s = self.collector_info.retry_interval_s as u64;
        let notification_interval_s = self.collector_info.notification_interval_s as u64;
        let mut result = json!({});
        for i in 0..max_retries {
            // "sh -c" is used to support environment variables.
            let mut p = Popen::create(
                &vec!["sh", "-c", &cmd],
                PopenConfig {
                    stdout: Redirection::Pipe,
                    stderr: Redirection::Pipe,
                    ..Default::default()
                },
            )
                .unwrap();

            let (stdout, stderr) = p.communicate(None).unwrap();
            // TODO: timeout configurable?
            let return_code: i64 = match p.wait_timeout(Duration::new(10, 0)) {
                Ok(Some(exit_status)) => {
                    match exit_status {
                        ExitStatus::Exited(n) => n as i64,
                        ExitStatus::Signaled(n) => n as i64,
                        ExitStatus::Other(n) => n as i64,
                        ExitStatus::Undetermined => -1,
                    }
                }
                // TODO: more detail?
                Ok(None) => -1,
                Err(err) => {
                    log::error!("{err}");
                    -1
                }
            };
            if return_code == 0 {
                return;
            }

            result = json!({
                "return_code": return_code,
                "stdout": stdout.unwrap_or(String::from("")),
                "stderr": stderr.unwrap_or(String::from("")),
            });
            sleep(Duration::from_secs(retry_interval_s)).await;
        }
        let elapsed = match self.last_notification_time.read().await.elapsed() {
            Ok(elapsed) => elapsed,
            Err(e) => {
                error!("Error when checking notification time: {e}");
                return;
            }
        };

        if elapsed.as_secs() > notification_interval_s {
            self.last_notification_time.write().await.clone_from(&SystemTime::now());
            if let Err(send_error) = self.sender_channel.send(result.to_string()).await {
                error!("Failed to send command result: {}", result);
                error!("{}", send_error);
            };
        }
    }
}
