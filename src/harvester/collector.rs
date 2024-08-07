use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use leon::Template;
use log::error;
use subprocess::{ExitStatus, Popen, PopenConfig, Redirection};
use tokio::sync::{mpsc, RwLock};

use crate::common::config::{CollectorInfo, HarvesterConfig};
use crate::model::cnc::CommandResult;

pub struct Collector {
    pub sender_channel: mpsc::Sender<String>,
    pub collector_info_key: String,
    pub collector_info: Arc<CollectorInfo>,
    pub harvester_config: Arc<RwLock<HarvesterConfig>>,
}

impl Collector {
    async fn resolve_command(&self) -> Option<String> {
        let command_name = &self.collector_info.command.name;

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
        //     TODO: global 변수도 명령에 주입시킬 수 있도록 하기
        if let Some(param) = self.collector_info.command.param.as_ref() {
            for (k, v) in param.iter() {
                values.insert(k.clone(), v.to_string());
            }
        }

        let cmd = cmd.render(&values).unwrap();
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
            Ok(None) => 1,
            Err(err) => {
                log::error!("{err}");
                1
            }
        };

        let hostname = (&self.harvester_config.read().await.hostname).clone();
        let host_group_name = (&self.collector_info.host_group_name).clone();
        let description = (&self.collector_info.description).clone();

        let result = serde_json::to_string(&CommandResult {
            collector_info_key: self.collector_info_key.clone(),
            hostname,
            host_group_name,
            description,
            return_code,
            stdout: stdout.unwrap_or(String::from("")),
            stderr: stderr.unwrap_or(String::from("")),
        }).unwrap();

        if let Err(send_error) = self.sender_channel.send(result.to_string()).await {
            error!("Failed to send command result: {}", result);
            error!("{}", send_error);
        };
    }
}
