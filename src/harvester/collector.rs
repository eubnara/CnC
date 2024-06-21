use std::collections::HashMap;
use std::ffi::OsStr;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use leon::Template;

use log::error;
use serde_json::json;
use subprocess::{ExitStatus, Popen, PopenConfig, Redirection};
use tokio::sync::mpsc;

use crate::common::config::{CollectorInfo, HarvesterConfig};

pub struct Collector {
    pub sender_channel: mpsc::Sender<String>,
    pub collector_info: Arc<CollectorInfo>,
    pub harvester_config: Arc<RwLock<HarvesterConfig>>,
}

impl Collector {
    fn resolve_command(&self) -> Option<String> {
        let command_name = &self.collector_info.command.name;

        //     TODO: params 에 있는 변수와 global 변수도 명령에 주입시킬 수 있도록 하기
        match self
            .harvester_config
            .read()
            .unwrap()
            .get_commands()
            .get(command_name)
        {
            Some(cmd) => Some(String::from(&cmd.command_line)),
            None => None,
        }
    }

    pub async fn run(&self) {
        // TODO: 아래 3가지 로직 구현
        // TODO: retry_interval_s: 재시도 사이 sleep 시간
        // TODO: max_retries: 최대 재시도 횟수
        // TODO: notification_interval_s: 같은 알람이 언제 마다 반복될지 (e.g. 실패알람이 너무 자주오는 경우를 막기 위함)
        let cmd = match self.resolve_command() {
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

        let (out, err) = p.communicate(None).unwrap();
        // TODO: timeout configurable?
        let return_code: i64 = match p.wait_timeout(Duration::new(10, 0)) {
            Ok(Some(exit_status)) => {
                match exit_status {
                    ExitStatus::Exited(n) => n as i64,
                    ExitStatus::Signaled(n) => n as i64,
                    ExitStatus::Other(n) => n as i64,
                    ExitStatus::Undetermined => -1,
                }
            },
            // TODO: more detail?
            Ok(None) => -1,
            Err(err) => {
                log::error!("{err}");
                -1
            }
        };
        // TODO: return_code 가 0 이 아닐 때만 (비정상일 때만) 메시지 보내도록 하기
        let out = out.unwrap_or(String::from(""));
        let err = err.unwrap_or(String::from(""));
        let result = json!({
            "return_code": return_code,
            "stdout": out,
            "stderr": err,
        });
        if let Err(send_error) = self.sender_channel.send(result.to_string()).await {
            error!("Failed to send command result: {}", result);
            error!("{}", send_error);
        };
    }
}
