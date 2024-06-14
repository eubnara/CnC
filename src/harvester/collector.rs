use std::ffi::OsStr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use log::error;
use serde_json::json;
use subprocess::{ExitStatus, Popen, PopenConfig, Redirection};
use tokio::sync::mpsc;

use crate::config::{CollectorInfo, HarvesterConfig};

pub struct Collector {
    pub sender_channel: mpsc::Sender<String>,
    pub collector_info: Arc<CollectorInfo>,
    pub harvester_config: Arc<RwLock<HarvesterConfig>>,
}

impl Collector {
    fn resolve_command(&self) -> Option<String> {
        let command_name = &self.collector_info.command_name;

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
        let command_line = match self.resolve_command() {
            None => {
                log::error!(
                    "Unknown command name: {}",
                    &self.collector_info.command_name
                );
                return;
            }
            Some(command_line) => command_line,
        };
        // TODO: ${var} 형태는 환경변수에서
        // TODO: {var} 형태는 다른 변수에서 가져와야, ambari 혹은 설정파일
        let env = Some(vec![(
            OsStr::new("my_env").into(),
            OsStr::new("my_value").into(),
        )]);
        let mut p = Popen::create(
            &vec!["sh", "-c", &command_line],
            PopenConfig {
                stdout: Redirection::Pipe,
                stderr: Redirection::Pipe,
                env,
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
