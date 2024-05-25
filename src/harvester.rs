use std::borrow::Cow;
use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::sync::{Arc, mpsc, Mutex, RwLock};
use std::thread;
use std::thread::JoinHandle;

use log::log;
use serde_json::json;
use subprocess::{ExitStatus, Popen, PopenConfig, Redirection};
use tokio_cron_scheduler::{Job, JobScheduler};

use super::config::*;

struct Collector {
    sender_channel: mpsc::Sender<String>,
    collector_info: Arc<CollectorInfo>,
    harvester_config: Arc<RwLock<HarvesterConfig>>,
}

impl Collector {
    fn resolve_command(&self) -> Option<String> {
        let command_name = &self.collector_info.command_name;

        //     TODO: params 에 있는 변수와 global 변수도 명령에 주입시킬 수 있도록 하기
        match self.harvester_config.read().unwrap().get_commands().get(command_name) {
            Some(cmd) => Some(String::from(&cmd.command_line)),
            None => None
        }
    }

    fn run(&self) {
        // TODO: 아래 3가지 로직 구현
        // TODO: retry_interval_s: 재시도 사이 sleep 시간
        // TODO: max_retries: 최대 재시도 횟수
        // TODO: notification_interval_s: 같은 알람이 언제 마다 반복될지 (e.g. 실패알람이 너무 자주오는 경우를 막기 위함)
        let command_line = match self.resolve_command() {
            None => {
                log::error!("Unknown command name: {}", &self.collector_info.command_name);
                return;
            }
            Some(command_line) => command_line
        };
        // TODO: ${var} 형태는 환경변수에서
        // TODO: {var} 형태는 다른 변수에서 가져와야, ambari 혹은 설정파일
        let env = Some(vec![
            (OsStr::new("my_env").into(), OsStr::new("my_value").into())
        ]);
        let mut p = Popen::create(&vec!["sh", "-c", &command_line], PopenConfig {
            stdout: Redirection::Pipe,
            stderr: Redirection::Pipe,
            env,
            ..Default::default()
        }).unwrap();

        let (out, err) = p.communicate(None).unwrap();
        let return_code: i64 = match p.exit_status().unwrap_or_else(|| ExitStatus::Undetermined) {
            ExitStatus::Exited(n) => n as i64,
            ExitStatus::Signaled(n) => n as i64,
            ExitStatus::Other(n) => n as i64,
            ExitStatus::Undetermined => -1,
        };
        let out = out.unwrap_or(String::from(""));
        let err = err.unwrap_or(String::from(""));
        let result = json!({
            "return_code": return_code,
            "stdout": out,
            "stderr": err,
        });
        if let Err(send_error) = self.sender_channel.send(result.to_string()) {
            log::error!("Failed to send command result: {}", result);
            log::error!("{}", send_error);
        };
    }
}

trait Sender {
    fn run(&self) {}
}

// struct KafkaSender {
//     receiver_channel: mpsc::Receiver<CommandResult>,
// }
//
// impl Sender for KafkaSender {
//     fn send(&self) {}
// }

struct StdoutSender {
    receiver_channel: mpsc::Receiver<String>,
}

impl Sender for StdoutSender {
    fn run(&self) {
        loop {
            match self.receiver_channel.recv() {
                Ok(result) => println!("{}", result),
                Err(err) => log::error!("{}", err),
            }
        }
    }
}

struct StoreChannel {
    tx: mpsc::Sender<String>,
    rx: Option<mpsc::Receiver<String>>,
}

pub struct Harvester {
    config: Arc<RwLock<HarvesterConfig>>,
    handlers: Vec<JoinHandle<()>>,
    channels: HashMap<String, StoreChannel>,
    sched: JobScheduler,
}

impl Harvester {
    pub async fn new(config: Arc<RwLock<HarvesterConfig>>) -> Harvester {
        let sched = JobScheduler::new().await.unwrap();

        let harvester = Harvester {
            config,
            handlers: vec![],
            channels: HashMap::new(),
            sched,
        };


        harvester
    }

    fn create_channels(&mut self) {
        for (store_name, datastore) in self.config.read().unwrap().get_datastores().into_iter() {
            let (tx, rx) = mpsc::channel::<String>();
            self.channels.insert(store_name.clone(), StoreChannel { tx, rx: Some(rx) });
        }
    }

    fn run_senders(&mut self) {
        // TODO: tokio 로 변경?
        for (store_name, datastore) in self.config.read().unwrap().get_datastores() {
            let kind = datastore.kind.as_str();
            let rx = self.channels.get_mut(store_name)
                .expect(&format!("Unknown store name: {} for sender", store_name))
                .rx.take()
                .expect(&format!("Receiver channel for {} already taken.", store_name));
            match kind {
                "stdout" => {
                    let handler = thread::spawn(move || {
                        let sender = StdoutSender {
                            receiver_channel: rx,
                        };
                        sender.run();
                    });
                    self.handlers.push(handler);
                }
                _ => { panic!("Unknown sender kind: {}", kind) }
            }
        }
    }

    async fn run_collectors(&self, config: Arc<RwLock<HarvesterConfig>>) {
        for (collector_name, collector_info) in self.config.read().unwrap().get_collector_infos().iter() {
            let tx = Arc::new(RwLock::new(self.channels.get(&collector_info.store_name).unwrap().tx.clone()));
            let info = Arc::new(RwLock::new(collector_info.clone()));
            let config = config.clone();

            self.sched.add(Job::new_async(collector_info.crontab.clone().as_str(), move |uuid, mut l| {
                let harvester_config = Arc::clone(&config.clone());
                let tx = tx.read().unwrap().clone();
                let info = Arc::new(info.read().unwrap().clone());
                Box::pin(async move {
                    let collector = Collector {
                        sender_channel: tx,
                        collector_info: info,
                        harvester_config,
                    };
                    collector.run();
                })
            }).unwrap()).await.unwrap();
        }
    }

    pub async fn run(&mut self, config: Arc<RwLock<HarvesterConfig>>) {
        self.create_channels();
        self.run_senders();
        self.run_collectors(config).await;
        if let Err(e) = self.sched.start().await {
            log::error!("Error on scheduler {:?}", e);
        }
        while let Some(h) = self.handlers.pop() {
            h.join().unwrap();
        }
    }
}
