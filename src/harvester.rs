use std::collections::HashMap;
use std::sync::mpsc;
use std::thread;
use std::thread::JoinHandle;
use subprocess::{ExitStatus, Popen, PopenConfig, Redirection};
use log::log;
use super::config::*;
use serde_json::json;

struct Collector<'a> {
    sender_channel: mpsc::Sender<String>,
    collector_info: &'a CollectorInfo,
    harvester_config: &'a HarvesterConfig,
}

impl<'a> Collector<'a> {
    fn new<'b>(collector_info: &'b CollectorInfo, sender_channel: mpsc::Sender<String>, harvester_config: &'b HarvesterConfig) -> Collector<'b> {
        Collector { collector_info, sender_channel, harvester_config }
    }

    fn resolve_command(&self) -> Option<&String> {
        let command_name = &self.collector_info.command_name;
        //     TODO: params 에 있는 변수와 global 변수도 명령에 주입시킬 수 있도록 하기
        match self.harvester_config.get_commands().get(command_name) {
            Some(cmd) => Some(&cmd.command_line),
            None => None
        }
    }

    fn run(&self) {
        let command_line = match self.resolve_command() {
            None => {
                log::error!("Unknown command name: {}", &self.collector_info.command_name);
                return;
            }
            Some(command_line) => command_line
        };
        let mut p = Popen::create(&command_line.split_whitespace().collect::<Vec<&str>>(), PopenConfig {
            stdout: Redirection::Pipe,
            stderr: Redirection::Pipe,
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
            for result in self.receiver_channel.recv() {
                println!("{}", result);
            }
        }
    }
}

struct StoreChannel {
    tx: mpsc::Sender<String>,
    rx: mpsc::Receiver<String>,
}

pub struct Harvester {
    config: HarvesterConfig,
    channel: HashMap<String, mpsc::Sender<String>>,
    handlers: Vec<JoinHandle<()>>,
}

impl Harvester {

    fn run_senders(&mut self) {
        for (store_name, datastore) in self.config.get_datastores() {
            let kind = datastore.kind.as_str();
            let (tx, rx) = mpsc::channel::<String>();
            self.channel.insert(store_name.clone(), tx);
            match kind {
                "stdout" => {
                    let handler = thread::spawn(move || {
                       let sender = StdoutSender {
                           receiver_channel: rx
                       };
                        sender.run();
                    });
                    self.handlers.push(handler);
                }
                _ => {panic!("Unknown sender kind: {}", kind)}
            }
        }
        
    }

    fn run_collectors(&self) {
        thread::scope(|scope| {
            let mut handlers = vec![];
            for (collector_name, collector_info) in self.config.get_collector_infos() {
                let store_name = &collector_info.store_name;
                let tx = self.channel.get(store_name).expect(&format!("Unknown store name: {} for collector {}", store_name, collector_name));
                let h = scope.spawn(move || {
                    let collector = Collector {
                        sender_channel: mpsc::Sender::clone(tx),
                        collector_info: &collector_info,
                        harvester_config: &self.config,
                    };
                    collector.run();
                });
                handlers.push(h);
            }
            while let Some(h) = handlers.pop() {
                h.join().unwrap();
            }
        });
    }

    pub fn new(config_dir: &str) -> Harvester {
        let config = HarvesterConfig::new(config_dir);

        let harvester = Harvester {
            config,
            channel: HashMap::new(),
            handlers: vec![],
        };


        harvester
    }

    pub fn run(&mut self) {
        self.run_senders();
        self.run_collectors();
        while let Some(h) = self.handlers.pop() {
            h.join().unwrap();
        }
        //     TODO: 간단한 커맨드 실행 후, StdoutSender 조합으로 테스트해보자.
        //     TODO: join? signal handler? graceful stop?
    }
}
