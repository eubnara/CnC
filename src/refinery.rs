use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

use rdkafka::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::common::config::{CheckerInfo, RefineryConfig};
use crate::common::store_channel::StoreChannel;
use crate::refinery::checker::{Checker, HttpAlertChecker, StdoutAlertChecker};
use crate::refinery::receiver::{KafkaReceiver, Receiver};

mod receiver;
mod checker;

pub struct Refinery {
    config: Arc<RwLock<RefineryConfig>>,
    handlers: Vec<JoinHandle<()>>,
    channels: HashMap<String, StoreChannel>,
}

impl Refinery {
    pub async fn new(config: Arc<RwLock<RefineryConfig>>) -> Refinery {
        Refinery {
            config,
            handlers: vec![],
            channels: HashMap::new(),
        }
    }

    async fn create_channels(&mut self) {
        for (checker_name, _) in self.config.read().unwrap().get_checkers() {
            let (tx, rx) = mpsc::channel::<String>(1000);
            self.channels.insert(checker_name.clone(), StoreChannel { tx, rx: Some(rx) });
        }
    }

    async fn run_checkers(&mut self) {
        for (checker_name, checker) in self.config.write().unwrap().get_checkers() {
            let kind = checker.kind.as_str();
            let rx = self
                .channels
                .get_mut(checker_name)
                .unwrap()
                .rx.take()
                .unwrap();
            match kind {
                "stdout_alert" => {
                    let handler = tokio::spawn(async move {
                        let mut checker = StdoutAlertChecker {
                            receiver_channel: rx,
                        };
                        checker.check().await;
                    });
                    self.handlers.push(handler);
                }
                "http_alert" => {
                    let checker_config = CheckerInfo {
                        kind: checker.kind.clone(),
                        source: checker.source.clone(),
                        param: checker.param.clone(),
                    };
                    let handler = tokio::spawn(async move {
                        let mut checker = HttpAlertChecker {
                            receiver_channel: rx,
                            checker_config,
                        };
                        checker.check().await;
                    });
                    self.handlers.push(handler);
                }
                _ => {
                    panic!("Unknown checker kind: {}", kind);
                }
            }
        }
    }

    async fn run_receivers(&mut self) {
        for (checker_name, checker) in self.config.read().unwrap().get_checkers() {
            let tx = self.channels
                .get(checker_name)
                .unwrap()
                .tx
                .clone();
            let source = &checker.source;
            let config = self.config.read().unwrap();
            let source_datastore = config.get_datastores().get(source)
                .expect("Unexpected source");
            let source_kind = source_datastore.kind.as_str();
            let param = source_datastore.param.as_ref().unwrap();
            let receiver = match source_kind {
                "kafka" => {
                    let group_instance_id = format!("refinery-{}", gethostname::gethostname().into_string().unwrap());
                    let consumer: StreamConsumer = ClientConfig::new()
                        .set("group.id", "refinery")
                        .set("group.instance.id", group_instance_id)
                        .set("bootstrap.servers", param.get("bootstrap.servers").unwrap().as_str().unwrap())
                        .create()
                        .expect("Kafka consumer creation failed");
                    let topic = String::from(param.get("topic").unwrap().as_str().unwrap());
                    KafkaReceiver {
                        sender_channel: tx,
                        consumer,
                        topic,
                    }
                }
                _ => panic!("Unsupported source datastore kind: {}", source_kind),
            };
            let handler = tokio::spawn(async move {
                receiver.receive().await;
            });
            self.handlers.push(handler);
        }
    }

    pub async fn run(&mut self) {
        self.create_channels().await;
        self.run_checkers().await;
        self.run_receivers().await;

        while let Some(h) = self.handlers.pop() {
            h.await.unwrap();
        }
    }
}
