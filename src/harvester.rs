use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use rdkafka::ClientConfig;
use rdkafka::producer::FutureProducer;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_cron_scheduler::{Job, JobScheduler};

use collector::Collector;
use sender::{KafkaSender, Sender, StdoutSender};

use crate::common::store_channel::StoreChannel;

use super::common::config::*;

mod sender;
mod collector;



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

    async fn create_channels(&mut self) {
        for (store_name, datastore) in self.config.read().unwrap().get_datastores() {
            // TODO: make buffer size configurable?
            let (tx, rx) = mpsc::channel::<String>(1000);
            self.channels
                .insert(store_name.clone(), StoreChannel { tx, rx: Some(rx) });
        }
    }

    async fn run_senders(&mut self) {
        // TODO: use crossbeam-channel instead of mpsc for more parallelism?
        for (store_name, datastore) in self.config.read().unwrap().get_datastores() {
            let kind = datastore.kind.as_str();
            // TODO: https://stackoverflow.com/questions/68976937/rust-future-cannot-be-sent-between-threads-safely
            let rx = self
                .channels
                .get_mut(store_name)
                .expect(&format!("Unknown store name: {} for sender", store_name))
                .rx
                .take()
                .expect(&format!(
                    "Receiver channel for {} already taken.",
                    store_name
                ));
            let config = self.config.read().unwrap();
            let param = config.get_datastores().get(&store_name.clone()).unwrap().param.as_ref().unwrap();
            match kind {
                // TODO: stdout, kafka 를 enum 형태로?, 설정파일에 들어갈 수 있는 값을 쉽게 찾을 수 있게끔?
                "stdout" => {
                    let handler = tokio::spawn(async move {
                        let mut sender = StdoutSender {
                            receiver_channel: rx,
                        };
                        sender.run().await;
                    });
                    self.handlers.push(handler);
                }
                "kafka" => {
                    let bootstrap_servers = String::from(param.get("bootstrap.servers").unwrap().as_str().unwrap());
                    let topic = String::from(param.get("topic").unwrap().as_str().unwrap());
                    let handler = tokio::spawn(async move {
                        let producer: FutureProducer = ClientConfig::new()
                            .set("bootstrap.servers", bootstrap_servers)
                            .set("message.timeout.ms", "5000")
                            .create()
                            .expect("Producer creation error");
                        let mut sender = KafkaSender {
                            receiver_channel: rx,
                            producer,
                            topic,
                        };
                        sender.run().await;
                    });
                    self.handlers.push(handler);
                }
                _ => {
                    panic!("Unknown sender kind: {}", kind)
                }
            }
        }
    }

    async fn run_collectors(&self, config: Arc<RwLock<HarvesterConfig>>) {
        for (collector_name, collector_info) in
            self.config.read().unwrap().get_collector_infos()
        {
            let tx = Arc::new(RwLock::new(
                self.channels
                    .get(&collector_info.store_name)
                    .unwrap()
                    .tx
                    .clone(),
            ));
            let info = Arc::new(RwLock::new(collector_info.clone()));
            let config = config.clone();
            self.sched
                .add(
                    Job::new_async(
                        collector_info.crontab.clone().as_str(),
                        move |uuid, mut l| {
                            let harvester_config = Arc::clone(&config.clone());
                            let tx = tx.read().unwrap().clone();
                            let info = Arc::new(info.read().unwrap().clone());
                            Box::pin(async move {
                                let collector = Collector {
                                    sender_channel: tx,
                                    collector_info: info,
                                    harvester_config,
                                };
                                collector.run().await;
                            })
                        },
                    )
                        .unwrap(),
                )
                .await
                .unwrap();
        }
    }

    pub async fn run(&mut self, config: Arc<RwLock<HarvesterConfig>>) {
        self.create_channels().await;
        self.run_senders().await;
        self.run_collectors(config).await;

        self.sched.start().await.unwrap();
        while let Some(h) = self.handlers.pop() {
            h.await.unwrap();
        }
    }
}
