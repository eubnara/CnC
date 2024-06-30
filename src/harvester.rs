use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use rdkafka::ClientConfig;
use rdkafka::producer::FutureProducer;
use tokio::sync::{mpsc, RwLock};
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
        for (store_name, datastore) in self.config.read().await.get_datastores() {
            // TODO: make buffer size configurable?
            let (tx, rx) = mpsc::channel::<String>(1000);
            self.channels
                .insert(store_name.clone(), StoreChannel { tx, rx: Some(rx) });
        }
    }

    async fn run_senders(&mut self) {
        // TODO: use crossbeam-channel instead of mpsc for more parallelism?
        for (store_name, datastore) in self.config.read().await.get_datastores() {
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
            let config = self.config.read().await;
            let param = config.get_datastores().get(&store_name.clone()).unwrap().param.as_ref();
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
                    let param = match param {
                        Some(param) => param,
                        None => panic!("Empty param for kafka sender: {}", store_name)
                    };
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
        // TODO: "ALL" keyword 는 enum 혹은 예약 키워드로.
        // TODO: harvester 가 속한 hostgroup 들이 어디있는지 알아오는 과정이 필요하다. hostgroup.toml 과 ambari 정보로부터 알아온다.
        // TODO: hostgroup.toml 정보와 ambari 정보를 합치는 건 별도의 대몬 프로세스가 진행. hdfs 로부터 저장하고 받아온다.
        for (collector_name, collector_info) in
            self.config.read().await.get_collector_infos()
        {
            let tx = Arc::new(
                self.channels
                    .get(&collector_info.store_name)
                    .unwrap()
                    .tx
                    .clone(),
            );
            let config = config.clone();
            let last_notification_time = Arc::new(RwLock::new(SystemTime::now()));
            let collector_info = Arc::new(collector_info.clone());
            self.sched
                .add(
                    Job::new_async(
                        collector_info.crontab.clone().as_str(),
                        move |_, _| {
                            let harvester_config = Arc::clone(&config.clone());
                            let info = Arc::clone(&collector_info);
                            let last_notification_time = Arc::clone(&last_notification_time);
                            let tx = (*tx).clone();
                            Box::pin(async move {
                                let mut collector = Collector {
                                    sender_channel: tx,
                                    collector_info: info,
                                    harvester_config,
                                    last_notification_time,
                                };
                                collector.run().await;
                            })
                        },
                    ).unwrap()
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
