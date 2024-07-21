use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use axum::{Extension, Router};
use axum::routing::get;
use log::debug;
use rdkafka::ClientConfig;
use rdkafka::producer::FutureProducer;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio_cron_scheduler::{Job, JobScheduler};

use collector::Collector;
use sender::{KafkaSender, Sender, StdoutSender};
use uuid::Uuid;
use crate::common::store_channel::StoreChannel;

use super::common::config::*;

mod sender;
mod collector;

struct HarvesterContainer {
    harvester: Arc<RwLock<Harvester>>,
}

impl HarvesterContainer {
    pub async fn new(harvester: Arc<RwLock<Harvester>>) -> Arc<RwLock<HarvesterContainer>> {
        harvester.write().await.start().await;
        Arc::new(RwLock::new(HarvesterContainer { harvester }))
    }

    pub async fn reload(&mut self) {
        self.harvester.write().await.config.write().await.reload_config().await;
        let new = Harvester::new(self.harvester.read().await.config.clone()).await;
        {
            let mut old = self.harvester.write().await;
            old.stop().await;
            drop(old);
        }
        new.write().await.start().await;
        self.harvester = new;
    }
}

pub struct Harvester {
    config: Arc<RwLock<HarvesterConfig>>,
    handlers: Vec<JoinHandle<()>>,
    channels: HashMap<String, StoreChannel>,
    sched: Arc<RwLock<JobScheduler>>,
    sched_uuids: Vec<Uuid>,
}

impl Harvester {

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
                    let kafka = config.get_datastores().get(&store_name.clone()).unwrap().kafka.as_ref()
                        .expect(&format!("Empty param for kafka sender: {}", store_name));
                    let bootstrap_servers = String::from(&kafka.bootstrap_servers);
                    let topic = String::from(&kafka.topic);
                    let message_timeout_ms = String::from(&kafka.message_timeout_ms);
                    let handler = tokio::spawn(async move {
                        let producer: FutureProducer = ClientConfig::new()
                            .set("bootstrap.servers", bootstrap_servers)
                            .set("message.timeout.ms", message_timeout_ms)
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

    async fn run_collectors(&mut self) {
        // TODO: "ALL" keyword 는 enum 혹은 예약 키워드로.
        // TODO: harvester 가 속한 host_group 들이 어디있는지 알아오는 과정이 필요하다. host_group.toml 과 ambari 정보로부터 알아온다.
        // TODO: host_group.toml 정보와 ambari 정보를 합치는 건 별도의 대몬 프로세스가 진행. hdfs 로부터 저장하고 받아온다.
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
            let config = Arc::clone(&self.config);
            let last_notification_time = Arc::new(RwLock::new(SystemTime::now()));
            let collector_info = Arc::new(collector_info.clone());

            let uuid = self.sched.write().await
                .add(
                    Job::new_async(
                        collector_info.crontab.clone().as_str(),
                        move |_, _| {
                            let harvester_config = Arc::clone(&config);
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
            self.sched_uuids.push(uuid);
        }
    }

    async fn stop(&mut self) {
        self.sched.write().await.shutdown().await.unwrap();
        for (_, store_channel) in self.channels.drain() {
            drop(store_channel.tx);
        }
        self.channels.clear();
        for uuid in &self.sched_uuids {
            self.sched.write().await.remove(uuid).await.unwrap();
        }
        debug!("Stopping harvester...");
        while let Some(h) = self.handlers.pop() {
            h.await.unwrap();
        }
    }

    async fn start(&mut self) {
        self.create_channels().await;
        self.run_senders().await;
        self.run_collectors().await;


        self.sched.write().await.start().await.unwrap();
        debug!("sched run");
    }
    
    async fn reload(Extension(harvester_container): Extension<Arc<RwLock<HarvesterContainer>>>) {
        debug!("Harvester reloading...");
        harvester_container.write().await.reload().await;
    }
    
    async fn version(Extension(harvester_container): Extension<Arc<RwLock<HarvesterContainer>>>) -> String {
        match &harvester_container.read().await.harvester.read().await.config.read().await.version {
            Some(version) => {
                String::from(version)
            },
            None => {
                String::from("Unknown")
            }
        }
    } 

    pub async fn run(harvester: Arc<RwLock<Harvester>>) {
        let port = harvester.read().await.config.read().await.get_cnc_config().harvester.port.unwrap_or_else(|| HARVESTER_PORT_DEFAULT);
        let harvester_container = HarvesterContainer::new(harvester);
        let app = Router::new()
            .route("/reload", get(Self::reload))
            .route("/version", get(Self::version))
            .layer(Extension(harvester_container.await));

        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await.unwrap();
        axum::serve(listener, app).await.unwrap();
    }

    pub async fn new(config: Arc<RwLock<HarvesterConfig>>) -> Arc<RwLock<Harvester>> {
        let sched = JobScheduler::new().await.unwrap();

        let harvester = Arc::new(RwLock::new(Harvester {
            config,
            handlers: vec![],
            channels: HashMap::new(),
            sched: Arc::new(RwLock::new(sched)),
            sched_uuids: vec![],
        }));
        harvester
    }
}
