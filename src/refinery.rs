use std::collections::HashMap;
use std::sync::Arc;
use axum::{Extension, Router};
use axum::routing::get;
use log::debug;
use rdkafka::ClientConfig;
use rdkafka::consumer::StreamConsumer;
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;

use crate::common::config::{CheckerInfo, REFINERY_PORT_DEFAULT, RefineryConfig};
use crate::common::store_channel::StoreChannel;
use crate::harvester::Harvester;
use crate::refinery::checker::{Checker, HttpAlertChecker, StdoutAlertChecker};
use crate::refinery::receiver::{KafkaReceiver, Receiver};

mod receiver;
mod checker;

struct RefineryContainer {
    refinery: Arc<RwLock<Refinery>>,
}

impl RefineryContainer {
    pub async fn new(refinery: Arc<RwLock<Refinery>>) -> Arc<RwLock<RefineryContainer>> {
        refinery.write().await.start().await;
        Arc::new(RwLock::new(RefineryContainer { refinery }))
    }

    pub async fn reload(&mut self) {
        self.refinery.write().await.config.write().await.reload_config().await;
        let new = Refinery::new(self.refinery.read().await.config.clone());
        {
            let mut old = self.refinery.write().await;
            old.stop().await;
            drop(old);
        }
        new.write().await.start().await;
        self.refinery = new;
    }
}

pub struct Refinery {
    config: Arc<RwLock<RefineryConfig>>,
    handlers: Vec<JoinHandle<()>>,
    channels: HashMap<String, StoreChannel>,
}

impl Refinery {

    async fn create_channels(&mut self) {
        for (checker_name, _) in self.config.read().await.get_checkers() {
            let (tx, rx) = mpsc::channel::<String>(1000);
            self.channels.insert(checker_name.clone(), StoreChannel { tx, rx: Some(rx) });
        }
    }

    async fn run_checkers(&mut self) {
        for (checker_name, checker) in self.config.write().await.get_checkers() {
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
        for (checker_name, checker) in self.config.read().await.get_checkers() {
            let tx = self.channels
                .get(checker_name)
                .unwrap()
                .tx
                .clone();
            let source = &checker.source;
            let config = self.config.read().await;
            let source_datastore = config.get_datastores().get(source)
                .expect("Unexpected source");
            let source_kind = source_datastore.kind.as_str();
            let receiver = match source_kind {
                "kafka" => {
                    let kafka = source_datastore.kafka.as_ref()
                        .expect(&format!("Empty param for kafka source: {}", source));
                    let group_instance_id = format!("refinery-{}", gethostname::gethostname().into_string().unwrap());
                    let consumer: StreamConsumer = ClientConfig::new()
                        .set("group.id", "refinery")
                        .set("group.instance.id", group_instance_id)
                        .set("bootstrap.servers", &kafka.bootstrap_servers)
                        .set("auto.offset.reset", "latest")
                        .create()
                        .expect("Kafka consumer creation failed");
                    let topic = String::from(&kafka.bootstrap_servers);
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
    
    async fn stop(&mut self) {
        for (_, store_channel) in self.channels.drain() {
            drop(store_channel.tx);
        }
        self.channels.clear();
        debug!("Stopping refinery...");
        while let Some(h) = self.handlers.pop() {
            h.abort();
        }
    }

    async fn start(&mut self) {
        self.create_channels().await;
        self.run_checkers().await;
        self.run_receivers().await;
    }
    
    async fn reload(Extension(refinery_container): Extension<Arc<RwLock<RefineryContainer>>>) {
        debug!("Refinery reloading...");
        refinery_container.write().await.reload().await;
    }
    
    async fn version(Extension(refinery_container): Extension<Arc<RwLock<RefineryContainer>>>) -> String {
        match &refinery_container.read().await.refinery.read().await.config.read().await.version {
            Some(version) => {
                String::from(version)
            },
            None => {
                String::from("Unknown")
            }
        }
    }
     
    pub async fn run(refinery: Arc<RwLock<Refinery>>) {
        let port = refinery.read().await.config.read().await.get_cnc_config().refinery.port.unwrap_or_else(|| REFINERY_PORT_DEFAULT);
        let refinery_container = RefineryContainer::new(refinery);
        let app = Router::new()
            .route("/reload", get(Self::reload))
            .route("/version", get(Self::version))
            .layer(Extension(refinery_container.await));

        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await.unwrap();
        axum::serve(listener, app).await.unwrap();
    }
    
    pub fn new(config: Arc<RwLock<RefineryConfig>>) -> Arc<RwLock<Refinery>> {
        Arc::new(RwLock::new(Refinery {
            config,
            handlers: vec![],
            channels: HashMap::new(),
        }))
    }
}
