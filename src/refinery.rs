use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use axum::{Extension, Router};
use axum::routing::get;
use log::{debug, error};
use rdkafka::ClientConfig;
use rdkafka::consumer::StreamConsumer;
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;
use tokio::time::sleep;

use crate::common::config::{CncConfigHandler, CollectorInfo, HarvesterConfig, HostGroup, REFINERY_PORT_DEFAULT, RefineryConfig};
use crate::common::store_channel::StoreChannel;
use crate::model::refinery::{AlertMessage, CollectorState, Host, Status};
use crate::refinery::alerter::get_alerter;
use crate::refinery::receiver::{HostsKafkaReceiver, InfosKafkaReceiver};

mod receiver;
mod alerter;

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
    alert_channel: Option<StoreChannel<AlertMessage>>,
    host_map: Arc<RwLock<HashMap<String, RwLock<Host>>>>,
    host_maintenance_map: Arc<RwLock<HashMap<String, bool>>>,
}

impl Refinery {

    async fn run_infos_alerter(&mut self) {
        let rx = self.alert_channel.as_mut().unwrap().rx.take().unwrap();
        let mut alerter = get_alerter(&self.config.read().await.get_cnc_config().refinery.alerter, rx);
        let handler = tokio::spawn(async move {
            alerter.run().await;
        });
        self.handlers.push(handler);
    }

    async fn run_history_checker(&mut self) {
        let host_map = Arc::clone(&self.host_map);
        let host_maintenance_map = Arc::clone(&self.host_maintenance_map);
        let handler = tokio::spawn(async move {
            let host_map = host_map;
            let host_maintenance_map = host_maintenance_map;

            loop {
                for (hostname, host) in host_map.read().await.iter() {
                    if let Some(true) = host_maintenance_map.read().await.get(hostname.as_str()) {
                        continue;
                    }
                    host.read().await.run().await;
                }
                sleep(Duration::from_secs(60)).await;
            }
        });
        self.handlers.push(handler);
    }

    async fn run_infos_kafka_receiver(&mut self) {
        let tx = self.alert_channel.as_ref().unwrap().tx.clone();

        let config = self.config.read().await;
        let kafka = &config.get_cnc_config().common.infos_kafka;
        let group_instance_id = format!("refinery-infos-{}", gethostname::gethostname().into_string().unwrap());
        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", "refinery")
            .set("group.instance.id", group_instance_id)
            .set("bootstrap.servers", &kafka.bootstrap_servers)
            .set("auto.offset.reset", "latest")
            .create()
            .expect("Kafka consumer creation failed");
        let topic = String::from(&kafka.topic);
        let host_map = Arc::clone(&self.host_map);
        let receiver = InfosKafkaReceiver {
            alert_sender_channel: tx,
            host_map,
            consumer,
            topic,
        };

        let handler = tokio::spawn(async move {
            receiver.receive().await;
        });
        self.handlers.push(handler);
    }

    async fn run_hosts_kafka_receiver(&mut self) {
        let config = self.config.read().await;
        let cnc_config = config.get_cnc_config();
        let hosts_kafka = &cnc_config.common.hosts_kafka;
        let brokers = hosts_kafka.bootstrap_servers.clone();
        let topic = hosts_kafka.topic.clone();
        let mut host_maintenance_map = Arc::clone(&self.host_maintenance_map);

        let handler = tokio::spawn(async move {
            let brokers = brokers;
            let topic = topic;
            let mut host_maintenance_map = host_maintenance_map;
            let group_instance_id = format!("refinery-hosts-{}", gethostname::gethostname().into_string().unwrap());
            let consumer: StreamConsumer = ClientConfig::new()
                .set("group.id", "refinery")
                .set("group.instance.id", group_instance_id)
                .set("bootstrap.servers", &brokers)
                .set("enable.auto.commit", "false")
                .set("auto.offset.reset", "earliest")
                .create()
                .unwrap();
            HostsKafkaReceiver {
                consumer,
                topic,
                host_maintenance_map,
            }.receive().await;
        });
        self.handlers.push(handler);
    }

    async fn initialize(&mut self) {
        // TODO: make buffer size configurable?
        let (tx, rx) = mpsc::channel::<AlertMessage>(1000);
        self.alert_channel = Some(StoreChannel {tx, rx: Some(rx)});


        self.host_map.write().await.clear();
        let config_dir = &self.config.read().await.config_dir;

        let mut group_to_host_map = RefineryConfig::read_items::<HostGroup>(
            config_dir,
            "host_group"
        ).unwrap();

        let mut all_hosts = HashSet::new();
        for (_, host_group) in &group_to_host_map {
            all_hosts.extend(host_group.members.clone());
        }
        group_to_host_map.insert(String::from("all"), HostGroup {members: all_hosts});

        let collector_infos = HarvesterConfig::read_items::<CollectorInfo>(
            config_dir,
            "collector_info",
        ).unwrap();


        for (collector_info_key, collector_info) in collector_infos {
            let mut state = CollectorState {
                hostname: "".to_string(),
                description: collector_info.description,
                host_group_name: collector_info.host_group_name,
                retry_interval_s: collector_info.retry_interval_s,
                max_retries: collector_info.max_retries,
                notification_interval_s: collector_info.notification_interval_s,
                last_notified_time: SystemTime::now(),
                last_updated_time: SystemTime::now(),
                failed_count: 0,
                status: Status::Ok,
                last_msg: "".to_string(),
            };

            let hosts = match group_to_host_map.get(&state.host_group_name) {
                Some(host_group) => &host_group.members,
                None => continue,
            };

            for host in hosts {
                let mut _host_map = self.host_map.write().await;
                let mut host_obj = match _host_map.get(host) {
                    Some(host_obj) => host_obj,
                    None => {
                        let host_obj = Host {
                            collector_states: Arc::new(RwLock::new(HashMap::new())),
                            sender_channel: self.alert_channel.as_ref().unwrap().tx.clone(),
                        };
                        _host_map.insert(host.clone(), RwLock::new(host_obj));
                        _host_map.get(host).unwrap()
                    }
                }.read().await;
                let mut states = host_obj.collector_states.write().await;
                match states.get(&collector_info_key) {
                    Some(collector_state) => {
                        error!("collector_state({}) for host {} already exists.", &collector_info_key, host);
                        continue;
                    },
                    None => {
                        let state = CollectorState {
                            hostname: host.clone(),
                            ..state.clone()
                        };
                        states.insert(collector_info_key.clone(), RwLock::new(state));

                        states.get(&collector_info_key).unwrap()
                    }
                };
            }

        }
    }
    
    async fn stop(&mut self) {
        self.alert_channel.take();
        debug!("Stopping refinery...");
        while let Some(h) = self.handlers.pop() {
            h.abort();
        }
        self.host_map.write().await.clear();
        self.host_maintenance_map.write().await.clear();
    }

    async fn start(&mut self) {
        self.initialize().await;
        self.run_infos_alerter().await;
        debug!("Start hosts kafka receiver");
        self.run_hosts_kafka_receiver().await;
        debug!("Start infos kafka receiver");
        self.run_infos_kafka_receiver().await;
        debug!("Start history checker");
        self.run_history_checker().await;
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
            alert_channel: None,
            host_map: Arc::new(RwLock::new(HashMap::new())),
            host_maintenance_map: Arc::new(RwLock::new(HashMap::new())),
        }))
    }
}
