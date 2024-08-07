use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use log::error;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{Message, TopicPartitionList};
use rdkafka::Offset;
use tokio::sync::{mpsc, RwLock};
use crate::model::cnc::CommandResult;
use crate::model::kafka::HostsKafka;
use crate::model::refinery::{AlertMessage, Host};

pub struct InfosKafkaReceiver {
    pub alert_sender_channel: mpsc::Sender<AlertMessage>,
    pub consumer: StreamConsumer,
    pub topic: String,
    pub host_map: Arc<RwLock<HashMap<String, RwLock<Host>>>>,
}

impl InfosKafkaReceiver {
    pub async fn receive(&self) {
        self.consumer
            .subscribe(&[&self.topic])
            .expect("Can't subscribe to specified topic.");

        loop {
            match self.consumer.recv().await {
                Err(e) => {
                    error!("KafkaReceiver error for {} in InfosKafkaReceiver: {}", &self.topic, e);
                },
                Ok(m) => {
                    let payload = match m.payload_view::<str>() {
                        None => "",
                        Some(Ok(s)) => s,
                        Some(Err(e)) => {
                            error!("Error while deserializing message payload: {:?}", e);
                            ""
                        }
                    };
                    let result: CommandResult = match serde_json::from_str(payload) {
                        Ok(result) => result,
                        Err(e) => {
                            error!("Error while deserializing message payload: {:?}", e);
                            continue;
                        },
                    };
                    let _host_map = self.host_map.read().await;
                    let host = match _host_map.get(&result.hostname) {
                        Some(host) => host,
                        None => {
                            error!("Unknown host: {}", &result.hostname);
                            continue;
                        },
                    }.read().await;
                    let _collector_states = host.collector_states.write().await;
                    let mut collector_state = match _collector_states.get(&result.collector_info_key) {
                        Some(collector_state) => collector_state,
                        None => {
                            error!("Unknown collector_state: {}", &result.collector_info_key);
                            continue;
                        }
                    }.write().await;
                    collector_state.process_result(result);
                    collector_state.check_alert(&self.alert_sender_channel).await;
                }
            }
        }
    }
}

// continuously update host_maintenance_map
pub struct HostsKafkaReceiver {
    pub consumer: StreamConsumer,
    pub topic: String,
    pub host_maintenance_map: Arc<RwLock<HashMap<String, bool>>>,
}

impl HostsKafkaReceiver {
    pub async fn receive(&self) {
        self.consumer
            .subscribe(&[&self.topic])
            .unwrap();

        let metadata = self.consumer.fetch_metadata(Some(self.topic.as_str()), Duration::from_secs(10)).unwrap();
        for partition in metadata.topics().iter().flat_map(|t| t.partitions()) {
            let mut tpl = TopicPartitionList::new();
            tpl.add_partition_offset(self.topic.as_str(), partition.id(), Offset::Beginning).unwrap();
            self.consumer.assign(&tpl).unwrap();
            self.consumer.seek_partitions(tpl, Duration::from_secs(10)).unwrap();
        }
        loop {
            match self.consumer.recv().await {
                Err(e) => {
                    error!("KafkaReceiver error for {} in HostsKafkaReceiver: {}", &self.topic, e);
                },
                Ok(m) => {
                    if let Some(Ok(s)) = m.payload_view::<str>() {
                        let host = String::from_utf8(Vec::from(m.key().unwrap())).unwrap();
                        let data: HostsKafka = serde_json::from_str(s).unwrap();
                        self.host_maintenance_map.write().await.insert(host, data.in_maintenance);
                    }
                }
            }
        }
    }
}
