use std::io::Read;
use std::str::from_utf8;
use tokio::time::{sleep, Duration};
use log::error;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::{Message, Offset, TopicPartitionList};
use tokio::sync::mpsc;

pub trait Receiver {
    async fn receive(&self);
}

pub struct KafkaReceiver {
    pub sender_channel: mpsc::Sender<String>,
    pub consumer: StreamConsumer,
    pub topic: String,
}

impl Receiver for KafkaReceiver {
    async fn receive(&self) {
        self.consumer
            .subscribe(&[&self.topic])
            .expect("Can't subscribe to specified topic.");

        loop {
            match self.consumer.recv().await {
                Err(e) => {
                    error!("KafkaReceiver error for {}: {}", &self.topic, e);
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
                    if !payload.is_empty() {
                        if let Err(e) = self.sender_channel.send(payload.to_string()).await {
                            error!("Failed to send message from KafkaReceiver. payload: {}", payload);
                            error!("{}", e);
                        }
                    }
                }
            }
        }
    }
}
