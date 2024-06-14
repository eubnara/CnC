use std::time::Duration;

use log::debug;
use rdkafka::producer::{FutureProducer, FutureRecord};
use tokio::sync::mpsc;

pub trait Sender {
    fn get_receiver_channel(&mut self) -> &mut mpsc::Receiver<String>;
    async fn send(&self, result: String);
    async fn run(&mut self) {
        loop {
            match self.get_receiver_channel().recv().await {
                Some(result) => self.send(result).await,
                None => break
            }
        }
    }
}

pub struct KafkaSender {
    pub receiver_channel: mpsc::Receiver<String>,
    pub producer: FutureProducer,
    pub topic_name: String,
}

impl Sender for KafkaSender {
    fn get_receiver_channel(&mut self) -> &mut mpsc::Receiver<String> {
        &mut self.receiver_channel
    }

    async fn send(&self, result: String) {
        // TODO: 키값은 호스트명으로 전달되도록 할 것.
        // TODO: duration 의미?
        let delivery_status = &self.producer.send(
            FutureRecord::to(&self.topic_name)
                .payload(&result)
                .key("localhost"),
            Duration::from_secs(0),
        )
            .await;
        debug!("Delivery status for message {} received: {:?}", result, delivery_status);
    }
}

pub struct StdoutSender {
    pub receiver_channel: mpsc::Receiver<String>,
}

impl Sender for StdoutSender {
    fn get_receiver_channel(&mut self) -> &mut mpsc::Receiver<String> {
        &mut self.receiver_channel
    }

    async fn send(&self, result: String) {
        println!("{}", result);
    }
}
