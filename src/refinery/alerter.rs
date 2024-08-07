use log::{debug, error};
use tokio::sync::mpsc::Receiver;

use crate::common::config::CncRefineryAlerter;
use crate::model::refinery::AlertMessage;

pub fn get_alerter(alerter_config: &CncRefineryAlerter, rx: Receiver<AlertMessage>) -> Alerter {
    let kind = alerter_config.kind.as_str();
    match kind {
        "stdout" => {
            Alerter::StdoutAlerter {
                receiver_channel: rx,
            }
        }
        "http" => {
            let url = alerter_config.param.as_ref().unwrap().get("url").unwrap().as_str().unwrap().to_string();
            Alerter::HttpAlerter {
                receiver_channel: rx,
                url,
            }
        }
        _ => {
            panic!("Unknown alerter kind: {}", kind);
        }
    }
}

pub enum Alerter {
    StdoutAlerter { receiver_channel: Receiver<AlertMessage> },
    HttpAlerter { receiver_channel: Receiver<AlertMessage>, url: String },

}

impl Alerter {
    pub async fn run(&mut self) {
        match self {
            Alerter::StdoutAlerter {ref mut receiver_channel} => {
                loop {
                    match receiver_channel.recv().await {
                        Some(msg) => println!("{}", serde_json::to_string_pretty(&msg).unwrap()),
                        None => break
                    }
                }
            },
            Alerter::HttpAlerter {ref mut receiver_channel, url} => {
                loop {
                    match receiver_channel.recv().await {
                        None => break,
                        Some(msg) => {
                            let client = reqwest::Client::new();
                            let res = client.post(url.clone())
                                .body(serde_json::to_string_pretty(&msg).unwrap())
                                .send()
                                .await;
                            match res {
                                Ok(res) => debug!("{res:?}"),
                                Err(err) => error!("{err:?}"),
                            }
                        }
                    }
                }
            },
        }
    }
}
