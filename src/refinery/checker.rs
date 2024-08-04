use std::collections::HashMap;
use std::sync::Arc;
use log::{debug, error};
use reqwest::{Error, Response};
use tokio::sync::{mpsc, RwLock};
use crate::common::config::CheckerInfo;

pub trait Checker {
    async fn check(&mut self);
}

pub struct StdoutAlertChecker {
    pub receiver_channel: mpsc::Receiver<String>,
}

impl Checker for StdoutAlertChecker {
    async fn check(&mut self) {
        loop {
            match self.receiver_channel.recv().await {
                Some(msg) => println!("{msg}"),
                None => break
            }
        }
    }
}

pub struct HttpAlertChecker {
    pub receiver_channel: mpsc::Receiver<String>,
    pub checker_config: CheckerInfo,
}

impl Checker for HttpAlertChecker {
    async fn check(&mut self) {
        let param = self.checker_config.param.as_ref().unwrap();
        let url = param.get("url").unwrap().as_str().unwrap();
        loop {
            match self.receiver_channel.recv().await {
                None => break,
                Some(msg) => {
                    let client = reqwest::Client::new();
                    let res = client.post(url)
                        .body(msg)
                        .send()
                        .await;
                    match res {
                        Ok(res) => debug!("{res:?}"),
                        Err(err) => error!("{err:?}"),
                    }
                }
            }
        }
    }
}