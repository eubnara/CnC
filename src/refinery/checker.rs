
use tokio::sync::mpsc;

// TODO: harvester/sender.rs 와 비교해서 trait 에 기본구현을 두는 것이 좋을지 없애는 것이 좋을지 검토하고 통일된 형태로 가자.
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
