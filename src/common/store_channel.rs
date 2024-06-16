use tokio::sync::mpsc;

pub struct StoreChannel {
    pub tx: mpsc::Sender<String>,
    pub rx: Option<mpsc::Receiver<String>>,
}
