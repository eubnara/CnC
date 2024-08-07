use tokio::sync::mpsc;

pub struct StoreChannel<T> {
    pub tx: mpsc::Sender<T>,
    pub rx: Option<mpsc::Receiver<T>>,
}
