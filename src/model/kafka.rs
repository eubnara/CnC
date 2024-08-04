use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub struct HostsKafka {
    pub in_maintenance: bool,
}
