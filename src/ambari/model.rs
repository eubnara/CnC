use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub struct HostRoles {
    pub cluster_name: String,
    pub host_name: String,
    pub component_name: String,
    pub state: String,
    pub ha_state: String,
    pub stale_configs: bool,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct HostComponent {
    pub href: String,
    #[serde(rename = "HostRoles")]
    pub host_roles: HostRoles,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct HostComponents {
    pub href: String,
    pub items: Vec<HostComponent>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct HostInfo {
    pub host_name: String,
    pub host_state: String,
    pub maintenance_state: String,
    pub last_heartbeat_time: String,
    pub last_registration_time: String,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Host {
    pub href: String,
    #[serde(rename = "Hosts")]
    pub host_info: HostInfo,
    pub host_components: Vec<HostComponent>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Hosts {
    pub href: String,
    pub items: Vec<Host>,
}
