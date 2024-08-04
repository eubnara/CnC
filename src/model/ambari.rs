use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub struct HostRoles {
    pub cluster_name: String,
    pub host_name: String,
    pub component_name: String,
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

#[derive(Deserialize, Serialize, Debug, Default)]
pub struct HostInfo {
    pub host_name: String,
    pub host_state: Option<String>,
    pub maintenance_state: String,
    pub last_heartbeat_time: Option<String>,
    pub last_registration_time: Option<String>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Host {
    pub href: String,
    #[serde(rename = "Hosts")]
    pub host_info: HostInfo,
    pub host_components: Option<Vec<HostComponent>>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Hosts {
    pub href: String,
    pub items: Vec<Host>,
}
