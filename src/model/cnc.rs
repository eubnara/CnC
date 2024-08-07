use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub struct CommandResult {
    pub collector_info_key: String,
    pub hostname: String,
    pub host_group_name: String,
    pub description: String,
    pub return_code: i64,
    pub stdout: String,
    pub stderr: String,
}
