use std::collections::HashMap;
use std::fmt::Debug;
use std::fs;

use serde::de::DeserializeOwned;
use serde::Deserialize;
use toml::Value;

#[derive(Deserialize, Debug)]
pub struct Command {
    pub command_line: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct CollectorInfo {
    pub host_group_name: String,
    pub description: String,
    pub command_name: String,
    pub store_name: String,
    pub crontab: String,
    pub retry_interval_s: u32,
    pub max_retries: u32,
    pub notification_interval_s: u32,
    pub critical: bool,
    pub param: Option<Value>,
}

#[derive(Deserialize, Debug)]
pub struct Datastore {
    pub kind: String,
    pub param: Option<Value>,
}

#[derive(Deserialize, Debug)]
pub struct Checker {
    pub kind: String,
    pub source: String,
    pub param: Option<Value>,
}

trait CncConfig {
    fn read_toml<T: DeserializeOwned>(config_dir: &str, config_name: &str) -> HashMap<String, T> {
        let config_path = &format!("{}/{}.toml", config_dir, config_name);
        let contents =
            fs::read_to_string(config_path).expect(&format!("{} not found", config_path));
        toml::from_str::<HashMap<String, T>>(&contents)
            .expect(&format!("Failed to parse {}", config_path))
    }
}

pub struct HarvesterConfig {
    commands: HashMap<String, Command>,
    pub collector_infos: HashMap<String, CollectorInfo>,
    datastores: HashMap<String, Datastore>,
}

impl CncConfig for HarvesterConfig {}

impl HarvesterConfig {

    pub fn get_commands(&self) -> &HashMap<String, Command> {
        &self.commands
    }

    pub fn get_collector_infos(&self) -> &HashMap<String, CollectorInfo> {
        &self.collector_infos
    }

    pub fn get_datastores(&self) -> &HashMap<String, Datastore> {
        &self.datastores
    }

    pub fn new(config_dir: &str) -> HarvesterConfig {
        let config = HarvesterConfig {
            commands: HarvesterConfig::read_toml::<Command>(config_dir, "command"),
            collector_infos: HarvesterConfig::read_toml::<CollectorInfo>(
                config_dir,
                "collector_info",
            ),
            datastores: HarvesterConfig::read_toml::<Datastore>(config_dir, "datastore"),
        };

        config
    }
}

pub struct RefineryConfig {
    datastores: HashMap<String, Datastore>,
    checkers: HashMap<String, Checker>,
}

impl CncConfig for RefineryConfig {}

impl RefineryConfig {

    pub fn get_datastores(&self) -> &HashMap<String, Datastore> {
        &self.datastores
    }
    
    pub fn get_checkers(&self) -> &HashMap<String, Checker> {
        &self.checkers
    }

    pub fn new(config_dir: &str) -> RefineryConfig {
        let config = RefineryConfig {
            datastores: RefineryConfig::read_toml::<Datastore>(config_dir, "datastore"),
            checkers: RefineryConfig::read_toml::<Checker>(config_dir, "checker"),
        };

        config
    }
}
