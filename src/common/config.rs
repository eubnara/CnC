use std::collections::HashMap;
use std::fmt::Debug;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::time::SystemTime;

use flate2::read::GzDecoder;
use log::debug;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use tar::Archive;
use toml::{Table, Value};

#[derive(Deserialize, Debug)]
pub struct Command {
    pub command_line: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct CollectorInfoParam {
    pub name: String,
    pub param: Option<Table>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct CollectorInfo {
    pub host_group_name: String,
    pub description: String,
    pub command: CollectorInfoParam,
    pub store_name: String,
    pub crontab: String,
    pub retry_interval_s: u32,
    pub max_retries: u32,
    pub notification_interval_s: u32,
    pub critical: bool,
    pub last_notification_time: Option<SystemTime>,
}

#[derive(Deserialize, Debug)]
pub struct Datastore {
    pub kind: String,
    pub param: Option<Value>,
}

#[derive(Deserialize, Debug)]
pub struct CheckerInfo {
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

    async fn download_config_tar(config_dir: &str, config_tar_url: &str) -> Option<String> {
        let tar_dir = format!("{config_dir}/_downloaded_config_tar");
        let tar_name_file = format!("{tar_dir}/TAR_NAME");
        if config_tar_url.is_empty() {
            return None;
        }
        let config_tar_name = Path::new(&config_tar_url).file_name().unwrap().to_str().unwrap().trim();

        if Path::new(&tar_dir).is_dir() {
            if Path::new(&tar_name_file).is_file() {
                let previous_tar_name = fs::read_to_string(&tar_name_file).unwrap();
                if previous_tar_name == config_tar_name {
                    debug!("Desired configs already exist. skip it.");
                    return Some(tar_dir);
                }
            }
        } else {
            fs::create_dir(&tar_dir).unwrap();
        }

        let temp_tar_gz_path = tempfile::Builder::new()
            .prefix("cnc-config")
            .suffix(".tar.gz")
            .tempfile()
            .unwrap();
        let mut temp_tar_gz = File::create(&temp_tar_gz_path).unwrap();
        let mut res = reqwest::get(config_tar_url).await.unwrap();
        while let Some(chunk) = res.chunk().await.unwrap() {
            temp_tar_gz.write(&chunk.to_vec()).unwrap();
        }
        let tar_gz = File::open(&temp_tar_gz_path).unwrap();
        let tar = GzDecoder::new(tar_gz);
        let mut archive = Archive::new(tar);
        archive.unpack(&tar_dir).unwrap();

        let mut file = File::create(tar_name_file)
            .expect("Failed to open TAR_NAME file.");
        file.write(&config_tar_name.as_bytes())
            .expect("Failed to write current tar name on TAR_NAME file.");
        
        // tempfile will be automatically removed. You don't have to remove it explicitly.
        Some(tar_dir)
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

    pub async fn new(config_dir: &str, config_tar_url: &str) -> HarvesterConfig {
        let mut datastores = HarvesterConfig::read_toml::<Datastore>(config_dir, "datastore");
        let mut commands = HarvesterConfig::read_toml::<Command>(config_dir, "command");
        let mut collector_infos = HarvesterConfig::read_toml::<CollectorInfo>(
            config_dir,
            "collector_info",
        );
        
        if let Some(tar_dir) = HarvesterConfig::download_config_tar(config_dir, config_tar_url).await {
            datastores.extend(HarvesterConfig::read_toml::<Datastore>(&tar_dir, "datastore"));
            commands.extend(HarvesterConfig::read_toml::<Command>(&tar_dir, "command"));
            collector_infos.extend(HarvesterConfig::read_toml::<CollectorInfo>(&tar_dir, "collector_info"));
        }

        debug!("datastores: {:?}", datastores);
        debug!("commands: {:?}", commands);
        debug!("collector_infos: {:?}", collector_infos);

        HarvesterConfig {
            commands,
            collector_infos,
            datastores,
        }
    }
}

pub struct RefineryConfig {
    datastores: HashMap<String, Datastore>,
    checkers: HashMap<String, CheckerInfo>,
}

impl CncConfig for RefineryConfig {}

impl RefineryConfig {
    pub fn get_datastores(&self) -> &HashMap<String, Datastore> {
        &self.datastores
    }

    pub fn get_checkers(&self) -> &HashMap<String, CheckerInfo> {
        &self.checkers
    }

    pub async fn new(config_dir: &str, config_tar_url: &str) -> RefineryConfig {
        let mut datastores = RefineryConfig::read_toml::<Datastore>(config_dir, "datastore");
        let mut checkers = RefineryConfig::read_toml::<CheckerInfo>(config_dir, "checker_info");

        if let Some(tar_dir) = RefineryConfig::download_config_tar(config_dir, config_tar_url).await {
            datastores.extend(RefineryConfig::read_toml::<Datastore>(&tar_dir, "datastore"));
            checkers.extend(RefineryConfig::read_toml::<CheckerInfo>(&tar_dir, "checker_info"));
        }
        debug!("datastores: {:?}", datastores);
        debug!("checkers: {:?}", checkers);

        RefineryConfig {
            datastores,
            checkers,
        }
    }
}
