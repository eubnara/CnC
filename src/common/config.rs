use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::time::{Duration, SystemTime};

use flate2::read::GzDecoder;
use log::{debug, error};
use rdkafka::message::ToBytes;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use subprocess::{ExitStatus, Popen, PopenConfig, Redirection};
use tar::Archive;
use tempfile::tempdir;
use toml::{Table, Value};

pub const HARVESTER_PORT_DEFAULT: u32 = 10023;
pub const REFINERY_PORT_DEFAULT: u32 = 10024;
pub const CONFIG_UPDATER_PORT_DEFAULT: u32 = 10025;

#[derive(Deserialize, Serialize, Debug)]
pub struct CheckerInfo {
    pub kind: String,
    pub source: String,
    pub param: Option<Value>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct KafkaInfo {
    pub bootstrap_servers: String,
    pub topic: String,
    #[serde(default = "KafkaInfo::message_timeout_ms")]
    pub message_timeout_ms: String,
}

impl KafkaInfo {
    pub fn message_timeout_ms() -> String {"5000".to_string()}
}

#[derive(Deserialize, Serialize, Debug)]
pub struct CncCommon {
    pub cluster_name: String,
    pub hosts_kafka: KafkaInfo,
    pub infos_kafka: KafkaInfo,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct CncHarvester {
    pub port: Option<u32>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct CncRefinery {
    pub port: Option<u32>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct CncConfigUpdaterAmbari {
    pub url: String,
    pub user: Option<String>,
    pub password_file: Option<String>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct CncConfigUpdaterUploader {
    pub kind: String,
    pub url: String,
    pub param: Option<Table>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct CncConfigUpdater {
    pub port: Option<u32>,
    pub poll_interval_s: u64,
    pub config_git_url: String,
    pub config_git_subdir: Option<String>,
    pub ambari: Option<CncConfigUpdaterAmbari>,
    pub uploader: CncConfigUpdaterUploader,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Cnc {
    pub common: CncCommon,
    pub harvester: CncHarvester,
    pub refinery: CncRefinery,
    pub config_updater: CncConfigUpdater,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CollectorInfoParam {
    pub name: String,
    pub param: Option<Table>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
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

#[derive(Deserialize, Serialize, Debug)]
pub struct Command {
    pub command_line: String,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Datastore {
    pub kind: String,
    pub kafka: Option<KafkaInfo>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct HostGroup {
    pub members: HashSet<String>,
}

pub struct AllConfig {
    pub checkers: HashMap<String, CheckerInfo>,
    pub collector_infos: HashMap<String, CollectorInfo>,
    pub datastores: HashMap<String, Datastore>,
    pub commands: HashMap<String, Command>,
    pub host_groups: HashMap<String, HostGroup>,
    pub cnc: Option<Cnc>,
}

impl CncConfigHandler for AllConfig {}

impl AllConfig {
    pub fn read_all(config_dir: &str, git_dir: &str, cluster_name: &str) -> AllConfig {
        let mut checkers = HashMap::new();
        let mut collector_infos = HashMap::new();
        let mut datastores = HashMap::new();
        let mut commands = HashMap::new();
        let mut host_groups = HashMap::new();
        let mut cnc: Option<Cnc> = None;

        for dir in vec![config_dir, git_dir, &format!("{}/{}", git_dir, cluster_name)] {
            checkers.extend(AllConfig::read_items::<CheckerInfo>(
                dir, "checker_info").unwrap_or_default());
            collector_infos.extend(AllConfig::read_items::<CollectorInfo>(
                dir, "collector_info").unwrap_or_default());
            datastores.extend(AllConfig::read_items::<Datastore>(
                dir, "datastore").unwrap_or_default());
            commands.extend(AllConfig::read_items::<Command>(
                dir, "command").unwrap_or_default());
            host_groups.extend(AllConfig::read_items::<HostGroup>(
                dir, "host_group").unwrap_or_default());
            if let Some(_cnc) = ConfigUpdaterConfig::read_item::<Cnc>(config_dir, "cnc") {
                cnc = Some(_cnc);
            }
        }

        AllConfig {
            checkers,
            collector_infos,
            datastores,
            commands,
            host_groups,
            cnc,
        }
    }
    pub fn dump_all(all_config: AllConfig, config_dir: &str) {
        File::create(format!("{config_dir}/checker_info.toml"))
            .unwrap()
            .write(
                toml::to_string_pretty(&all_config.checkers).unwrap().to_bytes()
            )
            .unwrap();

        File::create(format!("{config_dir}/collector_info.toml"))
            .unwrap()
            .write(
                toml::to_string_pretty(&all_config.collector_infos).unwrap().to_bytes()
            )
            .unwrap();

        File::create(format!("{config_dir}/datastore.toml"))
            .unwrap()
            .write(
                toml::to_string_pretty(&all_config.datastores).unwrap().to_bytes()
            )
            .unwrap();

        File::create(format!("{config_dir}/command.toml"))
            .unwrap()
            .write(
                toml::to_string_pretty(&all_config.commands).unwrap().to_bytes()
            )
            .unwrap();

        File::create(format!("{config_dir}/host_group.toml"))
            .unwrap()
            .write(
                toml::to_string_pretty(&all_config.host_groups).unwrap().to_bytes()
            )
            .unwrap();

        if let Some(cnc) = &all_config.cnc {
            File::create(format!("{config_dir}/cnc.toml"))
                .unwrap()
                .write(
                    toml::to_string_pretty(cnc).unwrap().to_bytes()
                )
                .unwrap();
        }
    }
}

pub trait CncConfigHandler {
    fn read_items<T: DeserializeOwned>(config_dir: &str, config_name: &str) -> Option<HashMap<String, T>> {
        let config_path = &format!("{}/{}.toml", config_dir, config_name);
        let contents = match fs::read_to_string(config_path) {
            Ok(contents) => contents,
            Err(err) => {
                error!("{}", err);
                return None;
            }
        };
        Some(toml::from_str::<HashMap<String, T>>(&contents)
            .expect(&format!("Failed to parse {}", config_path)))
    }

    fn read_item<T: DeserializeOwned>(config_dir: &str, config_name: &str) -> Option<T> {
        let config_path = &format!("{}/{}.toml", config_dir, config_name);
        let contents = match fs::read_to_string(config_path) {
            Ok(contents) => contents,
            Err(err) => {
                error!("{}", err);
                return None;
            }
        };
        Some(toml::from_str::<T>(&contents)
            .expect(&format!("Failed to parse {}", config_path)))
    }

    async fn download_config(config_dir: &str, config_tar_url: &str) -> Option<String> {
        if config_tar_url.is_empty() {
            return None;
        }
        let tar_dir = tempdir().unwrap();
        let temp_tar_gz_path = tempfile::Builder::new()
            .prefix("cnc-config")
            .suffix(".tar.gz")
            .tempfile()
            .unwrap();
        let mut temp_tar_gz = File::create(&temp_tar_gz_path).unwrap();
        let mut res = reqwest::ClientBuilder::new()
            .danger_accept_invalid_certs(true)
            .build()
            .unwrap()
            .get(config_tar_url)
            .send()
            .await
            .unwrap();
        match res.status() {
            StatusCode::UNAUTHORIZED => {
                // handle for SPNEGO
                match res.headers().get("WWW-Authenticate") {
                    Some(val) => {
                        if val != "Negotiate" {
                            panic!("Failed to get tar file. {}", res.text().await.unwrap());
                        }
                        let cmd = format!(
                            "curl -u : --negotiate -k -L {} -o {}",
                            config_tar_url,
                            temp_tar_gz_path.path().to_str().unwrap(),
                        );
                        let mut p = Popen::create(
                            &vec!["sh", "-c", &cmd],
                            PopenConfig {
                                stdout: Redirection::Pipe,
                                stderr: Redirection::Pipe,
                                ..Default::default()
                            },
                        ).unwrap();

                        let (stdout, stderr) = p.communicate(None).unwrap();
                        match p.wait_timeout(Duration::new(10, 0)) {
                            Ok(Some(ExitStatus::Exited(_))) => {}
                            _ => panic!("Failed to get tar file using curl.
stdout: {}
stderr: {}",
                                        stdout.unwrap_or_default(),
                                        stderr.unwrap_or_default(),
                            ),
                        };
                    }
                    _ => panic!("Failed to get tar file. {}", res.text().await.unwrap()),
                }
            }
            StatusCode::OK => {
                while let Some(chunk) = res.chunk().await.unwrap() {
                    temp_tar_gz.write(&chunk.to_vec()).unwrap();
                }
            }
            _ => panic!("Failed to get tar file. {}", res.text().await.unwrap()),
        }

        let tar_gz = File::open(&temp_tar_gz_path).unwrap();
        let tar = GzDecoder::new(tar_gz);
        let mut archive = Archive::new(tar);
        archive.unpack(&tar_dir).unwrap();

        let config_path = Path::new(config_dir);
        if config_path.is_dir() {
            fs::remove_dir_all(config_path).unwrap();
            fs::rename(&tar_dir.path(), config_path).unwrap();
            debug!("rename {:?} {:?}", &tar_dir.path().to_str(), config_path.to_str());
        }

        let version_file = format!("{}/version", config_dir);
        if Path::new(&version_file).is_file() {
            if let Ok(version) = fs::read_to_string(&version_file) {
                return Some(version);
            }
        }
        return None;
    }
}

#[derive(Default)]
pub struct HarvesterConfig {
    commands: HashMap<String, Command>,
    pub collector_infos: HashMap<String, CollectorInfo>,
    datastores: HashMap<String, Datastore>,
    cnc: Option<Cnc>,
    config_dir: String,
    config_tar_url: String,
    pub version: Option<String>,
}

impl CncConfigHandler for HarvesterConfig {}

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

    pub fn get_cnc_config(&self) -> &Cnc {
        self.cnc.as_ref().unwrap()
    }

    pub async fn reload_config(&mut self) {
        if let Some(version) = Self::download_config(&self.config_dir, &self.config_tar_url).await {
            self.version = Some(version);
        }

        self.datastores = HarvesterConfig::read_items::<Datastore>(&self.config_dir, "datastore");
        self.commands = HarvesterConfig::read_items::<Command>(&self.config_dir, "command");
        self.cnc = Some(HarvesterConfig::read_item::<Cnc>(&self.config_dir, "cnc"));
        self.collector_infos = HarvesterConfig::read_items::<CollectorInfo>(
            &self.config_dir,
            "collector_info",
        );

        debug!("datastores: {:?}", &self.datastores);
        debug!("commands: {:?}", &self.commands);
        debug!("collector_infos: {:?}", &self.collector_infos);
    }

    pub async fn new(config_dir: &str, config_tar_url: &str) -> HarvesterConfig {
        let mut config = HarvesterConfig {
            commands: HashMap::new(),
            collector_infos: HashMap::new(),
            datastores: HashMap::new(),
            config_dir: String::from(config_dir),
            config_tar_url: String::from(config_tar_url),
            ..Default::default()
        };
        config.reload_config().await;

        config
    }
}

#[derive(Default)]
pub struct RefineryConfig {
    datastores: HashMap<String, Datastore>,
    checkers: HashMap<String, CheckerInfo>,
    cnc: Option<Cnc>,
    config_dir: String,
    config_tar_url: String,
    pub version: Option<String>,
}

impl CncConfigHandler for RefineryConfig {}

impl RefineryConfig {
    pub fn get_datastores(&self) -> &HashMap<String, Datastore> {
        &self.datastores
    }

    pub fn get_checkers(&self) -> &HashMap<String, CheckerInfo> {
        &self.checkers
    }

    pub fn get_cnc_config(&self) -> &Cnc {
        self.cnc.as_ref().unwrap()
    }

    pub async fn reload_config(&mut self) {
        if let Some(version) = Self::download_config(&self.config_dir, &self.config_tar_url).await {
            self.version = Some(version);
        }

        self.datastores = RefineryConfig::read_items::<Datastore>(&self.config_dir, "datastore");
        self.checkers = RefineryConfig::read_items::<CheckerInfo>(&self.config_dir, "checker_info");
        self.cnc = Some(RefineryConfig::read_item::<Cnc>(&self.config_dir, "cnc"));

        debug!("datastores: {:?}", &self.datastores);
        debug!("checkers: {:?}", &self.checkers);
        debug!("cnc: {:?}", &self.cnc);
    }

    pub async fn new(config_dir: &str, config_tar_url: &str) -> RefineryConfig {
        let mut config = RefineryConfig {
            datastores: HashMap::new(),
            checkers: HashMap::new(),
            config_dir: String::from(config_dir),
            config_tar_url: String::from(config_tar_url),
            ..Default::default()
        };
        config.reload_config().await;

        config
    }
}

pub struct ConfigUpdaterConfig {
    cnc: Cnc,
    config_tar_url: String,
}

impl CncConfigHandler for ConfigUpdaterConfig {}

impl ConfigUpdaterConfig {
    pub fn get_cnc_config(&self) -> &Cnc {
        &self.cnc
    }

    pub fn set_cnc_config(&mut self, cnc: Cnc) {
        self.cnc = cnc;
    }

    pub async fn new(config_dir: &str, config_tar_url: &str) -> ConfigUpdaterConfig {
        let mut cnc = ConfigUpdaterConfig::read_item::<Cnc>(config_dir, "cnc");
        debug!("cnc: {:?}", cnc);

        ConfigUpdaterConfig {
            cnc,
            config_tar_url: String::from(config_tar_url),
        }
    }
}
