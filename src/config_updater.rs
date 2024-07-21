use std::collections::{HashMap, HashSet};
use std::fs::read_to_string;
use std::path::Path;
use std::thread::sleep;
use std::time::Duration;

use chrono::Local;
use log::{debug, error, info};
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use reqwest::Client;
use serde_json::json;
use tempfile::{NamedTempFile, tempdir};
use crate::ambari::model::{HostComponents, Hosts};
use crate::common::config::{AllConfig, CheckerInfo, CncConfigHandler, CncConfigUpdaterUploader, CollectorInfo, ConfigUpdaterConfig, Datastore, HarvesterConfig, HostGroup, RefineryConfig};
use crate::common::util::CommandHelper;

trait ConfigUploader {
    fn upload(&self, local_tar_path: &str) -> Result<String, String>;
}

struct HttpConfigUploader {
    url: String,
    method: String,
}

impl ConfigUploader for HttpConfigUploader {
    fn upload(&self, local_tar_path: &str) -> Result<String, String> {
        let cmd = format!(
            "curl -kL -X {} -T {} {}",
            self.method,
            local_tar_path,
            self.url,
        );

        CommandHelper {
            cmd,
            ..Default::default()
        }.run()
    }
}

struct WebhdfsConfigUploader {
    url: String,
    secure: bool,
}

impl ConfigUploader for WebhdfsConfigUploader {
    fn upload(&self, local_tar_path: &str) -> Result<String, String> {
        let mut cmd = format!("set -o pipefail; curl -vk -X PUT -T '{}' ", local_tar_path);
        if self.secure {
            cmd += " -u : --negotiate ";
        }
        cmd += &self.url;
        cmd += " | sed -nE 's/Location: (.*)/\\1/p' | head -n1";

        let location = CommandHelper {
            cmd,
            ..Default::default()
        }.run().unwrap();

        let mut cmd = format!("curl -vk -X PUT -T '{}' ", local_tar_path);
        if self.secure {
            cmd += " -u : --negotiate ";
        };
        cmd += &location;

        CommandHelper {
            cmd,
            ..Default::default()
        }.run()
    }
}

trait ConfigUpdater {
    fn pull_configs_from_git(config_dir: &str, config_git_url: &str) -> Option<String> {
        let git_dir = format!("{}/_configs_git_repo", config_dir);

        if !Path::new(&git_dir).is_dir() {
            CommandHelper {
                current_dir: Some(String::from(config_dir)),
                cmd: format!("git clone {} {}", config_git_url, git_dir),
            }.run().unwrap();
        }

        let prev_commit = CommandHelper {
            current_dir: Some(String::from(&git_dir)),
            cmd: String::from("git rev-parse HEAD"),
        }.run().unwrap();

        CommandHelper {
            current_dir: Some(String::from(&git_dir)),
            cmd: String::from("git pull"),
        }.run().unwrap();

        let cur_commit = CommandHelper {
            current_dir: Some(String::from(&git_dir)),
            cmd: String::from("git rev-parse HEAD"),
        }.run().unwrap();

        if prev_commit != cur_commit {
            Some(String::from(&git_dir))
        } else {
            None
        }
    }

    fn combine_configs_as_tar(all_config: AllConfig) -> NamedTempFile {
        let config_temp_dir = tempdir().unwrap();
        let config_temp_dir_path = config_temp_dir.as_ref().to_str().unwrap();
        AllConfig::dump_all(all_config, config_temp_dir_path);
        let version = Local::now().format("%Y%m%d_%H%M%S").to_string();

        let tar_temp_file = NamedTempFile::new().unwrap();
        let tar_temp_path = tar_temp_file.path();

        let cmd = format!(
            "echo {} > version && tar -cvzf {} *",
            &version,
            tar_temp_path.to_str().unwrap(),
        );
        CommandHelper {
            current_dir: Some(config_temp_dir_path.to_string()),
            cmd,
        }.run().unwrap();

        tar_temp_file
    }

    fn upload_configs(local_tar_path: &str, uploader_config: &CncConfigUpdaterUploader) -> Result<String, String> {
        let kind = &uploader_config.kind;
        let url = String::from(&uploader_config.url);
        let param = uploader_config.param.as_ref().unwrap();

        let uploader: Box<dyn ConfigUploader> = match kind.as_str() {
            "http" => Box::new(HttpConfigUploader {
                url,
                method: param.get("method").unwrap().as_str().unwrap().to_string(),
            }),
            "webhdfs" => Box::new(WebhdfsConfigUploader {
                url,
                secure: param.get("secure").unwrap().as_bool().unwrap(),
            }),
            _ => {
                panic!("Failed to create a config uploader");
            }
        };
        let mut last_fail_msg = String::new();
        for _ in 0..3 {
            match uploader.upload(local_tar_path) {
                Ok(msg) => {
                    return Ok(msg);
                }
                Err(err) => {
                    error!("{err}");
                    last_fail_msg = err;
                }
            }
            sleep(Duration::from_secs(3));
        }
        Err(last_fail_msg)
    }
}

pub struct SimpleConfigUpdater {
    pub config: ConfigUpdaterConfig,
    pub config_dir: String,
    pub config_tar_url: String,
}

impl ConfigUpdater for SimpleConfigUpdater {}

impl SimpleConfigUpdater {
    pub fn run(&mut self) {
        let config_git_url = &self.config.get_cnc_config().config_updater.config_git_url;
        let config_git_subdir = &self.config.get_cnc_config().config_updater.config_git_subdir;
        let cluster_name = &self.config.get_cnc_config().common.cluster_name;
        loop {
            sleep(Duration::from_secs(self.config.get_cnc_config().config_updater.poll_interval_s));
            let mut git_path = match Self::pull_configs_from_git(&self.config_dir, config_git_url) {
                None => {
                    // no change
                    continue;
                }
                Some(path) => path,
            };
            if let Some(subdir) = config_git_subdir {
                git_path = Path::new(&git_path).join(subdir).to_str().unwrap().to_string();
            }

            let all_configs = AllConfig::read_all(&self.config_dir, &git_path, cluster_name);
            let tar_file = Self::combine_configs_as_tar(all_configs);
            match Self::upload_configs(tar_file.path().to_str().unwrap(), &self.config.get_cnc_config().config_updater.uploader) {
                Ok(msg) => info!("{}", msg),
                Err(err) => error!("{}", err),
            }
        }
    }
}

pub struct AmbariConfigUpdater {
    pub config: ConfigUpdaterConfig,
    pub config_dir: String,
    pub config_tar_url: String,
}

impl AmbariConfigUpdater {
    fn get_info_from_ambari(&self) -> (HashMap<String, HostGroup>, HashMap<String, bool>) {
        let cnc_config = self.config.get_cnc_config();
        let cluster_name = &cnc_config.common.cluster_name;
        let ambari = cnc_config.config_updater.ambari.as_ref().unwrap();
        let url = &ambari.url;
        let user = match &ambari.user {
            Some(user) => String::from(user),
            None => String::from("admin"),
        };
        let password = match &ambari.password_file {
            Some(password_file) => read_to_string(Path::new(password_file)).unwrap(),
            None => String::from("admin"),
        };

        let mut host_group_map: HashMap<String, HostGroup> = HashMap::new();
        let mut host_maintenance_map: HashMap<String, bool> = HashMap::new();

        let host_components_json = reqwest::blocking::Client::new().get(
            format!(
                "{}/api/v1/clusters/{}/host_components",
                url,
                cluster_name,
            )
        )
            .basic_auth(&user, Some(&password))
            .send()
            .unwrap().json::<HostComponents>().unwrap();

        for host_component in host_components_json.items {
            let roles = &host_component.host_roles;
            let hostname = roles.host_name.to_lowercase();
            let component = roles.component_name.to_lowercase();

            match host_group_map.get_mut(&component) {
                Some(hostgroup) => {
                    hostgroup.members.insert(hostname);
                },
                None => {
                    let new_group = HostGroup {
                        members: HashSet::from([hostname]),
                    };
                    host_group_map.insert(String::from(&component), new_group);
                }
            }
        }

        let host_maintenance_json = reqwest::blocking::Client::new().get(
            format!(
                "{}/api/v1/clusters/{}/hosts?fields=Hosts/maintenance_state",
                url,
                cluster_name,
            )
        )
            .basic_auth(&user, Some(&password))
            .send()
            .unwrap().json::<Hosts>().unwrap();

        for host in host_maintenance_json.items {
            let host_info = host.host_info;
            let hostname = host_info.host_name.to_lowercase();
            let maintenance_state = host_info.maintenance_state.to_lowercase();
            host_maintenance_map.insert(hostname, maintenance_state == "on");
        }

        (host_group_map, host_maintenance_map)
    }

    async fn update_host_maintenance_state_on_kafka(&self, host_maintenance_map: HashMap<String, bool>) {
        let cnc_config = self.config.get_cnc_config();
        let hosts_kafka = &cnc_config.common.hosts_kafka;
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &hosts_kafka.bootstrap_servers)
            .set("message.timeout.ms", &hosts_kafka.message_timeout_ms)
            .create()
            .expect("Producer creation error");
        for (host, in_maintenance) in host_maintenance_map.into_iter() {
            let data = json!({
               "in_maintenance": in_maintenance, 
            });
            let delivery_status = &producer.send(
                FutureRecord::to(&hosts_kafka.topic)
                    .payload(&data.to_string())
                    .key(&host),
                Duration::from_secs(0),
            )
                .await;
            debug!("Delivery status for message {} received: {:?}", &data.to_string(), delivery_status);
        }
    }

    pub async fn run(&mut self) {
        let config_git_url = &self.config.get_cnc_config().config_updater.config_git_url;
        let config_git_subdir = &self.config.get_cnc_config().config_updater.config_git_subdir;
        let cluster_name = &self.config.get_cnc_config().common.cluster_name;
        loop {
            sleep(Duration::from_secs(self.config.get_cnc_config().config_updater.poll_interval_s));
            let mut git_path = match Self::pull_configs_from_git(&self.config_dir, config_git_url) {
                None => None,
                Some(path) => path,
            };

            let (host_group_map, host_maintenance_map) = self.get_info_from_ambari();
            
            self.update_host_maintenance_state_on_kafka(host_maintenance_map).await;

            // TODO: Get changed hosts. (compare previous and current host_group.toml)
            // TODO: 1. added hosts
            // TODO: 2. deleted hosts
            // TODO: 3. Some components are installed on host
            // TODO: 4. Some components are deleted on host

            if git_path == None {
                return;
            }

            if let Some(subdir) = config_git_subdir {
                git_path = Path::new(&git_path).join(subdir).to_str().unwrap().to_string();
            }

            let all_configs = AllConfig::read_all(&self.config_dir, &git_path, cluster_name);

            // TODO: merge additional host_group mapping from ambari (host_group.toml)

            let tar_file = Self::combine_configs_as_tar(all_configs);
            match Self::upload_configs(tar_file.path().to_str().unwrap(), &self.config.get_cnc_config().config_updater.uploader) {
                Ok(msg) => info!("{}", msg),
                Err(err) => error!("{}", err),
            }
        }
    }
}
