use std::collections::{HashMap, HashSet};
use std::fs;
use std::fs::{read_to_string, remove_dir_all};
use std::io::{Read, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;

use chrono::Local;
use futures::future::join_all;
use log::{debug, error, info};
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json::json;
use tempfile::{NamedTempFile, tempdir};
use tokio::time::sleep;

use crate::ambari::model::{HostComponents, Hosts};
use crate::common::config::{AllConfig, Cnc, CncConfigUpdaterUploader, ConfigUpdaterConfig, Datastore, HARVESTER_PORT_DEFAULT, HarvesterConfig, HostGroup, REFINERY_PORT_DEFAULT, RefineryConfig};
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
            "curl -kL -X '{}' -T '{}' '{}'",
            self.method,
            local_tar_path,
            self.url,
        );

        CommandHelper {
            cmd,
            ..Default::default()
        }.run_and_get_stdout()
    }
}

struct WebhdfsConfigUploader {
    url: String,
    secure: bool,
}

impl ConfigUploader for WebhdfsConfigUploader {
    fn upload(&self, local_tar_path: &str) -> Result<String, String> {
        let mut cmd = format!("curl -vk --fail -X PUT -T '{}' ", local_tar_path);
        if self.secure {
            cmd += " -u : --negotiate ";
        }
        cmd = format!("{} '{}'", cmd, &self.url);

        debug!("upload cmd phase1: {}", &cmd);
        let curl_phase1 = CommandHelper {
            cmd,
            ..Default::default()
        }.run();

        if !curl_phase1.status.success() {
            let stdout = String::from_utf8(curl_phase1.stdout).unwrap_or_default();
            let stderr = String::from_utf8(curl_phase1.stderr).unwrap_or_default();
            return Err(format!(r#"stdout: {}
            stderr: {}"#, stdout, stderr));
        }

        let mut child = Command::new("sh")
            .arg("-c")
            .arg("set -o pipefail; sed -nE 's/.*Location: (.*)/\\1/p' | head -n1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.unwrap().write_all(&curl_phase1.stderr).unwrap();

        let mut location = String::new();
        child.stdout.unwrap().read_to_string(&mut location).unwrap();

        let mut cmd = format!("curl -vk --fail -X PUT -T '{}' ", local_tar_path);
        if self.secure {
            cmd += " -u : --negotiate ";
        };
        cmd = format!("{} '{}'", cmd, &location.trim());
        debug!("upload cmd phase2: {}", &cmd);
        CommandHelper {
            cmd,
            ..Default::default()
        }.run_and_get_stdout()
    }
}

trait ConfigUpdater {
    fn pull_configs_from_git(git_root: &str, config_git_url: &str, config_git_branch: &Option<String>) -> bool {
        let git_parent_dir = Path::new(git_root).parent().unwrap().to_str().unwrap().to_string();
        let git_dir_name = Path::new(git_root).file_name().unwrap().to_str().unwrap().to_string();

        let mut prev_commit = String::new();
        if !Path::new(&git_root).is_dir() {
            let mut cmd = String::from("git clone");
            if let Some(config_git_branch) = config_git_branch {
                cmd = format!("{} --branch {}", cmd, config_git_branch);
            };
            CommandHelper {
                current_dir: Some(git_parent_dir),
                cmd: format!("{} {} {}", cmd, config_git_url, git_dir_name),
            }.run_and_get_stdout().unwrap();
        } else {
            prev_commit = CommandHelper {
                current_dir: Some(String::from(git_root)),
                cmd: String::from("git rev-parse HEAD"),
            }.run_and_get_stdout().unwrap();
        }

        CommandHelper {
            current_dir: Some(String::from(git_root)),
            cmd: String::from("git pull"),
        }.run_and_get_stdout().unwrap();

        let cur_commit = CommandHelper {
            current_dir: Some(String::from(git_root)),
            cmd: String::from("git rev-parse HEAD"),
        }.run_and_get_stdout().unwrap();

        prev_commit != cur_commit
    }

    fn combine_configs_as_tar(all_config: &AllConfig) -> NamedTempFile {
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
        }.run_and_get_stdout().unwrap();

        tar_temp_file
    }

    fn upload_configs(local_tar_path: &str, uploader_config: &CncConfigUpdaterUploader) -> Result<String, String> {
        debug!("local_tar_path: {}", local_tar_path);
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
                    last_fail_msg = err;
                }
            }
            std::thread::sleep(Duration::from_secs(3));
        }
        error!("{last_fail_msg}");
        Err(String::from("Failed to upload configs"))
    }

    async fn reload_daemons(changed_host: &HashSet<String>, host_group_map: &HashMap<String, HostGroup>, cnc_config: &Cnc) {
        let refinery_hosts = match host_group_map.get("refinery") {
            Some(host_group) => {
                let mut hosts = HashSet::new();
                for host in &host_group.members {
                    hosts.insert(host.to_string());
                }
                Some(hosts)
            }
            None => None,
        };
        let harvester_hosts = match host_group_map.get("harvester") {
            Some(host_group) => {
                let mut hosts = HashSet::new();
                for host in &host_group.members {
                    hosts.insert(host.to_string());
                }
                Some(hosts)
            }
            None => None,
        };

        let mut requests = vec![];
        if let Some(refinery_hosts) = refinery_hosts {
            let port = cnc_config.refinery.port.unwrap_or(REFINERY_PORT_DEFAULT);
            for host in refinery_hosts.intersection(changed_host) {
                requests.push(reqwest::get(format!("http://{}:{}", host, port)));
            }
        }

        if let Some(harvester_hosts) = harvester_hosts {
            let port = cnc_config.harvester.port.unwrap_or(HARVESTER_PORT_DEFAULT);
            for host in harvester_hosts.intersection(changed_host) {
                requests.push(reqwest::get(format!("http://{}:{}", host, port)));
            }
        }

        join_all(requests).await;
    }
}

pub struct SimpleConfigUpdater {
    pub config: ConfigUpdaterConfig,
    pub config_dir: String,
    pub config_tar_url: String,
}

impl ConfigUpdater for SimpleConfigUpdater {}

impl SimpleConfigUpdater {
    pub async fn run(&mut self) {
        let config_git_url = &self.config.get_cnc_config().config_updater.config_git_url;
        let config_git_branch = &self.config.get_cnc_config().config_updater.config_git_branch;

        let git_root = Path::new(&self.config_dir).join("_configs_from_git").to_str().unwrap().to_string();

        let mut git_path = String::from(&git_root);
        if let Some(subdir) = &self.config.get_cnc_config().config_updater.config_git_subdir {
            git_path = Path::new(&git_path).join(subdir).to_str().unwrap().to_string();
        }
        let cluster_name = &self.config.get_cnc_config().common.cluster_name;
        loop {
            sleep(Duration::from_secs(self.config.get_cnc_config().config_updater.poll_interval_s)).await;
            debug!("Check latest configs");

            if !Self::pull_configs_from_git(&git_root, config_git_url, config_git_branch) {
                continue;
            }

            let all_configs = AllConfig::read_all(&self.config_dir, &git_path, cluster_name);
            debug!("all_configs: {:?}", &all_configs);
            let tar_file = Self::combine_configs_as_tar(&all_configs);
            Self::upload_configs(tar_file.path().to_str().unwrap(), &self.config.get_cnc_config().config_updater.uploader).unwrap();
        }
    }
}

pub struct AmbariConfigUpdater {
    pub config: ConfigUpdaterConfig,
    pub config_dir: String,
    pub config_tar_url: String,
}

impl ConfigUpdater for AmbariConfigUpdater {}

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
                }
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

    async fn update_host_maintenance_state_on_kafka(&self, host_maintenance_map: &HashMap<String, bool>) {
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
                    .key(host),
                Duration::from_secs(0),
            )
                .await;
            debug!("Delivery status for message {} received: {:?}", &data.to_string(), delivery_status);
        }
    }

    fn get_changed_hosts(prev_group: &HashMap<String, HostGroup>, cur_group: &HashMap<String, HostGroup>) -> HashSet<String> {
        let mut changed_host = HashSet::new();
        let mut prev_group_names = HashSet::new();
        for key in prev_group.keys() {
            prev_group_names.insert(key.to_string());
        }
        let mut cur_group_names = HashSet::new();
        for key in cur_group.keys() {
            cur_group_names.insert(key.to_string());
        }
        let prev_only_names = prev_group_names.difference(&cur_group_names);
        let cur_only_names = cur_group_names.difference(&prev_group_names);
        let intersection = prev_group_names.intersection(&cur_group_names);

        for key in prev_only_names {
            for host in &prev_group.get(key).unwrap().members {
                changed_host.insert(host.to_string());
            }
        }

        for key in cur_only_names {
            for host in &cur_group.get(key).unwrap().members {
                changed_host.insert(host.to_string());
            }
        }

        for key in intersection {
            let prev_members = &prev_group.get(key).unwrap().members;
            let cur_members = &cur_group.get(key).unwrap().members;
            for host in prev_members.difference(&cur_members) {
                changed_host.insert(host.to_string());
            }
            for host in cur_members.difference(&prev_members) {
                changed_host.insert(host.to_string());
            }
        }

        changed_host
    }

    pub async fn run(&mut self) {
        let config_git_url = &self.config.get_cnc_config().config_updater.config_git_url;
        let config_git_branch = &self.config.get_cnc_config().config_updater.config_git_branch;
        let git_root = Path::new(&self.config_dir).join("_configs_from_git").to_str().unwrap().to_string();

        let mut git_path = String::from(&git_root);
        if let Some(subdir) = &self.config.get_cnc_config().config_updater.config_git_subdir {
            git_path = Path::new(&git_path).join(subdir).to_str().unwrap().to_string();
        }
        let cluster_name = &self.config.get_cnc_config().common.cluster_name;
        let prev_conf_dir = Path::new(&self.config_dir).join("prev_conf_dir");

        if !prev_conf_dir.exists() {
            fs::create_dir(&prev_conf_dir).expect("Failed to create prev_conf_dir");
        }

        loop {
            sleep(Duration::from_secs(self.config.get_cnc_config().config_updater.poll_interval_s)).await;
            debug!("Check latest configs");
            let git_changed = Self::pull_configs_from_git(&git_root, config_git_url, config_git_branch);
            let (host_group_map, host_maintenance_map) = self.get_info_from_ambari();

            self.update_host_maintenance_state_on_kafka(&host_maintenance_map).await;

            let mut cur_configs = AllConfig::read_all(&self.config_dir, &git_path, cluster_name);
            cur_configs.host_groups.extend(host_group_map);

            let mut changed_host = HashSet::new();

            if !git_changed {
                let prev_configs = AllConfig::read_dir(&prev_conf_dir.to_str().unwrap());
                changed_host = Self::get_changed_hosts(&prev_configs.host_groups, &cur_configs.host_groups);
                if changed_host.len() <= 0 {
                    continue;
                }
            } else {
                for host in host_maintenance_map.keys() {
                    changed_host.insert(host.to_string());
                }
            }
            debug!("cur_configs: {:?}", &cur_configs);
            let tar_file = Self::combine_configs_as_tar(&cur_configs);
            Self::upload_configs(tar_file.path().to_str().unwrap(), &self.config.get_cnc_config().config_updater.uploader).unwrap();
            remove_dir_all(&prev_conf_dir).expect("Failed to delete prev_conf_dir");
            fs::create_dir(&prev_conf_dir).expect("Failed to recreate prev_conf_dir after deletion");
            AllConfig::dump_all(&cur_configs, &prev_conf_dir.to_str().unwrap());

            Self::reload_daemons(&changed_host, &cur_configs.host_groups, self.config.get_cnc_config()).await;
        }
    }
}
