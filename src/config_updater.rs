use std::path::Path;
use std::process::Command;
use std::ptr::replace;
use std::thread::sleep;
use std::time::Duration;
use crate::common::config::{Cnc, ConfigUpdaterConfig, CncConfigHandler};


// TODO: local_tar_path 는 서빙할 때, 어딘가에 업로드할 때도 쓰인다. 파일명에 버전명({commit-id}_{yyyymmdd} 형식, 순서대로 정렬하고 5개 정도만 남기게끔?) 포함시키기 및 "version" 파일도 tar 에 포함시키기 관리. latest 버전은 고정 파일명?
// TODO: 버전명이 계속 바뀐다면 harvester, refinery 업데이트 할 때 바뀐 파일명을 계속 전달해야할텐데? => 파일명은 동일하게 단, 버전파일도 tar 안에 같이 보관하게하여 구분할 수 있도록. 어차피 tar 는 풀어서 사용하고 tar 를 보관하진 않을 것이다. 사용하는 harvester, refinery 측에서.

// TODO: cnc.toml 파일로부터 ConfigUpdater 설정 읽어오기
pub struct SimpleConfigUpdater {
    pub config: ConfigUpdaterConfig,
    pub config_dir: String,
    pub config_tar_url: String,
}

impl SimpleConfigUpdater {
    fn pull_configs_from_git(&self) -> bool {
        let git_dir = format!("{}/configs_from_git", &self.config_dir);
        let config_git_url = &self.config.get_cnc_config().config_updater.config_git_url;

        if !Path::new(&git_dir).is_dir() {
            Command::new(format!("git clone {} {}", config_git_url, git_dir))
                .current_dir(&self.config_dir)
                .spawn()
                .expect("Failed to clone git");
        }

        let prev_commit = String::from_utf8(
            Command::new("git rev-parse HEAD")
                .current_dir(&git_dir)
                .output()
                .expect("Failed to get git commit id").stdout
        ).unwrap();

        Command::new("git pull")
            .current_dir(&git_dir)
            .spawn()
            .expect("Failed to pull git");

        let cur_commit = String::from_utf8(
            Command::new("git rev-parse HEAD")
                .current_dir(&git_dir)
                .output()
                .expect("Failed to get git commit id").stdout
        ).unwrap();

        let changed = prev_commit != cur_commit;
        changed
    }

    fn upload_configs(&self, local_tar_path: &str) {
        let config_upload_curl_cmd = &self.config.get_cnc_config().config_updater.config_upload_curl_cmd;
        let cmd = str::replace(config_upload_curl_cmd, "{local_tar_path}", local_tar_path);
        Command::new("sh")
            .arg("-c")
            .arg(&cmd)
            .spawn()
            .expect("Failed to upload configs");
    }
    
    pub fn run(&mut self) {
        // TODO: git subdirectory 지원? 예제 설정에선 그냥 eubnara/cnc repo 사용할 수 있도록
        let config_git_url = &self.config.get_cnc_config().config_updater.config_git_url;
        let config_upload_curl_cmd = &self.config.get_cnc_config().config_updater.config_upload_curl_cmd;
        // TODO: 업로드 쓰레드
        loop {
            
            // no change
            if !self.pull_configs_from_git() {
                return;
            }
            self.config.set_cnc_config(ConfigUpdaterConfig::read_item::<Cnc>(&self.config_dir, "cnc"));
            // TODO: self.upload_configs()

            sleep(Duration::from_secs(self.config.get_cnc_config().config_updater.poll_interval_s));
        }

    }
}

pub struct AmbariConfigUpdater {
    pub config: ConfigUpdaterConfig,
    pub config_dir: String,
    pub config_tar_url: String,
}

impl AmbariConfigUpdater {

    pub fn run(&mut self) {
        todo!()
    }
}
