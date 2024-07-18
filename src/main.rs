use std::error::Error;
use std::sync::Arc;

use clap::Parser;
use log::{debug, error};
use tokio::sync::RwLock;

use CNC::common::config::{ConfigUpdaterConfig, HarvesterConfig};
use CNC::common::config::RefineryConfig;
use CNC::config_updater::{AmbariConfigUpdater, SimpleConfigUpdater};
use CNC::harvester::Harvester;
use CNC::refinery::Refinery;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    component: String,
    #[arg(long)]
    config_dir: String,
    #[arg(long, default_value(""))]
    config_tar_url: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // TODO: better logger? https://docs.rs/log/latest/log/
    env_logger::init();
    let args = Args::parse();
    let config_dir = args.config_dir.as_str();
    let config_tar_url = args.config_tar_url.as_str();
    match args.component.as_str() {
        "harvester" => {
            let config = Arc::new(
                RwLock::new(
                    HarvesterConfig::new(config_dir, config_tar_url).await));
            let mut harvester = Harvester::new(config).await;
            Harvester::run(harvester).await
        }
        "refinery" => {
            let config = Arc::new(
                RwLock::new(
                    RefineryConfig::new(config_dir, config_tar_url).await));
            let mut refinery = Refinery::new(config);
            Refinery::run(refinery).await;
        }
        "config_updater" => {
            let config = ConfigUpdaterConfig::new(config_dir, config_tar_url).await;
            match &config.get_cnc_config().config_updater.ambari {
                Some(_) =>
                    (AmbariConfigUpdater {
                        config,
                        config_dir: String::from(config_dir),
                        config_tar_url: String::from(config_tar_url),
                    }).run(),
                None =>
                    (SimpleConfigUpdater {
                        config,
                        config_dir: String::from(config_dir),
                        config_tar_url: String::from(config_tar_url),
                    }).run(),
            };
        }
        _ => {
            error!("Supported components are harvester and refinery.");
        }
    }

    Ok(())
}
