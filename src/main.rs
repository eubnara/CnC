use std::error::Error;
use std::sync::{Arc, RwLock};
use clap::Parser;
use log::error;

use serde::Deserialize;

use CNC::common::config::HarvesterConfig;
use CNC::common::config::RefineryConfig;
use CNC::harvester::Harvester;
use CNC::refinery::Refinery;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    component: String,
    #[arg(long)]
    config_dir: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // TODO: better logger? https://docs.rs/log/latest/log/
    env_logger::init();
    let args = Args::parse();
    let config_dir = args.config_dir.as_str();
    match args.component.as_str() {
        "harvester" => {
            let config = Arc::new(RwLock::new(HarvesterConfig::new(config_dir)));
            let mut harvester = Harvester::new(config.clone());
            harvester.await.run(config.clone()).await;
        },
        "refinery" => {
            let config = Arc::new(RwLock::new(RefineryConfig::new(config_dir)));
            let mut refinery = Refinery::new(config.clone());
            refinery.await.run().await;
        },
        _ => {
            error!("Supported components are harvester and refinery.");
        }
    }

    Ok(())
}
