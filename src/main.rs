use std::error::Error;
use std::sync::{Arc, RwLock};

use serde::Deserialize;

use GMS::config::HarvesterConfig;
use GMS::harvester::Harvester;

#[derive(Deserialize, Debug)]
struct Commands {
    commands: Vec<Command>,
}

#[derive(Deserialize, Debug)]
struct Command {
    command_name: String,
    command_line: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // TODO: better logger? https://docs.rs/log/latest/log/
    env_logger::init();
    // TODO: get cmd args
    let config = Arc::new(RwLock::new(HarvesterConfig::new("resources/sample")));
    let mut harvester = Harvester::new(config.clone());
    harvester.await.run(config.clone()).await;
    Ok(())
}
