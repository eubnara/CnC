use std::error::Error;
use std::fs;
use serde::Deserialize;
use subprocess::{Popen, PopenConfig, Redirection};
use GMS::harvester::Harvester;


#[derive(Deserialize,Debug)]
struct Commands {
    commands: Vec<Command>
}

#[derive(Deserialize,Debug)]
struct Command {
    command_name: String,
    command_line: String,
}

fn main() -> Result<(), Box<dyn Error>> {
    // TODO: better logger? https://docs.rs/log/latest/log/
    env_logger::init();
    // TODO: get cmd args
    let mut harvester = Harvester::new("resources/sample");
    harvester.run();
    Ok(())
}
