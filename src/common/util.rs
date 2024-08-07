use std::process::{Command, Output};

#[derive(Default)]
pub struct CommandHelper {
    pub current_dir: Option<String>,
    pub cmd: String,
}

impl CommandHelper {
    pub fn run_and_get_stdout(&self) -> Result<String, String> {
        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg(&self.cmd);
        if let Some(current_dir) = &self.current_dir {
            cmd.current_dir(current_dir);
        }
        let output = cmd.output().unwrap();
        if !output.status.success() {
            return Err(format!("stdout: {}\n\
            stderr: {}",
            String::from_utf8(output.stdout).unwrap_or_default(),
            String::from_utf8(output.stderr).unwrap_or_default()))
        }
        Ok(String::from_utf8(output.stdout).unwrap())
    }

    pub fn run(&self) -> Output {
        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg(&self.cmd);
        if let Some(current_dir) = &self.current_dir {
            cmd.current_dir(current_dir);
        }
        cmd.output().unwrap()
    }
}
