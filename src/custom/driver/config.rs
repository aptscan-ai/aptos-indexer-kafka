use std::collections::HashMap;
use std::fs;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct DriverConfig {
    pub kafka: HashMap<String, String>,
    pub topics: HashMap<String, String>,
}

impl DriverConfig {
    /// Read config from file.
    pub fn read_from(config_path: &str) -> DriverConfig {
        let data: String = fs::read_to_string(config_path).expect("Unable to read file");
        let res: DriverConfig = serde_json::from_str(&data).expect("Error when parsing config file content");
        return res;
    }
}
