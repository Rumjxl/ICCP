use std::collections::HashMap;
use tracing::{info, debug,warn};
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct FlowKey {
    // pub src_ip: u32,
    pub src_port: u32,
    // pub dst_ip: u32,
    pub dst_port: u32,
}

use std::sync::{Arc, Mutex};
pub struct AlgorithmManager {

    pub flow_to_alg: HashMap<FlowKey, String>,

    pub default_alg: String,
}

impl AlgorithmManager {
    pub fn new(default_alg: &str) -> Self {
        AlgorithmManager {
            flow_to_alg: HashMap::new(),
            default_alg: default_alg.to_string(),
        }
    }

    pub fn get_alg(&self, key: &FlowKey) -> &str {
        let alg = self.flow_to_alg.get(key).unwrap_or(&self.default_alg).as_str();
        info!{
            // src_ip = key.src_ip,
            src_port = key.src_port,
            // dst_ip = key.dst_ip,
            dst_port = key.dst_port,
            alg = alg,
            "get_alg"
        };
        alg
    }

    pub fn add_mapping(&mut self, key: FlowKey, alg_name: String) {
        info!{
            // src_ip = key.src_ip,
            src_port = key.src_port,
            // dst_ip = key.dst_ip,
            dst_port = key.dst_port,
            alg = alg_name.as_str(),
            "add_mapping"}
        self.flow_to_alg.insert(key, alg_name);
    }
}


pub type SharedAlgorithmManager = Arc<Mutex<AlgorithmManager>>;