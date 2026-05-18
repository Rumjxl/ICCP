use ccp_bbr::{BbrConfig, PROBE_RTT_INTERVAL_SECONDS};
use clap::{App, Arg};
use lotus::algorithm::AsyncCongAlg;
use lotus::compat::PortusCompatRuntime;
use std::collections::HashMap;
use std::time::Duration;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

fn make_args(name: &str) -> BbrConfig {
    let matches = App::new(name)
        .version("0.3.0")
        .about("BBR congestion control — lotus async framework edition")
        .arg(
            Arg::with_name("probe_rtt_interval")
                .long("probe_rtt_interval")
                .takes_value(true)
                .help(&format!(
                    "PROBE_RTT interval in seconds (default: {})",
                    PROBE_RTT_INTERVAL_SECONDS
                )),
        )
        .get_matches();

    let probe_rtt_interval = matches
        .value_of("probe_rtt_interval")
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(PROBE_RTT_INTERVAL_SECONDS as u64));

    BbrConfig { probe_rtt_interval }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("info"))
        .init();

    let cfg = make_args("CCP BBR (lotus)");

    info!(
        probe_rtt_interval_s = cfg.probe_rtt_interval.as_secs(),
        "Starting CCP BBR (lotus async framework)"
    );

    let runtime = PortusCompatRuntime::new()
        .await
        .expect("Failed to create lotus runtime");

    let mut algorithms: HashMap<String, Box<dyn AsyncCongAlg<()>>> = HashMap::new();
    algorithms.insert(cfg.name().to_string(), Box::new(cfg));

    if let Err(e) = runtime.run_netlink(algorithms).await {
        warn!("Runtime exited with error: {:?}", e);
    }

    info!("CCP BBR stopped");
}
