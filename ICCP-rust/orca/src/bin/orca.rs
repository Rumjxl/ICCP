use clap::{App, Arg};
use lotus::algorithm::AsyncCongAlg;
use lotus::compat::PortusCompatRuntime;
use orca::{
    cubic::Cubic, ConfigReport, GenericCongAvoidAlg, Orca, OrcaLogConfig, DEFAULT_SS_THRESH,
};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tracing::{info, warn};
use tracing_subscriber::{fmt, fmt::writer::MakeWriter, prelude::*, EnvFilter};

#[derive(Clone)]
struct SharedFileWriter {
    file: Arc<Mutex<std::fs::File>>,
}

struct SharedFileGuard<'a> {
    guard: MutexGuard<'a, std::fs::File>,
}

impl<'a> Write for SharedFileGuard<'a> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.guard.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.guard.flush()
    }
}

impl<'a> MakeWriter<'a> for SharedFileWriter {
    type Writer = SharedFileGuard<'a>;

    fn make_writer(&'a self) -> Self::Writer {
        SharedFileGuard {
            guard: self.file.lock().expect("log file mutex poisoned"),
        }
    }
}

fn make_env_filter(log_filter: Option<&str>) -> EnvFilter {
    if let Some(filter) = log_filter {
        EnvFilter::try_new(filter).unwrap_or_else(|_| EnvFilter::new("warn,orca=info"))
    } else {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn,orca=info"))
    }
}

fn open_log_file(log_file: &str) -> SharedFileWriter {
    let path = Path::new(log_file);
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let _file_name = path
        .file_name()
        .expect("log file path must include a file name");

    std::fs::create_dir_all(dir).expect("failed to create log directory");

    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("failed to open log file");

    SharedFileWriter {
        file: Arc::new(Mutex::new(file)),
    }
}

fn make_args(
    name: &str,
) -> Result<(Orca<Cubic>, Option<String>, Option<String>), std::num::ParseIntError> {
    let ss_thresh_default = format!("{}", DEFAULT_SS_THRESH);
    let matches = App::new(name)
        .version("0.1.0")
        .author("Xiaolan Ji")
        .about("ORCA — lotus async framework edition")
        .arg(
            Arg::with_name("server_addr")
                .long("addr")
                .help("RL agent server address (host:port)")
                .default_value("127.0.0.1:4826"),
        )
        .arg(
            Arg::with_name("init_cwnd")
                .long("init_cwnd")
                .help("Initial congestion window in packets (0 = datapath default)")
                .default_value("0"),
        )
        .arg(
            Arg::with_name("report_per_ack")
                .long("per_ack")
                .help("Report on every ACK"),
        )
        .arg(
            Arg::with_name("report_per_rtt")
                .long("per_rtt")
                .help("Report every RTT"),
        )
        .arg(
            Arg::with_name("report_per_interval")
                .long("report_interval_ms")
                .short("i")
                .takes_value(true)
                .help("Report every N milliseconds"),
        )
        .arg(
            Arg::with_name("ss_thresh")
                .long("ss_thresh")
                .help("Slow start threshold in bytes")
                .default_value(&ss_thresh_default),
        )
        .arg(
            Arg::with_name("compensate_update")
                .long("compensate_update")
                .help("Scale cwnd update during slow start to compensate for reporting delay"),
        )
        .arg(
            Arg::with_name("n_flows")
                .long("n_flows")
                .takes_value(true)
                .help("Expected number of concurrent flows (for channel sizing)")
                .default_value("16"),
        )
        .arg(
            Arg::with_name("rpc_ms")
                .long("rpc_ms")
                .takes_value(true)
                .help("Expected agent RPC latency in milliseconds")
                .default_value("20"),
        )
        .arg(
            Arg::with_name("rpc_timeout")
                .long("rpc_timeout")
                .takes_value(true)
                .help("Override agent_task Cap'n Proto RPC timeout in milliseconds; 0 = rpc_ms * 2")
                .default_value("0"),
        )
        .arg(
            Arg::with_name("resp_timeout")
                .long("resp_timeout")
                .takes_value(true)
                .help("Override flow response wait timeout in milliseconds; 0 = rpc_timeout + 10")
                .default_value("0"),
        )
        .arg(
            Arg::with_name("log_file")
                .long("log-file")
                .takes_value(true)
                .help("Write logs to the given file path, e.g. /tmp/orca.log"),
        )
        .arg(
            Arg::with_name("log_filter")
                .long("log-filter")
                .takes_value(true)
                .help("tracing EnvFilter; default is RUST_LOG or warn,orca=info"),
        )
        .arg(
            Arg::with_name("report_log_ms")
                .long("report-log-ms")
                .takes_value(true)
                .default_value("1000")
                .help("Minimum milliseconds between ORCA report summary logs; 0 disables summaries"),
        )
        .arg(
            Arg::with_name("warn_log_ms")
                .long("warn-log-ms")
                .takes_value(true)
                .default_value("1000")
                .help("Minimum milliseconds between repeated ORCA warning logs; 0 suppresses repeat warnings"),
        )
        .arg(
            Arg::with_name("log_report_details")
                .long("log-report-details")
                .help("Log every ORCA interval report at info level"),
        )
        .group(
            clap::ArgGroup::with_name("interval")
                .args(&["report_per_ack", "report_per_rtt", "report_per_interval"])
                .multiple(true)
                .required(true),
        )
        .get_matches();

    let report_option = if matches.is_present("report_per_ack")
        && !matches.is_present("report_per_interval")
    {
        ConfigReport::Ack
    } else if matches.is_present("report_per_interval") && !matches.is_present("report_per_ack") {
        let ms: u64 = matches
            .value_of("report_per_interval")
            .unwrap()
            .parse()
            .unwrap();
        ConfigReport::Interval(Duration::from_millis(ms))
    } else if matches.is_present("report_per_rtt")
        && !matches.is_present("report_per_interval")
        && !matches.is_present("report_per_ack")
    {
        ConfigReport::Rtt
    } else if matches.is_present("report_per_interval") && matches.is_present("report_per_ack") {
        let ms: u64 = matches
            .value_of("report_per_interval")
            .unwrap()
            .parse()
            .unwrap();
        ConfigReport::Hybrid(Duration::from_millis(ms))
    } else {
        ConfigReport::Ack
    };

    let report_interval = if matches.is_present("report_per_interval") {
        Duration::from_millis(
            matches
                .value_of("report_per_interval")
                .unwrap()
                .parse()
                .unwrap(),
        )
    } else {
        Duration::from_millis(20)
    };

    let n_flows: usize = matches.value_of("n_flows").unwrap().parse().unwrap_or(16);
    let rpc_ms: usize = matches.value_of("rpc_ms").unwrap().parse().unwrap_or(20);
    let report_ms: usize = matches
        .value_of("report_per_interval")
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);

    let rounds = rpc_ms.div_ceil(report_ms).max(1);
    let channel_capacity = (((n_flows * rounds) as f64 * 1.5) as usize).max(128);
    let max_inflight = n_flows.min(128).max(1);
    let rpc_timeout_ms: u64 = matches
        .value_of("rpc_timeout")
        .unwrap()
        .parse()
        .unwrap_or(0);
    let resp_timeout_ms: u64 = matches
        .value_of("resp_timeout")
        .unwrap()
        .parse()
        .unwrap_or(0);
    let rpc_timeout_ms = if rpc_timeout_ms > 0 {
        rpc_timeout_ms
    } else {
        (rpc_ms * 2) as u64
    };
    let resp_timeout_ms = if resp_timeout_ms > 0 {
        resp_timeout_ms
    } else {
        rpc_timeout_ms + 10
    };
    let rpc_timeout = Duration::from_millis(rpc_timeout_ms);
    let resp_timeout = Duration::from_millis(resp_timeout_ms);
    let log_file = matches.value_of("log_file").map(String::from);
    let log_filter = matches.value_of("log_filter").map(String::from);
    let report_log_ms: u64 = matches
        .value_of("report_log_ms")
        .unwrap()
        .parse()
        .unwrap_or(1000);
    let warn_log_ms: u64 = matches
        .value_of("warn_log_ms")
        .unwrap()
        .parse()
        .unwrap_or(1000);
    let log_config = OrcaLogConfig {
        report_summary_interval: Duration::from_millis(report_log_ms),
        warn_interval: Duration::from_millis(warn_log_ms),
        report_details: matches.is_present("log_report_details"),
    };

    Ok((
        Orca::new(
            String::from(matches.value_of("server_addr").unwrap()),
            u32::from_str_radix(matches.value_of("init_cwnd").unwrap(), 10)?,
            report_option,
            report_interval,
            u32::from_str_radix(matches.value_of("ss_thresh").unwrap(), 10)?,
            matches.is_present("compensate_update"),
            channel_capacity,
            max_inflight,
            rpc_timeout,
            resp_timeout,
            log_config,
            Cubic::with_args(matches),
        ),
        log_file,
        log_filter,
    ))
}

#[tokio::main]
async fn main() {
    let (cfg, log_file, log_filter) = match make_args("CCP ORCA (lotus)") {
        Ok(args) => args,
        Err(e) => {
            warn!("bad argument: {:?}", e);
            return;
        }
    };

    let stdout_layer = fmt::layer()
        .with_target(true)
        .with_filter(make_env_filter(log_filter.as_deref()));

    let _file_writer = if let Some(log_file) = log_file.as_ref() {
        let file_writer = open_log_file(log_file);
        let file_layer = fmt::layer()
            .with_ansi(false)
            .with_writer(file_writer.clone())
            .with_target(true)
            .with_filter(make_env_filter(log_filter.as_deref()));

        tracing_subscriber::registry()
            .with(stdout_layer)
            .with(file_layer)
            .init();

        Some(file_writer)
    } else {
        tracing_subscriber::registry().with(stdout_layer).init();
        None
    };

    info!(
        server_addr = %cfg.server_addr,
        init_cwnd = cfg.init_cwnd,
        report = ?cfg.report_option,
        report_interval_ms = cfg.report_interval.as_millis(),
        ss_thresh = cfg.ss_thresh,
        channel_capacity = cfg.channel_capacity,
        max_inflight = cfg.max_inflight,
        rpc_timeout_ms = cfg.rpc_timeout.as_millis(),
        resp_timeout_ms = cfg.resp_timeout.as_millis(),
        report_log_ms = cfg.log_config.report_summary_interval.as_millis(),
        warn_log_ms = cfg.log_config.warn_interval.as_millis(),
        report_details = cfg.log_config.report_details,
        log_filter = log_filter.as_deref().unwrap_or("RUST_LOG or warn,orca=info"),
        log_file = log_file.as_deref().unwrap_or("stdout only"),
        "Starting CCP ORCA (lotus async framework)"
    );

    let runtime = PortusCompatRuntime::new()
        .await
        .expect("Failed to create lotus runtime");

    let mut algorithms: HashMap<String, Box<dyn AsyncCongAlg<()>>> = HashMap::new();
    algorithms.insert(cfg.name().to_string(), Box::new(cfg));

    if let Err(e) = runtime.run_netlink(algorithms).await {
        warn!("Runtime exited with error: {:?}", e);
    }

    info!("CCP ORCA stopped");
}
