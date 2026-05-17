// fn main() {
//     println!("Hello, world!");
// }

use clap::{App,Arg};
use dtcc::{Dtcc,ConfigReport};
use portus::RunBuilder;
use portus::ipc::{BackendBuilder};
use std::path::Path;
use std::time::Duration;
use tracing::{info, warn};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

fn make_env_filter() -> EnvFilter {
    EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"))
}

fn make_args(name: &str) -> Result<(Dtcc, String, Option<String>), std::num::ParseIntError> {
    let matches = App::new(name)
        .version("0.1.0")
        .author("Xiaolan Ji")
        .about("Implementation of DTCC")
        .arg(
            Arg::with_name("ipc")
                .long("ipc")
                .help("Sets the type of ipc to use: (netlink|unix)")
                .default_value("unix")
                .validator(portus::algs::ipc_valid),
        )
        .arg(
            Arg::with_name("server_addr")
                .long("addr")
                .help("Sets the server address of ccp channel: (127.0.0.1:4826)")
                .default_value("127.0.0.1:4826"),
        )
        .arg(
            Arg::with_name("init_cwnd")
                .long("init_cwnd")
                .help(
                    "Sets the initial congestion window, in bytes. Setting 0 will use datapath default.",
                )
                .default_value("0"),
        )
        .arg(
            Arg::with_name("report_per_ack")
                .long("per_ack")
                .help("Specifies that the datapath should send a measurement upon every ACK"),
        )
        .arg(
            Arg::with_name("report_per_interval")
                .long("report_interval_ms")
                .short("i")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("rpc_ms")
                .long("rpc_ms")
                .help("Expected Python agent RPC latency in ms; rpc_timeout = rpc_ms * 2")
                .default_value("10"),
        )
        .arg(
            Arg::with_name("log_file")
                .long("log-file")
                .takes_value(true)
                .help("Write logs to the given file path, e.g. /tmp/dtcc.log"),
        )
        .group(
            clap::ArgGroup::with_name("interval")
                .args(&["report_per_ack", "report_per_interval"])
                .required(false),
        )
        .get_matches();

    let rpc_ms: u64 = matches.value_of("rpc_ms").unwrap().parse().unwrap_or(10);

    Ok((
        Dtcc {
            server_addr: String::from(matches.value_of("server_addr").unwrap()),
            init_cwnd: u32::from_str_radix(matches.value_of("init_cwnd").unwrap(), 10)?,
            report_option: if matches.is_present("report_per_ack") {
                ConfigReport::Ack
            } else if matches.is_present("report_per_interval") {
                ConfigReport::Interval(Duration::from_millis(
                    matches
                        .value_of("report_per_interval")
                        .unwrap()
                        .parse()
                        .unwrap(),
                ))
            } else {
                ConfigReport::Rtt
            },
            rpc_timeout: Duration::from_millis(rpc_ms * 2 + 10),
        },
        String::from(matches.value_of("ipc").unwrap()),
        matches.value_of("log_file").map(String::from),
    ))
}

fn main() {
    let (cfg, ipc, log_file) = make_args("CCP DTCC")
        .map_err(|e| {
            warn!("bad argument: err={:?}", e);
            e
        })
        .unwrap();

    let stdout_layer = fmt::layer()
        .with_target(true)
        .with_filter(make_env_filter());

    let file_guard = if let Some(log_file) = log_file.as_ref() {
        let path = Path::new(log_file);
        let dir = path.parent().unwrap_or_else(|| Path::new("."));
        let file_name = path
            .file_name()
            .expect("log file path must include a file name");

        std::fs::create_dir_all(dir).expect("failed to create log directory");

        let file_appender = tracing_appender::rolling::never(dir, file_name);
        let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
        let file_layer = fmt::layer()
            .with_ansi(false)
            .with_writer(non_blocking)
            .with_target(true)
            .with_filter(make_env_filter());

        tracing_subscriber::registry()
            .with(stdout_layer)
            .with(file_layer)
            .init();

        Some(guard)
    } else {
        tracing_subscriber::registry()
            .with(stdout_layer)
            .init();
        None
    };

    info!(
        ipc = ipc,
        init_cwnd = cfg.init_cwnd,
        server_addr = cfg.server_addr,
        log_file = log_file.as_deref().unwrap_or("stdout only"),
        "starting CCP_DTCC"
    );

    // portus::start!(ipc.as_str(), cfg).unwrap()
    let b = match ipc.as_str() {
        "netlink" => {
            portus::ipc::netlink::Socket::<portus::ipc::Nonblocking>::new()
            .map(|sk| BackendBuilder { sock: sk })
            .expect("ipc netlink initialization")
        }
        "unix" => {
            portus::ipc::netlink::Socket::<portus::ipc::Nonblocking>::new()
            .map(|sk| BackendBuilder { sock: sk })
            .expect("ipc unix initialization")
        }
        _ => panic!("Invalid IPC type"),
    };
    let rb = RunBuilder::new(b)
    .default_alg(cfg)
    .default_alg_name("DTCC");

    let _file_guard = file_guard;
    rb.run();
}
