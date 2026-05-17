// fn main() {
//     println!("Hello, world!");
// }
use clap::{App, Arg};
use orca::{cubic::Cubic, ConfigReport, GenericCongAvoidAlg, Orca, DEFAULT_SS_THRESH};
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::Path;

#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;
use slog::{o, Drain, Duplicate, Filter, Level, Logger};
use slog_term::{FullFormat, PlainSyncDecorator, TermDecorator};

fn open_log_file(log_file: &str) -> std::fs::File {
    let path = Path::new(log_file);
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let _file_name = path
        .file_name()
        .expect("log file path must include a file name");

    std::fs::create_dir_all(dir).expect("failed to create log directory");

    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("failed to open log file")
}

fn find_log_file_arg() -> Option<String> {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--log-file" {
            return args.next();
        }

        if let Some(value) = arg.strip_prefix("--log-file=") {
            return Some(value.to_string());
        }
    }

    None
}

fn make_logger(log_file: Option<&str>) -> Logger {
    let decorator = TermDecorator::new().build();
    let term_drain = FullFormat::new(decorator).build().fuse();

    let file_writer: Box<dyn Write + Send> = match log_file {
        Some(log_file) => Box::new(open_log_file(log_file)),
        None => Box::new(io::sink()),
    };
    let file_decorator = PlainSyncDecorator::new(file_writer);
    let file_drain = FullFormat::new(file_decorator).build().fuse();

    let drain = Duplicate::new(term_drain, file_drain).fuse();
    let drain = slog_async::Async::new(drain).build().fuse();

    // 创建一个闭包来决定日志级别
    let filter = |r: &slog::Record| r.level() <= Level::Info;

    // 使用 filter 闭包创建 Filter
    let filter_drain = Filter::new(drain, filter).fuse();

    Logger::root(filter_drain, o!())
}

fn make_args<A: GenericCongAvoidAlg>(
    name: &str,
    log: slog::Logger,
) -> Result<(Orca<A>, String, Option<String>), std::num::ParseIntError> {
    let ss_thresh_default = format!("{}", DEFAULT_SS_THRESH);
    let matches = App::new(name)
        .version("0.1.0")
        .author("Xiaolan Ji")
        .about("Implementation of Orca")
        .arg(Arg::with_name("ipc")
             .long("ipc")
             .help("Sets the type of ipc to use: (netlink|unix)")
             .default_value("unix")
             .validator(portus::algs::ipc_valid))
        .arg(Arg::with_name("server_addr")
             .long("addr")
             .help("Sets the server address of ccp channel: (127.0.0.1:4826)")
             .default_value("127.0.0.1:4826"))
        .arg(Arg::with_name("init_cwnd")
             .long("init_cwnd")
             .help("Sets the initial congestion window, in bytes. Setting 0 will use datapath default.")
             .default_value("0"))
        .arg(Arg::with_name("report_per_ack")
             .long("per_ack")
             .help("Specifies that the datapath should send a measurement upon every ACK"))
        .arg(Arg::with_name("report_per_rtt")
            .long("per_rtt")
            .help("Specifies that the datapath should send a measurement every RTT"))
        .arg(Arg::with_name("report_per_interval")
             .long("report_interval_ms")
             .short("i")
             .takes_value(true))
        .arg(Arg::with_name("ss_thresh")
             .long("ss_thresh")
             .help("Sets the slow start threshold, in bytes")
             .default_value(&ss_thresh_default))
        .arg(Arg::with_name("compensate_update")
             .long("compensate_update")
             .help("Scale the congestion window update during slow start to compensate for reporting delay"))
        .arg(Arg::with_name("log_file")
             .long("log-file")
             .takes_value(true)
             .help("Write logs to the given file path, e.g. /tmp/orca-portus.log"))
        .group(clap::ArgGroup::with_name("interval")
             .args(&["report_per_ack", "report_per_rtt","report_per_interval"])
             .multiple(true).required(true))
        .get_matches();
    let ipc = String::from(matches.value_of("ipc").unwrap());
    let log_file = matches.value_of("log_file").map(String::from);
    Ok((
        orca::Orca {
            logger: Some(log),
            server_addr: String::from(matches.value_of("server_addr").unwrap()),
            init_cwnd: u32::from_str_radix(matches.value_of("init_cwnd").unwrap(), 10)?,
            report_option: if matches.is_present("report_per_ack")
                && !matches.is_present("report_per_interval")
            {
                ConfigReport::Ack
            } else if matches.is_present("report_per_interval")
                && !matches.is_present("report_per_ack")
            {
                ConfigReport::Interval(std::time::Duration::from_millis(
                    matches
                        .value_of("report_per_interval")
                        .unwrap()
                        .parse()
                        .unwrap(),
                ))
            } else if matches.is_present("report_per_rtt") {
                ConfigReport::Rtt
            } else if matches.is_present("report_per_interval")
                && matches.is_present("report_per_ack")
            {
                ConfigReport::Hybrid(std::time::Duration::from_millis(
                    matches
                        .value_of("report_per_interval")
                        .unwrap()
                        .parse()
                        .unwrap(),
                ))
            } else {
                ConfigReport::Ack
            },
            report_interval: if matches.is_present("report_per_interval") {
                std::time::Duration::from_millis(
                    matches
                        .value_of("report_per_interval")
                        .unwrap()
                        .parse()
                        .unwrap(),
                )
            } else {
                std::time::Duration::from_millis(20)
            },
            ss_thresh: u32::from_str_radix(matches.value_of("ss_thresh").unwrap(), 10)?,
            use_compensation: matches.is_present("compensate_update"),
            alg: A::with_args(matches),
        },
        ipc,
        log_file,
    ))
}

fn main() {
    let requested_log_file = find_log_file_arg();
    let log = make_logger(requested_log_file.as_deref());
    let (cfg, ipc, log_file): (Orca<Cubic>, _, _) = make_args("CCP ORCA", log.clone())
        .map_err(|e| warn!(log, "bad argument"; "err" => ?e))
        .unwrap();

    info!(
        log,"starting CCP_ORCA";
        "ipc" => ipc.clone(),
        "init_cwnd" => cfg.init_cwnd,
        "server_addr" => cfg.server_addr.clone(),
        "log_file" => log_file.as_deref().unwrap_or("stdout only"),
    );

    portus::start!(ipc.as_str(), cfg).unwrap()
}
