//! dtcc — DTCC 拥塞控制算法二进制入口
//!
//! 原版使用 `portus::RunBuilder` + 同步 IPC 后端；
//! 本版本迁移至 **lotus** 异步框架：
//! - 主函数标注 `#[tokio::main]`，完全异步执行；
//! - 使用 `lotus::compat::PortusCompatRuntime` + `run_netlink()` 启动真实 Netlink IPC；
//! - `run_netlink()` 内部通过 `NetlinkBlockingBridge` + `DatapathListener` 与内核通信；
//! - 通过监听 Ctrl-C 退出。

use clap::{App, Arg};
use dtcc::{ConfigReport, Dtcc};
use lotus::algorithm::AsyncCongAlg;
use lotus::compat::PortusCompatRuntime;
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

fn make_env_filter() -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
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

fn make_args(name: &str) -> Result<(Dtcc, Option<String>), std::num::ParseIntError> {
    let matches = App::new(name)
        .version("0.1.0")
        .author("Xiaolan Ji")
        .about("DTCC — lotus async framework edition")
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
            Arg::with_name("report_per_interval")
                .long("report_interval_ms")
                .short("i")
                .takes_value(true)
                .help("Report every N milliseconds (also used as report_ms for channel sizing)"),
        )
        .group(
            clap::ArgGroup::with_name("interval")
                .args(&["report_per_ack", "report_per_interval"])
                .required(false),
        )
        .arg(
            Arg::with_name("n_flows")
                .long("n_flows")
                .takes_value(true)
                .help(
                    "Expected number of concurrent flows (used to size the agent channel). \
                       Formula: capacity = max(128, n_flows × ceil(rpc_ms/report_ms) × 1.5)",
                )
                .default_value("16"),
        )
        .arg(
            Arg::with_name("rpc_ms")
                .long("rpc_ms")
                .takes_value(true)
                .help(
                    "Expected Python agent batch inference latency in milliseconds \
                       (used together with --n_flows to size the agent channel)",
                )
                .default_value("5"),
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
                .help("Write logs to the given file path, e.g. /tmp/dtcc.log"),
        )
        .get_matches();

    let report_option = if matches.is_present("report_per_ack") {
        ConfigReport::Ack
    } else if matches.is_present("report_per_interval") {
        let ms: u64 = matches
            .value_of("report_per_interval")
            .unwrap()
            .parse()
            .unwrap();
        ConfigReport::Interval(Duration::from_millis(ms))
    } else {
        ConfigReport::Rtt
    };

    let n_flows: usize = matches.value_of("n_flows").unwrap().parse().unwrap_or(16);
    let rpc_ms: usize = matches.value_of("rpc_ms").unwrap().parse().unwrap_or(5);
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
    let rpc_timeout_override = if rpc_timeout_ms > 0 {
        Some(rpc_timeout_ms)
    } else {
        None
    };
    let resp_timeout_override = if resp_timeout_ms > 0 {
        Some(resp_timeout_ms)
    } else {
        None
    };
    // 直接从 -i 读取，未指定（Ack/Rtt 模式）时默认 10ms
    let report_ms: usize = matches
        .value_of("report_per_interval")
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);

    Ok((
        Dtcc::for_flows(
            matches.value_of("server_addr").unwrap().to_string(),
            u32::from_str_radix(matches.value_of("init_cwnd").unwrap(), 10)?,
            report_option,
            n_flows,
            rpc_ms,
            report_ms,
            rpc_timeout_override,
            resp_timeout_override,
        ),
        matches.value_of("log_file").map(String::from),
    ))
}

#[tokio::main]
async fn main() {
    // ── 解析命令行参数 ────────────────────────────────────────────────────────
    let (cfg, log_file) = make_args("CCP DTCC (lotus)")
        .map_err(|e| {
            warn!("bad argument: {:?}", e);
            e
        })
        .unwrap();

    let stdout_layer = fmt::layer()
        .with_target(true)
        .with_filter(make_env_filter());

    let file_writer = if let Some(log_file) = log_file.as_ref() {
        let file_writer = open_log_file(log_file);
        let file_layer = fmt::layer()
            .with_ansi(false)
            .with_writer(file_writer.clone())
            .with_target(true)
            .with_filter(make_env_filter());

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
        server_addr      = cfg.server_addr,
        init_cwnd        = cfg.init_cwnd,
        report           = ?cfg.report_option,
        channel_capacity = cfg.channel_capacity,
        max_inflight     = cfg.max_inflight,
        log_file         = log_file.as_deref().unwrap_or("stdout only"),
        "Starting CCP DTCC (lotus async framework)"
    );

    // ── 构建 PortusCompatRuntime ──────────────────────────────────────────────
    let runtime = PortusCompatRuntime::new()
        .await
        .expect("Failed to create lotus runtime");

    // ── 将 Dtcc 放入 algorithms HashMap，传给 run_netlink() ──────────────────
    //
    // run_netlink() 内部会：
    //   1. 调用 cfg.datapath_programs() 编译 CCP fold 程序为 INSTALL 帧
    //   2. 创建 NetlinkBlockingBridge（需 CAP_NET_ADMIN）
    //   3. spawn DatapathListener::run() 处理内核 CREATE / MEASURE / READY 消息
    //   4. 阻塞直至 Ctrl-C，然后 abort DatapathListener
    let mut algorithms: HashMap<String, Box<dyn AsyncCongAlg<()>>> = HashMap::new();
    algorithms.insert(cfg.name().to_string(), Box::new(cfg));

    if let Err(e) = runtime.run_netlink(algorithms).await {
        warn!("Runtime exited with error: {:?}", e);
    }

    let _file_writer = file_writer;
    info!("CCP DTCC stopped");
}
