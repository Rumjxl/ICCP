use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::{AsyncReadExt, TryFutureExt};
use tokio::net::TcpStream;

use lotus::{
    algorithm::{AsyncCongAlg, DatapathInfo, Report},
    flow::{AsyncFlow, FlowContext},
    LotusError, Result as LotusResult,
};

mod agg_measurement;
use agg_measurement::{AggMeasurement, GenericCongAvoidMeasurements, ReportStatus};

pub mod cubic;

mod ccp_capnp {
    include!(concat!(env!("OUT_DIR"), "/ccp_capnp.rs"));
}

pub const DEFAULT_SS_THRESH: u32 = 0x7fff_ffff;

pub trait GenericCongAvoidAlg: Send + Sync + 'static {
    type Flow: GenericCongAvoidFlow;
    fn name() -> &'static str;
    fn args<'a, 'b>() -> Vec<clap::Arg<'a, 'b>> {
        vec![]
    }
    fn with_args(matches: clap::ArgMatches) -> Self;
    fn new_flow(&self, init_cwnd: u32, mss: u32) -> Self::Flow;
}

pub trait GenericCongAvoidFlow: Send + Sync + 'static {
    fn curr_cwnd(&self) -> u32;
    fn curr_cwnd_bytes(&self) -> u32;
    fn set_cwnd(&mut self, cwnd: u32);
    fn increase(&mut self, m: &GenericCongAvoidMeasurements);
    fn reduction(&mut self, m: &GenericCongAvoidMeasurements);
    fn reset(&mut self) {}
}

#[derive(Debug, Clone, Copy)]
pub enum ConfigReport {
    Ack,
    Rtt,
    Interval(Duration),
    Hybrid(Duration),
}

#[derive(Debug, Clone, Copy)]
pub struct OrcaLogConfig {
    pub report_summary_interval: Duration,
    pub warn_interval: Duration,
    pub report_details: bool,
}

impl Default for OrcaLogConfig {
    fn default() -> Self {
        Self {
            report_summary_interval: Duration::from_secs(1),
            warn_interval: Duration::from_secs(1),
            report_details: false,
        }
    }
}

#[derive(Clone)]
struct LogLimiter {
    interval: Duration,
    last_log: Arc<Mutex<Instant>>,
}

impl LogLimiter {
    fn new(interval: Duration) -> Self {
        let now = Instant::now();
        Self {
            interval,
            last_log: Arc::new(Mutex::new(now.checked_sub(interval).unwrap_or(now))),
        }
    }

    fn should_log(&self) -> bool {
        if self.interval.is_zero() {
            return false;
        }

        let now = Instant::now();
        let mut last_log = self
            .last_log
            .lock()
            .expect("orca log limiter mutex poisoned");
        if now.saturating_duration_since(*last_log) >= self.interval {
            *last_log = now;
            true
        } else {
            false
        }
    }
}

struct AgentRequest {
    conn_id: u64,
    obs: AgentObservation,
    tx: oneshot::Sender<Option<u32>>,
}

struct AgentObservation {
    avg_rtt: u32,
    min_rtt: u32,
    cnt: u32,
    delivery_rate: u64,
    pacing_rate: u64,
    loss: u32,
    srtt: u32,
    snd_cwnd: u32,
    time_delta: u64,
}

pub struct Orca<A: GenericCongAvoidAlg> {
    pub server_addr: String,
    pub init_cwnd: u32,
    pub report_option: ConfigReport,
    pub report_interval: Duration,
    pub ss_thresh: u32,
    pub use_compensation: bool,
    pub channel_capacity: usize,
    pub max_inflight: usize,
    pub rpc_timeout: Duration,
    pub resp_timeout: Duration,
    pub log_config: OrcaLogConfig,
    pub alg: A,
    agent_tx: Arc<OnceLock<mpsc::Sender<AgentRequest>>>,
}

impl<A: GenericCongAvoidAlg> Orca<A> {
    pub fn new(
        server_addr: String,
        init_cwnd: u32,
        report_option: ConfigReport,
        report_interval: Duration,
        ss_thresh: u32,
        use_compensation: bool,
        channel_capacity: usize,
        max_inflight: usize,
        rpc_timeout: Duration,
        resp_timeout: Duration,
        log_config: OrcaLogConfig,
        alg: A,
    ) -> Self {
        Self {
            server_addr,
            init_cwnd,
            report_option,
            report_interval,
            ss_thresh,
            use_compensation,
            channel_capacity,
            max_inflight,
            rpc_timeout,
            resp_timeout,
            log_config,
            alg,
            agent_tx: Arc::new(OnceLock::new()),
        }
    }
}

#[async_trait]
impl<A: GenericCongAvoidAlg + Sync> AsyncCongAlg<()> for Orca<A> {
    fn name(&self) -> &'static str {
        "ORCA"
    }

    async fn datapath_programs(&self) -> HashMap<&'static str, String> {
        let mut h = HashMap::new();

        h.insert(
            "OrcaDatapathInterval",
            "
                (def
                    (Report
                        (volatile rtt 0)
                        (volatile delivery_rate 0)
                        (volatile pacing_rate 0)
                        (volatile loss 0)
                        (volatile srtt 0)
                    )
                    (ReportTime 0)
                )
                (when true
                    (:= Report.delivery_rate Flow.rate_delivery)
                    (:= Report.pacing_rate Flow.pacing_rate)
                    (:= Report.loss Ack.lost_pkts_sample)
                    (:= Report.srtt Flow.srtt)
                    (:= Report.rtt Flow.rtt_sample_us)
                    (fallthrough)
                )
                (when (> Micros ReportTime)
                    (report)
                    (:= Micros 0)
                )
            "
            .to_string(),
        );

        h.insert(
            "OrcaDatapathIntervalRTT",
            "
                (def
                    (Report
                        (volatile rtt 0)
                        (volatile delivery_rate 0)
                        (volatile pacing_rate 0)
                        (volatile loss 0)
                        (volatile srtt 0)
                    )
                    (ReportTime 0)
                )
                (when true
                    (:= Report.delivery_rate Flow.rate_delivery)
                    (:= Report.pacing_rate Flow.pacing_rate)
                    (:= Report.loss Ack.lost_pkts_sample)
                    (:= Report.srtt Flow.srtt)
                    (:= Report.rtt Flow.rtt_sample_us)
                    (fallthrough)
                )
                (when (|| Flow.was_timeout (> Report.loss 0))
                    (report)
                    (:= Micros 0)
                )
                (when (> Micros Flow.rtt_sample_us)
                    (report)
                    (:= Micros 0)
                )
            "
            .to_string(),
        );

        h.insert(
            "OrcaDatapathIntervalAck",
            "
                (def
                    (Report
                        (volatile rtt 0)
                        (volatile delivery_rate 0)
                        (volatile pacing_rate 0)
                        (volatile loss 0)
                        (volatile srtt 0)
                    )
                    (ReportTime 0)
                )
                (when true
                    (:= Report.delivery_rate Flow.rate_delivery)
                    (:= Report.pacing_rate Flow.pacing_rate)
                    (:= Report.loss Ack.lost_pkts_sample)
                    (:= Report.srtt Flow.srtt)
                    (:= Report.rtt Flow.rtt_sample_us)
                    (report)
                )
            "
            .to_string(),
        );

        h.insert(
            "OrcaHybridDatapath",
            "
                (def
                    (Report
                        (volatile rtt 0)
                        (volatile delivery_rate 0)
                        (volatile pacing_rate 0)
                        (volatile loss 0)
                        (volatile srtt 0)
                        (volatile now 0)
                        (volatile acked 0)
                        (volatile sacked 0)
                        (volatile timeout false)
                        (volatile inflight 0)
                    )
                )
                (when true
                    (:= Report.delivery_rate Flow.rate_delivery)
                    (:= Report.pacing_rate Flow.pacing_rate)
                    (:= Report.loss Ack.lost_pkts_sample)
                    (:= Report.srtt Flow.srtt)
                    (:= Report.rtt Flow.rtt_sample_us)
                    (:= Report.now Ack.now)
                    (:= Report.acked (+ Report.acked Ack.bytes_acked))
                    (:= Report.sacked (+ Report.sacked Ack.packets_misordered))
                    (:= Report.timeout Flow.was_timeout)
                    (:= Report.inflight Flow.packets_in_flight)
                    (report)
                )
            "
            .to_string(),
        );

        h
    }

    async fn new_flow(
        &self,
        control: FlowContext<()>,
        info: DatapathInfo,
    ) -> LotusResult<Box<dyn AsyncFlow>> {
        let init_cwnd = if self.init_cwnd != 0 {
            self.init_cwnd
        } else {
            info.init_cwnd
        };
        let mss = info.mss;

        let agent_tx = self
            .agent_tx
            .get_or_init(|| {
                info!(
                    server_addr = %self.server_addr,
                    channel_capacity = self.channel_capacity,
                    max_inflight = self.max_inflight,
                    rpc_timeout_ms = self.rpc_timeout.as_millis(),
                    resp_timeout_ms = self.resp_timeout.as_millis(),
                    "ORCA: spawning shared agent_task"
                );
                spawn_agent_task(
                    self.server_addr.clone(),
                    self.rpc_timeout,
                    self.channel_capacity,
                    self.max_inflight,
                    LogLimiter::new(self.log_config.warn_interval),
                )
            })
            .clone();

        debug!(
            sock_id = info.sock_id,
            src_port = info.src_port,
            dst_port = info.dst_port,
            init_cwnd,
            "ORCA: new_flow"
        );

        OrcaFlow::init(
            control,
            info,
            Some(agent_tx),
            self.resp_timeout,
            self.report_option,
            self.report_interval,
            init_cwnd,
            self.ss_thresh,
            self.use_compensation,
            self.log_config,
            self.alg.new_flow(init_cwnd, mss),
        )
        .await
        .map(|f| Box::new(f) as Box<dyn AsyncFlow>)
    }
}

fn monotonic_raw_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC_RAW, &mut ts) };
    if rc == 0 {
        (ts.tv_sec as u64)
            .saturating_mul(1_000_000_000)
            .saturating_add(ts.tv_nsec as u64)
    } else {
        0
    }
}

fn spawn_agent_task(
    server_addr: String,
    rpc_timeout: Duration,
    channel_capacity: usize,
    max_inflight: usize,
    warn_limiter: LogLimiter,
) -> mpsc::Sender<AgentRequest> {
    let (req_tx, mut req_rx) = mpsc::channel::<AgentRequest>(channel_capacity);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("ORCA: failed to build agent_task runtime");

    std::thread::spawn(move || {
        let local = tokio::task::LocalSet::new();
        rt.block_on(local.run_until(async move {
            let client = match connect_capnp(&server_addr).await {
                Ok(c) => {
                    info!(
                        server_addr,
                        max_inflight, "ORCA agent_task: connected to RL agent"
                    );
                    c
                }
                Err(e) => {
                    error!(error = ?e, server_addr, "ORCA agent_task: connection failed, exiting");
                    return;
                }
            };

            let sem = Arc::new(Semaphore::new(max_inflight));

            while let Some(AgentRequest { conn_id, obs, tx }) = req_rx.recv().await {
                let client = client.clone();
                let sem = Arc::clone(&sem);
                let warn_limiter = warn_limiter.clone();
                tokio::task::spawn_local(async move {
                    let _permit = sem.acquire().await;
                    let mut req = client.get_action_request();
                    {
                        let mut o = req.get().init_observation();
                        o.set_avgrtt(obs.avg_rtt);
                        o.set_minrtt(obs.min_rtt);
                        o.set_cnt(obs.cnt);
                        o.set_delivery_rate(obs.delivery_rate);
                        o.set_pacing_rate(obs.pacing_rate);
                        o.set_loss(obs.loss);
                        o.set_srtt(obs.srtt);
                        o.set_snd_cwnd(obs.snd_cwnd);
                        o.set_time_delta(obs.time_delta);
                        o.set_connection_id(conn_id);
                        o.set_rpc_send_mono_ns(monotonic_raw_ns());
                    }

                    let result = timeout(rpc_timeout, req.send().promise).await;
                    let cwnd_opt = match result {
                        Ok(Ok(resp)) => resp
                            .get()
                            .ok()
                            .and_then(|r| r.get_action().ok())
                            .map(|a| a.get_cwnd())
                            .filter(|&c| c > 0),
                        Ok(Err(e)) => {
                            if warn_limiter.should_log() {
                                warn!(conn_id, error = ?e, "ORCA agent_task: RPC error");
                            } else {
                                debug!(conn_id, error = ?e, "ORCA agent_task: RPC error");
                            }
                            None
                        }
                        Err(_) => {
                            if warn_limiter.should_log() {
                                warn!(
                                    conn_id,
                                    timeout_ms = rpc_timeout.as_millis(),
                                    "ORCA agent_task: RPC timed out"
                                );
                            } else {
                                debug!(
                                    conn_id,
                                    timeout_ms = rpc_timeout.as_millis(),
                                    "ORCA agent_task: RPC timed out"
                                );
                            }
                            None
                        }
                    };
                    let _ = tx.send(cwnd_opt);
                });
            }

            info!("ORCA agent_task: channel closed, exiting");
        }));
    });

    req_tx
}

async fn connect_capnp(server_addr: &str) -> std::io::Result<ccp_capnp::r_l_agent::Client> {
    use std::net::ToSocketAddrs;

    let addr = server_addr
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid addr"))?;

    let stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;

    debug!(local = ?stream.local_addr()?, peer = ?stream.peer_addr()?, "ORCA: TCP connected");

    let (reader, writer) = tokio_util::compat::TokioAsyncReadCompatExt::compat(stream).split();

    let network = Box::new(twoparty::VatNetwork::new(
        futures::io::BufReader::new(reader),
        futures::io::BufWriter::new(writer),
        rpc_twoparty_capnp::Side::Client,
        Default::default(),
    ));

    let mut rpc_system = RpcSystem::new(network, None);
    let client: ccp_capnp::r_l_agent::Client =
        rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);

    tokio::task::spawn_local(rpc_system.map_err(|e| eprintln!("ORCA RpcSystem error: {:?}", e)));

    Ok(client)
}

pub struct OrcaFlow<A: GenericCongAvoidFlow> {
    control: FlowContext<()>,
    info: DatapathInfo,
    agent_tx: Option<mpsc::Sender<AgentRequest>>,
    report_option: ConfigReport,
    agg_measurement: AggMeasurement,
    cwnd: u32,
    agent_cwnd: Arc<AtomicU32>,
    init_cwnd: u32,
    prev_report_time: Instant,
    start_timestep: Instant,
    pre_packet_lost: u32,
    alg: A,
    ss_thresh: u32,
    use_compensation: bool,
    deficit_timeout: u32,
    curr_cwnd_reduction: u32,
    last_cwnd_reduction: Instant,
    min_rtt: u32,
    slow_start_passed: bool,
    resp_timeout: Duration,
    counts: u32,
    resp_timeout_count: Arc<AtomicU32>,
    flow_started_at: Instant,
    last_report_rx: Option<Instant>,
    last_kernel_report_ns: Option<u64>,
    log_config: OrcaLogConfig,
    report_summary_limiter: LogLimiter,
    warn_limiter: LogLimiter,
}

impl<A: GenericCongAvoidFlow> OrcaFlow<A> {
    async fn init(
        control: FlowContext<()>,
        info: DatapathInfo,
        agent_tx: Option<mpsc::Sender<AgentRequest>>,
        resp_timeout: Duration,
        report_option: ConfigReport,
        report_interval: Duration,
        init_cwnd: u32,
        ss_thresh: u32,
        use_compensation: bool,
        log_config: OrcaLogConfig,
        alg: A,
    ) -> LotusResult<Self> {
        let now = Instant::now();
        let mut flow = OrcaFlow {
            control,
            info,
            agent_tx,
            report_option,
            agg_measurement: AggMeasurement::new(report_interval),
            cwnd: init_cwnd,
            agent_cwnd: Arc::new(AtomicU32::new(0)),
            init_cwnd,
            prev_report_time: now,
            start_timestep: now,
            pre_packet_lost: 0,
            alg,
            ss_thresh,
            use_compensation,
            deficit_timeout: 10,
            curr_cwnd_reduction: 0,
            last_cwnd_reduction: now - Duration::from_millis(500),
            min_rtt: 0,
            slow_start_passed: false,
            resp_timeout,
            counts: 0,
            resp_timeout_count: Arc::new(AtomicU32::new(0)),
            flow_started_at: now,
            last_report_rx: None,
            last_kernel_report_ns: None,
            log_config,
            report_summary_limiter: LogLimiter::new(log_config.report_summary_interval),
            warn_limiter: LogLimiter::new(log_config.warn_interval),
        };

        flow.install_datapath_program().await?;
        Ok(flow)
    }

    async fn install_datapath_program(&mut self) -> LotusResult<()> {
        match self.report_option {
            ConfigReport::Interval(i) => {
                debug!(us = i.as_micros(), "ORCA: installing interval program");
                self.control
                    .set_program_by_name(
                        &self.info,
                        "OrcaDatapathInterval",
                        &[("ReportTime", i.as_micros() as u64)],
                    )
                    .await
            }
            ConfigReport::Rtt => {
                debug!("ORCA: installing RTT program");
                let uid = self
                    .info
                    .programs
                    .get("OrcaDatapathIntervalRTT")
                    .copied()
                    .ok_or_else(|| {
                        LotusError::Algorithm("OrcaDatapathIntervalRTT not found".into())
                    })?;
                self.control.set_program(uid, &[]).await
            }
            ConfigReport::Ack => {
                debug!("ORCA: installing per-ACK program");
                let uid = self
                    .info
                    .programs
                    .get("OrcaDatapathIntervalAck")
                    .copied()
                    .ok_or_else(|| {
                        LotusError::Algorithm("OrcaDatapathIntervalAck not found".into())
                    })?;
                self.control.set_program(uid, &[]).await
            }
            ConfigReport::Hybrid(_) => {
                debug!("ORCA: installing hybrid program");
                let uid = self
                    .info
                    .programs
                    .get("OrcaHybridDatapath")
                    .copied()
                    .ok_or_else(|| LotusError::Algorithm("OrcaHybridDatapath not found".into()))?;
                self.control.set_program(uid, &[]).await
            }
        }
    }

    async fn update_cwnd_rate(&self, cwnd_bytes: u32) {
        let rate = if self.min_rtt > 0 {
            match (cwnd_bytes as u64).checked_mul(2) {
                Some(cwnd_times_2) => {
                    let minrtt_seconds = self.min_rtt as f64 / 1_000_000.0;
                    ((cwnd_times_2 as f64) / minrtt_seconds) as u64
                }
                None => 125000,
            }
        } else {
            125000
        };

        debug!(cwnd_bytes, rate, "ORCA: update cwnd and rate");
        let _ = self
            .control
            .update_field(&[(2u8, 4u32, cwnd_bytes as u64), (2u8, 5u32, rate)])
            .await;
    }

    fn handle_timeout(&mut self) {
        self.ss_thresh /= 2;
        if self.ss_thresh < self.init_cwnd * self.info.mss {
            self.ss_thresh = self.init_cwnd * self.info.mss;
        }

        self.alg.reset();
        self.alg.set_cwnd(self.init_cwnd);
        self.curr_cwnd_reduction = 0;
        self.cwnd = self.alg.curr_cwnd();
        self.slow_start_passed = false;

        if self.warn_limiter.should_log() {
            warn!(
                curr_cwnd_pkts = self.init_cwnd,
                ssthresh = self.ss_thresh,
                "Timeout occurred"
            );
        } else {
            debug!(
                curr_cwnd_pkts = self.init_cwnd,
                ssthresh = self.ss_thresh,
                "Timeout occurred"
            );
        }
    }

    fn maybe_reduce_cwnd(&mut self, m: &GenericCongAvoidMeasurements) -> bool {
        let mut reduced = false;

        if m.loss > 0 || m.sacked > 0 {
            if self.deficit_timeout > 0
                && (Instant::now() - self.last_cwnd_reduction)
                    > Duration::from_millis(
                        u64::from(m.rtt).saturating_mul(u64::from(self.deficit_timeout)),
                    )
            {
                self.curr_cwnd_reduction = 0;
            }

            if m.loss > 0 && self.curr_cwnd_reduction == 0
                || (m.acked > 0 && self.alg.curr_cwnd_bytes() == self.ss_thresh)
            {
                self.alg.reduction(m);
                self.last_cwnd_reduction = Instant::now();
                self.ss_thresh = self.alg.curr_cwnd_bytes();
                self.cwnd = self.alg.curr_cwnd();
                self.slow_start_passed = true;
                reduced = true;
            }

            self.curr_cwnd_reduction += m.sacked + m.loss;
        } else if m.acked < self.curr_cwnd_reduction {
            self.curr_cwnd_reduction -= (m.acked as f32 / self.info.mss as f32) as u32;
        } else {
            self.curr_cwnd_reduction = 0;
        }

        reduced
    }

    fn exit_slow_start_after_congestion(&mut self, m: &GenericCongAvoidMeasurements) -> bool {
        if self.slow_start_passed || (m.loss == 0 && m.sacked == 0) {
            return false;
        }

        self.alg.reduction(m);
        self.last_cwnd_reduction = Instant::now();
        self.ss_thresh = self.alg.curr_cwnd_bytes();
        self.cwnd = self.alg.curr_cwnd();
        self.slow_start_passed = true;
        self.curr_cwnd_reduction = self
            .curr_cwnd_reduction
            .saturating_add(m.sacked.saturating_add(m.loss));

        true
    }

    fn slow_start_increase(&mut self, acked: u32) -> u32 {
        let mut new_bytes_acked = acked;
        if self.alg.curr_cwnd_bytes() < self.ss_thresh {
            if self.alg.curr_cwnd_bytes() + new_bytes_acked > self.ss_thresh {
                new_bytes_acked -= self.ss_thresh - self.alg.curr_cwnd_bytes();
                self.alg.set_cwnd(self.ss_thresh / self.info.mss as u32);
            } else {
                let curr_cwnd = self.alg.curr_cwnd_bytes();
                if self.use_compensation {
                    let delta = f64::from(new_bytes_acked) / (2.0_f64).ln();
                    self.alg
                        .set_cwnd((curr_cwnd + delta as u32) / self.info.mss as u32);
                } else {
                    self.alg
                        .set_cwnd((curr_cwnd + new_bytes_acked) / self.info.mss as u32);
                }
                new_bytes_acked = 0
            }
        }
        new_bytes_acked
    }
}

#[async_trait]
impl<A: GenericCongAvoidFlow + 'static> AsyncFlow for OrcaFlow<A> {
    async fn on_report(&mut self, sock_id: u32, m: Report) -> LotusResult<()> {
        let now = Instant::now();
        let report_rx = m.timestamp;
        let kernel_report_time_ns = m
            .get_field("kernel_report_time_ns")
            .or_else(|| m.get_field("Report.kernel_report_time_ns"));
        let user_report_gap_us = self
            .last_report_rx
            .map(|last_rx| report_rx.saturating_duration_since(last_rx).as_micros() as u64);
        let kernel_report_gap_us = match (self.last_kernel_report_ns, kernel_report_time_ns) {
            (Some(last_kernel_ns), Some(current_kernel_ns)) => current_kernel_ns
                .checked_sub(last_kernel_ns)
                .map(|gap_ns| gap_ns / 1_000),
            _ => None,
        };
        let user_minus_kernel_gap_us = match (user_report_gap_us, kernel_report_gap_us) {
            (Some(user_gap), Some(kernel_gap)) => Some(user_gap as i64 - kernel_gap as i64),
            _ => None,
        };
        let report_rx_elapsed_us = report_rx
            .saturating_duration_since(self.flow_started_at)
            .as_micros() as u64;
        let on_report_queue_us = now.saturating_duration_since(report_rx).as_micros() as u64;

        self.last_report_rx = Some(report_rx);
        if let Some(kernel_report_time_ns) = kernel_report_time_ns {
            self.last_kernel_report_ns = Some(kernel_report_time_ns);
        }

        let latest_agent_cwnd = self.agent_cwnd.load(Ordering::Relaxed);
        if latest_agent_cwnd > 0 && latest_agent_cwnd != self.cwnd {
            self.cwnd = latest_agent_cwnd;
            self.alg.set_cwnd(self.cwnd);
            debug!(sock_id, cwnd = self.cwnd, "ORCA: applied agent cwnd");
        }

        let (report_status, mut ms) = self.agg_measurement.report(&m);
        self.min_rtt = ms.min_rtt;

        if !self.slow_start_passed {
            self.slow_start_passed = self.ss_thresh < (self.cwnd * self.info.mss);
        }

        if report_status == ReportStatus::AckReport {
            if ms.was_timeout {
                self.handle_timeout();
                self.update_cwnd_rate(self.alg.curr_cwnd_bytes()).await;
                return Ok(());
            }

            ms.acked = self.slow_start_increase(ms.acked);
            self.alg.increase(&ms);
            let left_slow_start = self.exit_slow_start_after_congestion(&ms);
            if !left_slow_start {
                self.maybe_reduce_cwnd(&ms);
            } else {
                self.update_cwnd_rate(self.alg.curr_cwnd_bytes()).await;
                info!(
                    sock_id,
                    loss = ms.loss,
                    sacked = ms.sacked,
                    cwnd_pkts = self.cwnd,
                    ss_packets = self.ss_thresh / self.info.mss,
                    curr_cwnd_reduction = self.curr_cwnd_reduction,
                    "ORCA: leaving slow start after first congestion event"
                );
            }

            if self.curr_cwnd_reduction > 0 {
                debug!(
                    curr_cwnd_reduction = self.curr_cwnd_reduction,
                    acked = ms.acked / self.info.mss,
                    "Cwnd reduction in progress"
                );
                return Ok(());
            }

            self.cwnd = self.alg.curr_cwnd();
            self.update_cwnd_rate(self.alg.curr_cwnd_bytes()).await;

            debug!(
                sock_id,
                acked = ms.acked / self.info.mss,
                inflight = ms.inflight,
                loss = ms.loss,
                ss_thresh_packets = self.ss_thresh / self.info.mss,
                rtt = ms.rtt,
                cwnd_pkts = self.cwnd,
                curr_cwnd_reduction = self.curr_cwnd_reduction,
                "Cubic update"
            );
        } else if report_status == ReportStatus::NoReport {
            // skip
        } else {
            // IntervalReport — send to RL agent
            if !self.slow_start_passed && (ms.loss > 0 || ms.sacked > 0) {
                if self.exit_slow_start_after_congestion(&ms) {
                    self.update_cwnd_rate(self.alg.curr_cwnd_bytes()).await;
                    info!(
                        sock_id,
                        loss = ms.loss,
                        sacked = ms.sacked,
                        cwnd_pkts = self.cwnd,
                        ss_packets = self.ss_thresh / self.info.mss,
                        curr_cwnd_reduction = self.curr_cwnd_reduction,
                        "ORCA: leaving slow start after first congestion event"
                    );
                }
            }

            if self.slow_start_passed {
                self.counts += 1;

                let loss_increment = if ms.loss > self.pre_packet_lost {
                    ms.loss - self.pre_packet_lost
                } else {
                    0
                };
                self.pre_packet_lost = ms.loss;

                let time_delta = self.prev_report_time.elapsed();
                self.prev_report_time = Instant::now();
                let resp_timeout_actions = self.resp_timeout_count.load(Ordering::Relaxed);
                if self.log_config.report_details {
                    info!(
                        sock_id,
                        total_reports = self.counts,
                        resp_timeout_actions,
                        time_delta_ms = time_delta.as_millis() as f64,
                        kernel_report_time_ns = kernel_report_time_ns.unwrap_or(0),
                        kernel_report_gap_us = kernel_report_gap_us.unwrap_or(0),
                        user_report_gap_us = user_report_gap_us.unwrap_or(0),
                        user_minus_kernel_gap_us = user_minus_kernel_gap_us.unwrap_or(0),
                        on_report_queue_us,
                        report_rx_elapsed_us,
                        "ORCA IntervalReport detail"
                    );
                } else {
                    debug!(
                        sock_id,
                        total_reports = self.counts,
                        resp_timeout_actions,
                        time_delta_ms = time_delta.as_millis() as f64,
                        kernel_report_time_ns = kernel_report_time_ns.unwrap_or(0),
                        kernel_report_gap_us = kernel_report_gap_us.unwrap_or(0),
                        user_report_gap_us = user_report_gap_us.unwrap_or(0),
                        user_minus_kernel_gap_us = user_minus_kernel_gap_us.unwrap_or(0),
                        on_report_queue_us,
                        report_rx_elapsed_us,
                        "ORCA IntervalReport detail"
                    );
                }

                if self.report_summary_limiter.should_log() {
                    info!(
                        sock_id,
                        total_reports = self.counts,
                        cwnd_pkts = self.cwnd,
                        loss_increment,
                        srtt = ms.srtt,
                        min_rtt = ms.min_rtt,
                        delivery_rate = ms.delivery_rate,
                        pacing_rate = ms.pacing_rate,
                        resp_timeout_actions,
                        on_report_queue_us,
                        "ORCA report summary"
                    );
                }

                if let Some(agent_tx) = &self.agent_tx {
                    let (resp_tx, resp_rx) = oneshot::channel();
                    let obs = AgentObservation {
                        avg_rtt: ms.avg_rtt,
                        min_rtt: ms.min_rtt,
                        cnt: ms.cnt,
                        delivery_rate: ms.delivery_rate,
                        pacing_rate: ms.pacing_rate,
                        loss: loss_increment,
                        srtt: ms.srtt,
                        snd_cwnd: self.cwnd,
                        time_delta: time_delta.as_micros() as u64,
                    };

                    match agent_tx.try_send(AgentRequest {
                        conn_id: sock_id as u64,
                        obs,
                        tx: resp_tx,
                    }) {
                        Ok(_) => {
                            let control = self.control.clone();
                            let agent_cwnd = Arc::clone(&self.agent_cwnd);
                            let mss = self.info.mss;
                            let min_rtt = self.min_rtt;
                            let resp_timeout = self.resp_timeout;
                            let timeout_counter = Arc::clone(&self.resp_timeout_count);
                            let warn_limiter = self.warn_limiter.clone();

                            tokio::spawn(async move {
                                match timeout(resp_timeout, resp_rx).await {
                                    Ok(Ok(Some(new_cwnd))) if new_cwnd > 0 => {
                                        agent_cwnd.store(new_cwnd, Ordering::Relaxed);
                                        let cwnd_bytes = new_cwnd * mss;
                                        let rate = if min_rtt > 0 {
                                            let minrtt_s = min_rtt as f64 / 1_000_000.0;
                                            (((cwnd_bytes as u64) * 2) as f64 / minrtt_s) as u64
                                        } else {
                                            125000
                                        };
                                        let _ = control
                                            .update_field(&[
                                                (2u8, 4u32, cwnd_bytes as u64),
                                                (2u8, 5u32, rate),
                                            ])
                                            .await;
                                        debug!(
                                            sock_id,
                                            cwnd_packets = new_cwnd,
                                            "ORCA: agent cwnd applied (async)"
                                        );
                                    }
                                    Ok(Ok(_)) => {
                                        debug!(
                                            sock_id,
                                            "ORCA: agent returned non-positive cwnd, keeping current"
                                        );
                                    }
                                    Ok(Err(_)) => {
                                        if warn_limiter.should_log() {
                                            warn!(sock_id, "ORCA: agent response channel dropped");
                                        } else {
                                            debug!(sock_id, "ORCA: agent response channel dropped");
                                        }
                                    }
                                    Err(_) => {
                                        if warn_limiter.should_log() {
                                            warn!(
                                                sock_id,
                                                resp_timeout_ms = resp_timeout.as_millis(),
                                                "ORCA: agent response timed out"
                                            );
                                        } else {
                                            debug!(
                                                sock_id,
                                                resp_timeout_ms = resp_timeout.as_millis(),
                                                "ORCA: agent response timed out"
                                            );
                                        }
                                        timeout_counter.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                            });
                        }
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            if self.warn_limiter.should_log() {
                                warn!(sock_id, "ORCA: agent channel full, keeping current cwnd");
                            } else {
                                debug!(sock_id, "ORCA: agent channel full, keeping current cwnd");
                            }
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            warn!(sock_id, "ORCA: agent channel closed, disabling");
                            self.agent_tx = None;
                        }
                    }
                }
            } else {
                let ss_cwnd = self.cwnd * 1.1 as u32;
                self.cwnd = ss_cwnd;
                self.update_cwnd_rate(ss_cwnd * self.info.mss).await;
                debug!(
                    sock_id = self.info.sock_id,
                    cwnd_pkts = ss_cwnd,
                    ss_packets = self.ss_thresh / self.info.mss,
                    loss = ms.loss,
                    "Agent slowstart update"
                );
            }
        }

        self.start_timestep = Instant::now();
        Ok(())
    }

    async fn close(&mut self) -> LotusResult<()> {
        let resp_timeout_count = self.resp_timeout_count.load(Ordering::Relaxed);
        info!(
            sock_id = self.info.sock_id,
            total_reports = self.counts,
            final_cwnd = self.cwnd,
            resp_timeout_actions = resp_timeout_count,
            resp_timeout_ms = self.resp_timeout.as_millis(),
            "ORCA: flow closed"
        );
        self.agent_tx = None;
        Ok(())
    }
}
