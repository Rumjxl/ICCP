#[macro_use]
extern crate slog;
use slog::Logger;

use portus::ipc::Ipc;
use portus::lang::Scope;
use portus::{CongAlg, Datapath, DatapathInfo, DatapathTrait, Error, Flow, Report, Result};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
mod agg_measurement;
use agg_measurement::{AggMeasurement, GenericCongAvoidMeasurements, ReportStatus};

use futures::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::runtime::Runtime;
use tokio::time::timeout;
use tokio_util::compat;

use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
pub const DEFAULT_SS_THRESH: u32 = 0x7fff_ffff;

pub mod cubic;
// use cubic::Cubic;

// #![allow(dead_code)]
mod ccp_capnp {
    include!(concat!(env!("OUT_DIR"), "/ccp_capnp.rs"));
}

pub trait GenericCongAvoidAlg {
    type Flow: GenericCongAvoidFlow;

    fn name() -> &'static str;
    fn args<'a, 'b>() -> Vec<clap::Arg<'a, 'b>> {
        vec![]
    }
    fn with_args(matches: clap::ArgMatches) -> Self;
    fn new_flow(&self, init_cwnd: u32, mss: u32) -> Self::Flow;
}

pub trait GenericCongAvoidFlow {
    /// Return the current cwnd.
    fn curr_cwnd(&self) -> u32;
    fn curr_cwnd_bytes(&self) -> u32;
    /// If the cwnd has been overridden, this method will be called to tell the implementation
    /// about it.
    fn set_cwnd(&mut self, cwnd: u32);
    /// An congestion increase event occurred: bytes were acked without an indication of loss.
    fn increase(&mut self, m: &GenericCongAvoidMeasurements);
    /// An congestion reduction event occurred: an indication of loss was present.
    fn reduction(&mut self, m: &GenericCongAvoidMeasurements);
    /// A timeout occurred. The implementation should reset its state.
    fn reset(&mut self) {}
}

#[derive(Debug, Clone, Copy)]
pub enum ConfigReport {
    Ack,
    Rtt,
    Interval(Duration),
    Hybrid(Duration),
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

pub struct Orca<A: GenericCongAvoidAlg> {
    pub logger: Option<Logger>,
    pub server_addr: String,
    pub init_cwnd: u32,
    pub report_option: ConfigReport,
    pub report_interval: Duration,
    pub ss_thresh: u32,
    pub use_compensation: bool,
    pub alg: A,
}

impl<T: Ipc, A: GenericCongAvoidAlg> CongAlg<T> for Orca<A> {
    type Flow = OrcaFlow<T, A::Flow>;

    fn name() -> &'static str {
        "ORCA"
    }

    fn datapath_programs(&self) -> HashMap<&'static str, String> {
        let mut h = HashMap::default();
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

    fn new_flow(&self, mut control: Datapath<T>, info: DatapathInfo) -> Self::Flow {
        //check configured init_cwnd
        let init_cwnd = if self.init_cwnd != 0 {
            self.init_cwnd
        } else {
            info.init_cwnd
        };
        let mss = info.mss;
        let now = Instant::now();
        OrcaFlow::init(
            Default::default(),
            control,
            info,
            &self.server_addr,
            self.report_option,
            self.report_interval,
            init_cwnd,
            self.logger.clone(),
            now,
            self.ss_thresh,
            self.use_compensation,
            self.alg.new_flow(init_cwnd, mss),
        )
        .unwrap()
    }
}

pub struct AgentObservationData {
    avg_rtt: u32,
    min_rtt: u32,
    cnt: u32,
    delivery_rate: u64,
    pacing_rate: u64,
    loss: u32,
    srtt: u32,
    cwnd: u32,
    time_delta: Duration,
}

pub struct OrcaFlow<T: Ipc, A: GenericCongAvoidFlow> {
    sc: Scope,
    control_channel: Datapath<T>,
    info: DatapathInfo,
    client: RefCell<Option<Rc<ccp_capnp::r_l_agent::Client>>>,
    report_option: ConfigReport,
    agg_measurement: AggMeasurement,
    runtime: RefCell<tokio::runtime::Runtime>,
    local: RefCell<tokio::task::LocalSet>,
    logger: Option<Logger>,
    cwnd: u32,
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
    cwnd_tx: mpsc::UnboundedSender<u32>,
    cwnd_rx: mpsc::UnboundedReceiver<u32>,
    counts: u32,
    resp_timeout_count: u32,
}

impl<T: Ipc, A: GenericCongAvoidFlow> OrcaFlow<T, A> {
    async fn connect(&self, server_addr: &str) -> std::io::Result<ccp_capnp::r_l_agent::Client> {
        use std::net::ToSocketAddrs;
        let stream =
            match TcpStream::connect(&server_addr.to_socket_addrs().unwrap().next().unwrap()).await
            {
                Ok(stream) => stream,
                Err(e) => {
                    self.logger.as_ref().map(|log| {
                        error!(log, "In starting orca flow";
                                "sock_id" => self.info.sock_id,
                                "error" => format!("{:?}", e))
                    });
                    std::process::exit(1);
                }
            };
        let local_addr = stream.local_addr()?;
        let peer_addr = stream.peer_addr()?;
        stream.set_nodelay(true);

        self.logger.as_ref().map(|log| {
            debug!(log, "In starting orca flow";
                "sock_id" => self.info.sock_id,
                "local" => format!("{:?}", local_addr),
                "peer" => format!("{:?}", peer_addr))
        });

        let (reader, writer) = tokio_util::compat::TokioAsyncReadCompatExt::compat(stream).split();
        let network = Box::new(twoparty::VatNetwork::new(
            futures::io::BufReader::new(reader),
            futures::io::BufWriter::new(writer),
            rpc_twoparty_capnp::Side::Client,
            Default::default(),
        ));
        let mut rpc_system = RpcSystem::new(network, None);

        let client = rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);

        use futures::TryFutureExt;

        self.local
            .borrow_mut()
            .run_until(async move {
                tokio::task::spawn_local(
                    rpc_system.map_err(|e| eprintln!("RPC system error: {:?}", e)),
                );
            })
            .await;
        Ok(client)
    }

    fn init(
        sc: Scope,
        control: Datapath<T>,
        info: DatapathInfo,
        rl_server_addr: &str,
        repopt: ConfigReport,
        report_interval: Duration,
        init_cwnd: u32,
        logger: Option<Logger>,
        time: Instant,
        ss_thresh: u32,
        use_compensation: bool,
        alg: A,
    ) -> std::io::Result<OrcaFlow<T, A>> {
        let (cwnd_tx, cwnd_rx) = mpsc::unbounded_channel();
        let mut runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create Tokio runtime");
        let local = tokio::task::LocalSet::new();
        let mut s = OrcaFlow {
            sc,
            control_channel: control,
            info,
            client: RefCell::new(None),
            report_option: repopt,
            agg_measurement: AggMeasurement::new(report_interval),
            runtime: RefCell::new(runtime),
            local: RefCell::new(local),
            logger: logger.clone(),
            cwnd: init_cwnd,
            init_cwnd,
            prev_report_time: Instant::now(),
            start_timestep: time,
            pre_packet_lost: 0,
            alg,
            ss_thresh,
            use_compensation,
            deficit_timeout: 10,
            curr_cwnd_reduction: 0,
            last_cwnd_reduction: Instant::now() - Duration::from_millis(500),
            min_rtt: 0,
            slow_start_passed: false,
            // 初始化通道
            cwnd_tx,
            cwnd_rx,
            counts: 0,
            resp_timeout_count: 0,
        };

        match s.report_option {
            ConfigReport::Ack => {
                s.logger.as_ref().map(|log| {
                    info!(log, "****************Set program: Orca datapath ack");
                });
                s.sc = s.install_ack_update();
            }
            ConfigReport::Rtt => {
                s.logger.as_ref().map(|log| {
                    info!(log, "****************Set program: Orca datapath rtt");
                });
                s.sc = s.install_datapath_interval_rtt();
            }
            ConfigReport::Interval(i) => {
                s.logger.as_ref().map(|log| {
                    info!(log, "****************Set program: Orca datapath mtp");
                });
                s.sc = s.install_datapath_interval(i);
            }
            ConfigReport::Hybrid(i) => {
                s.logger.as_ref().map(|log| {
                    info!(
                        log,
                        "****************Set program: Orca datapath ack and mtp"
                    );
                });
                s.sc = s.install_datapath_hybrid(i);
            }
        }
        s.runtime.borrow().block_on(async {
            match s.connect(rl_server_addr).await {
                Ok(client) => {
                    *s.client.borrow_mut() = Some(Rc::new(client));
                }
                Err(e) => {
                    if let Some(log) = &s.logger {
                        error!(log, "Failed to connect to CCP server: {:?}", e);
                    }
                }
            }
        });
        Ok(s)
    }
}

// TODO：ORCA has slowstart

impl<T: Ipc, A: GenericCongAvoidFlow> Flow for OrcaFlow<T, A> {
    fn on_report(&mut self, sock_id: u32, m: Report) {
        while let Ok(new_cwnd) = self.cwnd_rx.try_recv() {
            self.cwnd = new_cwnd;
            self.alg.set_cwnd(self.cwnd);
            self.update(self.alg.curr_cwnd_bytes());

            if let Some(log) = &self.logger {
                debug!(log, "Applied new CWND from channel";
                    "sock_id" => sock_id,
                    "new_cwnd" => new_cwnd,
                    "cwnd_bytes" => new_cwnd * self.info.mss
                );
            }
        }
        let (report_status, mut ms) = self.agg_measurement.report(m, &self.sc);
        self.min_rtt = ms.min_rtt;

        if !self.slow_start_passed {
            self.slow_start_passed = self.ss_thresh < (self.cwnd * self.info.mss);
        }
        if report_status == ReportStatus::AckReport {
            // TODO: function as Cubic
            if ms.was_timeout {
                self.handle_timeout();
                return;
            }
            self.logger.as_ref().map(|log| {
                debug!(log, "acked before slowstart";
                    "acked" => ms.acked / self.info.mss,
                );
            });
            ms.acked = self.slow_start_increase(ms.acked);

            // increase the cwnd corresponding to new in-order cumulative ACKs
            self.alg.increase(&ms);
            let left_slow_start = self.exit_slow_start_after_congestion(&ms);
            if !left_slow_start {
                self.maybe_reduce_cwnd(&ms);
            } else {
                self.update(self.alg.curr_cwnd_bytes());
                self.logger.as_ref().map(|log| {
                    info!(log, "ORCA: leaving slow start after first congestion event";
                        "sock_id" => sock_id,
                        "loss" => ms.loss,
                        "sacked" => ms.sacked,
                        "cwnd_pkts" => self.cwnd,
                        "ss_packets" => self.ss_thresh / self.info.mss,
                        "curr_cwnd_reduction" => self.curr_cwnd_reduction,
                    );
                });
            }
            if self.curr_cwnd_reduction > 0 {
                self.logger.as_ref().map(|log| {
                    debug!(log, "Cwnd reduction in progress";
                        "curr_cwnd_reduction" => self.curr_cwnd_reduction,
                        "acked" => ms.acked / self.info.mss,
                    );
                });
                return;
            }
            // cubic update to orca
            self.cwnd = self.alg.curr_cwnd();
            self.update(self.alg.curr_cwnd_bytes());
            self.logger.as_ref().map(|log| {
                debug!(log, "Cubic update";
                    "sock_id" => sock_id,
                    "acked" => ms.acked / self.info.mss,
                    "inflight" => ms.inflight,
                    "loss" => ms.loss,
                    "ss_thresh" => self.ss_thresh,
                    "rtt" => ms.rtt,
                    "cwnd_pkts" => self.cwnd,
                    "curr_cwnd_reduction"=> self.curr_cwnd_reduction,
                );
            });
        } else if report_status == ReportStatus::NoReport {
            // Do nothing
        } else {
            if !self.slow_start_passed && (ms.loss > 0 || ms.sacked > 0) {
                if self.exit_slow_start_after_congestion(&ms) {
                    self.update(self.alg.curr_cwnd_bytes());
                    self.logger.as_ref().map(|log| {
                        info!(log, "ORCA: leaving slow start after first congestion event";
                            "sock_id" => sock_id,
                            "loss" => ms.loss,
                            "sacked" => ms.sacked,
                            "cwnd_pkts" => self.cwnd,
                            "ss_packets" => self.ss_thresh / self.info.mss,
                            "curr_cwnd_reduction" => self.curr_cwnd_reduction,
                        );
                    });
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

                let time_delta = Instant::now() - self.prev_report_time;
                self.logger.as_ref().map(|log| {
                    info!(log, "ORCA IntervalReport to agent";
                        "sock_id" => sock_id,
                        "total_reports" => self.counts,
                        "resp_timeout_actions" => self.resp_timeout_count,
                        "time_delta" => time_delta.as_millis() as f64 / 1000.0,
                    );
                });
                self.prev_report_time = Instant::now();

                let obs_data = AgentObservationData {
                    avg_rtt: ms.avg_rtt,
                    min_rtt: ms.min_rtt,
                    cnt: ms.cnt,
                    delivery_rate: ms.delivery_rate,
                    pacing_rate: ms.pacing_rate,
                    loss: loss_increment,
                    srtt: ms.srtt,
                    cwnd: self.cwnd,
                    time_delta,
                };

                let client_clone = self.client.borrow().clone().unwrap();
                let mss = self.info.mss; // 复制MSS值用于异步任务

                // 3. 使用当前线程的运行时执行RPC
                let runtime = self.runtime.get_mut();
                let mut local = self.local.get_mut();
                let rpc_result = runtime.block_on(local.run_until(async move {
                    Self::request_rpc_action(&client_clone, obs_data).await
                }));

                match rpc_result {
                    Ok(Some(new_cwnd)) if new_cwnd > 0 => {
                        let cwnd_bytes = new_cwnd * mss;
                        if let Err(e) = self.cwnd_tx.send(new_cwnd) {
                            if let Some(log) = &self.logger {
                                warn!(log, "Failed to send new CWND: {:?}", e);
                            }
                        } else {
                            if let Some(log) = &self.logger {
                                debug!(log, "Sent new CWND to channel";
                                    "cwnd_packets" => new_cwnd,
                                    "cwnd_bytes" => cwnd_bytes
                                );
                            }
                        }
                    }
                    Ok(Some(_)) => {
                        if let Some(log) = &self.logger {
                            warn!(log, "RPC returned invalid CWND");
                        }
                    }
                    Ok(None) => {
                        self.resp_timeout_count += 1;
                        if let Some(log) = &self.logger {
                            warn!(log, "ORCA: agent response timed out";
                                "sock_id" => sock_id,
                                "resp_timeout_actions" => self.resp_timeout_count,
                            );
                        }
                    }
                    Err(_) => {
                        if let Some(log) = &self.logger {
                            warn!(log, "RPC request failed");
                        }
                    }
                }
            } else {
                let ss_cwnd = self.cwnd * 1.1 as u32;
                self.cwnd = ss_cwnd;
                self.logger.as_ref().map(|log| {
                    debug!(log, "Agent slowstart update";
                        "cwnd_packets" => ss_cwnd,
                        "ss_packets" => self.ss_thresh/self.info.mss,
                        "loss" => ms.loss,
                    );
                });
                self.update(ss_cwnd * self.info.mss);
            }
        }

        self.start_timestep = Instant::now();
    }

    fn close(&mut self) {
        self.logger.as_ref().map(|log| {
            info!(log, "ORCA: flow closed";
                "sock_id" => self.info.sock_id,
                "total_reports" => self.counts,
                "final_cwnd" => self.cwnd,
                "resp_timeout_actions" => self.resp_timeout_count,
                "resp_timeout_ms" => 40,
            );
        });
    }
}

//Orca private function implementation
impl<T: Ipc, A: GenericCongAvoidFlow> OrcaFlow<T, A> {
    /// Make no updates in the datapath, and send a report after an interval
    fn install_datapath_interval(&mut self, interval: Duration) -> Scope {
        self.logger.as_ref().map(|log| {
            debug!(log, "set program: Orca datapath interval";
            "report_interval_us" => interval.as_micros() as u32
            );
        });
        self.control_channel
            .set_program(
                "OrcaDatapathInterval",
                Some(&[("ReportTime", interval.as_micros() as u32)][..]),
            )
            .unwrap()
    }

    /// Make no updates in the datapath, and send a report after each RTT
    fn install_datapath_interval_rtt(&mut self) -> Scope {
        self.control_channel
            .set_program("OrcaDatapathIntervalRTT", None)
            .unwrap()
    }

    /// Make no updates in the datapath, but send a report on every ack.
    fn install_ack_update(&mut self) -> Scope {
        self.control_channel
            .set_program("OrcaDatapathIntervalAck", None)
            .unwrap()
    }

    fn install_datapath_hybrid(&mut self, interval: Duration) -> Scope {
        self.control_channel
            .set_program("OrcaHybridDatapath", None)
            .unwrap()
    }

    fn update(&mut self, cwnd: u32) {
        let mut updates: Vec<(&str, u32)> = Vec::new();
        if cwnd > 0 {
            // self.cwnd = cwnd;
            updates.push(("Cwnd", cwnd));
        }
        let rate = match (cwnd as u64).checked_mul(2) {
            Some(cwnd_times_2) => {
                let minrtt_seconds = self.min_rtt as f64 / 1_000_000.0;
                let rate_bps = (cwnd_times_2 as f64) / minrtt_seconds;
                rate_bps as u32
            }
            None => 125000,
        };
        if rate > 0 {
            updates.push(("Rate", rate));
        }
        self.logger.as_ref().map(|log| {
            debug!(log, "update cwnd and rate in datapath";
             "cwnd_bytes" => cwnd,
             "rate" => rate);
        });
        if let Err(e) = self.control_channel.update_field(&self.sc, &updates) {
            self.logger.as_ref().map(|log| {
                error!(log, "Failed to update cwnd and rate in datapath"; "error" => format!("{:?}", e));
            });
        }
    }

    async fn request_rpc_action(
        client: &ccp_capnp::r_l_agent::Client,
        obs_data: AgentObservationData,
    ) -> Result<Option<u32>> {
        let mut req = client.get_action_request();
        let mut obs = req.get().init_observation();

        obs.set_avgrtt(obs_data.avg_rtt);
        obs.set_minrtt(obs_data.min_rtt);
        obs.set_cnt(obs_data.cnt);
        obs.set_delivery_rate(obs_data.delivery_rate);
        obs.set_pacing_rate(obs_data.pacing_rate);
        obs.set_loss(obs_data.loss);
        obs.set_srtt(obs_data.srtt);
        obs.set_snd_cwnd(obs_data.cwnd as u32);
        obs.set_time_delta(obs_data.time_delta.as_micros() as u64);
        obs.set_rpc_send_mono_ns(monotonic_raw_ns());

        let response = match timeout(Duration::from_millis(40), req.send().promise).await {
            Ok(response) => response?,
            Err(_) => return Ok(None),
        };

        let action = response.get()?.get_action()?;
        Ok(Some(action.get_cwnd()))
    }

    fn handle_timeout(&mut self) {
        self.ss_thresh /= 2;
        if self.ss_thresh < self.init_cwnd * self.info.mss {
            self.ss_thresh = self.init_cwnd * self.info.mss;
        }

        self.alg.reset();
        self.alg.set_cwnd(self.init_cwnd);
        self.curr_cwnd_reduction = 0;

        self.logger.as_ref().map(|log| {
            warn!(log,"Timeout occurred";
                "curr_cwnd_pkts" => self.init_cwnd,
                "ssthresh" => self.ss_thresh
            );
        });
        self.update(self.alg.curr_cwnd_bytes());
        self.cwnd = self.alg.curr_cwnd();
        self.slow_start_passed = false;
        return;
    }

    fn maybe_reduce_cwnd(&mut self, m: &GenericCongAvoidMeasurements) -> bool {
        let mut cwnd_reduction_update = false;
        if m.loss > 0 || m.sacked > 0 {
            if self.deficit_timeout > 0
                && ((Instant::now() - self.last_cwnd_reduction)
                    > Duration::from_millis(
                        u64::from(m.rtt).saturating_mul(u64::from(self.deficit_timeout)),
                    ))
            {
                self.curr_cwnd_reduction = 0;
            }

            // if loss indicator is nonzero
            // AND the losses in the lossy cwnd have not yet been accounted for
            // OR there is a partial ACK AND cwnd was probing ss_thresh
            if m.loss > 0 && self.curr_cwnd_reduction == 0
                || (m.acked > 0 && self.alg.curr_cwnd_bytes() == self.ss_thresh)
            {
                cwnd_reduction_update = true;
                self.alg.reduction(m);
                self.last_cwnd_reduction = Instant::now();
                self.ss_thresh = self.alg.curr_cwnd_bytes();
                self.cwnd = self.alg.curr_cwnd();
                self.slow_start_passed = true;
                self.update(self.alg.curr_cwnd_bytes());
            }

            self.curr_cwnd_reduction += m.sacked + m.loss;
        } else if m.acked < self.curr_cwnd_reduction {
            self.curr_cwnd_reduction -= (m.acked as f32 / self.info.mss as f32) as u32;
        } else {
            self.curr_cwnd_reduction = 0;
        }
        if cwnd_reduction_update {
            self.logger.as_ref().map(|log| {
                debug!(log,"In cwnd reduction";
                    "curr_cwnd_reduction" => self.curr_cwnd_reduction,
                    "ssthresh" => self.ss_thresh,
                    "loss" => m.loss,
                    "cwnd" => self.alg.curr_cwnd(),
                    "acked" => m.acked,
                );
            });
        };

        cwnd_reduction_update
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
            // increase cwnd by 1 per packet, until ssthresh
            if self.alg.curr_cwnd_bytes() + new_bytes_acked > self.ss_thresh {
                new_bytes_acked -= self.ss_thresh - self.alg.curr_cwnd_bytes();
                self.alg.set_cwnd(self.ss_thresh / self.info.mss as u32);
            } else {
                let curr_cwnd = self.alg.curr_cwnd_bytes();
                if self.use_compensation {
                    // use a compensating increase function: deliberately overshoot
                    // the "correct" update to keep account for lost throughput due to
                    // infrequent updates. Usually this doesn't matter, but it can when
                    // the window is increasing exponentially (slow start).
                    let delta = f64::from(new_bytes_acked) / (2.0_f64).ln();
                    self.alg
                        .set_cwnd((curr_cwnd + delta as u32) / self.info.mss as u32);
                // let ccp_rtt = (rtt_us + 10_000) as f64;
                // let delta = ccp_rtt * ccp_rtt / (rtt_us as f64 * rtt_us as f64);
                // self.cwnd += (new_bytes_acked as f64 * delta) as u32;
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
