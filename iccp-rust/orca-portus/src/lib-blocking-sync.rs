#[macro_use]
extern crate slog;
use slog::Logger;

use portus::ipc::Ipc;
use portus::lang::Scope;
use portus::{CongAlg, Datapath, DatapathInfo, Flow, DatapathTrait, Report,Result,Error};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use std::rc::Rc;

mod agg_measurement;
use agg_measurement::{AggMeasurement, ReportStatus,GenericCongAvoidMeasurements};

use futures::{AsyncReadExt,AsyncWriteExt};
use tokio::runtime::Runtime;
use tokio::net::TcpStream;
use tokio::time::{timeout};
use tokio_util::compat;

use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
pub const DEFAULT_SS_THRESH: u32 = 0x7fff_ffff;

pub mod cubic;
// use cubic::Cubic;

// #![allow(dead_code)]
mod ccp_capnp{
    include!(concat!(env!("OUT_DIR"), "/ccp_capnp.rs"));
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

pub struct Orca<A: GenericCongAvoidAlg> {
    pub logger: Option<Logger>,
    pub server_addr: String,
    pub init_cwnd: u32,
    pub report_option: ConfigReport,
    pub report_interval: Duration,
    pub ss_thresh: u32,
    pub use_compensation: bool,
    pub alg: A
}

impl<T: Ipc,A: GenericCongAvoidAlg> CongAlg <T> for Orca <A> {
    type Flow = OrcaFlow<T, A::Flow>;

    fn name() -> &'static str {
        "ORCA"
    }

    fn datapath_programs(&self) -> HashMap<&'static str, String> {
        let mut h = HashMap::default();
        h.insert(
            "OrcaDatapathInterval", "
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
            ".to_string(),
        );

        h.insert(
            "OrcaDatapathIntervalRTT", "
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
            ".to_string(),
        );

        h.insert(
            "OrcaDatapathIntervalAck", "
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
            ".to_string(),
        );

        h.insert(
            "OrcaHybridDatapath", "
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
            ".to_string(),
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
        ).unwrap()

    }
}

pub struct OrcaFlow<T: Ipc, A: GenericCongAvoidFlow> {
    sc: Scope,
    control_channel: Datapath<T>,
    info: DatapathInfo,
    client: Option<Rc<ccp_capnp::r_l_agent::Client>>,
    report_option: ConfigReport,
    agg_measurement: AggMeasurement,
    runtime: Rc<tokio::runtime::Runtime>,
    local: Rc<tokio::task::LocalSet>,
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
}

impl<T: Ipc, A: GenericCongAvoidFlow> OrcaFlow<T,A>{
    async fn connect(&self, server_addr: &str) -> std::io::Result<ccp_capnp::r_l_agent::Client> {
        use std::net::ToSocketAddrs;
        let stream = match TcpStream::connect(&server_addr.to_socket_addrs().unwrap().next().unwrap()).await {
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

        self.local.run_until(
            async move {
                tokio::task::spawn_local(
                    rpc_system.map_err(|e| eprintln!("RPC system error: {:?}", e))
                );
            }).await;
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
        time:  Instant,
        ss_thresh: u32,
        use_compensation: bool,
        alg: A,
    ) -> std::io::Result<OrcaFlow<T,A>> {
        let mut runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build().expect("Failed to create Tokio runtime");
        let mut local = tokio::task::LocalSet::new();
        let mut s = OrcaFlow {
            sc,
            control_channel: control,
            info,
            client: None,
            report_option: repopt,
            agg_measurement: AggMeasurement::new(report_interval),
            runtime: Rc::new(runtime),
            local: Rc::new(local),
            logger: logger.clone(),
            cwnd: init_cwnd,
            init_cwnd: init_cwnd,
            prev_report_time: Instant::now(),
            start_timestep: time,
            pre_packet_lost: 0,
            alg: alg,
            ss_thresh,
            use_compensation,
            deficit_timeout: 10,
            curr_cwnd_reduction: 0,
            last_cwnd_reduction: Instant::now() - Duration::from_millis(500),
            min_rtt: 0,
            slow_start_passed:false
        };
        match s.report_option {
            ConfigReport::Ack => {
                s.logger.as_ref().map(
                    |log| {
                        info!(log, "****************Set program: Orca datapath ack");
                    });
                s.sc = s.install_ack_update();
            }
            ConfigReport::Rtt => {
                s.logger.as_ref().map(
                    |log| {
                        info!(log, "****************Set program: Orca datapath rtt");
                    });
                s.sc = s.install_datapath_interval_rtt();
            }
            ConfigReport::Interval(i) => {
                s.logger.as_ref().map(
                    |log| {
                        info!(log, "****************Set program: Orca datapath mtp");
                    });
                s.sc = s.install_datapath_interval(i);
            }
            ConfigReport::Hybrid(i) => {
                s.logger.as_ref().map(
                    |log| {
                        info!(log, "****************Set program: Orca datapath ack and mtp");
                    });
                s.sc = s.install_datapath_hybrid(i);
            }
        }
        if s.client.is_none(){
            s.client =  match s.runtime.block_on(
                s.connect(&rl_server_addr)
            )
            {
                Ok(client) => Some(Rc::new(client)),
                Err(e) => {
                    s.logger.as_ref().map(
                        |log| {
                            error!(log, "In starting orca flow";
                            "sock_id" => s.info.sock_id,
                            "error" => format!("{:?}", e));
                        });
                    None
                }
            };
        }
        Ok(s)
    }

}

// TODO：ORCA has slowstart

impl<T: Ipc, A: GenericCongAvoidFlow> Flow for OrcaFlow<T,A> {
    fn on_report(&mut self, sock_id: u32, m: Report) {
        let (report_status, mut ms) =
        self.agg_measurement.report(m, &self.sc);
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
            self.maybe_reduce_cwnd(&ms);
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
        } else if report_status == ReportStatus::NoReport{
            // Do nothing

        } else {
            if self.slow_start_passed {
                self.logger.as_ref().map(|log| {
                    debug!(log, "Agent not slowstart update";
                        "acked" => ms.acked / self.info.mss,
                        "sacked" => ms.sacked / self.info.mss,
                        );});
                self.agent_update(sock_id, ms.avg_rtt, ms.min_rtt, ms.cnt,ms.delivery_rate,ms.pacing_rate,ms.loss,ms.srtt);
            }
            else {
                let ss_cwnd = self.cwnd * 1.1 as u32;
                self.cwnd = ss_cwnd;
                self.logger.as_ref().map(|log| {
                    debug!(log, "Agent slowstart update";
                    "cwnd_packets" => ss_cwnd,
                    "ss_packets" => self.ss_thresh/self.info.mss,
                    "loss" => ms.loss,
                );});
                self.update(ss_cwnd * self.info.mss);
            }
        }
        
        self.start_timestep = Instant::now();
    }
}

//Orca private function implementation
impl<T: Ipc, A: GenericCongAvoidFlow> OrcaFlow<T,A> {
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
            .set_program("OrcaHybridDatapath",None)
            .unwrap()
    }

    fn update(&mut self,cwnd:u32) {
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
            },
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
        if let Err(e) = self.control_channel.update_field(&self.sc, &updates)
        {
            self.logger.as_ref().map(|log| {
                error!(log, "Failed to update cwnd and rate in datapath"; "error" => format!("{:?}", e));
            });
        }
    }

    fn agent_update(&mut self, sock_id: u32, avg_rtt: u32, min_rtt: u32, cnt: u32,delivery_rate: u64,pacing_rate: u64,loss: u32,srtt: u32) {
        // JXL: down is for agent request
        let client_clone = self.client.clone();
        let client = client_clone.as_ref().unwrap();
        let local = self.local.clone();
        // ask the RL agent what it wants to do
        let mut req = client.get_action_request();
        self.logger.as_ref().map(|log| {
            debug!(log, "got ack: rpc request init";
            "sock_id" => sock_id,
            "client_socketid" => client.client.hook.get_brand(),
            );
        });

        // jxl: Report register max 15
        let mut obs = req.get().init_observation();
        // TODO:sample rtt
        // let avgrtt = m.get_field("Report.avgrtt", &self.sc).unwrap() as u32;
        obs.set_avgrtt(avg_rtt as u32);
        obs.set_minrtt(min_rtt as u32);
        obs.set_cnt(cnt as u32);
        obs.set_delivery_rate(delivery_rate);
        obs.set_pacing_rate(pacing_rate);
        // loss packets*mss =loss_bytes
        
        if let Some(result) = loss.checked_sub(self.pre_packet_lost) {
            obs.set_loss((result as u32 * self.info.mss as u32));
        } else {
            obs.set_loss(0);
        }
        self.pre_packet_lost = loss;
        obs.set_srtt(srtt);

        let time_delta = Instant::now() - self.prev_report_time;
        obs.set_snd_cwnd(self.cwnd);
        obs.set_time_delta(time_delta.as_micros() as u64);
        obs.set_rpc_send_mono_ns(monotonic_raw_ns());

        let timeout_duration = Duration::from_millis(50000);

        let response = self.runtime.block_on(async move {
            local.run_until(async move{
                let response = timeout(timeout_duration, req.send().promise).await;
                response
            }).await
        });

        match response {
            Ok(Ok(resp)) => {
                let action = resp.get().unwrap().get_action().unwrap();
                let cwnd= match action.get_cwnd() {
                    c if (c > 0) => {
                        self.cwnd = c;
                        let cwnd_bytes = c * self.info.mss;
                        self.logger.as_ref().map(|log| {
                            debug!(log, "agent got ack: update cwnd";
                            "sock_id" => sock_id,
                            "mss" => self.info.mss,
                            "minrtt" => min_rtt,
                            "cwnd_packets" => c as u32,
                            "cwnd_bytes" => Some(cwnd_bytes as u32)
                            );});
                        Some(cwnd_bytes as u32)
                        }
                    0 => Some(self.cwnd * self.info.mss),
                    _ => Some(self.cwnd * self.info.mss),
                };
               self.update(cwnd.unwrap());
               self.alg.set_cwnd(cwnd.unwrap() / self.info.mss);
            },
            Ok(Err(e)) => {
                self.logger.as_ref().map(|log| {
                    error!(log, "Failed to get action from RL agent"; "error" => format!("{:?}", e),"sock_id" => sock_id);
                });
                self.update(self.cwnd * self.info.mss);
                self.alg.set_cwnd(self.cwnd);
            },
            Err(_) => {
                self.logger.as_ref().map(|log| {
                    error!(log, "Request timed out; using default action"; "sock_id" => sock_id);
                });
                self.update(self.cwnd * self.info.mss);
                self.alg.set_cwnd(self.cwnd);
            },
        }
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

    fn maybe_reduce_cwnd(&mut self, m: &GenericCongAvoidMeasurements) {
        let mut cwnd_reduction_update = false;
        if m.loss > 0 || m.sacked > 0 {
            if self.deficit_timeout > 0
                && ((Instant::now() - self.last_cwnd_reduction)
                    > Duration::from_millis(
                        (f64::from(m.rtt) * self.deficit_timeout as f64) as _,
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
                cwnd_reduction_update =true;
                self.alg.reduction(m);
                self.last_cwnd_reduction = Instant::now();
                self.ss_thresh = self.alg.curr_cwnd_bytes();
                self.update(self.alg.curr_cwnd_bytes());
            }

            self.curr_cwnd_reduction += m.sacked + m.loss;
        } else if m.acked < self.curr_cwnd_reduction {
            self.curr_cwnd_reduction -= (m.acked as f32 / self.info.mss as f32) as u32;
        } else {
            self.curr_cwnd_reduction = 0;
        }
        if cwnd_reduction_update{
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
                    self.alg.set_cwnd((curr_cwnd + delta as u32)/self.info.mss as u32);
                // let ccp_rtt = (rtt_us + 10_000) as f64;
                // let delta = ccp_rtt * ccp_rtt / (rtt_us as f64 * rtt_us as f64);
                // self.cwnd += (new_bytes_acked as f64 * delta) as u32;
                } else {
                    self.alg.set_cwnd((curr_cwnd + new_bytes_acked)/self.info.mss as u32);
                }

                new_bytes_acked = 0
            }
        }

        new_bytes_acked
    }
}