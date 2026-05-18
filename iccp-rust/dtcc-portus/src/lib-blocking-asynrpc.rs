use tracing::{info,debug,error, warn};

use portus::ipc::Ipc;
use portus::lang::Scope;
use portus::{CongAlg, Datapath, DatapathInfo, Flow, DatapathTrait, Report,Result};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use futures::{AsyncReadExt, TryFutureExt};
use tokio::net::TcpStream;
use tokio::time::{timeout};
use tokio::sync::mpsc;
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};

use std::net::Ipv4Addr;

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

/// Configuration option: how often reports should be collected?
#[derive(Debug, Clone, Copy)]
pub enum ConfigReport {
    Ack,
    Rtt,
    Interval(Duration),
}

#[derive(Debug, Clone)]
pub struct Dtcc{
    pub server_addr: String,
    pub init_cwnd: u32,
    pub report_option: ConfigReport,
    pub rpc_timeout: Duration,
}

#[derive(Debug)]
pub struct ObservationData {
    bytes_acked: u64,
    loss: u64,
    rtt: u64,
    rttvar: u64,
    castate: u64,
    minrtt: u64,
    rate_delivery: u64,
    packets_unacked: u64,
    snd_mss: u64,
    packets_delivered: u64,
    bytes_sent: u64,
    time_delta: u64,
    duration: u64,
    current_cwnd: u64,
}

impl Default for Dtcc {
    fn default() -> Self {
        Self {
            server_addr: "127.0.0.1:4826".to_string(),
            init_cwnd:10,
            report_option: ConfigReport::Interval(std::time::Duration::from_millis(10)),
            rpc_timeout: Duration::from_millis(20),
        }
    }
}

impl<T: Ipc> CongAlg<T> for Dtcc {
    type Flow = DtccFlow<T>;

    fn name() -> &'static str {
        "DTCC"
    }

    fn datapath_programs(&self) -> HashMap<&'static str, String> {
        let mut h = HashMap::default();
        h.insert(
            "DtccDatapathInterval", "
                (def
                    (Report
                        (volatile bytes_acked 0)
                        (volatile loss 0)
                        (volatile rtt 0)
                        (volatile rttvar 0)
                        (volatile castate 0)
                        (volatile minrtt +infinity)
                        (volatile rate_delivery 0)
                        (volatile packets_unacked 0)
                        (volatile snd_mss 0)
                        (volatile packets_delivered 0)
                        (volatile bytes_sent 0)
                    )
                    (ReportTime 0)
                )
                (when true
                    (:= Report.bytes_acked (+ Report.bytes_acked Ack.bytes_acked))
                    
                    (:= Report.loss Ack.lost_pkts_sample)
                    (:= Report.rtt Flow.rtt_sample_us)
                    (:= Report.rttvar Flow.rttvar)
                    (:= Report.castate Flow.castate)
                    (:= Report.minrtt Flow.min_rtt)
                    (:= Report.rate_delivery Flow.rate_delivery)
                    (:= Report.packets_unacked Flow.packets_in_flight)
                    (:= Report.snd_mss Flow.snd_mss)
                    (:= Report.packets_delivered Flow.packets_delivered)
                    (:= Report.bytes_sent Flow.bytes_sent)
                    

                    (fallthrough)
                )
                (when (> Micros ReportTime)
                    (report)
                    (:= Micros 0)
                )
            ".to_string(),
        );

        h.insert(
            "DtccDatapathIntervalRTT", "
                (def
                    (Report
                        (volatile bytes_acked 0)
                        (volatile loss 0)
                        (volatile rtt 0)
                        (volatile rttvar 0)
                        (volatile castate 0)
                        (volatile minrtt +infinity)
                        (volatile rate_delivery 0)
                        (volatile packets_unacked 0)
                        (volatile snd_mss 0)
                        (volatile packets_delivered 0)
                        (volatile bytes_sent 0)
                    )
                )
                (when true
                    (:= Report.bytes_acked (+ Report.bytes_acked Ack.bytes_acked))

                    (:= Report.loss Ack.lost_pkts_sample)
                    (:= Report.rtt Flow.rtt_sample_us)
                    (:= Report.rttvar Flow.rttvar)
                    (:= Report.castate Flow.castate)
                    (:= Report.minrtt Flow.min_rtt)
                    (:= Report.rate_delivery Flow.rate_delivery)
                    (:= Report.packets_unacked Flow.packets_unacked)
                    (:= Report.snd_mss Flow.snd_mss)
                    (:= Report.packets_delivered Flow.packets_delivered)
                    (:= Report.bytes_sent Flow.bytes_sent)

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
            "DtccDatapathIntervalAck", "
                (def
                    (Report
                        (volatile bytes_acked 0)
                        (volatile loss 0)
                        (volatile rtt 0)
                        (volatile rttvar 0)
                        (volatile castate 0)
                        (volatile minrtt +infinity)
                        (volatile rate_delivery 0)
                        (volatile packets_unacked 0)
                        (volatile snd_mss 0)
                        (volatile packets_delivered 0)
                        (volatile bytes_sent 0)
                    )
                )
                (when true
                    (:= Report.bytes_acked (+ Report.bytes_acked Ack.bytes_acked))

                    (:= Report.loss Ack.lost_pkts_sample)
                    (:= Report.rtt Flow.rtt_sample_us)
                    (:= Report.rttvar Flow.rttvar)
                    (:= Report.castate Flow.castate)
                    (:= Report.minrtt Flow.min_rtt)
                    (:= Report.rate_delivery Flow.rate_delivery)
                    (:= Report.packets_unacked Flow.packets_unacked)
                    (:= Report.snd_mss Flow.snd_mss)
                    (:= Report.packets_delivered Flow.packets_delivered)
                    (:= Report.bytes_sent Flow.bytes_sent)

                    (report)
                )
            ".to_string(),
        );

        h
    }

    fn new_flow(&self, mut control: Datapath<T>, info: DatapathInfo) -> Self::Flow {
        //check configured init_cwnd
        //In dtcc:self.init_cwnd
        let init_cwnd = if self.init_cwnd != 0 {
            self.init_cwnd
        } else {
            info.init_cwnd
        };

        debug!(
            sock_id = info.sock_id,
            src_ip_u32 = info.src_ip,
            src_ip = Ipv4Addr::from(info.src_ip.to_be()).to_string(),
            src_port = info.src_port,
            dst_ip_u32 = info.dst_ip,
            dst_ip = Ipv4Addr::from(info.dst_ip.to_be()).to_string(),
            dst_port = info.dst_port,
            "dtcc flow init, Little-Endian host byte order"
        );

        let now = Instant::now();
        DtccFlow::init(
            Default::default(),
            control,
            info,
            &self.server_addr,
            self.report_option,
            init_cwnd,
            self.rpc_timeout,
            now,
        ).unwrap()

    }
}

pub struct DtccFlow<T: Ipc> {
    sc: Scope,
    control_channel: Datapath<T>,
    info: DatapathInfo,
    report_option: ConfigReport,
    cwnd: u32,
    init_cwnd: u32,
    prev_report_time: Instant,
    start_timestep: Instant,
    pre_bytes_sent: u64,
    counts: u32,
    valid_actions: u32,
    min_rtt: u64,
    obs_tx: mpsc::UnboundedSender<ObservationData>,
    cwnd_rx: mpsc::UnboundedReceiver<u32>,
}

async fn connect_rpc(server_addr: &str, sock_id: u32) -> std::io::Result<ccp_capnp::r_l_agent::Client> {
    use std::net::ToSocketAddrs;
    let stream = match TcpStream::connect(&server_addr.to_socket_addrs().unwrap().next().unwrap()).await {
        Ok(stream) => stream,
        Err(e) => {
            error!(sock_id, "Failed to connect to RL server: {:?}", e);
            return Err(e);
        }
    };
    let local_addr = stream.local_addr()?;
    let peer_addr = stream.peer_addr()?;
    let _ = stream.set_nodelay(true);

    debug!(
        local = format!("{:?}", local_addr),
        peer = format!("{:?}", peer_addr),
        sock_id,
        "Connected to CCP server"
    );

    let (reader, writer) = tokio_util::compat::TokioAsyncReadCompatExt::compat(stream).split();
    let network = Box::new(twoparty::VatNetwork::new(
        futures::io::BufReader::new(reader),
        futures::io::BufWriter::new(writer),
        rpc_twoparty_capnp::Side::Client,
        Default::default(),
    ));
    let mut rpc_system = RpcSystem::new(network, None);
    let client = rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);

    tokio::task::spawn_local(rpc_system.map_err(|e| error!("RPC system error: {:?}", e)));

    Ok(client)
}

async fn request_rpc_action(
    client: &ccp_capnp::r_l_agent::Client,
    data: ObservationData,
    rpc_timeout: Duration,
) -> Result<u32> {
    let mut req = client.get_action_request();
    let mut obs = req.get().init_observation();

    obs.set_bytes_acked(data.bytes_acked);
    obs.set_loss(data.loss);
    obs.set_rtt(data.rtt);
    obs.set_rttvar(data.rttvar);
    obs.set_castate(data.castate);
    obs.set_minrtt(data.minrtt);
    obs.set_delivery_rate(data.rate_delivery);
    obs.set_unacked(data.packets_unacked);
    obs.set_snd_mss(data.snd_mss);
    obs.set_delivered(data.packets_delivered);
    obs.set_bytes_sent(data.bytes_sent);
    obs.set_time_delta(data.time_delta);
    obs.set_duration(data.duration);
        obs.set_snd_cwnd(data.current_cwnd as u64);
        obs.set_rpc_send_mono_ns(monotonic_raw_ns());

        let response = timeout(
        rpc_timeout,
        req.send().promise
    ).await??;

    let action = response.get()?.get_action()?;
    Ok(action.get_cwnd())
}

fn run_rpc_background(
    sock_id: u32,
    server_addr: String,
    mut obs_rx: mpsc::UnboundedReceiver<ObservationData>,
    cwnd_tx: mpsc::UnboundedSender<u32>,
    rpc_timeout: Duration,
) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime");
    let local = tokio::task::LocalSet::new();

    runtime.block_on(local.run_until(async move {
        let client = match connect_rpc(&server_addr, sock_id).await {
            Ok(c) => c,
            Err(e) => {
                error!(sock_id, "Failed to connect to RL server: {:?}", e);
                return;
            }
        };

        while let Some(obs_data) = obs_rx.recv().await {
            let mut latest = obs_data;
            while let Ok(newer) = obs_rx.try_recv() {
                latest = newer;
            }
            match request_rpc_action(&client, latest, rpc_timeout).await {
                Ok(new_cwnd) if new_cwnd > 0 => {
                    if let Err(e) = cwnd_tx.send(new_cwnd) {
                        debug!(sock_id, "Failed to send cwnd update: {:?}", e);
                        break;
                    }
                }
                Err(e) => warn!(sock_id, "RPC request failed: {:?}", e),
                _ => {}
            }
        }
    }));
}

impl<T: Ipc> DtccFlow<T>{
    fn init(
        sc: Scope,
        control: Datapath<T>,
        info: DatapathInfo,
        rl_server_addr: &str,
        repopt: ConfigReport,
        init_cwnd: u32,
        rpc_timeout: Duration,
        time:  Instant,
    ) -> std::io::Result<DtccFlow<T>> {
        let (obs_tx, obs_rx) = mpsc::unbounded_channel::<ObservationData>();
        let (cwnd_tx, cwnd_rx) = mpsc::unbounded_channel::<u32>();

        let server_addr = rl_server_addr.to_string();
        let sock_id = info.sock_id;
        std::thread::spawn(move || {
            run_rpc_background(sock_id, server_addr, obs_rx, cwnd_tx, rpc_timeout);
        });

        let mut s = DtccFlow {
            sc,
            control_channel: control,
            info,
            report_option: repopt,
            cwnd: init_cwnd,
            init_cwnd,
            prev_report_time: Instant::now(),
            start_timestep: time,
            pre_bytes_sent: 0,
            counts: 0,
            valid_actions: 0,
            min_rtt: 0,
            obs_tx,
            cwnd_rx,
        };

        match s.report_option {
            ConfigReport::Ack => {
                s.sc = s.install_ack_update();
            }
            ConfigReport::Rtt => {
                s.sc = s.install_datapath_interval_rtt();
            }
            ConfigReport::Interval(i) => {
                s.sc = s.install_datapath_interval(i);
            }
        }

        Ok(s)
    }

}

impl<T: Ipc> Flow for DtccFlow<T> {
    fn on_report(&mut self, _sock_id: u32, m: Report) {
        while let Ok(new_cwnd) = self.cwnd_rx.try_recv() {
            self.cwnd = new_cwnd;
            self.valid_actions += 1;
            let updates = self.compute_cwnd_and_rate_updates();
            self.control_channel.update_field(&self.sc, &updates);
        }

        self.counts += 1;
        debug!(
            report_count = self.counts,
            "Get report"
        );

        let obs_data = self.prepare_observation_data(&m);

        if let Err(e) = self.obs_tx.send(obs_data) {
            debug!("Failed to send observation: {:?}", e);
        }

        self.start_timestep = Instant::now();
    }

    fn close(&mut self) {
        info!(
            sock_id = self.info.sock_id,
            total_reports = self.counts,
            valid_actions = self.valid_actions,
            "Flow closing"
        );
    }
}

//Dtcc private function implementation
impl<T: Ipc> DtccFlow<T> {
    fn prepare_observation_data(&mut self, m: &Report) -> ObservationData {
        let bytes_acked = m.get_field("Report.bytes_acked", &self.sc).unwrap();
        let loss = m.get_field("Report.loss", &self.sc).unwrap();
        let rtt = m.get_field("Report.rtt", &self.sc).unwrap();
        let rttvar = m.get_field("Report.rttvar", &self.sc).unwrap();
        let castate = m.get_field("Report.castate", &self.sc).unwrap();
        let minrtt = m.get_field("Report.minrtt", &self.sc).unwrap();
        self.min_rtt = minrtt;
        let rate_delivery = m.get_field("Report.rate_delivery", &self.sc).unwrap();
        let packets_unacked = m.get_field("Report.packets_unacked", &self.sc).unwrap();
        let snd_mss = m.get_field("Report.snd_mss", &self.sc).unwrap();
        let packets_delivered = m.get_field("Report.packets_delivered", &self.sc).unwrap();
        
        let bytes_sent = {
            let total_bytes = m.get_field("Report.bytes_sent", &self.sc).unwrap();
            let bytes_sent = total_bytes - self.pre_bytes_sent;
            self.pre_bytes_sent = total_bytes;
            bytes_sent
        };
        
        let time_delta = (Instant::now() - self.start_timestep).as_micros() as u64;
        info!(counts = self.counts, duration_ms = time_delta as f64 / 1000.0, "DTCC: on_report");
        ObservationData {
            bytes_acked,
            loss,
            rtt,
            rttvar,
            castate,
            minrtt,
            rate_delivery,
            packets_unacked,
            snd_mss,
            packets_delivered,
            bytes_sent,
            time_delta,
            duration: self.prev_report_time.elapsed().as_micros() as u64,
            current_cwnd: self.cwnd as u64,
        }
    }
    
    fn compute_cwnd_and_rate_updates(&self) -> Vec<(&str, u32)> {
        let cwnd_bytes = self.cwnd * self.info.mss;
        let mut updates = vec![("Cwnd", cwnd_bytes)];
        
        let minrtt_seconds = self.min_rtt as f64 / 1_000_000.0;
        if minrtt_seconds > 0.0 {
            let rate = match (self.cwnd as u64).checked_mul(2) {
                Some(cwnd_times_2) => {
                    let rate_Bps = (cwnd_times_2 as f64) / minrtt_seconds;
                    rate_Bps as u32
                },
                None => 125000,
            };
            updates.push(("Rate", rate as u32));
        }
        updates
    }
    
    /// Make no updates in the datapath, and send a report after an interval
    fn install_datapath_interval(&mut self, interval: Duration) -> Scope {
        info!(
            report_interval_us = interval.as_micros(),
            "Installed interval-based reporting"
        );
        self.control_channel
            .set_program(
                "DtccDatapathInterval",
                Some(&[("ReportTime", interval.as_micros() as u32)][..]),
            )
            .unwrap()
    }

    /// Make no updates in the datapath, and send a report after each RTT
    fn install_datapath_interval_rtt(&mut self) -> Scope {
        self.control_channel
            .set_program("DtccDatapathIntervalRTT", None)
            .unwrap()
    }

    /// Make no updates in the datapath, but send a report on every ack.
    fn install_ack_update(&mut self) -> Scope {
        self.control_channel
            .set_program("DtccDatapathIntervalAck", None)
            .unwrap()
    }
}