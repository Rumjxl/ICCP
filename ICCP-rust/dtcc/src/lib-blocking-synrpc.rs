use tracing::{info,debug,error, warn};

use portus::ipc::Ipc;
use portus::lang::Scope;
use portus::{CongAlg, Datapath, DatapathInfo, Flow, DatapathTrait, Report,Result,Error};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use std::rc::Rc;

use futures::{AsyncReadExt,AsyncWriteExt};
use tokio::runtime::Runtime;
use tokio::net::TcpStream;
use tokio::time::{timeout};
use tokio_util::compat;

use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};

use std::net::{Ipv4Addr, SocketAddr};
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

pub struct Dtcc{
    pub server_addr: String,
    pub init_cwnd: u32,
    pub report_option: ConfigReport,
}

impl Default for Dtcc {
    fn default() -> Self {
        Self {
            server_addr: "127.0.0.1:4826".to_string(),
            init_cwnd:10,
            report_option: ConfigReport::Interval(std::time::Duration::from_millis(10)),
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
            now,
        ).unwrap()

    }
}

pub struct DtccFlow<T: Ipc> {
    sc: Scope,
    control_channel: Datapath<T>,
    info: DatapathInfo,
    client: Option<Rc<ccp_capnp::r_l_agent::Client>>,
    report_option: ConfigReport,
    runtime: Rc<tokio::runtime::Runtime>,
    local: Rc<tokio::task::LocalSet>,
    cwnd: u32,
    init_cwnd: u32,
    prev_report_time: Instant,
    start_timestep: Instant,
    pre_bytes_sent: u64,
    counts: u32
}

impl<T: Ipc> DtccFlow<T>{
    async fn connect(&self, server_addr: &str) -> std::io::Result<ccp_capnp::r_l_agent::Client> {
        use std::net::ToSocketAddrs;
        let stream = match TcpStream::connect(&server_addr.to_socket_addrs().unwrap().next().unwrap()).await {
                    Ok(stream) => stream,
                    Err(e) => {
                        error!(
                            "Failed to connect to server: {:?}",
                            e
                        );
                        std::process::exit(1);
                    }
                };
        let local_addr = stream.local_addr()?;
        let peer_addr = stream.peer_addr()?;
        stream.set_nodelay(true);

        debug!(
            local = format!("{:?}", local_addr),
            peer = format!("{:?}", peer_addr),
            sock_id = self.info.sock_id,
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
        init_cwnd: u32,
        time:  Instant,
    ) -> std::io::Result<DtccFlow<T>> {
        let mut runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build().expect("Failed to create Tokio runtime");
        let mut local = tokio::task::LocalSet::new();
        let mut s = DtccFlow {
            sc,
            control_channel: control,
            info,
            client: None,
            report_option: repopt,
            runtime: Rc::new(runtime),
            local: Rc::new(local),
            cwnd: init_cwnd,
            init_cwnd: init_cwnd,
            prev_report_time: Instant::now(),
            start_timestep: time,
            pre_bytes_sent: 0,
            counts: 0
        };
        if s.client.is_none(){
            s.client =  match s.runtime.block_on(
                s.connect(&rl_server_addr)
            )
            {
                Ok(client) => Some(Rc::new(client)),
                Err(e) => {
                    error!(
                        "Failed to initialize CCP connection: {:?}",
                        e
                    );
                    None
                }
            };
        }
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
    fn on_report(&mut self, sock_id: u32, m: Report) {
        self.counts = self.counts + 1;
        let client_clone = self.client.clone();
        let client = client_clone.as_ref().unwrap();
        let local = self.local.clone();
        // ask the RL agent what it wants to do
        let mut req = client.get_action_request();
        
        info!(
        sock_id = sock_id,
        counts = self.counts,
        "got ack: rpc request init"
        );

        // jxl: Report register max 15
        let mut obs = req.get().init_observation();

        obs.set_bytes_acked(m.get_field("Report.bytes_acked", &self.sc).unwrap());
        // obs.set_bytes_misordered(m.get_field("Report.bytes_misordered", &self.sc).unwrap());
        // obs.set_ecn_bytes(m.get_field("Report.ecn_bytes", &self.sc).unwrap());
        // obs.set_packets_acked(m.get_field("Report.packets_acked", &self.sc).unwrap());
        // obs.set_packets_misordered(m.get_field("Report.packets_misordered", &self.sc).unwrap());
        // obs.set_ecn_packets(m.get_field("Report.ecn_packets", &self.sc).unwrap());
        obs.set_loss(m.get_field("Report.loss", &self.sc).unwrap());
        // obs.set_timeout(m.get_field("Report.timeout", &self.sc).unwrap() != 0);
        // obs.set_bytes_in_flight(m.get_field("Report.bytes_in_flight", &self.sc).unwrap());
        // obs.set_packets_in_flight(m.get_field("Report.packets_in_flight", &self.sc).unwrap());
        // obs.set_bytes_pending(m.get_field("Report.bytes_pending", &self.sc).unwrap());
        let rtt = m.get_field("Report.rtt", &self.sc).unwrap();
        obs.set_rtt(rtt);
        // obs.set_rin(m.get_field("Report.rin", &self.sc).unwrap());
        // obs.set_rout(m.get_field("Report.rout", &self.sc).unwrap());
        obs.set_rttvar(m.get_field("Report.rttvar", &self.sc).unwrap());
        obs.set_castate(m.get_field("Report.castate", &self.sc).unwrap());
        let minrtt = m.get_field("Report.minrtt", &self.sc).unwrap();
        obs.set_minrtt(minrtt);
        let rate_delivery = m.get_field("Report.rate_delivery", &self.sc).unwrap();
        obs.set_delivery_rate(rate_delivery);
        obs.set_unacked(m.get_field("Report.packets_unacked", &self.sc).unwrap());
        obs.set_snd_mss(m.get_field("Report.snd_mss", &self.sc).unwrap());
        obs.set_delivered(m.get_field("Report.packets_delivered", &self.sc).unwrap());
        
        let bytes_sent = m.get_field("Report.bytes_sent", &self.sc).unwrap();
        // obs.set_bytes_sent(bytes_sent - self.pre_bytes_sent as u64);
        if let Some(result) = bytes_sent.checked_sub(self.pre_bytes_sent as u64) {
            obs.set_bytes_sent(result as u64);
        } else {
            obs.set_bytes_sent(0 as u64);
        }
        self.pre_bytes_sent = bytes_sent;
        let time_delta = Instant::now() - self.start_timestep;
        obs.set_snd_cwnd(self.cwnd as u64);
        if rtt > 0 {
            obs.set_time_delta(time_delta.as_micros() as u64);
        }
        else {
            if m.get_field("ReportTime", &self.sc).unwrap() != 0 {
                obs.set_time_delta(m.get_field("ReportTime", &self.sc).unwrap()*1000);
            }
        }

        let duration = match Instant::now() - self.prev_report_time  {
            d if d > Duration::from_secs(0) => d,
            _ => Duration::from_secs(1),
        };
        obs.set_duration(duration.as_micros() as u64);
        obs.set_rpc_send_mono_ns(monotonic_raw_ns());

        self.prev_report_time =Instant::now();
        let timeout_duration = Duration::from_millis(10); // 设置超时时间为50毫秒,应该少于reportinterval？

        let response = self.runtime.block_on(async move {
            local.run_until(async move{
                let response = timeout(timeout_duration, req.send().promise).await;
                response
            }).await
        });

        debug!(
            sock_id = sock_id,
            rtt = rtt,
            delivery_rate = rate_delivery,
            bytes_sent = bytes_sent,
            pre_bytes_sent = self.pre_bytes_sent,
            "Received report"
        );
        match response {
            Ok(Ok(resp)) => {
                let action = resp.get().unwrap().get_action().unwrap();
                let cwnd= match action.get_cwnd() {
                    c if (c > 0) => {
                        self.cwnd = c;
                        let cwnd_bytes = c * self.info.mss;
                        info!(
                            sock_id = sock_id,
                            mss = self.info.mss,
                            minrtt = minrtt,
                            cwnd_packets = c,
                            cwnd_bytes = cwnd_bytes,
                            "Updated congestion window"
                        );
                        Some(cwnd_bytes as u32)
                        }
                    0 => Some(self.cwnd * self.info.mss),
                    _ => Some(self.cwnd * self.info.mss),
                };
               
                let mut updates: Vec<(&str, u32)> = Vec::new();

                if let Some(cwnd_value) = cwnd {
                    updates.push(("Cwnd", cwnd_value));
                    
                    // 计算 rate，确保不溢出
                    let rate = match (cwnd_value as u64).checked_mul(2) {
                        Some(cwnd_times_2) => {
                            // 将 minrtt 从微秒转换为秒，即除以 1,000,000
                            let minrtt_seconds = minrtt as f64 / 1_000_000.0;
                            // 计算 bits per second (bps)
                            let rate_Bps = (cwnd_times_2 as f64) / minrtt_seconds; 
                            // 将结果转换为 u32，并确保不溢出
                            rate_Bps as u32
                        },
                        None => 125000, // 如果溢出，则设置 rate 为 0
                    };
                    
                    debug!(
                        sock_id = sock_id,
                        rate = rate,
                        "got ack: update rate"
                    );
                    updates.push(("Rate", rate));
                }

                // 更新字段
                self.control_channel.update_field(&self.sc, &updates);
                
                // let updates: Vec<(&str, u32)> = cwnd
                //     .into_iter()
                //     .map(|c| ("Cwnd", c))
                //     .collect();

                // self.control_channel.update_field(&self.sc, &updates);
            },
            Ok(Err(e)) => {
                // Handle the error, log it, or take appropriate action
                error!(
                    sock_id = sock_id,
                    "Failed to get RL action: {:?}",
                    e
                );
            },
            Err(_) => {
                error!(
                    sock_id = sock_id,
                    "Request timed out, using default action"
                );
                let default_cwnd = self.cwnd * self.info.mss; 
                let updates: Vec<(&str, u32)> = vec![("Cwnd", default_cwnd as u32)];
                self.control_channel.update_field(&self.sc, &updates);
            },
        }
        
        self.start_timestep = Instant::now();
    }
}

//Dtcc private function implementation
impl<T: Ipc> DtccFlow<T> {
    /// Make no updates in the datapath, and send a report after an interval
    fn install_datapath_interval(&mut self, interval: Duration) -> Scope {
        debug!(
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