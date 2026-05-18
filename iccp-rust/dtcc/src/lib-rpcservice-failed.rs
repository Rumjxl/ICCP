use tracing::{info,debug,error, warn};

use portus::ipc::Ipc;
use portus::lang::Scope;
use portus::{CongAlg, Datapath, DatapathInfo, Flow, DatapathTrait, Report, Result, Error};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use std::rc::Rc;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

use futures::{AsyncReadExt, AsyncWriteExt};
use tokio::runtime::Runtime;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::compat;

use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};

use std::net::{Ipv4Addr, SocketAddr};
// #![allow(dead_code)]
mod ccp_capnp{
    include!(concat!(env!("OUT_DIR"), "/ccp_capnp.rs"));
}

// // define report interval
// mod agg_measurement;
// use agg_measurement::{AggMeasurement, ReportStatus};
/// Configuration option: how often reports should be collected?
#[derive(Debug, Clone, Copy)]
pub enum ConfigReport {
    Ack,
    Rtt,
    Interval(Duration),
}

pub struct RpcTask {
    pub message: capnp::message::Builder<capnp::message::HeapAllocator>,
    pub respond_to: oneshot::Sender<Option<u32>>,
}

async fn rpc_task_processor(
    mut task_rx: mpsc::Receiver<RpcTask>,
    client: Arc<ccp_capnp::r_l_agent::Client>,
) {
    let mut tasks = Vec::new();
    let mut timeout = tokio::time::interval(Duration::from_millis(5));
    
    loop {
        tokio::select! {
            _ = timeout.tick() => {
                if tasks.is_empty() {
                    continue;
                }
                
                // Take tasks and process them
                let current_tasks = std::mem::take(&mut tasks);
                let responses = process_batch(&client, &current_tasks).await;
                
                for (task, action) in current_tasks.into_iter().zip(responses) {
                    if let Err(e) = task.respond_to.send(action) {
                        error!("Failed to send RPC response: {:?}", e);
                    }
                }
            }
            Some(task) = task_rx.recv() => {
                tasks.push(task);
                if tasks.len() >= 16 {
                    let current_tasks = std::mem::take(&mut tasks);
                    let responses = process_batch(&client, &current_tasks).await;
                    
                    for (task, action) in current_tasks.into_iter().zip(responses) {
                        if let Err(e) = task.respond_to.send(action) {
                            error!("Failed to send RPC response: {:?}", e);
                        }
                    }
                }
            }
        }
    }
}

async fn process_batch(
    client: &ccp_capnp::r_l_agent::Client,
    tasks: &[RpcTask],
) -> Vec<Option<u32>> {
    let mut requests = Vec::new();
    
    for task in tasks {
        let mut req = client.get_action_request();
        let reader = task.message.get_root_as_reader().unwrap();
        req.get().set_observation(reader);
        requests.push(req.send().promise);
    }
    
    let timeout_duration = Duration::from_millis(10);
    let batch_result = timeout(timeout_duration, futures::future::join_all(requests)).await;
    
    match batch_result {
        Ok(responses) => {
            responses.into_iter().map(|resp| {
                match resp {
                    Ok(response) => {
                        let action = response.get().unwrap().get_action().unwrap();
                        Some(action.get_cwnd())
                    }
                    Err(e) => {
                        error!("RPC future error: {:?}", e);
                        None
                    }
                }
            }).collect()
        }
        Err(_) => {
            warn!("RPC batch request timed out");
            vec![None; tasks.len()]
        }
    }
}

async fn create_shared_client(server_addr: &str) -> Result<ccp_capnp::r_l_agent::Client> {
    let socket_addr: SocketAddr = server_addr.parse()?;
    let stream = TcpStream::connect(&socket_addr).await?;
    stream.set_nodelay(true)?;

    let (reader, writer) = compat::TokioAsyncReadCompatExt::compat(stream).split();
    let network = Box::new(twoparty::VatNetwork::new(
        futures::io::BufReader::new(reader),
        futures::io::BufWriter::new(writer),
        rpc_twoparty_capnp::Side::Client,
        Default::default(),
    ));
    
    let mut rpc_system = RpcSystem::new(network, None);
    let client: ccp_capnp::r_l_agent::Client = rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);
    
    tokio::spawn(rpc_system.map_err(|e| error!("RPC system error: {:?}", e)));
    
    Ok(client)
}


pub struct Dtcc{
    pub server_addr: String,
    pub init_cwnd: u32,
    pub report_option: ConfigReport,
    task_tx: mpsc::Sender<RpcTask>,
    client: Arc<ccp_capnp::r_l_agent::Client>,
    runtime: Arc<Runtime>,
}

impl Default for Dtcc {
    fn default() -> Self {
        let server_addr = "127.0.0.1:4826".to_string();
        
        // 创建共享的Tokio运行时
        let mut runtime = Arc::new(tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build().expect("Failed to create Tokio runtime")
        );

        let client = runtime.block_on(async {
            match create_shared_client(&server_addr).await {
                Ok(client) => Arc::new(client),
                Err(e) => {
                    error!("Failed to connect to CCP server: {:?}", e);
                    std::process::exit(1);
                }
            }
        });
        
        let (task_tx, task_rx) = mpsc::channel(1024);
        
        {
            let client_clone = client.clone();
            runtime.spawn(async move {
                rpc_task_processor(task_rx, client_clone).await
            });
        }
        
        Self {
            server_addr,
            init_cwnd: 10,
            report_option: ConfigReport::Interval(std::time::Duration::from_millis(10)),
            task_tx,
            client,
            runtime,
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
            self.task_tx.clone(),
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
    task_tx: mpsc::Sender<RpcTask>, // 任务队列发送端
    report_option: ConfigReport,
    pending_responses: Vec<oneshot::Receiver<Option<u32>>>, // 等待中的响应
    cwnd: u32,
    init_cwnd: u32,
    prev_report_time: Instant,
    start_timestep: Instant,
    pre_bytes_sent: u64,
    counts: u32,
    min_rtt: u32
}

impl<T: Ipc> DtccFlow<T>{
    fn init(
        sc: Scope,
        control: Datapath<T>,
        info: DatapathInfo,
        task_tx: mpsc::Sender<RpcTask>,
        repopt: ConfigReport,
        init_cwnd: u32,
        time:  Instant,
    ) -> std::io::Result<Self> {
        let mut s = DtccFlow {
            sc,
            control_channel: control,
            info,
            task_tx,
            report_option: repopt,
            pending_responses: Vec::new(),
            cwnd: init_cwnd,
            init_cwnd,
            prev_report_time: Instant::now(),
            start_timestep: time,
            pre_bytes_sent: 0,
            counts: 0,
            min_rtt: 0
        };
        
        // 根据报告选项安装数据路径程序
        let sc = match repopt {
            ConfigReport::Ack => s.install_ack_update(),
            ConfigReport::Rtt => s.install_datapath_interval_rtt(),
            ConfigReport::Interval(i) => s.install_datapath_interval(i),
        };
        
        Ok(DtccFlow { sc, ..s })
    }

}

impl<T: Ipc> Flow for DtccFlow<T> {
    fn on_report(&mut self, sock_id: u32, m: Report) {
        self.counts += 1;
        
        // 1. 检查之前的响应
        let mut completed_responses = Vec::new();
        let mut new_pending = Vec::new();
        
        for mut recv in self.pending_responses.drain(..) {
            match recv.try_recv() {
                Ok(Some(cwnd)) => {
                    // 处理响应
                    completed_responses.push(cwnd);
                }
                Ok(None) => {
                    // RPC失败，使用当前cwnd
                    completed_responses.push(self.cwnd);
                }
                Err(oneshot::error::TryRecvError::Empty) => {
                    // 尚未完成，保留
                    new_pending.push(recv);
                }
                Err(oneshot::error::TryRecvError::Closed) => {
                    // 通道已关闭，忽略
                }
            }
        }
        self.pending_responses = new_pending;
        
        // 应用所有完成的响应
        if let Some(cwnd) = completed_responses.last() {
            self.apply_cwnd_update(*cwnd,sock_id);
        }
        
        // 2. 准备新的任务
        let mut message = capnp::message::Builder::new_default();
        let mut obs = message.init_root::<ccp_capnp::observation::Builder>();

        obs.set_bytes_acked(m.get_field("Report.bytes_acked", &self.sc).unwrap());
        obs.set_loss(m.get_field("Report.loss", &self.sc).unwrap());
        let rtt = m.get_field("Report.rtt", &self.sc).unwrap();
        obs.set_rtt(rtt);
        obs.set_rttvar(m.get_field("Report.rttvar", &self.sc).unwrap());
        obs.set_castate(m.get_field("Report.castate", &self.sc).unwrap());
        let minrtt = m.get_field("Report.minrtt", &self.sc).unwrap();
        self.min_rtt = minrtt as u32;
        obs.set_minrtt(minrtt);
        let rate_delivery = m.get_field("Report.rate_delivery", &self.sc).unwrap();
        obs.set_delivery_rate(rate_delivery);
        obs.set_unacked(m.get_field("Report.packets_unacked", &self.sc).unwrap());
        obs.set_snd_mss(m.get_field("Report.snd_mss", &self.sc).unwrap());
        obs.set_delivered(m.get_field("Report.packets_delivered", &self.sc).unwrap());
        
        let bytes_sent = m.get_field("Report.bytes_sent", &self.sc).unwrap();
        if let Some(result) = bytes_sent.checked_sub(self.pre_bytes_sent) {
            obs.set_bytes_sent(result);
        } else {
            obs.set_bytes_sent(0);
        }
        self.pre_bytes_sent = bytes_sent;
        
        let time_delta = Instant::now() - self.start_timestep;
        obs.set_snd_cwnd(self.cwnd as u64);
        obs.set_time_delta(time_delta.as_micros() as u64);
        
        let duration = match Instant::now() - self.prev_report_time  {
            d if d > Duration::from_secs(0) => d,
            _ => Duration::from_secs(1),
        };
        obs.set_duration(duration.as_micros() as u64);
        self.prev_report_time = Instant::now();
        
        // 3. 创建新任务
        let (resp_tx, resp_rx) = oneshot::channel();
        let task = RpcTask {
            message,
            respond_to: resp_tx,
        };
        
        if let Err(e) = self.task_tx.try_send(task) {
            error!("Failed to queue RPC task for sock {}: {:?}", sock_id, e);
        } else {
            self.pending_responses.push(resp_rx);
        }
        
        self.start_timestep = Instant::now();
    }
}

//Dtcc private function implementation
impl<T: Ipc> DtccFlow<T> {
    fn apply_cwnd_update(&mut self, cwnd_packets: u32,sock_id:u32) {
        if cwnd_packets > 0 {
            self.cwnd = cwnd_packets;
            let cwnd_bytes = cwnd_packets * self.info.mss;
            
            // 计算速率
            let minrtt = self.min_rtt;
            let rate = match (cwnd_packets as u64).checked_mul(2) {
                Some(cwnd_times_2) => {
                    let minrtt_seconds = minrtt as f64 / 1_000_000.0;
                    let rate_Bps = (cwnd_times_2 as f64) / minrtt_seconds;
                    rate_Bps as u32
                },
                None => 125000,
            };
            debug!(
                sock_id = sock_id,
                rate = rate,
                "got ack: update rate"
            );
            // 应用更新
            let updates = vec![("Cwnd", cwnd_bytes), ("Rate", rate)];
            self.control_channel.update_field(&self.sc, &updates);
        }
    }

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