//! Linux source: `net/ipv4/tcp_bbr.c`
//!
//! model of the network path:
//! ```no-run
//!    bottleneck_bandwidth = windowed_max(delivered / elapsed, 10 round trips)
//!    min_rtt = windowed_min(rtt, 10 seconds)
//! ```
//! ```no-run
//! pacing_rate = pacing_gain * bottleneck_bandwidth
//! cwnd = max(cwnd_gain * bottleneck_bandwidth * min_rtt, 4)
//! ```
//!
//! A BBR flow starts in STARTUP, and ramps up its sending rate quickly.
//! When it estimates the pipe is full, it enters DRAIN to drain the queue.
//! In steady state a BBR flow only uses `PROBE_BW` and `PROBE_RTT`.
//! A long-lived BBR flow spends the vast majority of its time remaining
//! (repeatedly) in `PROBE_BW`, fully probing and utilizing the pipe's bandwidth
//! in a fair manner, with a small, bounded queue. *If* a flow has been
//! continuously sending for the entire `min_rtt` window, and hasn't seen an RTT
//! sample that matches or decreases its `min_rtt` estimate for 10 seconds, then
//! it briefly enters `PROBE_RTT` to cut inflight to a minimum value to re-probe
//! the path's two-way propagation delay (`min_rtt`). When exiting `PROBE_RTT`, if
//! we estimated that we reached the full bw of the pipe then we enter `PROBE_BW`;
//! otherwise we enter STARTUP to try to fill the pipe.
//!
//! The goal of `PROBE_RTT` mode is to have BBR flows cooperatively and
//! periodically drain the bottleneck queue, to converge to measure the true
//! `min_rtt` (unloaded propagation delay). This allows the flows to keep queues
//! small (reducing queuing delay and packet loss) and achieve fairness among
//! BBR flows.
//!
//! The `min_rtt` filter window is 10 seconds. When the `min_rtt` estimate expires,
//! we enter `PROBE_RTT` mode and cap the cwnd at `bbr_cwnd_min_target=4` packets.
//! After at least `bbr_probe_rtt_mode_ms=200ms` and at least one packet-timed
//! round trip elapsed with that flight size <= 4, we leave `PROBE_RTT` mode and
//! re-enter the previous mode. BBR uses 200ms to approximately bound the
//! performance penalty of `PROBE_RTT`'s cwnd capping to roughly 2% (200ms/10s).
//!
//! Portus note:
//! This implementation does `PROBE_BW` and `PROBE_RTT`, but leaves as future work
//! an implementation of the finer points of other BBR implementations
//! (e.g. policing detection).

use async_trait::async_trait;
use lotus::algorithm::{AsyncCongAlg, DatapathInfo, Report};
use lotus::flow::{AsyncFlow, FlowContext};
use lotus::{LotusAlgorithm, Result};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{debug, info};

pub struct Bbr {
    control_channel: FlowContext<()>,
    info: DatapathInfo,
    probe_rtt_interval: Duration,
    bottle_rate: f64,
    bottle_rate_timeout: Instant,
    min_rtt_us: u32,
    min_rtt_timeout: Instant,
    curr_mode: BbrMode,
    mss: u32,
    init: bool,
    start: Instant,
    current_program: String,
    last_report_rx: Option<Instant>,
    last_kernel_report_ns: Option<u64>,
    full_bw: f64,
    full_bw_count: u32,
    startup_report_count: u32,
    startup_started_at: Instant,
}

enum BbrMode {
    Startup,
    ProbeBw,
    ProbeRtt,
}

pub const PROBE_RTT_INTERVAL_SECONDS: i64 = 10;
const STARTUP_GAIN: f64 = 2.885;
const PROBE_BW_CWND_GAIN: f64 = 2.0;
const PROBE_BW_UP_GAIN: f64 = 1.25;
const PROBE_BW_DOWN_GAIN: f64 = 0.75;
const FULL_BW_GROWTH_THRESH: f64 = 1.25;
const FULL_BW_COUNT_TARGET: u32 = 3;
const STARTUP_MIN_DURATION: Duration = Duration::from_secs(2);
const STARTUP_MIN_REPORTS: u32 = 16;
const MIN_CWND_PKTS: u64 = 4;
const INITIAL_BW_BYTES_PER_SEC: f64 = 125_000.0;

impl Default for BbrConfig {
    fn default() -> Self {
        BbrConfig {
            probe_rtt_interval: Duration::from_secs(PROBE_RTT_INTERVAL_SECONDS as u64),
        }
    }
}

#[derive(Clone)]
pub struct BbrConfig {
    pub probe_rtt_interval: Duration,
    // TODO make more things configurable
}

impl Bbr {
    fn min_cwnd_bytes(&self) -> u64 {
        MIN_CWND_PKTS * u64::from(self.mss)
    }

    fn target_cwnd(&self, gain: f64) -> u64 {
        let min_rtt_us = u64::from(self.min_rtt_us.max(1));
        let cwnd = (self.bottle_rate * gain * min_rtt_us as f64 / 1e6) as u64;
        cwnd.max(self.min_cwnd_bytes())
    }

    fn init_rate_from_cwnd(&self, min_rtt_us: u32) -> f64 {
        let min_rtt_us = min_rtt_us.max(1);
        f64::from(self.info.init_cwnd) * 1e6 / f64::from(min_rtt_us)
    }

    fn startup_rate(&self) -> u64 {
        (self.bottle_rate * STARTUP_GAIN) as u64
    }

    fn startup_cwnd(&self) -> u64 {
        self.target_cwnd(STARTUP_GAIN)
    }

    async fn install_update(&self, update: &[(&str, u64)]) -> Result<()> {
        self.control_channel
            .update_field_by_name(&self.info, &self.current_program, update)
            .await
    }

    async fn install_startup(&mut self) -> Result<()> {
        let startup_rate = self.startup_rate();
        let startup_cwnd = self.startup_cwnd();
        self.startup_started_at = Instant::now();
        self.startup_report_count = 0;
        self.full_bw = 0.0;
        self.full_bw_count = 0;

        info!(
            sockid = self.info.sock_id,
            cwnd = startup_cwnd,
            startup_rate_Mbps = startup_rate as f64 / 125_000.0,
            bottle_rate_Mbps = self.bottle_rate / 125_000.0,
            min_rtt_us = self.min_rtt_us,
            "switching to STARTUP"
        );

        self.install_update(&[("Cwnd", startup_cwnd), ("Rate", startup_rate)])
            .await?;
        self.control_channel
            .set_program_by_name(
                &self.info,
                "startup",
                &[("startupRate", startup_rate), ("startupCwnd", startup_cwnd)],
            )
            .await?;
        self.current_program = "startup".to_string();
        Ok(())
    }

    async fn replace_startup_rate(&self) -> Result<()> {
        let startup_rate = self.startup_rate();
        let startup_cwnd = self.startup_cwnd();
        self.install_update(&[
            ("startupRate", startup_rate),
            ("startupCwnd", startup_cwnd),
            ("Rate", startup_rate),
            ("Cwnd", startup_cwnd),
        ])
        .await?;
        debug!(
            sockid = self.info.sock_id,
            cwnd = startup_cwnd,
            startup_rate_Mbps = startup_rate as f64 / 125_000.0,
            bottle_rate_Mbps = self.bottle_rate / 125_000.0,
            "STARTUP: updating rate"
        );
        Ok(())
    }

    // replaces the variables in the probe bw program if the bottle rate or min_rtt changes
    async fn replace_probe_bw_rate(&self) -> Result<()> {
        let three_fourths_rate = (self.bottle_rate * PROBE_BW_DOWN_GAIN) as u64;
        let rate = self.bottle_rate as u64;
        let five_fourths_rate = (self.bottle_rate * PROBE_BW_UP_GAIN) as u64;
        let cwnd_cap = self.target_cwnd(PROBE_BW_CWND_GAIN);
        self.install_update(&[
            ("bottleRate", rate),
            ("threeFourthsRate", three_fourths_rate),
            ("fiveFourthsRate", five_fourths_rate),
            ("cwndCap", cwnd_cap),
            ("Cwnd", cwnd_cap),
        ])
        .await?;
        debug!(
            sockid = self.info.sock_id,
            cwnd = cwnd_cap,
            down_rate = three_fourths_rate as f64 / 125_000.0,
            bottle_rate = self.bottle_rate / 125_000.0,
            up_rate = five_fourths_rate as f64 / 125_000.0,
            "PROBE_BW: updating rate"
        );
        Ok(())
    }

    async fn install_probe_bw(&mut self) -> Result<()> {
        // first, install the rate and cwnd for state 0 for state 0
        let min_rtt = self.min_rtt_us as u64;
        let three_fourths_rate = (self.bottle_rate * PROBE_BW_DOWN_GAIN) as u64;
        let rate = self.bottle_rate as u64;
        let five_fourths_rate = (self.bottle_rate * PROBE_BW_UP_GAIN) as u64;
        let cwnd_cap = self.target_cwnd(PROBE_BW_CWND_GAIN);

        info!(
            sockid = self.info.sock_id,
            cwnd = cwnd_cap,
            down_rate = three_fourths_rate as f64 / 125_000.0,
            bottle_rate_Mbps = self.bottle_rate / 125_000.0,
            up_rate = five_fourths_rate as f64 / 125_000.0,
            min_rtt_us = min_rtt,
            "switching to PROBE_BW"
        );

        self.install_update(&[("Cwnd", cwnd_cap), ("Rate", five_fourths_rate)])
            .await?;
        self.control_channel
            .set_program_by_name(
                &self.info,
                "probe_bw",
                &[
                    ("cwndCap", cwnd_cap),
                    ("bottleRate", rate),
                    ("threeFourthsRate", three_fourths_rate),
                    ("fiveFourthsRate", five_fourths_rate),
                ],
            )
            .await?;
        self.current_program = "probe_bw".to_string();
        Ok(())
    }

    fn get_probe_bw_fields(&mut self, m: &Report) -> Option<(u32, u32, f64, u32)> {
        let rtt = m.get_field("Report.minrtt")? as u32;
        let loss = m.get_field("Report.loss")? as u32;
        let rate = m.get_field("Report.rate")? as f64;
        let state = m.get_field("Report.pulseState")? as u32;
        Some((loss, rtt, rate, state))
    }

    fn get_probe_minrtt(&mut self, m: &Report) -> u32 {
        m.get_field("Report.minrtt").unwrap_or(u64::MAX) as u32
    }
}

impl LotusAlgorithm for BbrConfig {
    type SyncImpl = Self;
    type AsyncImpl = Self;

    fn name(&self) -> &'static str {
        "bbr"
    }
}

#[async_trait]
impl AsyncCongAlg<()> for BbrConfig {
    fn name(&self) -> &'static str {
        "bbr"
    }

    async fn datapath_programs(&self) -> HashMap<&'static str, String> {
        vec![
            (
                "init_program",
                String::from(
                    "
                (def
                    (Report 
                        (volatile loss 0)
                        (minrtt +infinity)
                        (volatile rate 0) 
                        (pulseState 0)
                    )
                )
                (when true
                    (:= Report.loss (+ Report.loss Ack.lost_pkts_sample))
                    (:= Report.minrtt (min Report.minrtt Flow.rtt_sample_us))
                    (:= Report.rate (max Report.rate Flow.rate_delivery))
                    (:= Report.pulseState 5)
                    (fallthrough)
                )
                (when (> Micros Report.minrtt)
                    (report)
                )
            ",
                ),
            ),
            (
                "startup",
                String::from(
                    "
                (def
                    (Report
                        (volatile loss 0)
                        (volatile minrtt +infinity)
                        (volatile rate 0)
                        (pulseState 9)
                    )
                    (startupRate 0)
                    (startupCwnd 0)
                )
                (when true
                    (:= Report.loss (+ Report.loss Ack.lost_pkts_sample))
                    (:= Report.minrtt (min Report.minrtt Flow.rtt_sample_us))
                    (:= Report.rate (max Report.rate Flow.rate_delivery))
                    (fallthrough)
                )
                (when (> Micros Report.minrtt)
                    (:= Rate startupRate)
                    (:= Cwnd startupCwnd)
                    (:= Micros 0)
                    (report)
                )
            ",
                ),
            ),
            (
                "probe_rtt",
                String::from(
                    "
		(def 
		    (Report (volatile minrtt +infinity))
		    (volatile target_inflight_reached 0)
		)
		(when true
		    (:= Report.minrtt (min Report.minrtt Flow.rtt_sample_us))
		    (fallthrough)
		)
		(when (&& (== target_inflight_reached 0)
			  (|| (< Flow.packets_in_flight 4) (== Flow.packets_in_flight 4)))
		    (:= target_inflight_reached 1)
		    (:= Micros 0)
		)
		(when (&& (== target_inflight_reached 1) 
		          (&& (> Micros Flow.rtt_sample_us) (> Micros 200000))
                      )
                    (:= Micros 0)
		    (report)
		)
            ",
                ),
            ),
            (
                "probe_bw",
                String::from(
                    "
                (def
                    (Report 
                        (volatile loss 0)
                        (volatile minrtt +infinity)
                        (volatile rate 0) 
                        (pulseState 0)
                    )
                    (pulseState 0)
                    (cwndCap 0)
                    (bottleRate 0)
                    (threeFourthsRate 0)
                    (fiveFourthsRate 0)
                )
                (when true
                    (:= Report.loss (+ Report.loss Ack.lost_pkts_sample))
                    (:= Report.minrtt (min Report.minrtt Flow.rtt_sample_us))
                    (:= Report.pulseState pulseState)
                    (:= Report.rate (max Report.rate Flow.rate_delivery))
                    (fallthrough)
                )
                (when (&& (> Micros Report.minrtt) (== pulseState 0))
                    (:= Rate threeFourthsRate)
                    (:= pulseState 1)
                    (report)
                )
                (when (&& (> Micros (* Report.minrtt 2)) (== pulseState 1))
                    (:= Rate bottleRate)
                    (:= pulseState 2)
                    (report)
                )
                (when (&& (> Micros (* Report.minrtt 8)) (== pulseState 2))
                    (:= pulseState 0)
                    (:= Cwnd cwndCap)
                    (:= Rate fiveFourthsRate)
                    (:= Micros 0)
                    (report)
                )
	    ",
                ),
            ),
        ]
        .into_iter()
        .collect()
    }

    async fn new_flow(
        &self,
        control: FlowContext<()>,
        info: DatapathInfo,
    ) -> Result<Box<dyn AsyncFlow>> {
        let now = std::time::Instant::now();
        let s = Bbr {
            control_channel: control,
            info: info.clone(),
            probe_rtt_interval: self.probe_rtt_interval,
            bottle_rate: INITIAL_BW_BYTES_PER_SEC,
            bottle_rate_timeout: now + self.probe_rtt_interval,
            min_rtt_us: 1_000_000,
            min_rtt_timeout: now + self.probe_rtt_interval,
            curr_mode: BbrMode::Startup,
            mss: info.mss,
            init: true,
            start: now,
            current_program: "init_program".to_string(),
            last_report_rx: None,
            last_kernel_report_ns: None,
            full_bw: 0.0,
            full_bw_count: 0,
            startup_report_count: 0,
            startup_started_at: now,
        };
        info!(
            sock_id = info.sock_id,
            src_ip = info.src_ip,
            src_port = info.src_port,
            dst_ip = info.dst_ip,
            dst_port = info.dst_port,
            "Bbr: new flow"
        );

        s.control_channel
            .set_program_by_name(&info, "init_program", &[("Cwnd", info.init_cwnd as u64)])
            .await?;
        Ok(Box::new(s))
    }
}

#[async_trait::async_trait]
impl AsyncFlow for Bbr {
    async fn on_report(&mut self, sock_id: u32, m: Report) -> Result<()> {
        let now = std::time::Instant::now();
        let report_rx = m.timestamp;
        let kernel_report_ns = m
            .get_field("kernel_report_time_ns")
            .or_else(|| m.get_field("Report.kernel_report_time_ns"));
        let user_report_gap_us = self
            .last_report_rx
            .map(|last_rx| report_rx.duration_since(last_rx).as_micros() as u64);
        let kernel_report_gap_us = match (self.last_kernel_report_ns, kernel_report_ns) {
            (Some(last_kernel_ns), Some(current_kernel_ns)) => current_kernel_ns
                .checked_sub(last_kernel_ns)
                .map(|gap_ns| gap_ns / 1_000),
            _ => None,
        };
        let gap_delta_us = match (user_report_gap_us, kernel_report_gap_us) {
            (Some(user_gap), Some(kernel_gap)) => Some(user_gap as i64 - kernel_gap as i64),
            _ => None,
        };

        debug!(
            sock_id = sock_id,
            report_rx_elapsed_us = report_rx.duration_since(self.start).as_micros() as u64,
            on_report_queue_us = now.duration_since(report_rx).as_micros() as u64,
            kernel_report_time_ns = kernel_report_ns.unwrap_or(0),
            user_report_gap_us = user_report_gap_us.unwrap_or(0),
            kernel_report_gap_us = kernel_report_gap_us.unwrap_or(0),
            user_minus_kernel_gap_us = gap_delta_us.unwrap_or(0),
            "CCP report timing"
        );
        self.last_report_rx = Some(report_rx);
        if let Some(kernel_report_ns) = kernel_report_ns {
            self.last_kernel_report_ns = Some(kernel_report_ns);
        }

        match self.curr_mode {
            BbrMode::Startup => {
                let fields = self.get_probe_bw_fields(&m);
                if fields.is_none() {
                    return Ok(());
                }

                let (_loss, minrtt, measured_rate, _state) = fields.unwrap();
                if minrtt < self.min_rtt_us {
                    self.min_rtt_us = minrtt;
                    self.min_rtt_timeout = now + self.probe_rtt_interval;
                }

                if self.init {
                    self.bottle_rate = measured_rate
                        .max(self.init_rate_from_cwnd(minrtt))
                        .max(INITIAL_BW_BYTES_PER_SEC);
                    self.bottle_rate_timeout = now + self.probe_rtt_interval;
                    self.install_startup().await?;
                    self.init = false;
                    return Ok(());
                }

                let mut rate_increased = false;
                if self.bottle_rate < measured_rate {
                    self.bottle_rate = measured_rate;
                    self.bottle_rate_timeout = now + self.probe_rtt_interval;
                    rate_increased = true;
                }

                self.startup_report_count += 1;
                if measured_rate > 0.0 {
                    if self.full_bw == 0.0 || measured_rate >= self.full_bw * FULL_BW_GROWTH_THRESH
                    {
                        self.full_bw = measured_rate;
                        self.full_bw_count = 0;
                    } else {
                        self.full_bw_count += 1;
                    }
                }

                debug!(
                    sockid = self.info.sock_id,
                    measured_rate_Mbps = measured_rate / 125_000.0,
                    bottle_rate_Mbps = self.bottle_rate / 125_000.0,
                    full_bw_Mbps = self.full_bw / 125_000.0,
                    full_bw_count = self.full_bw_count,
                    startup_reports = self.startup_report_count,
                    startup_elapsed_ms =
                        now.duration_since(self.startup_started_at).as_millis() as u64,
                    min_rtt_us = self.min_rtt_us,
                    "STARTUP"
                );

                let startup_can_exit = now.duration_since(self.startup_started_at)
                    >= STARTUP_MIN_DURATION
                    && self.startup_report_count >= STARTUP_MIN_REPORTS;
                if self.full_bw_count >= FULL_BW_COUNT_TARGET && startup_can_exit {
                    self.install_probe_bw().await?;
                    self.curr_mode = BbrMode::ProbeBw;
                    return Ok(());
                }

                if rate_increased {
                    self.replace_startup_rate().await?;
                }
            }
            BbrMode::ProbeRtt => {
                self.min_rtt_us = self.get_probe_minrtt(&m);
                self.min_rtt_timeout = now + self.probe_rtt_interval;

                self.install_probe_bw().await?;
                self.curr_mode = BbrMode::ProbeBw;

                debug!(min_rtt_us = self.min_rtt_us, "PROBE_RTT");
            }
            BbrMode::ProbeBw => {
                let fields = self.get_probe_bw_fields(&m);
                if fields.is_none() {
                    return Ok(());
                }

                let (_loss, minrtt, rate, _state) = fields.unwrap();
                let elapsed = now - self.start;
                debug!(
                    elapsed_s = elapsed.as_secs_f32(),
                    rate_Mbps = rate / 125_000.0,
                    bottle_rate_Mbps = self.bottle_rate / 125_000.0,
                    "probe_bw"
                );

                // reset probe rtt counter and update cwnd cap
                if minrtt < self.min_rtt_us {
                    // datapath automatically uses minrtt for when condition (non volatile),
                    // this isn't reset, so no need to install again
                    self.min_rtt_us = minrtt;
                    self.min_rtt_timeout = now + self.probe_rtt_interval;
                    debug!(
                        min_rtt_us = self.min_rtt_us,
                        bottle_rate_Mbps = self.bottle_rate / 125_000.0,
                        "new min_rtt"
                    );

                    if !(self.init) {
                        // probe bw program is installed
                        self.install_update(&[
                            ("cwndCap", self.target_cwnd(PROBE_BW_CWND_GAIN)), // reinstall cwnd cap value
                        ])
                        .await?;
                    }
                }

                if now > self.min_rtt_timeout {
                    self.curr_mode = BbrMode::ProbeRtt;
                    debug!(
                        min_rtt_us = self.min_rtt_us,
                        bottle_rate_Mbps = self.bottle_rate / 125_000.0,
                        "switching to PROBE_RTT"
                    );

                    self.min_rtt_us = 0x3fff_ffff;
                    self.control_channel
                        .set_program_by_name(&self.info, "probe_rtt", &[])
                        .await?;
                    self.current_program = "probe_rtt".to_string();
                    self.install_update(&[("Cwnd", (4 * self.mss) as u64)])
                        .await?;
                    return Ok(());
                }

                if self.bottle_rate < rate {
                    self.bottle_rate = rate;
                    self.bottle_rate_timeout = now + self.probe_rtt_interval;
                    // restart the pulse state
                    // here, we must reinstall the program for substitution with the correct values
                    if !(self.init) {
                        self.replace_probe_bw_rate().await?;
                    }
                }

                if self.init {
                    debug!("new_flow");
                    self.install_probe_bw().await?;
                    self.init = false;
                }
            }
        }
        Ok(())
    }
}
