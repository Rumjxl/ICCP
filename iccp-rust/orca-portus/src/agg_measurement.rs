use std;
use std::time::{Duration, Instant};

use portus::lang::Scope;
use portus::Report;

#[derive(Clone, PartialEq, Eq)]
pub enum ReportStatus {
    AckReport,
    IntervalReport,
    NoReport,
}

pub struct GenericCongAvoidMeasurements {
    /// In bytes.
    pub acked: u32,
    pub was_timeout: bool,
    /// In packets.
    pub sacked: u32,
    /// In packets.
    pub loss: u32,
    /// In microseconds.
    pub rtt: u32,
    /// In packets.
    pub inflight: u32,
    pub srtt: u32,
    pub min_rtt: u32,
    pub avg_rtt: u32,
    pub delivery_rate: u64,
    pub pacing_rate: u64,
    pub cnt: u32,
}

// CCP may return before the specified time. This struct will aggregate relevant
// values till the time is right
pub struct AggMeasurement {
    // In fraction of a (smoothed) RTT
    reporting_interval: Duration,
    last_report_time: u64,
    // Aggregate variables that are reset every measurement interval
    cnt: u32,
    min_rtt: u32,
    avg_rtt: u32,
    pacing_rate: u64,
}

impl AggMeasurement {
    pub fn new(reporting_interval: Duration) -> Self {
        Self {
            reporting_interval: reporting_interval,
            last_report_time: 0,
            cnt: 0,
            min_rtt: 0,
            avg_rtt: 0,
            pacing_rate: 0,
        }
    }

    pub fn report(
        &mut self,
        m: Report,
        sc: &Scope,
    ) -> (ReportStatus, GenericCongAvoidMeasurements) {
        let rtt = m
            .get_field("Report.rtt", sc)
            .expect("expected rtt field in returned measurement") as u32;

        let now = m
            .get_field("Report.now", sc)
            .expect("expected now field in returned measurement") as u64;

        let delivery_rate = m
            .get_field("Report.delivery_rate", sc)
            .expect("expected delivery_rate field in returned measurement");
        let pacing_rate = m
            .get_field("Report.pacing_rate", sc)
            .expect("expected pacing_rate field in returned measurement");
        let lost_packets =
            m.get_field("Report.loss", sc)
                .expect("expected loss field in returned measurement") as u32;
        let srtt = m
            .get_field("Report.srtt", sc)
            .expect("expected srtt field in returned measurement") as u32;
        let acked = m
            .get_field(&String::from("Report.acked"), sc)
            .expect("expected acked field in returned measurement") as u32;

        let sacked = m
            .get_field(&String::from("Report.sacked"), sc)
            .expect("expected sacked field in returned measurement") as u32;

        let timeout = m
            .get_field(&String::from("Report.timeout"), sc)
            .expect("expected timeout field in returned measurement") as u32;
        let was_timeout = if timeout == 1 { true } else { false };
        let inflight =
            m.get_field(&String::from("Report.inflight"), sc)
                .expect("expected inflight field in returned measurement") as u32;
        if rtt <= 0 {
            // TODO: skip this report
            return (
                ReportStatus::NoReport,
                GenericCongAvoidMeasurements {
                    acked: 0,
                    was_timeout: false,
                    sacked: 0,
                    loss: 0,
                    rtt: 0,
                    inflight: 0,
                    srtt: 0,
                    min_rtt: 0,
                    avg_rtt: 0,
                    delivery_rate: 0,
                    pacing_rate: 0,
                    cnt: 0,
                },
            );
        }
        if self.min_rtt == 0 || self.min_rtt > rtt {
            self.min_rtt = rtt;
        }
        if rtt > 0 {
            let mut tmp_avg = 0;
            let mut tmp_avg2 = 0;
            tmp_avg = self.cnt * self.avg_rtt + rtt;
            self.cnt += 1;
            tmp_avg2 = self.cnt;
            tmp_avg2 = tmp_avg / self.cnt;
            self.avg_rtt = tmp_avg2 as u32;
        }
        let ms = GenericCongAvoidMeasurements {
            acked: acked,
            was_timeout: was_timeout,
            sacked: sacked,
            loss: lost_packets,
            rtt: rtt,
            inflight: inflight,
            srtt: srtt,
            min_rtt: self.min_rtt,
            avg_rtt: self.avg_rtt,
            delivery_rate: delivery_rate,
            pacing_rate: pacing_rate,
            cnt: self.cnt,
        };

        let duration = now - self.last_report_time;
        // duration = 0;
        if now > 0 && duration > self.reporting_interval.as_micros() as u64 {
            self.pacing_rate = pacing_rate;
            let res = (ReportStatus::IntervalReport, ms);
            self.last_report_time = now;
            self.min_rtt = 0;
            self.avg_rtt = 0;
            self.cnt = 0;
            return res;
        } else {
            return (ReportStatus::AckReport, ms);
        }
    }
}
