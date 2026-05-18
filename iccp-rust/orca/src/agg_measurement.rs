use std::time::Duration;

use lotus::algorithm::Report;

#[derive(Clone, PartialEq, Eq)]
pub enum ReportStatus {
    AckReport,
    IntervalReport,
    NoReport,
}

pub struct GenericCongAvoidMeasurements {
    pub acked: u32,
    pub was_timeout: bool,
    pub sacked: u32,
    pub loss: u32,
    pub rtt: u32,
    pub inflight: u32,
    pub srtt: u32,
    pub min_rtt: u32,
    pub avg_rtt: u32,
    pub delivery_rate: u64,
    pub pacing_rate: u64,
    pub cnt: u32,
}

pub struct AggMeasurement {
    reporting_interval: Duration,
    last_report_time: u64,
    cnt: u32,
    min_rtt: u32,
    avg_rtt: u32,
    pacing_rate: u64,
}

impl AggMeasurement {
    pub fn new(reporting_interval: Duration) -> Self {
        Self {
            reporting_interval,
            last_report_time: 0,
            cnt: 0,
            min_rtt: 0,
            avg_rtt: 0,
            pacing_rate: 0,
        }
    }

    pub fn report(&mut self, m: &Report) -> (ReportStatus, GenericCongAvoidMeasurements) {
        let rtt = m.get_field("Report.rtt").unwrap_or(0) as u32;
        let now = m.get_field("Report.now").unwrap_or(0) as u64;
        let delivery_rate = m.get_field("Report.delivery_rate").unwrap_or(0);
        let pacing_rate = m.get_field("Report.pacing_rate").unwrap_or(0);
        let lost_packets = m.get_field("Report.loss").unwrap_or(0) as u32;
        let srtt = m.get_field("Report.srtt").unwrap_or(0) as u32;
        let acked = m.get_field("Report.acked").unwrap_or(0) as u32;
        let sacked = m.get_field("Report.sacked").unwrap_or(0) as u32;
        let timeout = m.get_field("Report.timeout").unwrap_or(0) as u32;
        let was_timeout = timeout == 1;
        let inflight = m.get_field("Report.inflight").unwrap_or(0) as u32;

        if rtt == 0 {
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
            let tmp_avg = self.cnt * self.avg_rtt + rtt;
            self.cnt += 1;
            self.avg_rtt = tmp_avg / self.cnt;
        }

        let ms = GenericCongAvoidMeasurements {
            acked,
            was_timeout,
            sacked,
            loss: lost_packets,
            rtt,
            inflight,
            srtt,
            min_rtt: self.min_rtt,
            avg_rtt: self.avg_rtt,
            delivery_rate,
            pacing_rate,
            cnt: self.cnt,
        };

        let duration = now.wrapping_sub(self.last_report_time);
        if now > 0 && duration > self.reporting_interval.as_micros() as u64 {
            self.pacing_rate = pacing_rate;
            self.last_report_time = now;
            self.min_rtt = 0;
            self.avg_rtt = 0;
            self.cnt = 0;
            return (ReportStatus::IntervalReport, ms);
        } else {
            return (ReportStatus::AckReport, ms);
        }
    }
}
