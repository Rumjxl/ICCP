//! End-to-end example for migrated Portus datapath communication in Lotus.

use async_trait::async_trait;
use lotus::{
    algorithm::{CongAlg, DatapathInfo, Report},
    flow::{Flow, FlowContext},
    ipc::AsyncIpc,
    portus_datapath::PortusDatapathBridge,
    LotusConfig, LotusError, LotusRuntime, Result,
};
use portus::serialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::info;

#[derive(Clone, Default)]
struct BridgeDemoAlg;

impl CongAlg<()> for BridgeDemoAlg {
    fn name(&self) -> &'static str {
        "bridge_demo"
    }

    fn datapath_programs(&self) -> HashMap<&'static str, String> {
        let mut p = HashMap::new();
        p.insert(
            "BridgeProgram",
            r#"
            (def (Report
                (volatile sample_rtt 0)
                (volatile acked 0)
            ))
            (when true
                (:= Report.sample_rtt Flow.rtt_sample_us)
                (:= Report.acked Ack.bytes_acked)
            )
            (when (> Micros 10000)
                (report)
                (reset)
            )
            "#
            .to_string(),
        );
        p
    }

    fn new_flow(&self, _control: FlowContext<()>, _info: DatapathInfo) -> Box<dyn Flow> {
        Box::new(BridgeDemoFlow)
    }
}

struct BridgeDemoFlow;

impl Flow for BridgeDemoFlow {
    fn on_report(&mut self, sock_id: u32, report: Report) {
        info!(
            sock_id,
            rtt = report.get_field("field_0").unwrap_or_default(),
            acked = report.get_field("field_1").unwrap_or_default(),
            "received datapath measurement"
        );
    }
}

#[derive(Clone, Default)]
struct MockIpc {
    sent: Arc<Mutex<Vec<Vec<u8>>>>,
}

#[async_trait]
impl AsyncIpc<()> for MockIpc {
    async fn send(&self, msg: &[u8], _to: &()) -> Result<()> {
        self.sent.lock().expect("lock").push(msg.to_vec());
        Ok(())
    }

    async fn recv(&self, _buf: &mut [u8]) -> Result<(usize, ())> {
        Ok((0, ()))
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn name(&self) -> &'static str {
        "mock"
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let runtime = Arc::new(LotusRuntime::new(LotusConfig::default()).await?);
    runtime.register_sync_algorithm(BridgeDemoAlg).await?;

    let alg = BridgeDemoAlg;
    let bridge =
        PortusDatapathBridge::new(runtime.clone(), "bridge_demo", &alg.datapath_programs())?;

    let ipc = MockIpc::default();

    // 1) READY -> bridge sends INSTALL messages.
    let ready = serialize::serialize(&serialize::ready::Msg { id: 1 })
        .map_err(|e| LotusError::Serialization(format!("serialize READY failed: {e:?}")))?;
    bridge.handle_packet(&ipc, (), &ready).await?;

    // 2) CREATE -> bridge creates Lotus flow.
    let create = serialize::serialize(&serialize::create::Msg {
        sid: 42,
        init_cwnd: 10,
        mss: 1460,
        src_ip: 0x0a000001,
        src_port: 5000,
        dst_ip: 0x0a000002,
        dst_port: 443,
        cong_alg: Some("bridge_demo".to_string()),
    })
    .map_err(|e| LotusError::Serialization(format!("serialize CREATE failed: {e:?}")))?;
    bridge.handle_packet(&ipc, (), &create).await?;

    // 3) MEASURE -> bridge forwards report to Lotus flow.
    let measure = serialize::serialize(&serialize::measure::Msg {
        sid: 42,
        program_uid: 1,
        num_fields: 2,
        fields: vec![25_000, 1460],
    })
    .map_err(|e| LotusError::Serialization(format!("serialize MEASURE failed: {e:?}")))?;
    bridge.handle_packet(&ipc, (), &measure).await?;

    // 4) Zero-field MEASURE -> close flow.
    let close = serialize::serialize(&serialize::measure::Msg {
        sid: 42,
        program_uid: 1,
        num_fields: 0,
        fields: vec![],
    })
    .map_err(|e| LotusError::Serialization(format!("serialize CLOSE failed: {e:?}")))?;
    bridge.handle_packet(&ipc, (), &close).await?;

    info!(
        sent_control_msgs = ipc.sent.lock().expect("lock").len(),
        "bridge demo completed"
    );
    Ok(())
}
