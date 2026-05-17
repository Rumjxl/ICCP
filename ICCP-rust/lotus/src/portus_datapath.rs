//! Portus datapath communication bridge for Lotus.
//!
//! This module ports the critical datapath communication path from `portus/src`:
//! - message parsing (`Ready`, `Create`, `Measure`)
//! - control message encoding (`Install`, `ChangeProg`, `UpdateField`)
//! - flow lifecycle dispatch (`create_flow`, `handle_report`, `close_flow`)

use crate::{
    algorithm::DatapathInfo, algorithm::Report, flow::FlowId, ipc::AsyncIpc, LotusError,
    LotusRuntime, Result,
};
use portus::lang::{self, Reg, Scope};
use portus::serialize;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Compile a datapath program source to Portus bin/scope.
pub fn compile_datapath_program(
    source: &str,
    updates: &[(&str, u32)],
) -> Result<(portus::lang::Bin, Scope)> {
    lang::compile(source.as_bytes(), updates)
        .map_err(|e| LotusError::Serialization(format!("compile datapath program failed: {e}")))
}

fn resolve_mutable_fields(scope: &Scope, fields: &[(&str, u32)]) -> Result<Vec<(Reg, u64)>> {
    fields
        .iter()
        .map(|(field_name, value)| {
            if field_name.starts_with("__") {
                return Err(LotusError::Serialization(format!(
                    "cannot update reserved field: {field_name}"
                )));
            }

            let reg = scope
                .get(field_name)
                .ok_or_else(|| LotusError::Serialization(format!("unknown field: {field_name}")))?;

            match reg {
                Reg::Control(idx, typ, volatile) => Ok((
                    Reg::Control(*idx, typ.clone(), *volatile),
                    u64::from(*value),
                )),
                Reg::Implicit(idx, typ) if *idx == 4 || *idx == 5 => {
                    Ok((Reg::Implicit(*idx, typ.clone()), u64::from(*value)))
                }
                _ => Err(LotusError::Serialization(format!(
                    "field is not mutable from CCP side: {field_name}"
                ))),
            }
        })
        .collect()
}

/// Build a serialized `INSTALL` control message.
pub fn build_install_message(
    sock_id: u32,
    scope: &Scope,
    bin: &portus::lang::Bin,
) -> Result<Vec<u8>> {
    let msg = serialize::install::Msg {
        sid: sock_id,
        program_uid: scope.program_uid,
        num_events: bin.events.len() as u32,
        num_instrs: bin.instrs.len() as u32,
        instrs: bin.clone(),
    };

    serialize::serialize(&msg)
        .map_err(|e| LotusError::Serialization(format!("serialize install message failed: {e:?}")))
}

/// Build a serialized `CHANGEPROG` message, optionally with field updates.
pub fn build_change_program_message(
    sock_id: u32,
    scope: &Scope,
    fields: &[(&str, u32)],
) -> Result<Vec<u8>> {
    let fields = resolve_mutable_fields(scope, fields)?;
    let msg = serialize::changeprog::Msg {
        sid: sock_id,
        program_uid: scope.program_uid,
        num_fields: fields.len() as u32,
        fields,
    };

    serialize::serialize(&msg).map_err(|e| {
        LotusError::Serialization(format!("serialize changeprog message failed: {e:?}"))
    })
}

/// Build a serialized `UPDATE_FIELD` message.
pub fn build_update_field_message(
    sock_id: u32,
    scope: &Scope,
    fields: &[(&str, u32)],
) -> Result<Vec<u8>> {
    let fields = resolve_mutable_fields(scope, fields)?;
    let msg = serialize::update_field::Msg {
        sid: sock_id,
        num_fields: fields.len() as u8,
        fields,
    };

    serialize::serialize(&msg).map_err(|e| {
        LotusError::Serialization(format!("serialize update_field message failed: {e:?}"))
    })
}

/// Send a control message over Lotus async IPC.
pub async fn send_control_message<Addr>(
    sender: &dyn AsyncIpc<Addr>,
    to: &Addr,
    payload: &[u8],
) -> Result<()>
where
    Addr: Send + Sync + 'static,
{
    sender
        .send(payload, to)
        .await
        .map_err(|e| LotusError::Ipc(format!("send control message failed: {e}")))
}

/// Synchronous wrapper for sending control messages from Portus-compatible sync APIs.
pub fn send_control_message_blocking<Addr>(
    sender: &dyn AsyncIpc<Addr>,
    to: Addr,
    payload: Vec<u8>,
) -> Result<()>
where
    Addr: Send + Sync + 'static,
{
    let fut = async move { sender.send(&payload, &to).await };

    match Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(fut))
            .map_err(|e| LotusError::Ipc(format!("send control message failed: {e}"))),
        Err(_) => futures::executor::block_on(fut)
            .map_err(|e| LotusError::Ipc(format!("send control message failed: {e}"))),
    }
}

/// Bridge that processes Portus datapath packets and dispatches into Lotus runtime.
///
/// This mirrors the critical `run_inner()` control flow from Portus:
/// - `Ready`: install all datapath programs
/// - `Create`: create flow in runtime
/// - `Measure`: route report or close flow
pub struct PortusDatapathBridge<Addr>
where
    Addr: Clone + Eq + Hash + Send + Sync + 'static,
{
    runtime: Arc<LotusRuntime>,
    default_algorithm: String,
    install_payloads: Vec<Vec<u8>>,
    flow_map: RwLock<HashMap<(Addr, u32), FlowId>>,
}

impl<Addr> PortusDatapathBridge<Addr>
where
    Addr: Clone + Eq + Hash + Send + Sync + 'static,
{
    /// Pre-compiles all datapath programs and prepares install payloads.
    pub fn new(
        runtime: Arc<LotusRuntime>,
        default_algorithm: impl Into<String>,
        datapath_programs: &HashMap<&'static str, String>,
    ) -> Result<Self> {
        let mut install_payloads = Vec::new();

        for source in datapath_programs.values() {
            let (bin, scope) = compile_datapath_program(source, &[])?;
            install_payloads.push(build_install_message(0, &scope, &bin)?);
        }

        Ok(Self {
            runtime,
            default_algorithm: default_algorithm.into(),
            install_payloads,
            flow_map: RwLock::new(HashMap::new()),
        })
    }

    /// Parse and process one incoming packet that may contain multiple CCP messages.
    pub async fn handle_packet(
        &self,
        sender: &dyn AsyncIpc<Addr>,
        from: Addr,
        packet: &[u8],
    ) -> Result<()> {
        let mut offset = 0usize;

        while offset < packet.len() {
            let (msg, consumed) = serialize::Msg::from_buf(&packet[offset..]).map_err(|e| {
                LotusError::Serialization(format!("deserialize datapath message failed: {e:?}"))
            })?;

            if consumed == 0 {
                break;
            }
            offset += consumed;

            match msg {
                serialize::Msg::Rdy(_) => {
                    info!("received READY; installing datapath programs");
                    for payload in &self.install_payloads {
                        send_control_message(sender, &from, payload).await?;
                    }
                }
                serialize::Msg::Cr(c) => {
                    let algorithm_name = c
                        .cong_alg
                        .as_deref()
                        .filter(|s| !s.is_empty())
                        .unwrap_or(&self.default_algorithm)
                        .to_string();

                    let info = DatapathInfo {
                        sock_id: c.sid,
                        init_cwnd: c.init_cwnd,
                        mss: c.mss,
                        src_ip: c.src_ip,
                        src_port: (c.src_port as u16),
                        dst_ip: c.dst_ip,
                        dst_port: (c.dst_port as u16),
                        programs: std::collections::HashMap::new(),
                        scopes: std::collections::HashMap::new(),
                        report_fields: std::collections::HashMap::new(),
                    };

                    let flow_id = self
                        .runtime
                        .create_flow(c.sid, &algorithm_name, info)
                        .await
                        .map_err(|e| LotusError::Flow(format!("create flow failed: {e}")))?;

                    self.flow_map
                        .write()
                        .await
                        .insert((from.clone(), c.sid), flow_id);
                    info!(sock_id = c.sid, algorithm = %algorithm_name, "created flow from CREATE message");
                }
                serialize::Msg::Ms(m) => {
                    let flow_id = {
                        let map = self.flow_map.read().await;
                        map.get(&(from.clone(), m.sid)).copied()
                    };

                    let Some(flow_id) = flow_id else {
                        debug!(sock_id = m.sid, "measurement for unknown flow; skipping");
                        continue;
                    };

                    if m.num_fields == 0 {
                        self.runtime
                            .close_flow(flow_id)
                            .await
                            .map_err(|e| LotusError::Flow(format!("close flow failed: {e}")))?;
                        self.flow_map.write().await.remove(&(from.clone(), m.sid));
                        continue;
                    }

                    let mut fields = HashMap::new();
                    fields.insert("program_uid".to_string(), m.program_uid as u64);
                    for (idx, value) in m.fields.iter().enumerate() {
                        fields.insert(format!("field_{idx}"), *value);
                    }

                    self.runtime
                        .handle_report(
                            flow_id,
                            m.sid,
                            Report {
                                fields,
                                timestamp: std::time::Instant::now(),
                            },
                        )
                        .await
                        .map_err(|e| LotusError::Flow(format!("handle report failed: {e}")))?;
                }
                serialize::Msg::Ins(_) => {
                    warn!("unexpected INSTALL from datapath side; ignoring");
                }
                serialize::Msg::Other(other) => {
                    debug!(
                        msg_type = other.typ,
                        sid = other.sid,
                        "received unknown datapath message"
                    );
                }
            }
        }

        Ok(())
    }
}
