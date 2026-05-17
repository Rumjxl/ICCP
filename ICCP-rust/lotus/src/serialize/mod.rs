//! CCP 二进制协议编解码层
//!
//! 移植自 portus/src/serialize，去除 lang::Bin / lang::Reg 依赖，
//! RawMsg 使用拥有所有权的 Vec<u8>（async 安全，无生命周期约束）。
//!
//! ## 线路格式（小端）
//!
//! ```text
//! ┌──────────┬──────────┬──────────────────┐
//! │ type:u16 │ len:u16  │   sock_id:u32     │  = 8 bytes header
//! └──────────┴──────────┴──────────────────┘
//! │          payload (len - 8 bytes)        │
//! └─────────────────────────────────────────┘
//! ```
//!
//! ## 消息类型常量
//!
//! | 类型         | type 值 | 方向        |
//! |--------------|---------|-------------|
//! | CREATE       | 0       | 内核 → CCP  |
//! | MEASURE      | 1       | 内核 → CCP  |
//! | INSTALL      | 2       | CCP → 内核  |
//! | UPDATE_FIELD | 3       | CCP → 内核  |
//! | CHANGE_PROG  | 4       | CCP → 内核  |
//! | READY        | 5       | 内核 → CCP  |

use std::io::{self, Write};

pub mod changeprog;
pub mod create;
pub mod install;
pub mod measure;
pub mod ready;
pub mod update_field;

// ── LE 辅助函数（内部使用）──────────────────────────────────────────

#[inline]
pub(crate) fn write_u16_le(w: &mut impl Write, v: u16) -> io::Result<()> {
    w.write_all(&v.to_le_bytes())
}

#[inline]
pub(crate) fn write_u32_le(w: &mut impl Write, v: u32) -> io::Result<()> {
    w.write_all(&v.to_le_bytes())
}

#[inline]
pub(crate) fn write_u64_le(w: &mut impl Write, v: u64) -> io::Result<()> {
    w.write_all(&v.to_le_bytes())
}

#[inline]
pub(crate) fn read_u16_le(buf: &[u8]) -> u16 {
    u16::from_le_bytes([buf[0], buf[1]])
}

#[inline]
pub(crate) fn read_u32_le(buf: &[u8]) -> u32 {
    u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]])
}

#[inline]
pub(crate) fn read_u64_le(buf: &[u8]) -> u64 {
    u64::from_le_bytes([
        buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
    ])
}

// ── 错误类型 ────────────────────────────────────────────────────────

/// 协议解析错误
#[derive(Debug, thiserror::Error)]
pub enum SerializeError {
    #[error("buffer too short: need at least {need} bytes, got {got}")]
    Truncated { need: usize, got: usize },
    #[error("invalid length field {len} (header claims {len} but buf has {buf_len})")]
    InvalidLength { len: usize, buf_len: usize },
    #[error("length field too small: {0} < 8 (header size)")]
    LengthTooSmall(usize),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("utf8 error: {0}")]
    Utf8(#[from] std::str::Utf8Error),
    #[error(
        "invalid install message: instrs.len()={got} but (num_events+num_instrs)*16={expected}"
    )]
    InvalidInstrs { expected: usize, got: usize },
}

/// HDR_LEN: 8字节头部
pub const HDR_LEN: usize = 8;

// ── RawMsg ───────────────────────────────────────────────────────────

/// 已解析头部、payload 拥有所有权的原始消息
#[derive(Debug, Clone, PartialEq)]
pub struct RawMsg {
    pub typ: u16,
    /// 含头部的总长度（与 portus len 字段语义一致）
    pub len: u16,
    pub sid: u32,
    /// payload = buf[8..len]
    pub payload: Vec<u8>,
}

// ── AsRawMsg trait ───────────────────────────────────────────────────

/// 可序列化的消息类型实现此 trait
pub trait AsRawMsg {
    /// 返回 (type, total_len, sock_id)
    fn get_hdr(&self) -> (u16, u16, u32);
    fn get_u32s(&self, w: &mut Vec<u8>) -> io::Result<()> {
        let _ = w;
        Ok(())
    }
    fn get_u64s(&self, w: &mut Vec<u8>) -> io::Result<()> {
        let _ = w;
        Ok(())
    }
    fn get_bytes(&self, w: &mut Vec<u8>) -> io::Result<()>;
}

// ── 序列化 ───────────────────────────────────────────────────────────

/// 将实现 AsRawMsg 的消息序列化为字节向量
pub fn serialize<T: AsRawMsg>(m: &T) -> Result<Vec<u8>, SerializeError> {
    let (typ, len, sid) = m.get_hdr();
    let mut buf = Vec::with_capacity(len as usize);
    write_u16_le(&mut buf, typ)?;
    write_u16_le(&mut buf, len)?;
    write_u32_le(&mut buf, sid)?;
    m.get_u32s(&mut buf)?;
    m.get_u64s(&mut buf)?;
    m.get_bytes(&mut buf)?;
    Ok(buf)
}

// ── 反序列化 ─────────────────────────────────────────────────────────

/// 从字节缓冲区解析一条 RawMsg，返回 (RawMsg, 消耗字节数)
pub fn deserialize(buf: &[u8]) -> Result<(RawMsg, usize), SerializeError> {
    if buf.len() < HDR_LEN {
        return Err(SerializeError::Truncated {
            need: HDR_LEN,
            got: buf.len(),
        });
    }
    let typ = read_u16_le(&buf[0..2]);
    let len = read_u16_le(&buf[2..4]) as usize;
    let sid = read_u32_le(&buf[4..8]);

    if len < HDR_LEN {
        return Err(SerializeError::LengthTooSmall(len));
    }
    if len > buf.len() {
        return Err(SerializeError::InvalidLength {
            len,
            buf_len: buf.len(),
        });
    }

    let payload = buf[HDR_LEN..len].to_vec();
    Ok((
        RawMsg {
            typ,
            len: len as u16,
            sid,
            payload,
        },
        len,
    ))
}

// ── Msg 枚举 ─────────────────────────────────────────────────────────

/// 顶层消息枚举
#[derive(Debug, PartialEq)]
pub enum Msg {
    /// READY(5)：内核启动时发送
    Rdy(ready::Msg),
    /// CREATE(0)：新流创建
    Cr(create::Msg),
    /// MEASURE(1)：统计上报；num_fields==0 表示流关闭
    Ms(measure::Msg),
    /// INSTALL(2)：CCP→内核（仅出站，不会被反序列化）
    Ins(install::Msg),
    /// UPDATE_FIELD(3)：CCP→内核（仅出站）
    Upd(update_field::Msg),
    /// CHANGE_PROG(4)：CCP→内核，切换激活的 datapath 程序
    ChProg(changeprog::Msg),
    /// 未知类型
    Other(u16),
}

impl Msg {
    /// 从字节缓冲区解析一条消息，返回 (Msg, 消耗字节数)
    pub fn from_buf(buf: &[u8]) -> Result<(Msg, usize), SerializeError> {
        let (raw, consumed) = deserialize(buf)?;
        let msg = match raw.typ {
            ready::READY => Msg::Rdy(ready::Msg::from_raw_msg(&raw)?),
            create::CREATE => Msg::Cr(create::Msg::from_raw_msg(&raw)?),
            measure::MEASURE => Msg::Ms(measure::Msg::from_raw_msg(&raw)?),
            install::INSTALL => Msg::Ins(install::Msg::from_raw_msg(&raw)?),
            update_field::UPDATE_FIELD => Msg::Upd(update_field::Msg::from_raw_msg(&raw)?),
            changeprog::CHANGE_PROG => Msg::ChProg(changeprog::Msg::from_raw_msg(&raw)?),
            other => Msg::Other(other),
        };
        Ok((msg, consumed))
    }
}

// ── 单元测试 ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_le_helpers_u16() {
        let mut buf = Vec::new();
        write_u16_le(&mut buf, 0x0203u16).unwrap();
        assert_eq!(buf, [0x03, 0x02]);
        assert_eq!(read_u16_le(&buf), 0x0203);
    }

    #[test]
    fn test_le_helpers_u32() {
        let mut buf = Vec::new();
        write_u32_le(&mut buf, 42u32).unwrap();
        assert_eq!(buf, [0x2a, 0, 0, 0]);
        assert_eq!(read_u32_le(&buf), 42);
    }

    #[test]
    fn test_le_helpers_u64() {
        let mut buf = Vec::new();
        write_u64_le(&mut buf, 42u64).unwrap();
        assert_eq!(buf, [0x2a, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(read_u64_le(&buf), 42);
    }

    #[test]
    fn test_truncated_header() {
        let buf = [0u8; 5];
        match deserialize(&buf) {
            Err(SerializeError::Truncated { need: 8, got: 5 }) => {}
            other => panic!("expected Truncated, got {:?}", other),
        }
    }

    #[test]
    fn test_length_too_small() {
        // len=4 < HDR_LEN=8
        let buf = [5u8, 0, 4, 0, 0, 0, 0, 0];
        match deserialize(&buf) {
            Err(SerializeError::LengthTooSmall(4)) => {}
            other => panic!("expected LengthTooSmall, got {:?}", other),
        }
    }

    #[test]
    fn test_invalid_length() {
        // len=100 but buf只有8字节
        let buf = [5u8, 0, 100, 0, 0, 0, 0, 0];
        match deserialize(&buf) {
            Err(SerializeError::InvalidLength {
                len: 100,
                buf_len: 8,
            }) => {}
            other => panic!("expected InvalidLength, got {:?}", other),
        }
    }
}
