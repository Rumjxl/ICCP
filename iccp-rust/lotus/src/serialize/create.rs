//! CREATE 消息（type=0）：内核新建 TCP 流时发送
//!
//! 线路格式：
//! ```text
//! [type=0:u16 | len=80:u16 | sid:u32]  <- 8字节头
//! [init_cwnd:u32 | mss:u32 | src_ip:u32 | src_port:u32 | dst_ip:u32 | dst_port:u32]  <- 24字节
//! [cong_alg: 64字节 null-padded ASCII]
//! 总长 = 8 + 24 + 64 = 96 字节（注意：portus 中 6*4+64 = 88，sid 在头部，实际 len=80）
//! ```
//!
//! portus 实际：len = HDR_LENGTH(8) + 6*4 + 64 = 96，sid 在 hdr 中不计入 payload 长度
//! ── portus get_hdr 返回 (CREATE, 8+24+64, sid) = (0, 96, sid) ──
//! 注意 portus HDR_LENGTH=8 已包含 sid，len 字段含头部。

use super::{read_u32_le, write_u32_le, AsRawMsg, RawMsg, SerializeError, HDR_LEN};

pub(crate) const CREATE: u16 = 0;

/// CREATE 消息
#[derive(Clone, Debug, PartialEq)]
pub struct Msg {
    pub sid: u32,
    pub init_cwnd: u32,
    pub mss: u32,
    pub src_ip: u32,
    pub src_port: u32,
    pub dst_ip: u32,
    pub dst_port: u32,
    /// 内核上报的拥塞算法名（最长 63 字节，null 截止）
    pub cong_alg: Option<String>,
}

/// payload 中 u32 区域：6 个 u32 = 24 字节
const U32_SECTION: usize = 6 * 4;
/// cong_alg 固定 64 字节
const ALG_BYTES: usize = 64;
/// 总 len = HDR + U32_SECTION + ALG_BYTES
const TOTAL_LEN: u16 = (HDR_LEN + U32_SECTION + ALG_BYTES) as u16;

impl AsRawMsg for Msg {
    fn get_hdr(&self) -> (u16, u16, u32) {
        (CREATE, TOTAL_LEN, self.sid)
    }

    fn get_u32s(&self, w: &mut Vec<u8>) -> std::io::Result<()> {
        write_u32_le(w, self.init_cwnd)?;
        write_u32_le(w, self.mss)?;
        write_u32_le(w, self.src_ip)?;
        write_u32_le(w, self.src_port)?;
        write_u32_le(w, self.dst_ip)?;
        write_u32_le(w, self.dst_port)?;
        Ok(())
    }

    fn get_bytes(&self, w: &mut Vec<u8>) -> std::io::Result<()> {
        let mut buf = [0u8; ALG_BYTES];
        if let Some(alg) = &self.cong_alg {
            let bytes = alg.as_bytes();
            let copy_len = bytes.len().min(ALG_BYTES - 1);
            buf[..copy_len].copy_from_slice(&bytes[..copy_len]);
        }
        w.extend_from_slice(&buf);
        Ok(())
    }
}

impl Msg {
    pub(crate) fn from_raw_msg(raw: &RawMsg) -> Result<Self, SerializeError> {
        // payload = u32s(24) + alg_bytes(64)
        let need = U32_SECTION + ALG_BYTES;
        if raw.payload.len() < need {
            return Err(SerializeError::Truncated {
                need,
                got: raw.payload.len(),
            });
        }
        let p = &raw.payload;
        let init_cwnd = read_u32_le(&p[0..4]);
        let mss = read_u32_le(&p[4..8]);
        let src_ip = read_u32_le(&p[8..12]);
        let src_port = read_u32_le(&p[12..16]);
        let dst_ip = read_u32_le(&p[16..20]);
        let dst_port = read_u32_le(&p[20..24]);

        let alg_buf = &p[24..24 + ALG_BYTES];
        let cong_alg = if alg_buf[0] == 0 {
            None
        } else {
            let end = alg_buf.iter().position(|&c| c == 0).unwrap_or(ALG_BYTES);
            Some(std::str::from_utf8(&alg_buf[..end])?.to_owned())
        };

        Ok(Msg {
            sid: raw.sid,
            init_cwnd,
            mss,
            src_ip,
            src_port,
            dst_ip,
            dst_port,
            cong_alg,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serialize::{serialize, Msg};

    fn roundtrip(m: super::Msg) -> super::Msg {
        let buf = serialize(&m).expect("serialize");
        assert_eq!(buf.len(), TOTAL_LEN as usize, "wire length mismatch");
        match Msg::from_buf(&buf).expect("from_buf") {
            (Msg::Cr(got), consumed) => {
                assert_eq!(consumed, buf.len());
                got
            }
            other => panic!("wrong variant: {:?}", other),
        }
    }

    fn base_msg() -> super::Msg {
        super::Msg {
            sid: 15,
            init_cwnd: 1448 * 10,
            mss: 1448,
            src_ip: 0,
            src_port: 4242,
            dst_ip: 0,
            dst_port: 4242,
            cong_alg: None,
        }
    }

    #[test]
    fn test_create_no_alg() {
        let m = base_msg();
        assert_eq!(roundtrip(m.clone()), m);
    }

    #[test]
    fn test_create_with_alg() {
        let mut m = base_msg();
        m.cong_alg = Some("reno".to_string());
        assert_eq!(roundtrip(m.clone()), m);
    }

    #[test]
    fn test_create_wire_type_and_len() {
        let m = base_msg();
        let buf = serialize(&m).unwrap();
        // type=0 LE
        assert_eq!(&buf[0..2], &[0, 0]);
        // len=96 LE
        assert_eq!(u16::from_le_bytes([buf[2], buf[3]]), TOTAL_LEN);
        // sid=15 LE
        assert_eq!(u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]), 15);
    }

    #[test]
    fn test_create_cong_alg_null_padded() {
        let mut m = base_msg();
        m.cong_alg = Some("cubic".to_string());
        let buf = serialize(&m).unwrap();
        // alg_bytes 从 payload 偏移 24 开始，即 buf 偏移 32
        let alg_start = HDR_LEN + U32_SECTION;
        assert_eq!(&buf[alg_start..alg_start + 5], b"cubic");
        assert_eq!(buf[alg_start + 5], 0); // null terminator
    }
}
