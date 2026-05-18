//! INSTALL 消息（type=2）：CCP → 内核，安装 fold 程序
//!
//! 线路格式：
//! ```text
//! [type=2:u16 | len:u16 | sid:u32]
//! [program_uid:u32 | num_events:u32 | num_instrs:u32]  <- 12字节
//! [instrs: (num_events + num_instrs) * 16 字节]
//! ```
//!
//! ## lotus 与 portus 的差异
//!
//! portus `install::Msg.instrs` 类型为 `lang::Bin`（依赖 fold 编译器）。
//! lotus 使用 `instrs: Vec<u8>` 存储预编译字节，解耦编译器依赖。
//!
//! `from_raw_msg` 实现了反序列化（portus 此处为 unimplemented!），
//! 用于测试往返，生产中 CCP 不会收到内核发回的 INSTALL。

use super::{read_u32_le, write_u32_le, AsRawMsg, RawMsg, SerializeError, HDR_LEN};

pub(crate) const INSTALL: u16 = 2;

/// INSTALL 消息
#[derive(Clone, Debug, PartialEq)]
pub struct Msg {
    pub sid: u32,
    pub program_uid: u32,
    pub num_events: u32,
    pub num_instrs: u32,
    /// 已编译的 fold 字节码：(num_events + num_instrs) * 16 字节
    pub instrs: Vec<u8>,
}

/// 固定 u32 区域：program_uid + num_events + num_instrs = 12 字节
const U32_SECTION: usize = 12;

impl Msg {
    /// 构造时校验 instrs 长度
    pub fn new(
        sid: u32,
        program_uid: u32,
        num_events: u32,
        num_instrs: u32,
        instrs: Vec<u8>,
    ) -> Result<Self, SerializeError> {
        let expected = (num_events + num_instrs) as usize * 16;
        if instrs.len() != expected {
            return Err(SerializeError::InvalidInstrs {
                expected,
                got: instrs.len(),
            });
        }
        Ok(Msg {
            sid,
            program_uid,
            num_events,
            num_instrs,
            instrs,
        })
    }
}

impl AsRawMsg for Msg {
    fn get_hdr(&self) -> (u16, u16, u32) {
        let len = HDR_LEN + U32_SECTION + self.instrs.len();
        (INSTALL, len as u16, self.sid)
    }

    fn get_u32s(&self, w: &mut Vec<u8>) -> std::io::Result<()> {
        write_u32_le(w, self.program_uid)?;
        write_u32_le(w, self.num_events)?;
        write_u32_le(w, self.num_instrs)?;
        Ok(())
    }

    fn get_bytes(&self, w: &mut Vec<u8>) -> std::io::Result<()> {
        w.extend_from_slice(&self.instrs);
        Ok(())
    }
}

impl Msg {
    pub(crate) fn from_raw_msg(raw: &RawMsg) -> Result<Self, SerializeError> {
        if raw.payload.len() < U32_SECTION {
            return Err(SerializeError::Truncated {
                need: U32_SECTION,
                got: raw.payload.len(),
            });
        }
        let p = &raw.payload;
        let program_uid = read_u32_le(&p[0..4]);
        let num_events = read_u32_le(&p[4..8]);
        let num_instrs = read_u32_le(&p[8..12]);

        let expected = (num_events + num_instrs) as usize * 16;
        let need = U32_SECTION + expected;
        if raw.payload.len() < need {
            return Err(SerializeError::Truncated {
                need,
                got: raw.payload.len(),
            });
        }

        let instrs = p[U32_SECTION..U32_SECTION + expected].to_vec();
        Ok(Msg {
            sid: raw.sid,
            program_uid,
            num_events,
            num_instrs,
            instrs,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{Msg, SerializeError};
    use crate::serialize::serialize;
    use crate::serialize::Msg as TopMsg;

    /// 构造最小合法 instrs：1 event + 1 instr = 2 * 16 = 32 字节
    fn minimal_instrs() -> Vec<u8> {
        vec![0xAAu8; 32]
    }

    fn roundtrip(m: Msg) -> Msg {
        let buf = serialize(&m).expect("serialize");
        match TopMsg::from_buf(&buf).expect("from_buf") {
            (TopMsg::Ins(got), consumed) => {
                assert_eq!(consumed, buf.len());
                got
            }
            other => panic!("wrong variant: {:?}", other),
        }
    }

    #[test]
    fn test_install_roundtrip() {
        let m = Msg::new(1, 7, 1, 1, minimal_instrs()).unwrap();
        assert_eq!(roundtrip(m.clone()), m);
    }

    #[test]
    fn test_install_zero_instrs() {
        // 0 events + 0 instrs，空 instrs
        let m = Msg::new(0, 0, 0, 0, vec![]).unwrap();
        assert_eq!(roundtrip(m.clone()), m);
    }

    #[test]
    fn test_install_wire_layout() {
        // 与 portus serialize_install_msg 测试对应的简化版（仅验证头部字段）
        let instrs = vec![0xBBu8; 16 * 4]; // 2 events + 2 instrs
        let m = Msg::new(1, 7, 2, 2, instrs).unwrap();
        let buf = serialize(&m).unwrap();
        // type=2
        assert_eq!(&buf[0..2], &[2, 0]);
        // len = 8 + 12 + 64 = 84
        assert_eq!(u16::from_le_bytes([buf[2], buf[3]]), 84);
        // sid=1
        assert_eq!(u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]), 1);
        // program_uid=7
        assert_eq!(u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]), 7);
        // num_events=2
        assert_eq!(u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]), 2);
        // num_instrs=2
        assert_eq!(u32::from_le_bytes([buf[16], buf[17], buf[18], buf[19]]), 2);
    }

    #[test]
    fn test_install_invalid_instrs_len() {
        let err = Msg::new(0, 0, 1, 1, vec![0u8; 10]).unwrap_err();
        match err {
            SerializeError::InvalidInstrs {
                expected: 32,
                got: 10,
            } => {}
            other => panic!("unexpected error: {:?}", other),
        }
    }
}
