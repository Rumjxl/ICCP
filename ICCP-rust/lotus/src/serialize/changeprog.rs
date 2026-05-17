//! CHANGE_PROG 消息（type=4）：CCP → 内核，切换当前激活的 datapath 程序
//!
//! 内核收到此消息后：
//!   1. 根据 `program_uid` 在已安装的程序表中查找；
//!   2. 将该流的 `staged_program_index` 设为查找到的下标；
//!   3. 下次 `ccp_invoke` 时将 `staged_program_index` 应用为 `program_index`；
//!   4. 可选地用 `fields` 覆盖程序寄存器的初始值（如 `ReportTime`）。
//!
//! ## 线路格式（小端，与 portus changeprog 完全兼容）
//!
//! ```text
//! [type=4:u16 | len:u16 | sid:u32]          <- 8字节 header
//! [program_uid:u32 | num_fields:u32]         <- 8字节
//! [(reg_type:u8 | reg_index:u32 | value:u64) * num_fields]  <- 每条13字节
//! ```
//!
//! `reg_type` 对应 portus `Reg` tag byte：
//! - `Reg::Implicit(idx, _)` → tag = 2
//! - `Reg::Control(idx, _, _)` → tag = 4  
//! - `Reg::Primitive(idx, _)` → tag = 0
//! - `Reg::Report(idx, _)` → tag = 1
//!
//! 对于 `ReportTime`（Implicit register index 4），使用 reg_type=2, reg_index=4。

use super::{
    read_u32_le, read_u64_le, write_u32_le, write_u64_le, AsRawMsg, RawMsg, SerializeError, HDR_LEN,
};

pub(crate) const CHANGE_PROG: u16 = 4;

/// 每条 field 的字节大小：reg_type(1) + reg_index(4) + value(8) = 13 字节
const FIELD_SIZE: usize = 13;

/// CHANGE_PROG 消息
#[derive(Clone, Debug, PartialEq)]
pub struct Msg {
    /// 目标连接的 socket ID（对应内核 `ccp_connection->index`）
    pub sid: u32,
    /// 要切换到的程序的 UID（由编译时分配，`portus::lang::Scope.program_uid`）
    pub program_uid: u32,
    /// 可选的寄存器更新列表 `(reg_type, reg_index, value)`
    pub num_fields: u32,
    pub fields: Vec<(u8, u32, u64)>,
}

impl Msg {
    /// 构造不带寄存器更新的 CHANGE_PROG（最简形式）
    pub fn new_simple(sid: u32, program_uid: u32) -> Self {
        Self {
            sid,
            program_uid,
            num_fields: 0,
            fields: Vec::new(),
        }
    }

    /// 构造带寄存器更新的 CHANGE_PROG
    ///
    /// `fields`: `(reg_type, reg_index, value)` 三元组列表
    pub fn new_with_fields(sid: u32, program_uid: u32, fields: Vec<(u8, u32, u64)>) -> Self {
        let num_fields = fields.len() as u32;
        Self {
            sid,
            program_uid,
            num_fields,
            fields,
        }
    }
}

impl AsRawMsg for Msg {
    fn get_hdr(&self) -> (u16, u16, u32) {
        let len = HDR_LEN + 8 + self.num_fields as usize * FIELD_SIZE;
        (CHANGE_PROG, len as u16, self.sid)
    }

    fn get_u32s(&self, w: &mut Vec<u8>) -> std::io::Result<()> {
        write_u32_le(w, self.program_uid)?;
        write_u32_le(w, self.num_fields)?;
        Ok(())
    }

    fn get_bytes(&self, w: &mut Vec<u8>) -> std::io::Result<()> {
        for &(reg_type, reg_index, value) in &self.fields {
            w.push(reg_type);
            write_u32_le(w, reg_index)?;
            write_u64_le(w, value)?;
        }
        Ok(())
    }
}

impl Msg {
    pub(crate) fn from_raw_msg(raw: &RawMsg) -> Result<Self, SerializeError> {
        if raw.payload.len() < 8 {
            return Err(SerializeError::Truncated {
                need: 8,
                got: raw.payload.len(),
            });
        }
        let program_uid = read_u32_le(&raw.payload[0..4]);
        let num_fields = read_u32_le(&raw.payload[4..8]);

        let need = 8 + num_fields as usize * FIELD_SIZE;
        if raw.payload.len() < need {
            return Err(SerializeError::Truncated {
                need,
                got: raw.payload.len(),
            });
        }

        let mut fields = Vec::with_capacity(num_fields as usize);
        for i in 0..num_fields as usize {
            let off = 8 + i * FIELD_SIZE;
            let reg_type = raw.payload[off];
            let reg_index = read_u32_le(&raw.payload[off + 1..off + 5]);
            let value = read_u64_le(&raw.payload[off + 5..off + 13]);
            fields.push((reg_type, reg_index, value));
        }

        Ok(Msg {
            sid: raw.sid,
            program_uid,
            num_fields,
            fields,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::Msg;
    use crate::serialize::{serialize, Msg as TopMsg};

    fn roundtrip(m: Msg) -> Msg {
        let buf = serialize(&m).expect("serialize");
        match TopMsg::from_buf(&buf).expect("from_buf") {
            (TopMsg::ChProg(got), consumed) => {
                assert_eq!(consumed, buf.len());
                got
            }
            other => panic!("wrong variant: {:?}", other),
        }
    }

    #[test]
    fn test_changeprog_simple_roundtrip() {
        let m = Msg::new_simple(3, 7);
        let rt = roundtrip(m.clone());
        assert_eq!(rt.sid, 3);
        assert_eq!(rt.program_uid, 7);
        assert_eq!(rt.num_fields, 0);
    }

    #[test]
    fn test_changeprog_with_reporttime() {
        // 模拟 SET_PROGRAM DtccDatapathInterval ReportTime=10000
        // ReportTime 是 Implicit register index 4 (对应 portus Reg::Implicit(4, _))
        // reg_type=2 (Implicit tag), reg_index=4, value=10000 (微秒)
        let fields = vec![(2u8, 4u32, 10_000u64)];
        let m = Msg::new_with_fields(1, 3, fields.clone());
        let rt = roundtrip(m.clone());
        assert_eq!(rt.program_uid, 3);
        assert_eq!(rt.num_fields, 1);
        assert_eq!(rt.fields, fields);
    }

    #[test]
    fn test_changeprog_wire_format() {
        // 与 portus serialize_changeprog_msg 测试对齐
        // Msg { sid=1, program_uid=7, fields=[(Implicit(4), 42)] }
        // portus 产生: [4,0, 29,0, 1,0,0,0, 7,0,0,0, 1,0,0,0, 2,4,0,0,0, 42,0,0,0,0,0,0,0]
        let m = Msg::new_with_fields(1, 7, vec![(2, 4, 42)]);
        let buf = serialize(&m).expect("serialize");
        assert_eq!(buf[0], 4); // type = CHANGE_PROG
        assert_eq!(buf[1], 0);
        // length = 8(hdr) + 8(uid+fields) + 13(1 field) = 29
        assert_eq!(u16::from_le_bytes([buf[2], buf[3]]), 29);
        assert_eq!(u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]), 1); // sid
        assert_eq!(u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]), 7); // program_uid
        assert_eq!(u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]), 1); // num_fields
        assert_eq!(buf[16], 2); // reg_type = Implicit
        assert_eq!(u32::from_le_bytes([buf[17], buf[18], buf[19], buf[20]]), 4); // reg_index
        assert_eq!(u64::from_le_bytes(buf[21..29].try_into().unwrap()), 42); // value
    }
}
