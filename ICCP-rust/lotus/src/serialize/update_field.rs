//! UPDATE_FIELD 消息（type=3）：CCP → 内核，更新 fold 寄存器值
//!
//! 线路格式：
//! ```text
//! [type=3:u16 | len:u16 | sid:u32]
//! [num_fields:u32]                               <- 4字节
//! [(reg_type:u8 | reg_index:u32 | value:u64) * n]  <- 每条13字节
//! ```
//!
//! ## lotus 与 portus 的差异
//!
//! portus 使用 `lang::Reg` 枚举（依赖 fold 编译器，序列化为 5 字节）。
//! lotus 用原始三元组 `(reg_type: u8, reg_index: u32, value: u64)` = 1+4+8 = 13 字节，
//! 与 portus 线路字节完全兼容（portus Reg 序列化 = [tag:u8, index:u32_le]）。
//!
//! `from_raw_msg` 用于往返测试；生产中 CCP 不会收到内核发回的 UPDATE_FIELD。

use super::{
    read_u32_le, read_u64_le, write_u32_le, write_u64_le, AsRawMsg, RawMsg, SerializeError, HDR_LEN,
};

pub(crate) const UPDATE_FIELD: u16 = 3;

/// UPDATE_FIELD 消息
///
/// 每个字段为三元组 `(reg_type, reg_index, value)`，对应 portus `Reg` 的线路表示：
/// - `reg_type`: Reg 标签字节（如 portus `Reg::Implicit` = 2）
/// - `reg_index`: 寄存器下标（u32 LE）
/// - `value`: 要写入的值（u64 LE）
#[derive(Clone, Debug, PartialEq)]
pub struct Msg {
    pub sid: u32,
    pub num_fields: u8,
    pub fields: Vec<(u8, u32, u64)>,
}

/// 每条字段 = reg_type(1) + reg_index(4) + value(8) = 13 字节
const FIELD_SIZE: usize = 13;

impl AsRawMsg for Msg {
    fn get_hdr(&self) -> (u16, u16, u32) {
        let len = HDR_LEN + 4 + self.num_fields as usize * FIELD_SIZE;
        (UPDATE_FIELD, len as u16, self.sid)
    }

    fn get_u32s(&self, w: &mut Vec<u8>) -> std::io::Result<()> {
        write_u32_le(w, self.num_fields as u32)
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
        if raw.payload.len() < 4 {
            return Err(SerializeError::Truncated {
                need: 4,
                got: raw.payload.len(),
            });
        }
        let num_fields = read_u32_le(&raw.payload[0..4]) as u8;
        let need = 4 + num_fields as usize * FIELD_SIZE;
        if raw.payload.len() < need {
            return Err(SerializeError::Truncated {
                need,
                got: raw.payload.len(),
            });
        }

        let mut fields = Vec::with_capacity(num_fields as usize);
        for i in 0..num_fields as usize {
            let off = 4 + i * FIELD_SIZE;
            let reg_type = raw.payload[off];
            let reg_index = read_u32_le(&raw.payload[off + 1..off + 5]);
            let value = read_u64_le(&raw.payload[off + 5..off + 13]);
            fields.push((reg_type, reg_index, value));
        }

        Ok(Msg {
            sid: raw.sid,
            num_fields,
            fields,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::serialize::{serialize, Msg};

    fn roundtrip(m: super::Msg) -> super::Msg {
        let buf = serialize(&m).expect("serialize");
        match Msg::from_buf(&buf).expect("from_buf") {
            (Msg::Upd(got), consumed) => {
                assert_eq!(consumed, buf.len());
                got
            }
            other => panic!("wrong variant: {:?}", other),
        }
    }

    #[test]
    fn test_update_field_single() {
        // 对应 portus test: Reg::Implicit(4) = [2, 4, 0, 0, 0], value=42
        // reg_type=2, reg_index=4, value=42
        let m = super::Msg {
            sid: 1,
            num_fields: 1,
            fields: vec![(2u8, 4u32, 42u64)],
        };
        assert_eq!(roundtrip(m.clone()), m);
    }

    #[test]
    fn test_update_field_wire_layout() {
        // 与 portus serialize_update_msg 字节对比
        let m = super::Msg {
            sid: 1,
            num_fields: 1,
            fields: vec![(2u8, 4u32, 42u64)],
        };
        let buf = serialize(&m).unwrap();
        assert_eq!(
            buf,
            vec![
                3, 0, // UPDATE_FIELD
                25, 0, // length = 8 + 4 + 13 = 25
                1, 0, 0, 0, // sid = 1
                1, 0, 0, 0, // num_fields = 1
                2, // reg_type = 2 (Reg::Implicit tag)
                4, 0, 0, 0, // reg_index = 4
                42, 0, 0, 0, 0, 0, 0, 0, // value = 42
            ],
        );
    }

    #[test]
    fn test_update_field_zero_fields() {
        let m = super::Msg {
            sid: 0,
            num_fields: 0,
            fields: vec![],
        };
        assert_eq!(roundtrip(m.clone()), m);
    }

    #[test]
    fn test_update_field_multiple() {
        let m = super::Msg {
            sid: 5,
            num_fields: 3,
            fields: vec![
                (1u8, 0u32, 100u64),
                (2u8, 1u32, 200u64),
                (3u8, 2u32, 300u64),
            ],
        };
        assert_eq!(roundtrip(m.clone()), m);
    }
}
