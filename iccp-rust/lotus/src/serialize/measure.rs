//! MEASURE 消息（type=1）：内核统计上报
//!
//! 线路格式：
//! ```text
//! [type=1:u16 | len:u16 | sid:u32]
//! [program_uid:u32 | num_fields:u32]   <- 8字节（注：num_fields 在 libccp 中用 u32 对齐）
//! [field_0:u64 | field_1:u64 | ...]    <- num_fields * 8 字节
//! ```
//!
//! **特殊语义**：`num_fields == 0` 表示流关闭信号。

use super::{
    read_u32_le, read_u64_le, write_u32_le, write_u64_le, AsRawMsg, RawMsg, SerializeError, HDR_LEN,
};

pub(crate) const MEASURE: u16 = 1;

/// MEASURE 消息
#[derive(Clone, Debug, PartialEq)]
pub struct Msg {
    pub sid: u32,
    pub program_uid: u32,
    /// `num_fields == 0` 表示流关闭
    pub num_fields: u8,
    pub fields: Vec<u64>,
}

/// 固定 u32 区域：program_uid + num_fields = 8 字节
const U32_SECTION: usize = 8;

impl AsRawMsg for Msg {
    fn get_hdr(&self) -> (u16, u16, u32) {
        let len = HDR_LEN + U32_SECTION + self.num_fields as usize * 8;
        (MEASURE, len as u16, self.sid)
    }

    fn get_u32s(&self, w: &mut Vec<u8>) -> std::io::Result<()> {
        write_u32_le(w, self.program_uid)?;
        write_u32_le(w, self.num_fields as u32)?;
        Ok(())
    }

    fn get_bytes(&self, w: &mut Vec<u8>) -> std::io::Result<()> {
        for &f in &self.fields {
            write_u64_le(w, f)?;
        }
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
        let num_fields = read_u32_le(&p[4..8]) as u8;

        let need = U32_SECTION + num_fields as usize * 8;
        if raw.payload.len() < need {
            return Err(SerializeError::Truncated {
                need,
                got: raw.payload.len(),
            });
        }

        let mut fields = Vec::with_capacity(num_fields as usize);
        for i in 0..num_fields as usize {
            let off = U32_SECTION + i * 8;
            fields.push(read_u64_le(&p[off..off + 8]));
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
    use crate::serialize::{serialize, Msg};

    fn roundtrip(m: super::Msg) -> super::Msg {
        let buf = serialize(&m).expect("serialize");
        match Msg::from_buf(&buf).expect("from_buf") {
            (Msg::Ms(got), consumed) => {
                assert_eq!(consumed, buf.len());
                got
            }
            other => panic!("wrong variant: {:?}", other),
        }
    }

    #[test]
    fn test_measure_basic() {
        let m = super::Msg {
            sid: 15,
            program_uid: 72,
            num_fields: 5,
            fields: vec![424242, 65535, 65530, 200000, 150000],
        };
        assert_eq!(roundtrip(m.clone()), m);
    }

    #[test]
    fn test_measure_close_signal() {
        // num_fields == 0 是流关闭信号
        let m = super::Msg {
            sid: 1,
            program_uid: 3,
            num_fields: 0,
            fields: vec![],
        };
        let got = roundtrip(m.clone());
        assert_eq!(got.num_fields, 0);
        assert!(got.fields.is_empty());
    }

    #[test]
    fn test_measure_many_fields() {
        let fields: Vec<u64> = (0..32).map(|i| 42424242u64 * (i + 1)).collect();
        let m = super::Msg {
            sid: 32,
            program_uid: 3,
            num_fields: 32,
            fields: fields.clone(),
        };
        assert_eq!(roundtrip(m.clone()), m);
    }

    #[test]
    fn test_measure_wire_layout() {
        let m = super::Msg {
            sid: 1,
            program_uid: 7,
            num_fields: 1,
            fields: vec![42],
        };
        let buf = serialize(&m).unwrap();
        // type=1
        assert_eq!(&buf[0..2], &[1, 0]);
        // len = 8+8+8 = 24
        assert_eq!(u16::from_le_bytes([buf[2], buf[3]]), 24);
        // sid=1
        assert_eq!(u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]), 1);
        // program_uid=7
        assert_eq!(u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]), 7);
        // num_fields=1
        assert_eq!(u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]), 1);
        // field[0]=42
        assert_eq!(u64::from_le_bytes(buf[16..24].try_into().unwrap()), 42);
    }
}
