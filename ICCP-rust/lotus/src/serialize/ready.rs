//! READY 消息（type=5）：内核启动时发送，通知 CCP 开始工作
//!
//! 线路格式：
//! ```text
//! [type=5:u16 | len=12:u16 | sid=0:u32 | id:u32]
//! ```

use super::{read_u32_le, write_u32_le, AsRawMsg, RawMsg, SerializeError, HDR_LEN};

pub(crate) const READY: u16 = 5;

/// READY 消息
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Msg {
    /// datapath 自报的 id（通常为 0）
    pub id: u32,
}

impl AsRawMsg for Msg {
    fn get_hdr(&self) -> (u16, u16, u32) {
        // len = 8(hdr) + 4(id) = 12, sid=0
        (READY, (HDR_LEN + 4) as u16, 0)
    }

    fn get_u32s(&self, w: &mut Vec<u8>) -> std::io::Result<()> {
        write_u32_le(w, self.id)
    }

    fn get_bytes(&self, _w: &mut Vec<u8>) -> std::io::Result<()> {
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
        Ok(Msg {
            id: read_u32_le(&raw.payload[0..4]),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::serialize::{serialize, Msg};

    fn roundtrip(m: super::Msg) -> super::Msg {
        let buf = serialize(&m).expect("serialize");
        match Msg::from_buf(&buf).expect("from_buf") {
            (Msg::Rdy(got), consumed) => {
                assert_eq!(consumed, buf.len());
                got
            }
            other => panic!("wrong variant: {:?}", other),
        }
    }

    #[test]
    fn test_ready_id0() {
        let m = super::Msg { id: 0 };
        assert_eq!(roundtrip(m), m);
    }

    #[test]
    fn test_ready_id7() {
        let m = super::Msg { id: 7 };
        assert_eq!(roundtrip(m), m);
    }

    #[test]
    fn test_ready_wire_layout() {
        let m = super::Msg { id: 1 };
        let buf = serialize(&m).unwrap();
        assert_eq!(
            buf,
            vec![
                5, 0, // type = READY
                12, 0, // len = 12
                0, 0, 0, 0, // sid = 0
                1, 0, 0, 0, // id = 1
            ]
        );
    }
}
