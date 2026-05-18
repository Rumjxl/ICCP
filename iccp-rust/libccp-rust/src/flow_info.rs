use super::{ccp, LibccpError};

pub struct FlowInfo(ccp::ccp_datapath_info);

impl Default for FlowInfo {
    fn default() -> Self {
        FlowInfo(ccp::ccp_datapath_info {
            init_cwnd: 10,
            mss: 1460,
            src_ip: 0,
            src_port: 0,
            dst_ip: 0,
            dst_port: 0,
            congAlg: [0; 64],
        })
    }
}

setters!(FlowInfo =>
    init_cwnd: u32,
    mss: u32,
    src_ip: u32,
    dst_ip: u32,
    src_port: u32,
    dst_port: u32
);

impl FlowInfo {
    pub fn with_four_tuple(self, src_ip: u32, src_port: u32, dst_ip: u32, dst_port: u32) -> Self {
        self.with_src_ip(src_ip)
            .with_src_port(src_port)
            .with_dst_ip(dst_ip)
            .with_dst_port(dst_port)
    }

    /// Request a congestion control algorithm to use.
    ///
    /// CCP is free to ignore this value. The maximum string length allowed is 63.
    pub fn with_cong_alg(mut self, name: &str) -> Result<Self, LibccpError> {
        if name.len() > 63 {
            return Err(LibccpError::OtherError(
                "Name of congestion control alg too long. Max length is 63.",
            ));
        }

        // safety: i8 and u8 are the same size.
        let name_c = unsafe { &*(name.as_bytes() as *const [u8] as *const [i8]) };
        self.0.congAlg[0..name_c.len()].copy_from_slice(name_c);
        Ok(self)
    }

    pub(crate) fn get_dp_info(&self) -> ccp::ccp_datapath_info {
        self.0
    }
}
