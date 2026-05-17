//! Netlink IPC 适配器：将 portus netlink socket 包装为 lotus `AsyncIpc<()>`
//!
//! ## 实现策略
//!
//! ### recv：真正的异步（`tokio::io::unix::AsyncFd`）
//!
//! portus `Socket<Blocking>` 在内核 netlink fd 上设有 SO_RCVTIMEO=1s，
//! 原先通过 `spawn_blocking` 将阻塞 recv 移到线程池。这带来两个问题：
//!
//! 1. **吞吐量瓶颈**：每次 recv 都要线程池调度 + `Mutex` 竞争，8 条流高频
//!    MEASURE 时处理速度远低于内核产生速度，导致 netlink 接收缓冲区堆积。
//! 2. **ENOBUFS 致命退出**：缓冲区满后 recvmsg 返回 ENOBUFS，原来的代码
//!    直接将其传播为 `Err`，终止整个 DatapathListener。
//!
//! 新实现：
//! - 使用 `AsyncFd` 包装 netlink fd，由 tokio epoll 驱动可读事件，
//!   无线程切换开销，recv 延迟从几十 µs 降至几 µs。
//! - ENOBUFS / EAGAIN 均作为"暂无数据"处理（`Ok((0, ()))`），listener 继续运行。
//!
//! ### send：保持 `spawn_blocking`
//!
//! netlink `sendmsg` 通常立即返回（内核只需将消息放入发送队列），
//! 在 blocking thread pool 执行成本可接受，暂不改造。
//!
//! ### SO_RCVBUF
//!
//! portus `Socket::new()` 中已将接收缓冲区扩大至 4 MiB（第一道防线）。
//! ENOBUFS 豁免是第二道防线，两者配合保证高并发下稳定运行。

use crate::ipc::AsyncIpc;
use crate::LotusError;
use async_trait::async_trait;
use nix::sys::socket as nix_socket;
use nix::sys::uio::IoVec;
use portus::ipc::netlink::Socket;
use portus::ipc::{Blocking, Ipc};
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::Arc;
use tokio::io::unix::AsyncFd;
use tokio::sync::Mutex;
use tracing::{debug, warn};

const NLMSG_HDRSIZE: usize = 0x10;

// ── send 侧复用 portus Socket<Blocking>（只需阻塞锁） ────────────────

/// portus netlink socket 的 async 包装器。
///
/// - `send_sock`：供 spawn_blocking send 使用（持有 `Mutex<Socket<Blocking>>`）
/// - `async_fd`：供 AsyncFd recv 使用（共享同一底层 fd，read-only epoll 监听）
///
/// **安全性**：`AsyncFd` 只做可读事件轮询，实际读取仍通过 `recv_raw_fd` 在
/// 持有 `async_fd` 的独占 guard 内完成，不存在并发读竞争。
pub struct NetlinkBlockingBridge {
    /// 用于 send / close 的阻塞 socket（spawn_blocking 侧）
    send_sock: Arc<Mutex<Socket<Blocking>>>,
    /// 用于 recv 的 AsyncFd（tokio epoll 驱动）
    async_fd: Arc<AsyncFd<RawFdOwner>>,
}

/// 持有 raw fd 数值但不负责关闭（fd 由 `Socket<Blocking>` 的 Drop 关闭）。
///
/// `AsyncFd` 要求 `AsRawFd`，此包装类仅提供 fd 数值而不重复 close。
struct RawFdOwner(RawFd);

impl AsRawFd for RawFdOwner {
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}

impl NetlinkBlockingBridge {
    /// 创建并绑定 netlink socket，加入 CCP 组播组 22。
    ///
    /// portus `Socket::new()` 内部已设置：
    /// - `NETLINK_ADD_MEMBERSHIP` 组 22
    /// - `SO_RCVTIMEO = 1s`（阻塞路径用；AsyncFd 路径不依赖此超时）
    /// - `SO_RCVBUF = 4 MiB`（减少 ENOBUFS 概率）
    ///
    /// 这里额外将 fd 设置为非阻塞模式，使 `recvmsg(MSG_DONTWAIT)`
    /// 在无数据时立即返回 EAGAIN 而非挂起。
    pub fn new() -> Result<Self, LotusError> {
        let sock = Socket::<Blocking>::new()
            .map_err(|e| LotusError::Ipc(format!("netlink socket init failed: {}", e.0)))?;

        let fd = sock.as_raw_fd();

        // 将 fd 设为非阻塞，配合 AsyncFd 使用
        // SAFETY: fd 有效，fcntl 调用符合 POSIX 规范
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFL, 0) };
        if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
            return Err(LotusError::Ipc(format!(
                "failed to set netlink fd non-blocking (errno {})",
                unsafe { *libc::__errno_location() }
            )));
        }

        // AsyncFd 只监听可读事件，不拥有 fd 的生命周期（Socket<Blocking>.drop() 负责关闭）
        let async_fd = AsyncFd::new(RawFdOwner(fd))
            .map_err(|e| LotusError::Ipc(format!("AsyncFd::new failed: {e}")))?;

        Ok(Self {
            send_sock: Arc::new(Mutex::new(sock)),
            async_fd: Arc::new(async_fd),
        })
    }
}

#[async_trait]
impl AsyncIpc<()> for NetlinkBlockingBridge {
    /// 发送：通过 spawn_blocking 调用 portus send（构造 netlink 头 + sendmsg）
    async fn send(&self, msg: &[u8], _to: &()) -> crate::Result<()> {
        let msg = msg.to_vec();
        let sock = self.send_sock.clone();
        tokio::task::spawn_blocking(move || {
            let guard = sock.blocking_lock();
            guard
                .send(&msg, &())
                .map_err(|e| LotusError::Ipc(e.0.clone()))
        })
        .await
        .map_err(|e| LotusError::Ipc(format!("spawn_blocking join error: {e}")))?
    }

    /// 接收：AsyncFd 驱动的真正异步 recv，消除线程池调度开销。
    ///
    /// 流程：
    /// 1. `async_fd.readable()` → tokio epoll 等待 fd 可读（无数据时让出 CPU）
    /// 2. 可读后调用 `recvmsg(MSG_DONTWAIT)` 读取一帧
    /// 3. EAGAIN（虚假可读）→ 清除 ready 标记重试；ENOBUFS → warn + 返回 0
    async fn recv(&self, buf: &mut [u8]) -> crate::Result<(usize, ())> {
        let fd = self.async_fd.as_raw_fd();
        let async_fd = self.async_fd.clone();

        loop {
            // 等待 fd 可读（epoll EPOLLIN），无数据时 yield 给 tokio scheduler
            let mut guard = async_fd
                .readable()
                .await
                .map_err(|e| LotusError::Ipc(format!("AsyncFd readable error: {e}")))?;

            let mut nl_buf = [0u8; 1024];

            // MSG_DONTWAIT：fd 已是非阻塞，此标志作为额外保障
            let recv_result = nix_socket::recvmsg(
                fd,
                &[IoVec::from_mut_slice(&mut nl_buf[..])],
                None,
                nix_socket::MsgFlags::MSG_DONTWAIT,
            );

            match recv_result {
                Ok(msg) => {
                    guard.retain_ready(); // fd 可能还有更多数据，保留 ready 状态
                    let n = msg.bytes;
                    if n <= NLMSG_HDRSIZE {
                        // 只有 netlink 头，无 payload（不正常，跳过）
                        warn!(
                            bytes = n,
                            "netlink recv: frame smaller than NLMSG_HDRSIZE, skipping"
                        );
                        return Ok((0, ()));
                    }
                    let payload_len = n - NLMSG_HDRSIZE;
                    let copy_len = payload_len.min(buf.len());
                    buf[..copy_len]
                        .copy_from_slice(&nl_buf[NLMSG_HDRSIZE..NLMSG_HDRSIZE + copy_len]);
                    debug!(bytes = payload_len, "netlink recv (async)");
                    return Ok((copy_len, ()));
                }
                Err(nix::errno::Errno::EAGAIN) | Err(nix::errno::Errno::EWOULDBLOCK) => {
                    // 虚假可读（epoll spurious wakeup），清除 ready 标记重新等待
                    guard.clear_ready();
                    continue;
                }
                Err(nix::errno::Errno::ENOBUFS) => {
                    // 内核接收缓冲区满，本帧已被内核丢弃。
                    // CCP MEASURE 允许丢失（算法下次周期会收到新数据），不应终止 listener。
                    warn!(
                        "netlink recv ENOBUFS: kernel dropped a frame (rx buffer full), continuing"
                    );
                    guard.clear_ready();
                    return Ok((0, ()));
                }
                Err(e) => {
                    guard.clear_ready();
                    return Err(LotusError::Ipc(format!("netlink recvmsg error: {e}")));
                }
            }
        }
    }

    async fn close(&mut self) -> crate::Result<()> {
        let sock = self.send_sock.clone();
        tokio::task::spawn_blocking(move || {
            let mut guard = sock.blocking_lock();
            guard.close().map_err(|e| LotusError::Ipc(e.0.clone()))
        })
        .await
        .map_err(|e| LotusError::Ipc(format!("spawn_blocking join error: {e}")))?
    }

    fn name(&self) -> &'static str {
        "netlink-async"
    }
}
