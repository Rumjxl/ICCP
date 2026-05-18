//! Asynchronous IPC layer with high-throughput message routing
//!
//! This module provides async-first IPC mechanisms with support for
//! batching, load balancing, and concurrent message processing.

use crate::Result;
use async_trait::async_trait;
use flume::{Receiver, Sender};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::UnixStream;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Asynchronous IPC trait
#[async_trait]
pub trait AsyncIpc<Addr>: Send + Sync + 'static {
    /// Send a message to the specified address
    async fn send(&self, msg: &[u8], to: &Addr) -> Result<()>;

    /// Receive a message (returns message and sender address)
    async fn recv(&self, buf: &mut [u8]) -> Result<(usize, Addr)>;

    /// Close the IPC connection
    async fn close(&mut self) -> Result<()>;

    /// Get the name of this IPC mechanism
    fn name(&self) -> &'static str;
}

/// IPC message with metadata
#[derive(Debug, Clone)]
pub struct IpcMessage {
    pub data: Vec<u8>,
    pub timestamp: std::time::Instant,
    pub source: String,
    pub message_type: MessageType,
}

/// Types of IPC messages
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MessageType {
    Ready,
    CreateFlow,
    MeasurementReport,
    InstallProgram,
    DropFlow,
    Control,
}

/// High-performance message router with load balancing
pub struct MessageRouter {
    workers: Vec<MessageWorker>,
    sender_channels: Vec<Sender<IpcMessage>>,
    round_robin_counter: std::sync::atomic::AtomicUsize,
    config: RouterConfig,
}

#[derive(Debug, Clone)]
pub struct RouterConfig {
    pub worker_count: usize,
    pub channel_buffer_size: usize,
    pub enable_batching: bool,
    pub batch_size: usize,
    pub batch_timeout_ms: u64,
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self {
            worker_count: num_cpus::get(),
            channel_buffer_size: 1000,
            enable_batching: true,
            batch_size: 32,
            batch_timeout_ms: 1,
        }
    }
}

impl MessageRouter {
    pub fn new(config: RouterConfig) -> Self {
        let mut workers = Vec::new();
        let mut sender_channels = Vec::new();

        for worker_id in 0..config.worker_count {
            let (sender, receiver) = flume::bounded(config.channel_buffer_size);
            let worker = MessageWorker::new(worker_id, receiver, config.clone());

            workers.push(worker);
            sender_channels.push(sender);
        }

        info!(worker_count = config.worker_count, "Created message router");

        Self {
            workers,
            sender_channels,
            round_robin_counter: std::sync::atomic::AtomicUsize::new(0),
            config,
        }
    }

    /// Start all worker threads
    pub async fn start(&mut self) -> Result<Vec<tokio::task::JoinHandle<()>>> {
        let mut handles = Vec::new();

        for worker in self.workers.drain(..) {
            let handle = tokio::spawn(async move {
                if let Err(e) = worker.run().await {
                    error!(error = %e, "Message worker failed");
                }
            });
            handles.push(handle);
        }

        info!("Started all message workers");
        Ok(handles)
    }

    /// Route a message to an appropriate worker
    pub async fn route_message(&self, message: IpcMessage) -> Result<()> {
        let worker_id = self.select_worker(&message);

        if let Some(sender) = self.sender_channels.get(worker_id) {
            sender
                .send_async(message)
                .await
                .map_err(|e| crate::LotusError::Ipc(format!("Failed to route message: {}", e)))?;
            Ok(())
        } else {
            Err(crate::LotusError::Ipc(format!(
                "Invalid worker ID: {}",
                worker_id
            )))
        }
    }

    /// Select worker for load balancing
    fn select_worker(&self, message: &IpcMessage) -> usize {
        match message.message_type {
            // Use round-robin for general messages
            MessageType::Ready | MessageType::InstallProgram | MessageType::Control => {
                let counter = self
                    .round_robin_counter
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                counter % self.config.worker_count
            }
            // Hash-based routing for flow-specific messages to maintain affinity
            MessageType::CreateFlow | MessageType::MeasurementReport | MessageType::DropFlow => {
                let hash = self.hash_message(message);
                hash % self.config.worker_count
            }
        }
    }

    /// Simple hash function for message routing
    fn hash_message(&self, message: &IpcMessage) -> usize {
        // Simple hash based on message content
        let mut hash = 0usize;
        for byte in message.data.iter().take(16) {
            hash = hash.wrapping_mul(31).wrapping_add(*byte as usize);
        }
        hash
    }

    /// Get router statistics
    pub fn get_stats(&self) -> RouterStats {
        RouterStats {
            worker_count: self.config.worker_count,
            total_messages_routed: 0, // TODO: Add counters
        }
    }
}

#[derive(Debug)]
pub struct RouterStats {
    pub worker_count: usize,
    pub total_messages_routed: u64,
}

/// Message processing worker
struct MessageWorker {
    worker_id: usize,
    receiver: Receiver<IpcMessage>,
    config: RouterConfig,
    message_handlers: HashMap<MessageType, Box<dyn MessageHandler>>,
}

impl MessageWorker {
    fn new(worker_id: usize, receiver: Receiver<IpcMessage>, config: RouterConfig) -> Self {
        let mut message_handlers: HashMap<MessageType, Box<dyn MessageHandler>> = HashMap::new();

        // Register default handlers
        message_handlers.insert(MessageType::Ready, Box::new(ReadyHandler));
        message_handlers.insert(MessageType::CreateFlow, Box::new(CreateFlowHandler));
        message_handlers.insert(MessageType::MeasurementReport, Box::new(ReportHandler));
        message_handlers.insert(MessageType::InstallProgram, Box::new(InstallHandler));
        message_handlers.insert(MessageType::DropFlow, Box::new(DropFlowHandler));
        message_handlers.insert(MessageType::Control, Box::new(ControlHandler));

        Self {
            worker_id,
            receiver,
            config,
            message_handlers,
        }
    }

    async fn run(self) -> Result<()> {
        info!(worker_id = self.worker_id, "Starting message worker");

        if self.config.enable_batching {
            self.run_batched().await
        } else {
            self.run_single().await
        }
    }

    async fn run_single(self) -> Result<()> {
        while let Ok(message) = self.receiver.recv_async().await {
            self.process_message(message).await?;
        }
        Ok(())
    }

    async fn run_batched(self) -> Result<()> {
        let mut batch = Vec::with_capacity(self.config.batch_size);
        let batch_timeout = std::time::Duration::from_millis(self.config.batch_timeout_ms);

        loop {
            // Collect messages for batch
            let start_time = std::time::Instant::now();

            while batch.len() < self.config.batch_size {
                match tokio::time::timeout(batch_timeout, self.receiver.recv_async()).await {
                    Ok(Ok(message)) => batch.push(message),
                    Ok(Err(_)) => break, // Channel closed
                    Err(_) => break,     // Timeout
                }

                // Check if we've exceeded batch timeout
                if start_time.elapsed() >= batch_timeout {
                    break;
                }
            }

            if batch.is_empty() {
                continue;
            }

            // Process batch
            debug!(
                worker_id = self.worker_id,
                batch_size = batch.len(),
                "Processing message batch"
            );

            for message in batch.drain(..) {
                if let Err(e) = self.process_message(message).await {
                    warn!(worker_id = self.worker_id, error = %e, "Failed to process message in batch");
                }
            }
        }
    }

    async fn process_message(&self, message: IpcMessage) -> Result<()> {
        debug!(worker_id = self.worker_id, message_type = ?message.message_type, "Processing message");

        if let Some(handler) = self.message_handlers.get(&message.message_type) {
            handler.handle(message).await
        } else {
            warn!(worker_id = self.worker_id, message_type = ?message.message_type, "No handler for message type");
            Ok(())
        }
    }
}

/// Message handler trait
#[async_trait]
trait MessageHandler: Send + Sync {
    async fn handle(&self, message: IpcMessage) -> Result<()>;
}

// Default message handlers
struct ReadyHandler;
#[async_trait]
impl MessageHandler for ReadyHandler {
    async fn handle(&self, _message: IpcMessage) -> Result<()> {
        debug!("Handling Ready message");
        Ok(())
    }
}

struct CreateFlowHandler;
#[async_trait]
impl MessageHandler for CreateFlowHandler {
    async fn handle(&self, _message: IpcMessage) -> Result<()> {
        debug!("Handling CreateFlow message");
        Ok(())
    }
}

struct ReportHandler;
#[async_trait]
impl MessageHandler for ReportHandler {
    async fn handle(&self, _message: IpcMessage) -> Result<()> {
        debug!("Handling MeasurementReport message");
        Ok(())
    }
}

struct InstallHandler;
#[async_trait]
impl MessageHandler for InstallHandler {
    async fn handle(&self, _message: IpcMessage) -> Result<()> {
        debug!("Handling InstallProgram message");
        Ok(())
    }
}

struct DropFlowHandler;
#[async_trait]
impl MessageHandler for DropFlowHandler {
    async fn handle(&self, _message: IpcMessage) -> Result<()> {
        debug!("Handling DropFlow message");
        Ok(())
    }
}

struct ControlHandler;
#[async_trait]
impl MessageHandler for ControlHandler {
    async fn handle(&self, _message: IpcMessage) -> Result<()> {
        debug!("Handling Control message");
        Ok(())
    }
}

/// Unix domain socket implementation
pub struct AsyncUnixSocket {
    stream: Arc<RwLock<Option<UnixStream>>>,
    path: String,
}

impl AsyncUnixSocket {
    pub async fn new(path: &str) -> Result<Self> {
        let stream = UnixStream::connect(path)
            .await
            .map_err(|e| crate::LotusError::Ipc(format!("Failed to connect to {}: {}", path, e)))?;

        Ok(Self {
            stream: Arc::new(RwLock::new(Some(stream))),
            path: path.to_string(),
        })
    }
}

#[async_trait]
impl AsyncIpc<()> for AsyncUnixSocket {
    async fn send(&self, msg: &[u8], _to: &()) -> Result<()> {
        use tokio::io::AsyncWriteExt;

        let mut stream_guard = self.stream.write().await;
        if let Some(ref mut stream) = *stream_guard {
            stream
                .write_all(msg)
                .await
                .map_err(|e| crate::LotusError::Ipc(format!("Send failed: {}", e)))?;
            Ok(())
        } else {
            Err(crate::LotusError::Ipc("Socket is closed".to_string()))
        }
    }

    async fn recv(&self, buf: &mut [u8]) -> Result<(usize, ())> {
        use tokio::io::AsyncReadExt;

        let mut stream_guard = self.stream.write().await;
        if let Some(ref mut stream) = *stream_guard {
            let bytes_read = stream
                .read(buf)
                .await
                .map_err(|e| crate::LotusError::Ipc(format!("Recv failed: {}", e)))?;
            Ok((bytes_read, ()))
        } else {
            Err(crate::LotusError::Ipc("Socket is closed".to_string()))
        }
    }

    async fn close(&mut self) -> Result<()> {
        let mut stream_guard = self.stream.write().await;
        *stream_guard = None;
        info!(path = %self.path, "Closed Unix socket");
        Ok(())
    }

    fn name(&self) -> &'static str {
        "unix"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_message_router_creation() {
        let config = RouterConfig::default();
        let router = MessageRouter::new(config);

        assert_eq!(router.sender_channels.len(), num_cpus::get());
    }

    #[tokio::test]
    async fn test_message_routing() {
        let config = RouterConfig {
            worker_count: 2,
            ..Default::default()
        };
        let router = MessageRouter::new(config);

        let message = IpcMessage {
            data: vec![1, 2, 3, 4],
            timestamp: std::time::Instant::now(),
            source: "test".to_string(),
            message_type: MessageType::Ready,
        };

        // This should not panic
        let worker_id = router.select_worker(&message);
        assert!(worker_id < 2);
    }
}
