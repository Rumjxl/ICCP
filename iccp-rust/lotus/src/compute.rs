//! Compute-intensive algorithm execution with work-stealing and parallel processing
//!
//! This module provides a high-performance compute engine for running
//! CPU-intensive congestion control algorithms in parallel.

use crate::{flow::FlowId, Result};
use crossbeam::deque::{Injector, Stealer, Worker};
use rayon::prelude::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, RwLock};
use tracing::{debug, error, info, warn};

/// Compute task that can be executed in parallel
pub trait ComputeTask: Send + 'static {
    type Output: Send + 'static;

    /// Execute the compute task
    fn execute(self: Box<Self>) -> Self::Output;

    /// Get task priority (higher = more important)
    fn priority(&self) -> u8 {
        0
    }

    /// Get estimated execution time in microseconds
    fn estimated_duration_us(&self) -> u64 {
        1000 // Default 1ms
    }
}

/// Compute task with metadata
struct TaskWrapper {
    task: Box<dyn ComputeTask<Output = Box<dyn std::any::Any + Send>>>,
    flow_id: FlowId,
    created_at: Instant,
    priority: u8,
    estimated_duration_us: u64,
    result_sender: oneshot::Sender<Box<dyn std::any::Any + Send>>,
}

/// Work-stealing compute engine
pub struct ComputeEngine {
    workers: Vec<ComputeWorker>,
    global_queue: Arc<Injector<TaskWrapper>>,
    stealers: Vec<Stealer<TaskWrapper>>,
    shutdown_signal: Arc<AtomicBool>,
    stats: Arc<ComputeStats>,
    config: ComputeConfig,
}

#[derive(Debug, Clone)]
pub struct ComputeConfig {
    pub worker_count: usize,
    pub queue_capacity: usize,
    pub work_stealing_enabled: bool,
    pub task_timeout_ms: u64,
    pub priority_scheduling: bool,
    pub load_balancing_interval_ms: u64,
}

impl Default for ComputeConfig {
    fn default() -> Self {
        Self {
            worker_count: num_cpus::get(),
            queue_capacity: 10000,
            work_stealing_enabled: true,
            task_timeout_ms: 100,
            priority_scheduling: true,
            load_balancing_interval_ms: 10,
        }
    }
}

/// Compute engine statistics
#[derive(Debug, Default)]
pub struct ComputeStats {
    pub tasks_submitted: AtomicU64,
    pub tasks_completed: AtomicU64,
    pub tasks_failed: AtomicU64,
    pub tasks_timed_out: AtomicU64,
    pub total_execution_time_us: AtomicU64,
    pub work_steals: AtomicU64,
}

impl ComputeStats {
    pub fn get_throughput(&self) -> f64 {
        let completed = self.tasks_completed.load(Ordering::Relaxed) as f64;
        let total_time_s =
            self.total_execution_time_us.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        if total_time_s > 0.0 {
            completed / total_time_s
        } else {
            0.0
        }
    }

    pub fn get_success_rate(&self) -> f64 {
        let completed = self.tasks_completed.load(Ordering::Relaxed) as f64;
        let submitted = self.tasks_submitted.load(Ordering::Relaxed) as f64;
        if submitted > 0.0 {
            completed / submitted
        } else {
            0.0
        }
    }
}

impl ComputeEngine {
    pub fn new(config: ComputeConfig) -> Self {
        let global_queue = Arc::new(Injector::new());
        let mut workers = Vec::new();
        let mut stealers = Vec::new();
        let shutdown_signal = Arc::new(AtomicBool::new(false));
        let stats = Arc::new(ComputeStats::default());

        // Create workers with local queues
        for worker_id in 0..config.worker_count {
            let worker_queue = Worker::new_fifo();
            let stealer = worker_queue.stealer();

            let worker = ComputeWorker::new(
                worker_id,
                worker_queue,
                global_queue.clone(),
                shutdown_signal.clone(),
                stats.clone(),
                config.clone(),
            );

            workers.push(worker);
            stealers.push(stealer);
        }

        // Share stealers with all workers
        for worker in &mut workers {
            worker.set_stealers(stealers.clone());
        }

        info!(worker_count = config.worker_count, "Created compute engine");

        Self {
            workers,
            global_queue,
            stealers,
            shutdown_signal,
            stats,
            config,
        }
    }

    /// Start all compute workers
    pub async fn start(&mut self) -> Result<Vec<tokio::task::JoinHandle<()>>> {
        let mut handles = Vec::new();

        for worker in self.workers.drain(..) {
            let handle = tokio::task::spawn_blocking(move || {
                worker.run();
            });
            handles.push(handle);
        }

        info!("Started all compute workers");
        Ok(handles)
    }

    /// Submit a compute task for execution
    pub async fn submit_task<T>(&self, task: T, flow_id: FlowId) -> Result<T::Output>
    where
        T: ComputeTask + 'static,
        T::Output: 'static,
    {
        let (result_sender, result_receiver) = oneshot::channel();

        let task_wrapper = TaskWrapper {
            priority: task.priority(),
            estimated_duration_us: task.estimated_duration_us(),
            task: Box::new(TaskAdapter::new(task)),
            flow_id,
            created_at: Instant::now(),
            result_sender,
        };

        // Submit to global queue
        self.global_queue.push(task_wrapper);
        self.stats.tasks_submitted.fetch_add(1, Ordering::Relaxed);

        debug!(flow_id = %flow_id, "Submitted compute task");

        // Wait for result with timeout
        let timeout_duration = Duration::from_millis(self.config.task_timeout_ms);
        match tokio::time::timeout(timeout_duration, result_receiver).await {
            Ok(Ok(result)) => {
                // Downcast the result back to the original type
                let boxed_result = result.downcast::<T::Output>().map_err(|_| {
                    crate::LotusError::Runtime("Failed to downcast task result".to_string())
                })?;
                Ok(*boxed_result)
            }
            Ok(Err(_)) => {
                self.stats.tasks_failed.fetch_add(1, Ordering::Relaxed);
                Err(crate::LotusError::Runtime(
                    "Task execution failed".to_string(),
                ))
            }
            Err(_) => {
                self.stats.tasks_timed_out.fetch_add(1, Ordering::Relaxed);
                Err(crate::LotusError::Runtime(format!(
                    "Task timed out after {:?}",
                    timeout_duration
                )))
            }
        }
    }

    /// Submit multiple tasks in parallel
    pub async fn submit_batch<T>(&self, tasks: Vec<(T, FlowId)>) -> Vec<Result<T::Output>>
    where
        T: ComputeTask + 'static,
        T::Output: 'static,
    {
        let futures: Vec<_> = tasks
            .into_iter()
            .map(|(task, flow_id)| self.submit_task(task, flow_id))
            .collect();

        futures::future::join_all(futures).await
    }

    /// Get compute engine statistics
    pub fn get_stats(&self) -> ComputeStats {
        ComputeStats {
            tasks_submitted: AtomicU64::new(self.stats.tasks_submitted.load(Ordering::Relaxed)),
            tasks_completed: AtomicU64::new(self.stats.tasks_completed.load(Ordering::Relaxed)),
            tasks_failed: AtomicU64::new(self.stats.tasks_failed.load(Ordering::Relaxed)),
            tasks_timed_out: AtomicU64::new(self.stats.tasks_timed_out.load(Ordering::Relaxed)),
            total_execution_time_us: AtomicU64::new(
                self.stats.total_execution_time_us.load(Ordering::Relaxed),
            ),
            work_steals: AtomicU64::new(self.stats.work_steals.load(Ordering::Relaxed)),
        }
    }

    /// Shutdown the compute engine
    pub async fn shutdown(&self) {
        info!("Shutting down compute engine");
        self.shutdown_signal.store(true, Ordering::Relaxed);
    }
}

/// Individual compute worker with work-stealing capability
struct ComputeWorker {
    worker_id: usize,
    local_queue: Worker<TaskWrapper>,
    global_queue: Arc<Injector<TaskWrapper>>,
    stealers: Vec<Stealer<TaskWrapper>>,
    shutdown_signal: Arc<AtomicBool>,
    stats: Arc<ComputeStats>,
    config: ComputeConfig,
}

impl ComputeWorker {
    fn new(
        worker_id: usize,
        local_queue: Worker<TaskWrapper>,
        global_queue: Arc<Injector<TaskWrapper>>,
        shutdown_signal: Arc<AtomicBool>,
        stats: Arc<ComputeStats>,
        config: ComputeConfig,
    ) -> Self {
        Self {
            worker_id,
            local_queue,
            global_queue,
            stealers: Vec::new(),
            shutdown_signal,
            stats,
            config,
        }
    }

    fn set_stealers(&mut self, stealers: Vec<Stealer<TaskWrapper>>) {
        self.stealers = stealers;
    }

    fn run(self) {
        info!(worker_id = self.worker_id, "Starting compute worker");

        while !self.shutdown_signal.load(Ordering::Relaxed) {
            if let Some(task) = self.find_task() {
                self.execute_task(task);
            } else {
                // No tasks available, sleep briefly
                std::thread::sleep(Duration::from_micros(100));
            }
        }

        info!(worker_id = self.worker_id, "Compute worker stopped");
    }

    fn find_task(&self) -> Option<TaskWrapper> {
        // Try local queue first
        if let Some(task) = self.local_queue.pop() {
            return Some(task);
        }

        // Try global queue
        if let Some(task) = self.global_queue.steal().success() {
            return Some(task);
        }

        // Try work stealing from other workers
        if self.config.work_stealing_enabled {
            for stealer in &self.stealers {
                if let Some(task) = stealer.steal().success() {
                    self.stats.work_steals.fetch_add(1, Ordering::Relaxed);
                    return Some(task);
                }
            }
        }

        None
    }

    fn execute_task(&self, task: TaskWrapper) {
        let start_time = Instant::now();

        debug!(
            worker_id = self.worker_id,
            flow_id = %task.flow_id,
            priority = task.priority,
            "Executing compute task"
        );

        // Execute the task
        let result = task.task.execute();
        let execution_time = start_time.elapsed();

        // Send result back
        if task.result_sender.send(result).is_ok() {
            self.stats.tasks_completed.fetch_add(1, Ordering::Relaxed);
            debug!(
                worker_id = self.worker_id,
                flow_id = %task.flow_id,
                execution_time_us = execution_time.as_micros() as u64,
                "Task completed successfully"
            );
        } else {
            self.stats.tasks_failed.fetch_add(1, Ordering::Relaxed);
            warn!(
                worker_id = self.worker_id,
                flow_id = %task.flow_id,
                "Failed to send task result (receiver dropped)"
            );
        }

        self.stats
            .total_execution_time_us
            .fetch_add(execution_time.as_micros() as u64, Ordering::Relaxed);
    }
}

/// Adapter to make any ComputeTask work with the type-erased system
struct TaskAdapter<T: ComputeTask> {
    task: Option<T>,
}

impl<T: ComputeTask> TaskAdapter<T> {
    fn new(task: T) -> Self {
        Self { task: Some(task) }
    }
}

impl<T: ComputeTask + 'static> ComputeTask for TaskAdapter<T>
where
    T::Output: 'static,
{
    type Output = Box<dyn std::any::Any + Send>;

    fn execute(mut self: Box<Self>) -> Self::Output {
        let task = self.task.take().unwrap();
        let result = Box::new(task).execute();
        Box::new(result)
    }

    fn priority(&self) -> u8 {
        self.task.as_ref().map(|t| t.priority()).unwrap_or(0)
    }

    fn estimated_duration_us(&self) -> u64 {
        self.task
            .as_ref()
            .map(|t| t.estimated_duration_us())
            .unwrap_or(1000)
    }
}

/// Parallel algorithm execution utilities
pub struct ParallelAlgorithm;

impl ParallelAlgorithm {
    /// Execute a function in parallel across multiple data points
    pub fn parallel_map<T, R, F>(data: Vec<T>, f: F) -> Vec<R>
    where
        T: Send,
        R: Send,
        F: Fn(T) -> R + Send + Sync,
    {
        data.into_par_iter().map(f).collect()
    }

    /// Execute a reduction operation in parallel
    pub fn parallel_reduce<T, F, C, R>(data: Vec<T>, identity: R, f: F, combine: C) -> R
    where
        T: Send,
        R: Send + Sync + Clone,
        F: Fn(R, T) -> R + Send + Sync,
        C: Fn(R, R) -> R + Send + Sync,
    {
        let identity2 = identity.clone();
        data.into_par_iter()
            .fold(|| identity.clone(), &f)
            .reduce(|| identity2.clone(), |a, b| combine(a, b))
    }

    /// Execute multiple independent computations in parallel
    pub async fn parallel_execute<F, R>(computations: Vec<F>) -> Vec<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let tasks: Vec<_> = computations
            .into_iter()
            .map(|f| tokio::task::spawn_blocking(f))
            .collect();

        let mut results = Vec::new();
        for task in tasks {
            if let Ok(result) = task.await {
                results.push(result);
            }
        }
        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestTask {
        value: u32,
        duration_us: u64,
    }

    impl ComputeTask for TestTask {
        type Output = u32;

        fn execute(self: Box<Self>) -> Self::Output {
            // Simulate some computation
            std::thread::sleep(Duration::from_micros(self.duration_us));
            self.value * 2
        }

        fn estimated_duration_us(&self) -> u64 {
            self.duration_us
        }
    }

    #[tokio::test]
    async fn test_compute_engine() {
        let config = ComputeConfig {
            worker_count: 2,
            task_timeout_ms: 1000,
            ..Default::default()
        };

        let mut engine = ComputeEngine::new(config);
        let _handles = engine.start().await.unwrap();

        let task = TestTask {
            value: 21,
            duration_us: 1000,
        };
        let flow_id = FlowId::new();

        let result = engine.submit_task(task, flow_id).await.unwrap();
        assert_eq!(result, 42);

        engine.shutdown().await;
    }

    #[tokio::test]
    async fn test_parallel_batch() {
        let config = ComputeConfig {
            worker_count: 4,
            ..Default::default()
        };

        let mut engine = ComputeEngine::new(config);
        let _handles = engine.start().await.unwrap();

        let tasks: Vec<_> = (0..10)
            .map(|i| {
                let task = TestTask {
                    value: i,
                    duration_us: 100,
                };
                let flow_id = FlowId::new();
                (task, flow_id)
            })
            .collect();

        let results = engine.submit_batch(tasks).await;

        assert_eq!(results.len(), 10);
        for (i, result) in results.iter().enumerate() {
            assert_eq!(result.as_ref().unwrap(), &(i as u32 * 2));
        }

        engine.shutdown().await;
    }
}
