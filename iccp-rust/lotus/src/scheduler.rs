//! Advanced scheduling and load balancing for flows and algorithms
//!
//! This module provides sophisticated scheduling algorithms for optimal
//! resource utilization and performance under high concurrent loads.

use crate::{flow::FlowId, Result};
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Flow scheduling priority
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Priority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

impl From<u8> for Priority {
    fn from(value: u8) -> Self {
        match value {
            0 => Priority::Low,
            1 => Priority::Normal,
            2 => Priority::High,
            _ => Priority::Critical,
        }
    }
}

/// Schedulable task with priority and timing information
#[derive(Debug, Clone)]
pub struct ScheduledTask {
    pub flow_id: FlowId,
    pub priority: Priority,
    pub created_at: Instant,
    pub deadline: Option<Instant>,
    pub estimated_duration: Duration,
    pub retry_count: u32,
    pub task_type: TaskType,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TaskType {
    FlowCreation,
    ReportProcessing,
    AlgorithmExecution,
    FlowCleanup,
    Maintenance,
}

impl PartialEq for ScheduledTask {
    fn eq(&self, other: &Self) -> bool {
        self.flow_id == other.flow_id && self.task_type == other.task_type
    }
}

impl Eq for ScheduledTask {}

impl PartialOrd for ScheduledTask {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScheduledTask {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Higher priority first, then earlier deadline, then earlier creation time
        self.priority
            .cmp(&other.priority)
            .then_with(|| match (self.deadline, other.deadline) {
                (Some(a), Some(b)) => a.cmp(&b),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            })
            .then_with(|| self.created_at.cmp(&other.created_at))
    }
}

/// Advanced scheduler with multiple scheduling algorithms
pub struct FlowScheduler {
    /// Priority queue for tasks
    task_queue: Arc<RwLock<BinaryHeap<ScheduledTask>>>,
    /// Round-robin queues per priority level
    round_robin_queues: Arc<RwLock<HashMap<Priority, VecDeque<ScheduledTask>>>>,
    /// Worker load tracking
    worker_loads: Arc<RwLock<Vec<WorkerLoad>>>,
    /// Scheduling configuration
    config: SchedulerConfig,
    /// Statistics
    stats: SchedulerStats,
    /// Current scheduling algorithm
    algorithm: SchedulingAlgorithm,
}

#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    pub worker_count: usize,
    pub max_queue_size: usize,
    pub load_balancing_interval_ms: u64,
    pub priority_boost_threshold: u32,
    pub starvation_prevention_ms: u64,
    pub deadline_enforcement: bool,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            worker_count: num_cpus::get(),
            max_queue_size: 10000,
            load_balancing_interval_ms: 100,
            priority_boost_threshold: 10,
            starvation_prevention_ms: 1000,
            deadline_enforcement: true,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum SchedulingAlgorithm {
    /// First-Come-First-Served
    FCFS,
    /// Priority-based scheduling
    Priority,
    /// Round-robin with priority levels
    RoundRobin,
    /// Shortest Job First
    SJF,
    /// Earliest Deadline First
    EDF,
    /// Completely Fair Scheduler (Linux CFS-inspired)
    CFS,
}

/// Worker load information
#[derive(Debug, Clone)]
pub struct WorkerLoad {
    pub worker_id: usize,
    pub active_tasks: usize,
    pub total_execution_time: Duration,
    pub last_task_completion: Instant,
    pub load_factor: f64,
}

impl WorkerLoad {
    pub fn new(worker_id: usize) -> Self {
        Self {
            worker_id,
            active_tasks: 0,
            total_execution_time: Duration::ZERO,
            last_task_completion: Instant::now(),
            load_factor: 0.0,
        }
    }

    pub fn update_load(&mut self, task_duration: Duration) {
        self.active_tasks = self.active_tasks.saturating_sub(1);
        self.total_execution_time += task_duration;
        self.last_task_completion = Instant::now();

        // Simple load factor calculation
        let time_since_last = self.last_task_completion.elapsed().as_secs_f64();
        self.load_factor = self.active_tasks as f64 / (1.0 + time_since_last);
    }
}

/// Scheduler statistics
#[derive(Debug, Default)]
pub struct SchedulerStats {
    pub tasks_scheduled: AtomicU64,
    pub tasks_completed: AtomicU64,
    pub tasks_dropped: AtomicU64,
    pub average_wait_time_us: AtomicU64,
    pub priority_inversions: AtomicU64,
    pub deadline_misses: AtomicU64,
    pub load_balancing_operations: AtomicU64,
}

impl FlowScheduler {
    pub fn new(config: SchedulerConfig, algorithm: SchedulingAlgorithm) -> Self {
        let worker_loads = (0..config.worker_count).map(WorkerLoad::new).collect();

        let mut round_robin_queues = HashMap::new();
        round_robin_queues.insert(Priority::Low, VecDeque::new());
        round_robin_queues.insert(Priority::Normal, VecDeque::new());
        round_robin_queues.insert(Priority::High, VecDeque::new());
        round_robin_queues.insert(Priority::Critical, VecDeque::new());

        Self {
            task_queue: Arc::new(RwLock::new(BinaryHeap::new())),
            round_robin_queues: Arc::new(RwLock::new(round_robin_queues)),
            worker_loads: Arc::new(RwLock::new(worker_loads)),
            config,
            stats: SchedulerStats::default(),
            algorithm,
        }
    }

    /// Schedule a new task
    pub async fn schedule_task(&self, task: ScheduledTask) -> Result<()> {
        // Check queue capacity
        let current_size = match self.algorithm {
            SchedulingAlgorithm::RoundRobin => {
                let queues = self.round_robin_queues.read().await;
                queues.values().map(|q| q.len()).sum()
            }
            _ => {
                let queue = self.task_queue.read().await;
                queue.len()
            }
        };

        if current_size >= self.config.max_queue_size {
            self.stats.tasks_dropped.fetch_add(1, Ordering::Relaxed);
            warn!(
                flow_id = %task.flow_id,
                queue_size = current_size,
                "Dropping task due to queue overflow"
            );
            return Err(crate::LotusError::Runtime(
                "Scheduler queue overflow".to_string(),
            ));
        }

        // Add task to appropriate queue based on algorithm
        match self.algorithm {
            SchedulingAlgorithm::RoundRobin => {
                let mut queues = self.round_robin_queues.write().await;
                if let Some(queue) = queues.get_mut(&task.priority) {
                    queue.push_back(task.clone());
                }
            }
            _ => {
                let mut queue = self.task_queue.write().await;
                queue.push(task.clone());
            }
        }

        self.stats.tasks_scheduled.fetch_add(1, Ordering::Relaxed);
        debug!(
            flow_id = %task.flow_id,
            priority = ?task.priority,
            task_type = ?task.task_type,
            "Scheduled task"
        );

        Ok(())
    }

    /// Get the next task for execution
    pub async fn get_next_task(&self, worker_id: usize) -> Option<ScheduledTask> {
        match self.algorithm {
            SchedulingAlgorithm::FCFS => self.get_next_fcfs().await,
            SchedulingAlgorithm::Priority => self.get_next_priority().await,
            SchedulingAlgorithm::RoundRobin => self.get_next_round_robin().await,
            SchedulingAlgorithm::SJF => self.get_next_sjf().await,
            SchedulingAlgorithm::EDF => self.get_next_edf().await,
            SchedulingAlgorithm::CFS => self.get_next_cfs(worker_id).await,
        }
    }

    async fn get_next_fcfs(&self) -> Option<ScheduledTask> {
        let mut queue = self.task_queue.write().await;
        queue.pop()
    }

    async fn get_next_priority(&self) -> Option<ScheduledTask> {
        let mut queue = self.task_queue.write().await;
        queue.pop()
    }

    async fn get_next_round_robin(&self) -> Option<ScheduledTask> {
        let mut queues = self.round_robin_queues.write().await;

        // Check priorities in order: Critical, High, Normal, Low
        for priority in [
            Priority::Critical,
            Priority::High,
            Priority::Normal,
            Priority::Low,
        ] {
            if let Some(queue) = queues.get_mut(&priority) {
                if let Some(task) = queue.pop_front() {
                    return Some(task);
                }
            }
        }
        None
    }

    async fn get_next_sjf(&self) -> Option<ScheduledTask> {
        let mut queue = self.task_queue.write().await;

        // Find task with shortest estimated duration
        let mut shortest_task = None;
        let mut shortest_duration = Duration::MAX;

        // Convert heap to vector, find shortest, rebuild heap
        let mut tasks: Vec<_> = queue.drain().collect();

        for (i, task) in tasks.iter().enumerate() {
            if task.estimated_duration < shortest_duration {
                shortest_duration = task.estimated_duration;
                shortest_task = Some(i);
            }
        }

        if let Some(index) = shortest_task {
            let task = tasks.remove(index);
            // Rebuild heap with remaining tasks
            for remaining_task in tasks {
                queue.push(remaining_task);
            }
            Some(task)
        } else {
            None
        }
    }

    async fn get_next_edf(&self) -> Option<ScheduledTask> {
        let mut queue = self.task_queue.write().await;

        // Find task with earliest deadline
        let mut earliest_task = None;
        let mut earliest_deadline = None;

        let mut tasks: Vec<_> = queue.drain().collect();

        for (i, task) in tasks.iter().enumerate() {
            match (task.deadline, earliest_deadline) {
                (Some(deadline), Some(current_earliest)) => {
                    if deadline < current_earliest {
                        earliest_deadline = Some(deadline);
                        earliest_task = Some(i);
                    }
                }
                (Some(deadline), None) => {
                    earliest_deadline = Some(deadline);
                    earliest_task = Some(i);
                }
                _ => {}
            }
        }

        if let Some(index) = earliest_task {
            let task = tasks.remove(index);
            // Rebuild heap
            for remaining_task in tasks {
                queue.push(remaining_task);
            }
            Some(task)
        } else {
            // No tasks with deadlines, fall back to priority
            queue.extend(tasks);
            queue.pop()
        }
    }

    async fn get_next_cfs(&self, worker_id: usize) -> Option<ScheduledTask> {
        // Simplified CFS implementation
        // In a real implementation, this would use virtual runtime tracking
        let worker_loads = self.worker_loads.read().await;
        let current_load = worker_loads.get(worker_id)?.load_factor;

        let mut queue = self.task_queue.write().await;

        // Prefer tasks that would balance the load
        let mut best_task = None;
        let mut best_score = f64::MIN;
        let mut tasks: Vec<_> = queue.drain().collect();

        for (i, task) in tasks.iter().enumerate() {
            // Score based on priority and load balancing
            let priority_score = task.priority as u8 as f64;
            let load_balance_score = 1.0 / (1.0 + current_load);
            let age_score = task.created_at.elapsed().as_secs_f64() / 10.0;

            let total_score = priority_score + load_balance_score + age_score;

            if total_score > best_score {
                best_score = total_score;
                best_task = Some(i);
            }
        }

        if let Some(index) = best_task {
            let task = tasks.remove(index);
            // Rebuild heap
            for remaining_task in tasks {
                queue.push(remaining_task);
            }
            Some(task)
        } else {
            None
        }
    }

    /// Mark a task as completed and update worker load
    pub async fn complete_task(
        &self,
        worker_id: usize,
        task: &ScheduledTask,
        execution_time: Duration,
    ) {
        self.stats.tasks_completed.fetch_add(1, Ordering::Relaxed);

        // Update worker load
        if let Ok(mut worker_loads) = self.worker_loads.try_write() {
            if let Some(worker_load) = worker_loads.get_mut(worker_id) {
                worker_load.update_load(execution_time);
            }
        }

        // Update average wait time
        let total_elapsed = task.created_at.elapsed();
        let wait_time = total_elapsed.saturating_sub(execution_time);
        let wait_time_us = wait_time.as_micros() as u64;

        // Simple moving average (in a real implementation, use a more sophisticated method)
        let current_avg = self.stats.average_wait_time_us.load(Ordering::Relaxed);
        let new_avg = (current_avg + wait_time_us) / 2;
        self.stats
            .average_wait_time_us
            .store(new_avg, Ordering::Relaxed);

        // Check for deadline miss
        if let Some(deadline) = task.deadline {
            if Instant::now() > deadline {
                self.stats.deadline_misses.fetch_add(1, Ordering::Relaxed);
                warn!(
                    flow_id = %task.flow_id,
                    deadline_miss_ms = deadline.elapsed().as_millis() as u64,
                    "Task missed deadline"
                );
            }
        }

        debug!(
            flow_id = %task.flow_id,
            worker_id = worker_id,
            execution_time_us = execution_time.as_micros() as u64,
            wait_time_us = wait_time_us,
            "Task completed"
        );
    }

    /// Get scheduler statistics
    pub fn get_stats(&self) -> SchedulerStats {
        SchedulerStats {
            tasks_scheduled: AtomicU64::new(self.stats.tasks_scheduled.load(Ordering::Relaxed)),
            tasks_completed: AtomicU64::new(self.stats.tasks_completed.load(Ordering::Relaxed)),
            tasks_dropped: AtomicU64::new(self.stats.tasks_dropped.load(Ordering::Relaxed)),
            average_wait_time_us: AtomicU64::new(
                self.stats.average_wait_time_us.load(Ordering::Relaxed),
            ),
            priority_inversions: AtomicU64::new(
                self.stats.priority_inversions.load(Ordering::Relaxed),
            ),
            deadline_misses: AtomicU64::new(self.stats.deadline_misses.load(Ordering::Relaxed)),
            load_balancing_operations: AtomicU64::new(
                self.stats.load_balancing_operations.load(Ordering::Relaxed),
            ),
        }
    }

    /// Get current queue sizes
    pub async fn get_queue_sizes(&self) -> HashMap<String, usize> {
        let mut sizes = HashMap::new();

        match self.algorithm {
            SchedulingAlgorithm::RoundRobin => {
                let queues = self.round_robin_queues.read().await;
                for (priority, queue) in queues.iter() {
                    sizes.insert(format!("{:?}", priority), queue.len());
                }
            }
            _ => {
                let queue = self.task_queue.read().await;
                sizes.insert("main".to_string(), queue.len());
            }
        }

        sizes
    }

    /// Perform load balancing across workers
    pub async fn balance_load(&self) -> Result<()> {
        let worker_loads = self.worker_loads.read().await;

        // Find most and least loaded workers
        let mut max_load = 0.0;
        let mut min_load = f64::MAX;
        let mut max_worker = 0;
        let mut min_worker = 0;

        for (i, load) in worker_loads.iter().enumerate() {
            if load.load_factor > max_load {
                max_load = load.load_factor;
                max_worker = i;
            }
            if load.load_factor < min_load {
                min_load = load.load_factor;
                min_worker = i;
            }
        }

        // If load imbalance is significant, trigger rebalancing
        let load_imbalance = max_load - min_load;
        if load_imbalance > 0.5 {
            self.stats
                .load_balancing_operations
                .fetch_add(1, Ordering::Relaxed);
            info!(
                max_worker = max_worker,
                min_worker = min_worker,
                load_imbalance = load_imbalance,
                "Performing load balancing"
            );

            // In a real implementation, you would move tasks between workers
            // For now, just log the operation
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_priority_scheduling() {
        let config = SchedulerConfig::default();
        let scheduler = FlowScheduler::new(config, SchedulingAlgorithm::Priority);

        // Create tasks with different priorities
        let low_task = ScheduledTask {
            flow_id: FlowId::new(),
            priority: Priority::Low,
            created_at: Instant::now(),
            deadline: None,
            estimated_duration: Duration::from_millis(100),
            retry_count: 0,
            task_type: TaskType::ReportProcessing,
        };

        let high_task = ScheduledTask {
            flow_id: FlowId::new(),
            priority: Priority::High,
            created_at: Instant::now(),
            deadline: None,
            estimated_duration: Duration::from_millis(50),
            retry_count: 0,
            task_type: TaskType::FlowCreation,
        };

        // Schedule tasks
        scheduler.schedule_task(low_task).await.unwrap();
        scheduler.schedule_task(high_task.clone()).await.unwrap();

        // High priority task should be returned first
        let next_task = scheduler.get_next_task(0).await.unwrap();
        assert_eq!(next_task.priority, Priority::High);
        assert_eq!(next_task.flow_id, high_task.flow_id);
    }

    #[tokio::test]
    async fn test_round_robin_scheduling() {
        let config = SchedulerConfig::default();
        let scheduler = FlowScheduler::new(config, SchedulingAlgorithm::RoundRobin);

        // Create multiple tasks with same priority
        for i in 0..5 {
            let task = ScheduledTask {
                flow_id: FlowId::new(),
                priority: Priority::Normal,
                created_at: Instant::now(),
                deadline: None,
                estimated_duration: Duration::from_millis(100),
                retry_count: 0,
                task_type: TaskType::ReportProcessing,
            };
            scheduler.schedule_task(task).await.unwrap();
        }

        // Should be able to get all tasks
        for _ in 0..5 {
            assert!(scheduler.get_next_task(0).await.is_some());
        }

        // Queue should be empty now
        assert!(scheduler.get_next_task(0).await.is_none());
    }

    #[tokio::test]
    async fn test_scheduler_stats() {
        let config = SchedulerConfig::default();
        let scheduler = FlowScheduler::new(config, SchedulingAlgorithm::Priority);

        let task = ScheduledTask {
            flow_id: FlowId::new(),
            priority: Priority::Normal,
            created_at: Instant::now(),
            deadline: None,
            estimated_duration: Duration::from_millis(100),
            retry_count: 0,
            task_type: TaskType::ReportProcessing,
        };

        scheduler.schedule_task(task.clone()).await.unwrap();
        let retrieved_task = scheduler.get_next_task(0).await.unwrap();
        scheduler
            .complete_task(0, &retrieved_task, Duration::from_millis(50))
            .await;

        let stats = scheduler.get_stats();
        assert_eq!(stats.tasks_scheduled.load(Ordering::Relaxed), 1);
        assert_eq!(stats.tasks_completed.load(Ordering::Relaxed), 1);
    }
}
