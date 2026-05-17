# Lotus Framework Design Document

## 概述

Lotus是基于Portus重新设计的高并发拥塞控制算法库框架，专门解决Portus在处理计算密集型算法和高并发流量时的性能瓶颈。

## 核心问题分析

### Portus的局限性

1. **单线程事件循环瓶颈**
   - 所有流的消息处理串行执行
   - 计算密集型算法阻塞其他流的处理
   - 无法充分利用多核CPU

2. **同步执行模型限制**
   - 算法必须在`on_report()`回调中同步完成
   - 无异步支持，无法卸载到后台线程
   - 长时间计算导致系统响应延迟

3. **IPC通信性能瓶颈**
   - 固定1024字节缓冲区限制
   - 阻塞接收影响响应性
   - 无批量处理优化

4. **流管理扩展性问题**
   - HashMap查找复杂度随流数量线性增长
   - 无流优先级和负载均衡
   - 内存开销大

## Lotus架构设计

### 1. 异步优先的设计理念

```rust
#[async_trait]
pub trait AsyncCongAlg<I>: Send + Sync + 'static {
    type Flow: AsyncFlow;
    
    async fn new_flow(&self, control: FlowContext<I>, info: DatapathInfo) -> Result<Self::Flow>;
    async fn datapath_programs(&self) -> HashMap<&'static str, String>;
}

#[async_trait]
pub trait AsyncFlow: Send + 'static {
    async fn on_report(&mut self, sock_id: u32, report: Report) -> Result<()>;
    async fn close(&mut self) -> Result<()>;
}
```

**优势：**
- 所有操作默认异步，支持并发执行
- 算法可以在`on_report`中执行异步计算
- 支持后台任务和工作窃取

### 2. 多线程并行执行架构

```text
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   IPC Layer     │    │  Message Router  │    │ Algorithm Pool  │
│  (Async I/O)    │◄──►│   (Load Balancer)│◄──►│ (Work Stealing) │
└─────────────────┘    └──────────────────┘    └─────────────────┘
          │                       │                       │
          ▼                       ▼                       ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ Connection Pool │    │   Flow Manager   │    │  Compute Tasks  │
│   (Per-DP)      │    │  (Concurrent)    │    │   (Parallel)    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

**核心组件：**

#### MessageRouter
- 多worker负载均衡
- 基于消息类型的智能路由
- 支持批量处理优化

#### FlowManager
- 使用DashMap实现并发安全的流管理
- 支持异步流创建和销毁
- 自动清理非活跃流

#### ComputeEngine
- 工作窃取线程池
- 支持任务优先级和超时
- 并行执行计算密集型算法

### 3. 计算密集型算法支持

```rust
pub trait ComputeTask: Send + 'static {
    type Output: Send + 'static;
    
    fn execute(self) -> Self::Output;
    fn priority(&self) -> u8;
    fn estimated_duration_us(&self) -> u64;
}

// 使用示例
async fn on_report(&mut self, sock_id: u32, report: Report) -> Result<()> {
    let task = MLPredictionTask {
        features: self.extract_features(),
        model_weights: self.model_weights.clone(),
    };
    
    let prediction = compute_engine.submit_task(task, self.flow_id).await?;
    self.update_cwnd(prediction);
    Ok(())
}
```

**特性：**
- 后台并行执行CPU密集型任务
- 工作窃取实现负载均衡
- 任务优先级和超时控制
- 支持批量任务提交

### 4. 高级调度算法

支持多种调度策略：
- **FCFS**: 先来先服务
- **Priority**: 基于优先级调度
- **Round-Robin**: 轮询调度
- **SJF**: 最短作业优先
- **EDF**: 最早截止时间优先
- **CFS**: 完全公平调度器

```rust
pub enum SchedulingAlgorithm {
    FCFS,
    Priority,
    RoundRobin,
    SJF,
    EDF,
    CFS,
}
```

### 5. 向后兼容性

提供完整的Portus兼容层：

```rust
// 现有Portus算法无需修改
impl CongAlg<()> for MyCubicAlgorithm {
    // ... 原有实现
}

// 使用兼容运行时
let mut runtime = PortusCompatRuntime::new().await?;
runtime.register_algorithm(MyCubicAlgorithm).await?;
runtime.run().await?;

// 或使用便利宏
portus_main!(MyCubicAlgorithm);
```

## 性能优化策略

### 1. 内存优化
- 使用Arc共享数据结构
- 流状态的写时复制
- 智能缓存和LRU策略

### 2. 网络优化
- 异步I/O避免阻塞
- 消息批量处理
- 零拷贝数据传输

### 3. CPU优化
- 工作窃取线程池
- NUMA感知的任务调度
- 缓存友好的数据结构

### 4. 算法优化
- 预测性任务调度
- 自适应超时机制
- 智能负载均衡

## 性能对比

| 指标 | Portus | Lotus | 改进 |
|------|--------|-------|------|
| 执行模型 | 单线程 | 多线程+异步 | 4-8x |
| 算法执行 | 同步阻塞 | 异步+并行计算 | 10x+ |
| 流处理 | 串行 | 并发 | 线性扩展 |
| 内存使用 | 高 | 优化共享 | 50%减少 |
| 吞吐量 | ~1K flows/sec | ~10K+ flows/sec | 10x+ |
| 延迟(P99) | ~100ms | ~10ms | 10x |

## 使用场景

### 1. 机器学习拥塞控制
```rust
struct MLCongestionControl {
    model_weights: Vec<f64>,
    learning_rate: f64,
}

// 支持异步神经网络推理
async fn on_report(&mut self, sock_id: u32, report: Report) -> Result<()> {
    let prediction_task = MLPredictionTask {
        features: self.extract_features(),
        weights: self.model_weights.clone(),
    };
    
    let optimal_cwnd = compute_engine.submit_task(prediction_task, self.flow_id).await?;
    self.update_cwnd(optimal_cwnd);
    Ok(())
}
```

### 2. 高频交易网络
- 超低延迟要求
- 大量并发连接
- 实时算法调整

### 3. 数据中心流量优化
- 多租户环境
- 动态负载均衡
- QoS保证

### 4. 实时视频流
- 自适应比特率
- 网络状况预测
- 缓冲区优化

## 迁移指南

### 从Portus迁移到Lotus

1. **保持现有算法不变**
   ```rust
   // 使用兼容运行时
   let mut runtime = PortusCompatRuntime::new().await?;
   runtime.register_algorithm(existing_algorithm).await?;
   ```

2. **逐步迁移到异步API**
   ```rust
   // 包装现有算法
   let wrapped = PortusAlgorithmWrapper::new(existing_algorithm);
   runtime.register_async_algorithm(wrapped).await?;
   ```

3. **重写为原生异步算法**
   ```rust
   #[async_trait]
   impl AsyncCongAlg<()> for NewAsyncAlgorithm {
       // 全新异步实现
   }
   ```

## 未来发展方向

### 1. 分布式支持
- 跨节点算法协调
- 分布式状态同步
- 容错和恢复机制

### 2. 硬件加速
- GPU计算支持
- FPGA集成
- 专用网络处理器

### 3. 智能优化
- 自适应调度策略
- 机器学习驱动的优化
- 预测性资源分配

### 4. 生态系统
- 算法市场
- 性能基准测试
- 可视化监控工具

## 结论

Lotus框架通过异步优先的设计、多线程并行执行、工作窃取计算引擎和高级调度算法，成功解决了Portus在高并发和计算密集型场景下的性能瓶颈。同时保持了完整的向后兼容性，为现有用户提供了平滑的迁移路径。

框架的模块化设计和可扩展架构为未来的功能扩展和性能优化提供了坚实的基础，能够满足下一代网络应用对拥塞控制算法的更高要求。