//! DTCC算法在Portus vs Lotus下的性能对比
//!
//! 这个示例展示了Lotus在处理外部RPC调用时相比Portus的性能优势

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, warn};

/// 模拟外部RPC调用的延迟
async fn simulate_rpc_call(processing_time_ms: u64) -> u64 {
    tokio::time::sleep(Duration::from_millis(processing_time_ms)).await;
    42 // 模拟返回的cwnd值
}

/// 模拟Portus的同步处理方式
fn simulate_portus_processing(
    flow_count: usize,
    reports_per_flow: usize,
    rpc_latency_ms: u64,
) -> (Duration, u64) {
    let start = Instant::now();
    let mut total_reports = 0;

    // Portus: 单线程串行处理所有流
    for flow_id in 0..flow_count {
        for report_id in 0..reports_per_flow {
            // 模拟报告处理
            std::thread::sleep(Duration::from_micros(100)); // 基础处理时间

            // 模拟同步RPC调用（阻塞）
            std::thread::sleep(Duration::from_millis(rpc_latency_ms));

            total_reports += 1;

            if report_id % 10 == 0 {
                println!(
                    "Portus: Flow {} processed {} reports",
                    flow_id,
                    report_id + 1
                );
            }
        }
    }

    (start.elapsed(), total_reports)
}

/// 模拟Lotus的异步并行处理方式
async fn simulate_lotus_processing(
    flow_count: usize,
    reports_per_flow: usize,
    rpc_latency_ms: u64,
) -> (Duration, u64) {
    let start = Instant::now();
    let total_reports = Arc::new(AtomicU64::new(0));

    // Lotus: 并行处理所有流
    let mut flow_tasks = Vec::new();

    for flow_id in 0..flow_count {
        let reports_counter = total_reports.clone();
        let task = tokio::spawn(async move {
            for report_id in 0..reports_per_flow {
                // 模拟报告处理
                tokio::time::sleep(Duration::from_micros(100)).await;

                // 模拟异步RPC调用（非阻塞）
                let _cwnd = simulate_rpc_call(rpc_latency_ms).await;

                reports_counter.fetch_add(1, Ordering::Relaxed);

                if report_id % 10 == 0 {
                    println!(
                        "Lotus: Flow {} processed {} reports",
                        flow_id,
                        report_id + 1
                    );
                }
            }
        });
        flow_tasks.push(task);
    }

    // 等待所有流处理完成
    for task in flow_tasks {
        if let Err(e) = task.await {
            warn!("Flow task failed: {}", e);
        }
    }

    let final_reports = total_reports.load(Ordering::Relaxed);
    (start.elapsed(), final_reports)
}

/// 模拟高负载场景下的性能对比
async fn run_performance_comparison() {
    println!("🚀 DTCC Performance Comparison: Portus vs Lotus");
    println!("================================================");

    let test_scenarios = vec![
        (5, 20, 5),   // 5 flows, 20 reports each, 5ms RPC latency
        (10, 15, 10), // 10 flows, 15 reports each, 10ms RPC latency
        (20, 10, 15), // 20 flows, 10 reports each, 15ms RPC latency
    ];

    for (flow_count, reports_per_flow, rpc_latency_ms) in test_scenarios {
        println!("\n📊 Test Scenario:");
        println!("   • Flows: {}", flow_count);
        println!("   • Reports per flow: {}", reports_per_flow);
        println!("   • RPC latency: {}ms", rpc_latency_ms);
        println!("   • Total reports: {}", flow_count * reports_per_flow);

        // 测试Portus性能
        println!("\n🔄 Testing Portus (synchronous)...");
        let (portus_duration, portus_reports) =
            simulate_portus_processing(flow_count, reports_per_flow, rpc_latency_ms);
        let portus_throughput = portus_reports as f64 / portus_duration.as_secs_f64();

        println!("✅ Portus Results:");
        println!("   • Duration: {:.2}s", portus_duration.as_secs_f64());
        println!("   • Reports processed: {}", portus_reports);
        println!("   • Throughput: {:.1} reports/sec", portus_throughput);

        // 测试Lotus性能
        println!("\n🚀 Testing Lotus (asynchronous)...");
        let (lotus_duration, lotus_reports) =
            simulate_lotus_processing(flow_count, reports_per_flow, rpc_latency_ms).await;
        let lotus_throughput = lotus_reports as f64 / lotus_duration.as_secs_f64();

        println!("✅ Lotus Results:");
        println!("   • Duration: {:.2}s", lotus_duration.as_secs_f64());
        println!("   • Reports processed: {}", lotus_reports);
        println!("   • Throughput: {:.1} reports/sec", lotus_throughput);

        // 计算性能提升
        let speedup = lotus_throughput / portus_throughput;
        let time_saved = portus_duration.as_secs_f64() - lotus_duration.as_secs_f64();
        let time_saved_percent = (time_saved / portus_duration.as_secs_f64()) * 100.0;

        println!("\n🎉 Performance Improvement:");
        println!("   • Speedup: {:.1}x faster", speedup);
        println!(
            "   • Time saved: {:.2}s ({:.1}%)",
            time_saved, time_saved_percent
        );

        println!("\n{}", "=".repeat(60));
    }
}

/// 模拟网络延迟对性能的影响
async fn analyze_latency_impact() {
    println!("\n📈 Latency Impact Analysis");
    println!("==========================");

    let flow_count = 10;
    let reports_per_flow = 10;
    let latencies = vec![1, 5, 10, 20, 50]; // ms

    println!(
        "Fixed: {} flows, {} reports per flow",
        flow_count, reports_per_flow
    );
    println!("\nRPC Latency Impact:");
    println!(
        "{:<12} {:<15} {:<15} {:<10}",
        "Latency(ms)", "Portus(s)", "Lotus(s)", "Speedup"
    );
    println!("{}", "-".repeat(55));

    for latency_ms in latencies {
        // Portus测试
        let (portus_duration, _) =
            simulate_portus_processing(flow_count, reports_per_flow, latency_ms);

        // Lotus测试
        let (lotus_duration, _) =
            simulate_lotus_processing(flow_count, reports_per_flow, latency_ms).await;

        let speedup = portus_duration.as_secs_f64() / lotus_duration.as_secs_f64();

        println!(
            "{:<12} {:<15.2} {:<15.2} {:<10.1}x",
            latency_ms,
            portus_duration.as_secs_f64(),
            lotus_duration.as_secs_f64(),
            speedup
        );
    }
}

/// 模拟并发流数量对性能的影响
async fn analyze_concurrency_impact() {
    println!("\n📊 Concurrency Impact Analysis");
    println!("===============================");

    let reports_per_flow = 10;
    let rpc_latency_ms = 10;
    let flow_counts = vec![1, 5, 10, 20, 50];

    println!(
        "Fixed: {} reports per flow, {}ms RPC latency",
        reports_per_flow, rpc_latency_ms
    );
    println!("\nConcurrency Scaling:");
    println!(
        "{:<12} {:<15} {:<15} {:<15} {:<10}",
        "Flows", "Portus(rps)", "Lotus(rps)", "Efficiency", "Speedup"
    );
    println!("{}", "-".repeat(70));

    for flow_count in flow_counts {
        // Portus测试
        let (portus_duration, portus_reports) =
            simulate_portus_processing(flow_count, reports_per_flow, rpc_latency_ms);
        let portus_throughput = portus_reports as f64 / portus_duration.as_secs_f64();

        // Lotus测试
        let (lotus_duration, lotus_reports) =
            simulate_lotus_processing(flow_count, reports_per_flow, rpc_latency_ms).await;
        let lotus_throughput = lotus_reports as f64 / lotus_duration.as_secs_f64();

        let speedup = lotus_throughput / portus_throughput;
        let efficiency = (lotus_throughput / flow_count as f64)
            / (lotus_throughput / 1.0).min(portus_throughput / 1.0);

        println!(
            "{:<12} {:<15.1} {:<15.1} {:<15.2} {:<10.1}x",
            flow_count, portus_throughput, lotus_throughput, efficiency, speedup
        );
    }
}

#[tokio::main]
async fn main() {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    // 运行性能对比测试
    run_performance_comparison().await;

    // 分析延迟影响
    analyze_latency_impact().await;

    // 分析并发影响
    analyze_concurrency_impact().await;

    println!("\n🎯 Key Takeaways:");
    println!("================");
    println!("1. Lotus的异步架构在RPC调用场景下显著优于Portus");
    println!("2. 随着RPC延迟增加，Lotus的优势更加明显");
    println!("3. 并发流数量越多，Lotus的并行处理优势越突出");
    println!("4. Lotus能够充分利用等待时间处理其他流，提高整体吞吐量");
    println!("5. 在实际网络环境中，这种性能提升对用户体验至关重要");

    println!("\n💡 Recommendations:");
    println!("===================");
    println!("• 对于需要外部RPC调用的算法，强烈推荐使用Lotus");
    println!("• 利用Lotus的异步特性可以显著提高系统响应性");
    println!("• 在高并发场景下，Lotus的性能优势更加明显");
    println!("• 可以通过兼容层平滑迁移现有Portus算法");
}
