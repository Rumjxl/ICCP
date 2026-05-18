import numpy as np
from time import sleep
from gym import spaces
import asyncio
import socket
import capnp
import os
import time 
import functools
import threading
import subprocess as sh
import multiprocessing
from concurrent.futures import Future, ThreadPoolExecutor
from collections import defaultdict
import queue
import traceback
import faulthandler
# from dtcc_agent import Prims
import tensorflow as tf
import psutil

# TODO: schema and client need abstract for different cca
capnp.remove_import_hook()
schema_name = os.environ.get("DTCC_CCP_SCHEMA", "ccp_dtcc.capnp")
schema_path = schema_name if os.path.isabs(schema_name) else os.path.join(os.getcwd(), "schema", schema_name)
ccp_capnp = capnp.load(schema_path)
print(f"[cc.py] loaded Cap'n Proto schema: {schema_path}", flush=True)

async def kj_loop():
    async with capnp.kj_loop() as loop:
        await loop

class BatchProcessor:
    def __init__(self, agent, batch_size=8, timeout=0.005, stale_timeout=0.030):
        """
        初始化批处理器
        参数:
        agent: 预测模型代理
        batch_size: 最大批处理大小
        timeout: 最大等待时间(秒)
        stale_timeout: 请求在队列中的最大存活时间(秒)，超过则视为 Rust 已放弃等待，直接丢弃
        """
        self.agent = agent
        self.batch_size = batch_size
        self.timeout = timeout
        # stale_timeout 应略小于 Rust 侧的 resp_timeout，确保过期请求不被无效推理
        self.stale_timeout = stale_timeout
        self.stats = {"total_batches": 0, "total_requests": 0, "total_stale_dropped": 0}
        self.queue = queue.Queue()
        self.batch_event = threading.Event()
        self.batch_lock = threading.Lock()
        self.processing = False
        self._stop_event = threading.Event()  # 修复：将 _stop_event 初始化放入 __init__ 方法
        
        # 启动批量处理线程
        self.batch_thread = threading.Thread(target=self._process_batches)
        # make it non-daemon so we can join it cleanly on shutdown
        self.batch_thread.daemon = False
        self.batch_thread.start()
    
    def add_request(self, observation, prims, connection_id, future, loop):
        """添加请求到批量处理队列"""
        timestamp = time.time()
        with self.batch_lock:
            self.queue.put({
                'observation': observation,
                'prims': prims,
                'connection_id': connection_id,
                'future': future,
                'loop': loop,
                'timestamp': timestamp
            })
            # 只有队列凑满 batch_size 时才立即触发，否则让 timeout 自然等待积累。
            # 这样批处理线程能等到更多请求到达后再一起推理，提升 batch 利用率。
            if self.queue.qsize() >= self.batch_size:
                self.batch_event.set()
    
    def _process_batches(self):
        """批量处理线程的主函数"""
        while not self._stop_event.is_set():
            # 等待批量处理信号或超时（也会在 stop 时被唤醒）
            self.batch_event.wait(self.timeout)
            # 清除事件信号
            self.batch_event.clear()

            if self._stop_event.is_set():
                break

            batch = []
            try:
                # 只取最多 batch_size 个，剩余的留在队列里，下一轮立即继续处理
                # 不在锁内做推理，只做 O(1) 的出队操作
                with self.batch_lock:
                    while len(batch) < self.batch_size and not self.queue.empty():
                        batch.append(self.queue.get())

                    # 如果队列中没有请求，继续等待
                    if not batch:
                        continue

                    # 按时间戳排序，确保先进先出
                    batch.sort(key=lambda x: x['timestamp'])

                    # 若队列仍有剩余，立即再次触发处理（不等超时）
                    if not self.queue.empty():
                        self.batch_event.set()

                # ── 过时请求剔除 ─────────────────────────────────────────────
                # 若请求在队列中等待时间已超过 stale_timeout（Rust resp_timeout 略小值），
                # Rust 侧早已放弃等待该响应，继续推理只是白费 CPU 并拖慢后续有效请求。
                # 直接丢弃：回退 prim.iterations，不调用 set_result。
                now = time.time()
                valid_batch = []
                stale_count = 0
                for item in batch:
                    age = now - item['timestamp']
                    if age >= self.stale_timeout:
                        # Rust 已超时放弃，回退 iterations 避免状态错位
                        try:
                            item['prims'].iterations -= 1
                        except Exception:
                            pass
                        self.stats["total_stale_dropped"] += 1
                        stale_count += 1
                        print(f"[BatchProcessor] stale request dropped: conn_id={item['connection_id']} "
                              f"age={age*1000:.1f}ms >= stale_timeout={self.stale_timeout*1000:.0f}ms")
                    else:
                        valid_batch.append(item)
                
                # ── 诊断日志：dequeue 统计 ────────────────────────
                if stale_count > 0:
                    print(f"[BatchProcessor.dequeue] "
                          f"dequeued={len(batch):2d} "
                          f"stale_dropped={stale_count:2d} "
                          f"valid={len(valid_batch):2d}")
                
                batch = valid_batch
                if not batch:
                    continue

                # 在锁外处理批量（推理可能耗时较长）
                self._process_batch(batch)
                self.stats["total_batches"] += 1
                self.stats["total_requests"] += len(batch)
            except Exception as e:
                traceback_str = traceback.format_exc()
                print(f"Error processing batch: {e}\n{traceback_str}")
                # 处理失败时设置每个future的异常
                for item in batch:
                    try:
                        item['future'].set_exception(e)
                    except Exception:
                        pass

        # 清理剩余队列里的请求，告知调用方批处理器已停止
        try:
            while not self.queue.empty():
                item = self.queue.get_nowait()
                try:
                    item['loop'].call_soon_threadsafe(item['future'].set_exception, RuntimeError('BatchProcessor stopped'))
                except Exception:
                    pass
        except Exception:
            pass

        # 线程退出
        # print('BatchProcessor: processing thread exited')

    def stop(self, timeout: float = 5.0):
        """优雅停止批处理器，等待后台线程结束并对未处理请求设置异常。"""
        if self._stop_event.is_set():
            return
        self._stop_event.set()
        # 唤醒等待中的线程
        self.batch_event.set()
        # 等待线程退出
        self.batch_thread.join(timeout)
        if self.batch_thread.is_alive():
            print('BatchProcessor: thread did not exit within timeout')
    
    def _process_batch(self, batch):
        """处理单个批量的请求"""
        if not batch:
            return
        
        # ── 诊断日志：batch 利用率 ──────────────────────────────
        batch_start_time = time.time()
        actual_batch_size = len(batch)
        utilization = (actual_batch_size / self.batch_size) * 100
        
        # print("Batch length:{}".format(len(batch)))
        # 为每个请求计算状态
        states = []
        rewards = []
        conn_ids = []
        futures = []
        loops = []
        prims_list = []
        
        for item in batch:
            # 使用agent的get_state_reward方法转换原始observation
            state, reward = self.agent.get_state_reward(item['observation'], item['prims'])
            if state is None:
                # get_state_reward 返回 None 表示本帧数据无效（如 bytesAcked<=0）
                # 跳过该请求，直接返回上次的 cwnd/rate 默认值，不更新 prim 状态
                print("Batch state get error: invalid observation, skipping with default action")
                # 回退 iterations，防止下次调用因 iterations>1 走 else 分支
                # 但 prim 内部序列（states/actions/rewards）仍未初始化，
                # 导致 else 分支追加后长度比 states 少 1 造成维度不匹配
                item['prims'].iterations -= 1
                def _set_default(fut, loop=item['loop']):
                    if not fut.done():
                        fut.set_result((10, 1000000))
                item['loop'].call_soon_threadsafe(_set_default, item['future'])
                continue
            if reward is None:
                reward = 0.0

            states.append(state)
            rewards.append(reward)
            conn_ids.append(item['connection_id'])
            futures.append(item['future'])
            loops.append(item['loop'])
            prims_list.append(item['prims'])
        
        # 将状态列表转换为numpy数组（若全部被skip则直接返回）
        if not states:
            return
        
        # ── 诊断日志：有效 batch 大小 ────────────────────────────
        valid_batch_size = len(states)
        valid_utilization = (valid_batch_size / self.batch_size) * 100
        skipped_count = actual_batch_size - valid_batch_size
        print(f"[_process_batch] "
              f"dequeued={actual_batch_size:2d} "
              f"(util={utilization:.0f}%) "
              f"→ skipped={skipped_count:2d} "
              f"→ valid={valid_batch_size:2d} "
              f"(util={valid_utilization:.0f}%) "
              f"→ infer")
        
        states_array = np.array(states)
        reward_array = np.array(rewards)
        # 批量预测
        try:
            # print("Batch process: states:{}".format(states_array))
            infer_start = time.time()
            actions , rates= self.agent.batch_predict(states_array,reward_array,conn_ids)
            infer_duration = (time.time() - infer_start) * 1000  # Convert to ms
            
            # ── 诊断日志：推理完成 ────────────────────────────────
            print(f"[batch_predict] N={valid_batch_size} "
                  f"infer_ms={infer_duration:.2f} "
                  f"per_item_ms={infer_duration/valid_batch_size:.2f} "
                  f"success={valid_batch_size}")
            
            # 为每个请求设置结果 - 线程安全方式
            for i, (cwnd, rate) in enumerate(zip(actions, rates)):
                # 使用事件循环在正确的线程中设置结果
                def set_result(fut, cwnd , rate):
                    if not fut.done():
                        fut.set_result((cwnd, rate))  # 假设reward为0
                
                loops[i].call_soon_threadsafe(set_result, futures[i], cwnd, rate)
                
                # print(f"Batch processed for connection {conn_ids[i]}, cwnd_packets: {cwnd}, rate:{rate}")  
        except Exception as e:
            traceback_str = traceback.format_exc()
            print(f"Error in batch prediction: {e}\n{traceback_str}")
            # 只对进入推理的有效请求回填异常；无效帧已在上面返回默认值。
            for loop, fut in zip(loops, futures):
                loop.call_soon_threadsafe(fut.set_exception, e)


# 全局批处理器实例
batch_processor = None

# 用于管理连接状态的字典
connections = defaultdict(lambda: {'last_active_time': time.time(), 'server': None})
max_connections = 300
avg_inference_times = []

RPC_LATENCY_LOG_EVERY = 100
_rpc_latency_by_conn = defaultdict(lambda: {
    "window_count": 0,
    "window_sum_us": 0.0,
    "total_count": 0,
})
_rpc_latency_lock = threading.Lock()


def _monotonic_raw_ns():
    clock = getattr(time, "CLOCK_MONOTONIC_RAW", None)
    if clock is not None:
        return time.clock_gettime_ns(clock)
    return time.monotonic_ns()


def _record_rpc_observation_latency(conn_id, observation, recv_ns):
    try:
        send_ns = int(observation.rpcSendMonoNs)
    except Exception:
        return

    if send_ns <= 0:
        return

    latency_us = (recv_ns - send_ns) / 1000.0
    if latency_us < 0:
        return

    with _rpc_latency_lock:
        stats = _rpc_latency_by_conn[conn_id]
        stats["window_count"] += 1
        stats["window_sum_us"] += latency_us
        stats["total_count"] += 1

        if stats["window_count"] >= RPC_LATENCY_LOG_EVERY:
            avg_us = stats["window_sum_us"] / stats["window_count"]
            print(
                f"[rpc-observation-latency] conn_id={conn_id} "
                f"window_n={stats['window_count']} "
                f"avg_us={avg_us:.2f} "
                f"total_n={stats['total_count']}",
                flush=True,
            )
            stats["window_count"] = 0
            stats["window_sum_us"] = 0.0


class CcpRlAgentImpl(ccp_capnp.RLAgent.Server):
    def __init__(self, agent, connection_id):
        global batch_processor
        
        self.agent = agent
        self.connection_id = connection_id   # TCP 连接级别的 ID（通常只有 1 个）
        # 不再持有单个 self.prims；各流的 prim 由 agent.conn_prims[conn_id] 统一管理
        self.last_active_time = time.time()
        if agent.use_batch:
            if batch_processor is None:
                bs = getattr(agent, '_batch_size_hint', 8)
                _user_bp_timeout_ms = getattr(agent, '_user_bp_timeout_ms', 0)
                if _user_bp_timeout_ms > 0:
                    bp_timeout = _user_bp_timeout_ms * 0.001
                else:
                    _mtp = getattr(agent, 'mtp', 10)
                    _nf  = max(1, int(bs))
                    bp_timeout = max(0.001, _mtp * (1.0 - 1.0 / _nf) * 0.001 * 0.7)
                # stale_timeout：来自 Rust resp_timeout（由 init_ccprlagent_env 写入 agent）
                # 设为 resp_timeout 的 90%，确保在 Rust 放弃之前就丢弃过时请求
                rust_resp_timeout_ms = getattr(agent, '_rust_resp_timeout_ms', 30)
                stale_timeout = rust_resp_timeout_ms * 0.001 * 0.9  # 转换为秒，取 90%
                batch_processor = BatchProcessor(agent, batch_size=bs, timeout=bp_timeout,
                                                 stale_timeout=stale_timeout)
                _src = f"user({_user_bp_timeout_ms}ms)" if _user_bp_timeout_ms > 0 else f"mtp({_mtp}ms) × (1-1/{_nf}) × 0.7"
                print(f"[BatchProcessor] created: batch_size={bs}, "
                      f"timeout={bp_timeout*1000:.2f}ms "
                      f"(= {_src}), "
                      f"stale_timeout={stale_timeout*1000:.1f}ms")
        
        print(f"New agent instance for connection {connection_id}")

    def _get_or_create_prim(self, conn_id, is_new_flow=False):
        """按流 conn_id 懒建 prim，线程安全（GIL 保护）。
        
        is_new_flow=True 时（Rust 端流刚刚创建），强制重建 prim，
        重置 iterations/timesteps 等历史状态。
        """
        if conn_id in self.agent.conn_prims:
            if is_new_flow:
                del self.agent.conn_prims[conn_id]
            else:
                return self.agent.conn_prims[conn_id]
        prim = self.agent.prims_init()
        self.agent.conn_prims[conn_id] = prim
        if self.agent.use_batch:
            self.agent.register_prim(conn_id, prim)
        print(f"[CcpRlAgentImpl] created prim for flow conn_id={conn_id}")
        return prim

    async def getAction(self, observation, **kwargs):
        faulthandler.enable()
        recv_ns = _monotonic_raw_ns()
        # 从 observation 读取流级别的 conn_id（Rust 端填入 sock_id）
        conn_id = int(observation.connectionId) if observation.connectionId != 0 else self.connection_id
        # 读取 isNewFlow 标记（兼容旧 schema 缺失此字段）
        is_new_flow = bool(getattr(observation, 'isNewFlow', False))
        _record_rpc_observation_latency(conn_id, observation, recv_ns)
        try:
            prim = self._get_or_create_prim(conn_id, is_new_flow=is_new_flow)
            prim.iterations += 1
            get_action_time = time.time()
            
            # 更新 TCP 连接的最后活跃时间
            if self.connection_id in connections:
                connections[self.connection_id]['last_active_time'] = time.time()
            
            # 根据配置选择处理方式
            if self.agent.use_batch:
                loop = asyncio.get_running_loop()
                future = loop.create_future()
                batch_processor.add_request(
                    observation=observation,
                    prims=prim,
                    connection_id=conn_id,
                    future=future,
                    loop=loop
                )
                c, r = await future
            else:
                c, r = self.agent.get_action(obs=observation,
                                             prim=prim,
                                             conn_id=conn_id)
            self.last_active_time = time.time()
            
            print(f"python getAction conn_id={conn_id}, "
                  f"iterations:{prim.iterations}, "
                  f"cwnd_packets:{c}, rate:{r}, "
                  f"duration:{(time.time()-get_action_time)*1000:.2f}ms")
            
            return ccp_capnp.Action.new_message(cwnd=int(c), rate=int(r))
        except Exception as e:
            traceback_str = traceback.format_exc()
            print(f"An error occurred in getAction() conn_id={conn_id}: {e}\n{traceback_str}")
            return ccp_capnp.Action.new_message(cwnd=10, rate=1000000)

# 通过连接 ID 管理每个连接的超时和状态
async def manage_connection_timeout(connection_id, server, timeout=200):
    while True:
        await asyncio.sleep(timeout)
        if connection_id in connections:
            current_time = time.time()
            last_active_time = connections[connection_id]['last_active_time']
            if current_time - last_active_time > timeout:
                print(f"Connection {connection_id} timed out. Closing...")
                await server.close()
                break

# 新连接管理函数
async def new_connection(stream, agent):
    connection_id = len(connections) + 1

    if len(connections) >= max_connections:
        print(f"Connection rejected: Maximum connections reached. ID = {connection_id}")
        return 

    server = capnp.TwoPartyServer(stream, bootstrap=CcpRlAgentImpl(agent, connection_id))
    
    # 保存连接和对应的服务器实例
    connections[connection_id] = {'server': server, 'last_active_time': time.time()}
    
    print(f"New connection established: Connection ID = {connection_id}")

    # 启动超时管理任务
    timeout_task = asyncio.create_task(manage_connection_timeout(connection_id, server))

    try:
        await server.on_disconnect()
    finally:
        print(f"Connection closed: Connection ID = {connection_id}")
        try:
            if hasattr(agent, 'unregister_prim'):
                if getattr(agent, 'arch', 'lotus') == 'portus':
                    # portus 架构：每流独立 TCP 连接，conn_id == connection_id
                    # 只删除该连接对应的 prim
                    agent.unregister_prim(connection_id)
                    print(f"[portus cleanup] unregistered prim for conn_id={connection_id}")
                else:
                    # lotus 架构：单 TCP 连接承载多个流（sock_id）
                    # TCP 断开时清空所有 prim（Rust 重连时会重新建立）
                    flow_ids = list(agent.conn_prims.keys())
                    for fid in flow_ids:
                        agent.unregister_prim(fid)
                    print(f"[lotus cleanup] unregistered all {len(flow_ids)} prims")
        except Exception:
            pass
        # 确保连接存在再删除
        if connection_id in connections:
            del connections[connection_id]
        # 确保任务被取消
        if not timeout_task.done():
            timeout_task.cancel()

# 修复后的服务器运行函数
async def run_server(addr, agent, ready_event=None):
    host, port = addr.split(":")
    
    # 使用 kj_loop() 正确包装服务器
    async with capnp.kj_loop():
        server = await capnp.AsyncIoStream.create_server(
            functools.partial(new_connection, agent=agent), 
            host, port, family=socket.AF_INET
        )
        print(f"Server listening on {host}:{port}", flush=True)
        if ready_event is not None:
            ready_event.set()
        await server.serve_forever()

def start_server(addr, agent, ready_event=None):
    try:
        asyncio.run(run_server(addr=addr, agent=agent, ready_event=ready_event))
    except Exception:
        traceback.print_exc()
        if ready_event is not None:
            ready_event.set()
        raise

def start_client(addr, agent_client, mtp, n_flows=1, rpc_ms=5,
                 rpc_timeout_ms=0, resp_timeout_ms=0):
    server_addr = "127.0.0.1:"+addr.split(':')[1]
    print("Trying to connect RPC server in %s" %(server_addr))
    path_to_dtcc_client = os.path.join(os.getcwd(),"../iccp-rust/",agent_client)
    path_to_rust_log = os.path.join(os.getcwd(),"rust_log")
    os.makedirs(path_to_rust_log, exist_ok=True)
    if agent_client == "dtcc/target/debug/dtcc":
        # n_flows 和 rpc_ms 透传给 Rust-lotus 客户端，用于其内部流量/容量计算
        log_file_name = os.path.join(path_to_rust_log, "dtcc-lotus-{}.log".format(time.strftime("%Y%m%d-%H%M%S")))
        cmd = ("sudo " + path_to_dtcc_client
               + " --addr=" + server_addr
               + " --init_cwnd=10"
               + " --report_interval_ms=" + str(mtp)
               + " --n_flows=" + str(n_flows)
               + " --rpc_ms=" + str(rpc_ms))
        if rpc_timeout_ms and rpc_timeout_ms > 0:
            cmd += " --rpc_timeout=" + str(int(rpc_timeout_ms))
        if resp_timeout_ms and resp_timeout_ms > 0:
            cmd += " --resp_timeout=" + str(int(resp_timeout_ms))
        cmd += " --log-file=" + log_file_name
        print(cmd)
    elif agent_client == "dtcc-portus/target/debug/dtcc":
        log_file_name = os.path.join(path_to_rust_log, "dtcc-portus-{}.log".format(time.strftime("%Y%m%d-%H%M%S")))
        cmd = ("sudo " + path_to_dtcc_client
               + " --ipc=netlink --addr=" + server_addr
               + " --init_cwnd=10"
               + " --report_interval_ms=" + str(mtp)
        + " --rpc_ms=" + str(rpc_ms)
        + " --log-file=" + log_file_name)
        print(cmd)
    elif agent_client == "orca/target/debug/orca":
        log_file_name = os.path.join(path_to_rust_log, "orca-lotus-{}.log".format(time.strftime("%Y%m%d-%H%M%S")))
        cmd = ("sudo " + path_to_dtcc_client
               + " --addr=" + server_addr
               + " --init_cwnd=10 --per_ack"
               + " --report_interval_ms=" + str(mtp)
               + " --n_flows=" + str(n_flows)
               + " --rpc_ms=" + str(rpc_ms))
        if rpc_timeout_ms and rpc_timeout_ms > 0:
            cmd += " --rpc_timeout=" + str(int(rpc_timeout_ms))
        if resp_timeout_ms and resp_timeout_ms > 0:
            cmd += " --resp_timeout=" + str(int(resp_timeout_ms))
        cmd += " --log-file=" + log_file_name
        print(cmd)
    elif agent_client == "orca-portus/target/debug/orca":
        log_file_name = os.path.join(path_to_rust_log, "orca-portus-{}.log".format(time.strftime("%Y%m%d-%H%M%S")))
        cmd = ("sudo " + path_to_dtcc_client
               + " --ipc=netlink --addr=" + server_addr
               + " --init_cwnd=10 --per_ack"
               + " --report_interval_ms=" + str(mtp)
               + " --n_flows=" + str(n_flows)
               + " --rpc_ms=" + str(rpc_ms))
        cmd += " --log-file=" + log_file_name
        print(cmd)
    elif agent_client == "iccp/target/debug/iccp":
        cmd = ("sudo " + path_to_dtcc_client
               + " --default=dtcc"
               + " --dtcc-addr=" + server_addr
               + " --dtcc-init-cwnd=10")
        print(cmd)

    print("CC client is in %s" %(path_to_dtcc_client))
    process = sh.Popen(
        cmd,
        shell=True,
        stdout=sh.PIPE,
        stderr=sh.STDOUT,
        text=True,
        preexec_fn=os.setsid,
    )
    print(f"The PID of the subprocess is: {process.pid}")
    threading.Thread(target=log_reader, args=(process.stdout,)).start()
    return process

def kill_old_server(port):
    try:
        result = sh.run(["lsof", "-i", f":{port}"], capture_output=True, text=True)
        if result.returncode == 0:
            lines = result.stdout.splitlines()
            for line in lines[1:]:  
                parts = line.split()
                pid = parts[1]
                print(f"Found process with PID {pid} using port {port}")
                sh.run(["kill", "-9", pid], check=True)
                print(f"Process with PID {pid} has been killed.")
        elif result.returncode == 1:
            print(f"No process is listening on port {port}.")
        else:
            print(f"Failed to run lsof command: rc={result.returncode}, stderr={result.stderr.strip()}")
    except sh.CalledProcessError as e:
        print(f"An error occurred: {e}")


def init_ccprlagent_env(addr, agent, env):
    """
    启动 RPC server 和 Rust client。
    env: CCEnv 实例，提供 num_flows / env_bw / rpc_ms / batch_size 等参数。
    """
    # kill old ccp-rust process
    sh.run("sudo pkill -9 dtcc", shell=True)
    # release port
    kill_old_server(addr.split(':')[1])

    # ---------- 计算 batch_size / rpc_ms / resp_timeout（必须在 fork 之前） ----------
    rpc_ms   = getattr(env, 'rpc_ms',   5)      # Python 推理延迟经验值 (ms)
    num_flows = getattr(env, 'num_flows', 1)
    mtp      = agent.mtp                         # 上报周期 (ms)
    user_bs  = getattr(env, 'batch_size', 0)     # 用户手动指定 (0 = 自动)
    user_rpc_timeout_ms = int(getattr(env, 'rpc_timeout_ms', 0) or 0)
    user_resp_timeout_ms = int(getattr(env, 'resp_timeout_ms', 0) or 0)
    user_bp_timeout_ms = int(getattr(env, 'bp_timeout', 0) or 0)

    if user_bs > 0:
        recommended_bs = user_bs
    else:
        import math as _math
        recommended_bs = max(1, _math.ceil(rpc_ms / mtp) * num_flows)
    recommended_bs = min(recommended_bs, 512)

    agent._batch_size_hint = recommended_bs

    rpc_ms_for_rust = max(int(rpc_ms), mtp)
    rust_rpc_timeout_ms = user_rpc_timeout_ms if user_rpc_timeout_ms > 0 else rpc_ms_for_rust * 2
    rust_resp_timeout_ms = user_resp_timeout_ms if user_resp_timeout_ms > 0 else rust_rpc_timeout_ms + 10
    if rust_resp_timeout_ms <= rust_rpc_timeout_ms:
        print(f"[init_ccprlagent_env] WARNING: resp_timeout_ms={rust_resp_timeout_ms} "
              f"<= rpc_timeout_ms={rust_rpc_timeout_ms}; response wait may expire before RPC timeout.",
              flush=True)
    agent._rust_resp_timeout_ms = rust_resp_timeout_ms
    agent._user_bp_timeout_ms = user_bp_timeout_ms

    print(f"[init_ccprlagent_env] rpc_ms={rpc_ms}ms  num_flows={num_flows}  "
          f"mtp={mtp}ms  → recommended batch_size={recommended_bs}  "
          f"rpc_ms_for_rust={rpc_ms_for_rust}ms (rpc_timeout={rust_rpc_timeout_ms}ms, "
          f"resp_timeout={rust_resp_timeout_ms}ms, stale_timeout={rust_resp_timeout_ms*0.9:.1f}ms"
          f"{', bp_timeout=' + str(user_bp_timeout_ms) + 'ms' if user_bp_timeout_ms > 0 else ', bp_timeout=auto'} )",
          flush=True)

    # start RPC server（fork 在此之后，agent 已携带 _rust_resp_timeout_ms）
    print("To start RPC server in addr: %s" %(addr), flush=True)

    server_ready = multiprocessing.Event()
    server_process = multiprocessing.Process(target=start_server, args=(addr, agent, server_ready))
    server_process.daemon = True
    print("[init_ccprlagent_env] starting RPC server process...", flush=True)
    server_process.start()
    print("RPC server PID is %d "%server_process.pid, flush=True)

    if not server_ready.wait(timeout=10.0):
        print("RPC server did not become ready within 10s; terminating it.", flush=True)
        try:
            server_process.terminate()
            server_process.join(timeout=3)
            if server_process.is_alive():
                server_process.kill()
                server_process.join(timeout=2)
        except Exception:
            pass
        return server_process, None, False

    if not server_process.is_alive():
        print(f"RPC server process exited before client start, exitcode={server_process.exitcode}.", flush=True)
        return server_process, None, False

    # start RPC client in RUST
    print("Start RPC client RUST process", flush=True)
    agent_client = agent.clientapp
    client_process = start_client(addr, agent_client, mtp,
                                  n_flows=num_flows, rpc_ms=rpc_ms_for_rust,
                                  rpc_timeout_ms=rust_rpc_timeout_ms,
                                  resp_timeout_ms=rust_resp_timeout_ms)

    sleep(1.0)  # spawn has delay
    print("End of RPC channel init \n")

    if server_process.is_alive() and (client_process.poll() is None):
        print("Both RPC server and client are running successfully.")
        return server_process, client_process, True
    else:
        print("Failed to start RPC server or client.")
        return server_process, client_process, False
    
def log_reader(stream):
    for line in stream:
        print('RUST: '+line, end='')

_proc_cache = {}

def _process_tree_stats(root_process):
    """Return aggregate CPU/RSS for a process and its live children."""
    processes = [root_process]
    try:
        for child in root_process.children(recursive=True):
            pid = child.pid
            if pid not in _proc_cache:
                _proc_cache[pid] = child
                child.cpu_percent(interval=None)
            processes.append(_proc_cache[pid])
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        pass

    root_pid = root_process.pid
    if root_pid not in _proc_cache:
        _proc_cache[root_pid] = root_process
        root_process.cpu_percent(interval=None)
        processes[0] = _proc_cache[root_pid]

    total_cpu_percent = 0.0
    total_rss = 0
    live_pids = []
    for proc in processes:
        try:
            total_cpu_percent += proc.cpu_percent(interval=None)
            total_rss += proc.memory_info().rss
            live_pids.append(proc.pid)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            _proc_cache.pop(proc.pid, None)
            continue
    return total_cpu_percent, total_rss, live_pids


def monitor_process(agent_pid, client_pid=None, interval=1, stop_event=None):
    try:
        agent_process = psutil.Process(agent_pid)
    except psutil.NoSuchProcess:
        print(f"monitor_process: agent PID {agent_pid} does not exist, exiting.")
        return

    client_process = None
    if client_pid is not None:
        try:
            client_process = psutil.Process(client_pid)
        except psutil.NoSuchProcess:
            print(f"monitor_process: client PID {client_pid} does not exist; monitoring agent only.")

    monitored = {
        "python_agent": agent_process,
    }
    if client_process is not None:
        monitored["rust_client"] = client_process

    num_cores = psutil.cpu_count(logical=True) or 1
    while True:
        # 检查停止事件
        if stop_event is not None and stop_event.is_set():
            print(f"monitor_process: stop_event set, exiting.")
            break

        output_parts = []
        live_count = 0
        for label, proc in list(monitored.items()):
            try:
                if not proc.is_running() or proc.status() == psutil.STATUS_ZOMBIE:
                    raise psutil.NoSuchProcess(proc.pid)
                total_cpu_percent, rss_mem, live_pids = _process_tree_stats(proc)
                normalized_percent = total_cpu_percent / num_cores
                live_count += 1
                pids_text = ",".join(str(pid) for pid in live_pids) if live_pids else str(proc.pid)
                output_parts.append(
                    f"{label}(pids={pids_text}) "
                    f"Total CPU%: {total_cpu_percent:.1f}, "
                    f"Single-core CPU%:{normalized_percent:.1f}, "
                    f"Memory RSS: {rss_mem/(1024 * 1024):.2f} MB"
                )
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                output_parts.append(f"{label}(pid={proc.pid}) exited")
                del monitored[label]

        if output_parts:
            # avg_inference_time = np.mean(avg_inference_times)
            # avg_inference_times.clear()
            print("[monitor] " + " | ".join(output_parts))
        if live_count == 0:
            print("monitor_process: all monitored processes exited.")
            break
        time.sleep(interval)

class CCEnv():
    # metadata = {'render.modes': ['human']}
    def __init__(self, name='TCP', rl_channel_addr="", agent=None, params=None, config=None,
                 for_init_only=False, use_normalizer=False, id=0, num_flows=1, env_bw=48,
                 rpc_ms=5, batch_size=0, arch='lotus',
                 rpc_timeout_ms=0, resp_timeout_ms=0, bp_timeout=0):
        # self.action_space = spaces.Discrete(1)
        # self.observation_space = spaces.Dict()

        self.config = config
        self.params = params
        self.prev_rid = 99999
        self.wid = 23
        self.local_counter = 0
        self.pre_samples = 0.0
        self.new_samples = 0.0
        self.avg_delay = 0.0
        self.avg_thr = 0.0
        self.thr_ = 0.0
        self.del_ = 0.0
        self.max_bw = 0.0
        self.max_cwnd = 0.0
        self.max_smp = 0.0
        self.min_del = 9999999.0
        self.new_max = 1
        self.pre_loss = 0
        self.min_bw = 9999999
        self.min_rev_rtt = 0.5

        self.cnt_er =0

        self.pre_alpha = 0.0
        self._reset_next_step = False
        self.use_normalizer = use_normalizer
        self.id = id
        self.num_flows = num_flows
        self.env_bw = env_bw
        self.rpc_ms = rpc_ms        # 推理延迟经验值 (ms)，用于自动计算 batch_size
        self.batch_size = batch_size  # 0 = 自动推断
        self.arch = arch              # lotus (单TCP多流) 或 portus (每流独立TCP)
        self.rpc_timeout_ms = rpc_timeout_ms    # 0 = Rust 端兼容旧逻辑
        self.resp_timeout_ms = resp_timeout_ms  # 0 = rpc_timeout_ms + 10
        self.bp_timeout = bp_timeout            # 0 = 按安全量自动计算

        self.observation_space=spaces.Box(low=-1e6, high=1e6, shape=(self.config['obs_dim'],), dtype=np.float32)

        if self.config['action_version'] == 9:
            self.action_space=spaces.Box(low=-self.config['action_max'], high=self.config['action_max'], shape=(1,), dtype=np.float32)
        else:
            self.action_space=spaces.Box(low=0., high=1500., shape=(1,), dtype=np.float32)

        if agent is not None:
            self.agent=agent

        if self.use_normalizer == True:
            raise NotImplementedError('Normalizer is disabled')
        else:
            self.normalizer = None

        self.rl_channel_addr = rl_channel_addr
    
    def run_ccp_agent(self):
        # init and run CCP-RLAgent
        if self.agent is not None:
            sp, cp, ec = init_ccprlagent_env(self.rl_channel_addr, self.agent, self)
            self._monitor_stop_event = threading.Event()
            monitoring_thread = threading.Thread(
                target=monitor_process,
                args=(sp.pid, cp.pid if cp is not None else None),
                kwargs={'stop_event': self._monitor_stop_event}
            )
            monitoring_thread.daemon = True
            monitoring_thread.start()
            self._monitoring_thread = monitoring_thread
        else:
            print("Agent is not ready!\n")
        return sp, cp, ec

    # def render(self, mode='human'):
    #     # print("INFO:render!\n")
    #     pass

    # def seed(self, seed=None):
    #     self.np_random, seed = seeding.np_random(seed)
    #     return [seed]
