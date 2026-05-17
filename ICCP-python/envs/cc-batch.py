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
import torch
import tensorflow as tf
import psutil

# TODO: schema and client need abstract for different cca
capnp.remove_import_hook()
schema_path = os.path.join(os.getcwd(),"schema/ccp_dtcc.capnp")
ccp_capnp = capnp.load(schema_path)

async def kj_loop():
    async with capnp.kj_loop() as loop:
        await loop

class BatchProcessor:
    def __init__(self, agent, batch_size=8, timeout=0.005):
        """
        初始化批处理器
        参数:
        agent: 预测模型代理
        batch_size: 最大批处理大小
        timeout: 最大等待时间(秒)
        """
        self.agent = agent
        self.batch_size = batch_size
        self.timeout = timeout
        self.stats = {"total_batches": 0, "total_requests": 0}
        self.queue = queue.Queue()
        self.batch_event = threading.Event()
        self.batch_lock = threading.Lock()
        self.processing = False
        
        # 启动批量处理线程
        self.batch_thread = threading.Thread(target=self._process_batches)
        self.batch_thread.daemon = True
        self.batch_thread.start()
    
    def add_request(self, observation, prims, connection_id, future, loop):
        """添加请求到批量处理队列"""
        timestamp = time.time()
        # print("Batch add requeset for connection:{}, arrival time:{}".format(connection_id,timestamp))
        with self.batch_lock:
            self.queue.put({
                'observation': observation,
                'prims': prims,
                'connection_id': connection_id,
                'future': future,
                'loop': loop,
                'timestamp': timestamp
            })
            
            # 如果队列大小达到批处理大小，通知处理
            if self.queue.qsize() >= self.batch_size:
                self.batch_event.set()
    
    def _process_batches(self):
        """批量处理线程的主函数"""
        while True:
            # 等待批量处理信号或超时
            self.batch_event.wait(self.timeout)
            self.batch_event.clear()  # 清除事件信号
            
            batch = []
            try:
                with self.batch_lock:
                    # 获取队列中的所有请求
                    while not self.queue.empty():
                        batch.append(self.queue.get())
                    
                    # 如果队列中没有请求，继续等待
                    if not batch:
                        continue
                    
                    # 按时间戳排序，确保先进先出
                    batch.sort(key=lambda x: x['timestamp'])
                    
                    # 如果批量大于batch_size，只取前batch_size个
                    if len(batch) > self.batch_size:
                        # 将超过的部分放回队列
                        for item in batch[self.batch_size:]:
                            self.queue.put(item)
                        batch = batch[:self.batch_size]
                
                # 处理批量
                self._process_batch(batch)
                self.stats["total_batches"] += 1
                self.stats["total_requests"] += len(batch) 
                efficiency = len(batch) / self.batch_size
                # print(f"Batch efficiency: {efficiency:.1%} ({len(batch)}/{self.batch_size})")
            except Exception as e:
                traceback_str = traceback.format_exc()
                print(f"Error processing batch: {e}\n{traceback_str}")
                # 处理失败时设置每个future的异常
                for item in batch:
                    item['future'].set_exception(e)
    
    def _process_batch(self, batch):
        """处理单个批量的请求"""
        if not batch:
            return
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
                # 错误处理：提供默认状态
                print("Batch state get error")
                state = np.zeros(self.agent.config['state_dim'], dtype=np.float32)
            
            states.append(state)
            rewards.append(reward)
            conn_ids.append(item['connection_id'])
            futures.append(item['future'])
            loops.append(item['loop'])
            prims_list.append(item['prims'])
        
        # 将状态列表转换为numpy数组
        states_array = np.array(states)
        reward_array = np.array(rewards)
        # 批量预测
        try:
            # print("Batch process: states:{}".format(states_array))
            actions , rates= self.agent.batch_predict(states_array,reward_array,conn_ids)
            
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
            # 处理失败时设置每个future的异常
            for i in range(len(batch)):
                loops[i].call_soon_threadsafe(futures[i].set_exception, e)


# 全局批处理器实例
batch_processor = None

# 用于管理连接状态的字典
connections = defaultdict(lambda: {'last_active_time': time.time(), 'server': None})
max_connections = 300

class CcpRlAgentImpl(ccp_capnp.RLAgent.Server):
    def __init__(self, agent, connection_id):
        global batch_processor
        
        self.agent = agent
        self.connection_id = connection_id
        self.prims = agent.prims_init()
        self.last_active_time = time.time()
        if agent.use_batch:
            # 注册连接状态（仅批量处理需要）
            self.agent.register_prim(connection_id, self.prims)
            if batch_processor is None:
                batch_processor = BatchProcessor(agent, batch_size=8, timeout=0.003)
        
        print(f"New agent instance for connection {connection_id}")

    async def getAction(self, observation, **kwargs):
        faulthandler.enable()
        try:
            self.prims.iterations += 1
            get_action_time = time.time()
            
            # 更新连接的最后活跃时间
            if self.connection_id in connections:
                connections[self.connection_id]['last_active_time'] = time.time()
            
            # 根据配置选择处理方式
            if self.agent.use_batch:
                # 批量处理路径
                loop = asyncio.get_running_loop()
                future = loop.create_future()
                
                # 将请求添加到批处理器
                batch_processor.add_request(
                    observation=observation,
                    prims=self.prims,
                    connection_id=self.connection_id,
                    future=future,
                    loop=loop
                )
                # 等待批处理结果
                c, r = await future
            else:
                c, r = self.agent.get_action(obs=observation, 
                                            prim=self.prims, 
                                            conn_id=self.connection_id)
            self.last_active_time = time.time()
            
            print(f"python getAction for {self.connection_id}, "
                  f"iterations:{self.prims.iterations}, "
                  f"cwnd_packets:{c}, rate:{r}, "
                  f"duration:{(time.time()-get_action_time)*1000:.2f}ms")
            
            return ccp_capnp.Action.new_message(cwnd=int(c), rate=int(r))
        except Exception as e:
            traceback_str = traceback.format_exc()
            print(f"An error occurred in getAction(): {e}\n{traceback_str}")
            # 返回默认值以防出错
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
        # 连接关闭时清理
        print(f"Connection closed: Connection ID = {connection_id}")
        # 确保连接存在再删除
        if connection_id in connections:
            del connections[connection_id]
        # 确保任务被取消
        if not timeout_task.done():
            timeout_task.cancel()

# 修复后的服务器运行函数
async def run_server(addr, agent):
    host, port = addr.split(":")
    
    # 使用 kj_loop() 正确包装服务器
    async with capnp.kj_loop():
        server = await capnp.AsyncIoStream.create_server(
            functools.partial(new_connection, agent=agent), 
            host, port, family=socket.AF_INET
        )
        print(f"Server listening on {host}:{port}")
        await server.serve_forever()

def start_server(addr, agent):
    asyncio.run(run_server(addr=addr, agent=agent))

def start_client(addr,agent_client,mtp):
    server_addr = "127.0.0.1:"+addr.split(':')[1]
    print("Trying to connect RPC server in %s" %(server_addr))
    path_to_dtcc_client = os.path.join(os.getcwd(),"../dtcc-rust/",agent_client)
    if agent_client == "dtcc/target/debug/dtcc":
        cmd = "sudo " + path_to_dtcc_client +" --ipc=netlink --addr="+server_addr+" --init_cwnd=10 --report_interval_ms="+str(mtp)
    elif agent_client == "orca/target/debug/orca":
        cmd = "sudo " + path_to_dtcc_client +" --ipc=netlink --addr="+server_addr+" --init_cwnd=10 --per_ack --report_interval_ms="+str(mtp)
        # cmd = "sudo ../dtcc-rust/generic-cong-avoid/target/debug/cubic --ipc=netlink --init_cwnd=10 --report_interval_ms=10 --deficit_timeout=10"
    elif agent_client == "iccp/target/debug/iccp":
        cmd = "sudo " + path_to_dtcc_client +" --ipc=netlink"
    elif agent_client == "aurora/target/debug/aurora":
        cmd = "sudo " + path_to_dtcc_client +" --ipc=netlink --addr="+server_addr+" --init_cwnd=4 --report_interval_rtt=0.5"

    print("CC client is in %s" %(path_to_dtcc_client))
    process = sh.Popen(cmd, shell=True, stdout=sh.PIPE, stderr=sh.STDOUT, text=True)
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
        else:
            print("Failed to run lsof command.")
    except sh.CalledProcessError as e:
        print(f"An error occurred: {e}")


def init_ccprlagent_env(addr,agent):
    # kill old ccp-rust process
    sh.run("sudo pkill -9 dtcc", shell=True)
    # release port
    kill_old_server(addr.split(':')[1])
    # start RPC server
    print("Start RPC server in addr: %s" %(addr))
    # t = threading.Thread(target=start_server,args=(addr,agent))
    # t.daemon = False
    # t.start()
    
    server_process = multiprocessing.Process(target=start_server, args=(addr, agent))
    server_process.daemon = False
    server_process.start()
    print("RPC server PID is %d "%server_process.pid)

    sleep(5.0)
    # start RPC client in RUST
    print("Start RPC client RUST process")
    agent_client = agent.clientapp
    mtp = agent.mtp
    client_process = start_client(addr,agent_client,mtp)

    sleep(1.0)  # spawn has delay
    print("End of RPC channel init \n")
    #TODO:add monitor the main process inference


    if server_process.is_alive() and (client_process.poll() is None):
        print("Both RPC server and client are running successfully.")
        return server_process,client_process,True
    else:
        print("Failed to start RPC server or client.")
        return server_process,client_process,False
    
def log_reader(stream):
    for line in stream:
        print('RUST: '+line, end='')

def monitor_process(pid, interval=1):
    process = psutil.Process(pid)
    while True:
        try:
            num_cores = psutil.cpu_count(logical=True)
            total_cpu_percent = process.cpu_percent(interval=None)
            normalized_percent = total_cpu_percent / num_cores
            mem_info = process.memory_info()
            rss_mem = mem_info.rss  # bytes
            print(f"Total CPU%: {total_cpu_percent}, Single-core CPU%:{normalized_percent:.1f} ,Memory RSS: {rss_mem/(1024 * 1024):.2f} MB")
        except psutil.NoSuchProcess:
            break
        time.sleep(interval)

class CCEnv():
    # metadata = {'render.modes': ['human']}
    def __init__(self,name='TCP',rl_channel_addr="",agent=None,params=None, config=None, for_init_only=False, use_normalizer=False,id=0,num_flows=1,env_bw = 48):
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
            sp, cp, ec = init_ccprlagent_env(self.rl_channel_addr,self.agent)
            # monitoring_thread = threading.Thread(target=monitor_process, args=(sp.pid,))
            # monitoring_thread.daemon = True
            # monitoring_thread.start()
        else:
            print("Agent is not ready!\n")
        return sp, cp, ec

    # def render(self, mode='human'):
    #     # print("INFO:render!\n")
    #     pass

    # def seed(self, seed=None):
    #     self.np_random, seed = seeding.np_random(seed)
    #     return [seed]
