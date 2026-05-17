import numpy as np
import math
import signal
import sys
from time import sleep
import gym
from gym import spaces
from gym.utils import seeding
import decision_transformer.envs.timestep as ts
import asyncio
import socket
import capnp
import os
from functools import partial
import time 
import functools
import threading
import subprocess as sh
import multiprocessing
import traceback
import faulthandler
# from dtcc_agent import Prims
import torch
import tensorflow as tf

# TODO: schema and client need abstract for different cca
capnp.remove_import_hook()
schema_path = os.path.join(os.getcwd(),"schema/ccp_dtcc.capnp")
ccp_capnp = capnp.load(schema_path)

async def kj_loop():
    async with capnp.kj_loop():
        yield

class CcpRlAgentImpl(ccp_capnp.RLAgent.Server):
    def __init__(self, agent, connection_id):
        self.agent = agent
        self.connection_id = connection_id  # 存储连接 ID
        self.prims = agent.prims_init()
        self.last_active_time = time.time()  # 当前活跃时间

    async def getAction(self, observation, **kwargs):
        faulthandler.enable()
        try:
            self.prims.iterations += 1
            # c,r= await self.agent.get_action(obs)
            get_action_time = time.time()
            c, r = self.agent.get_action(observation, self.prims)
            self.last_active_time = time.time()

            # 每次调用 getAction 时更新对应连接的活跃时间
            if self.connection_id in connections:
                connections[self.connection_id]['last_active_time'] = time.time()
            
            print(f"python getAction iterations:{self.prims.iterations},cwnd_packets:{c},rate:{r},duration:{(time.time()-get_action_time)*1000}ms\n")
            
            return ccp_capnp.Action.new_message(cwnd=int(c), rate=int(r))
        except Exception as e:
            traceback_str = traceback.format_exc()
            print(f"An error occurred in getAction(): {e}\n{traceback_str}")
            with open("./log/error_log.txt", "a") as log_file:
                log_file.write(traceback_str)


from collections import defaultdict
# 用于管理连接状态的字典，存储与连接相关的信息
connections = defaultdict(lambda: {'last_active_time': time.time(), 'server': None})
max_connections = 30

# 通过连接 ID 管理每个连接的超时和状态
async def manage_connection_timeout(connection_id, server, timeout=100):
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

async def run_server(addr, agent):
    host, port = addr.split(":")
    # server = await asyncio.start_server(
    #     functools.partial(new_connection,agent=agent), host, port,family=socket.AF_INET
    # )
    server = await capnp.AsyncIoStream.create_server(functools.partial(new_connection,agent=agent), host, port,family=socket.AF_INET)
    print("Thread_server listening on server: ", server)
    async with server:
        await server.serve_forever()

def start_server(addr, agent):
    # import debugpy
    # debugpy.listen(5678)  # 监听 5678 端口
    # print("子进程调试器已启动，等待连接...")
    # time.sleep(3)  # 给调试器附加的时间
    asyncio.run(capnp.run(run_server(addr=addr,agent=agent)))

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
    #TODO:add monitor the socket connection

    if server_process.is_alive() and (client_process.poll() is None):
        print("Both RPC server and client are running successfully.")
        return server_process,client_process,True
    else:
        print("Failed to start RPC server or client.")
        return server_process,client_process,False
    
def log_reader(stream):
    for line in stream:
        print('RUST: '+line, end='')

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
        else:
            print("Agent is not ready!\n")
        return sp, cp, ec

    # def render(self, mode='human'):
    #     # print("INFO:render!\n")
    #     pass

    # def seed(self, seed=None):
    #     self.np_random, seed = seeding.np_random(seed)
    #     return [seed]
