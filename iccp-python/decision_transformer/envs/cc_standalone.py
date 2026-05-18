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
import torch
capnp.remove_import_hook()
schema_name = os.environ.get("DTCC_CCP_SCHEMA", "ccp_dtcc.capnp")
schema_path = schema_name if os.path.isabs(schema_name) else os.path.join(os.getcwd(), "schema", schema_name)
ccp_capnp = capnp.load(schema_path)
print(f"[cc_standalone.py] loaded Cap'n Proto schema: {schema_path}", flush=True)

async def kj_loop():
    async with capnp.kj_loop():
        yield

class CcpRlAgentImpl(ccp_capnp.RLAgent.Server):
    def __init__(self, agent, connection_id):
        self.agent = agent
        self.connection_id = connection_id
        self.last_active_time = time.time() 
        # print(type(agent))

    def _get_or_create_prim(self, conn_id, is_new_flow=False):
        if not hasattr(self.agent, 'conn_prims'):
            self.agent.conn_prims = {}
        if conn_id in self.agent.conn_prims:
            if is_new_flow:
                del self.agent.conn_prims[conn_id]
            else:
                return self.agent.conn_prims[conn_id]
        prim = self.agent.prims_init()
        self.agent.conn_prims[conn_id] = prim
        print(f"[standalone] created prim for conn_id={conn_id}")
        return prim

    #TODO! getAction need return a Promise! 
     
    async def getAction(self,observation,**kwargs):
        #TODO:connection is established but no call getAction
        # modified to state input
        faulthandler.enable()
        conn_id = int(observation.connectionId) if observation.connectionId != 0 else self.connection_id
        is_new_flow = bool(getattr(observation, 'isNewFlow', False))
        try:
            prim = self._get_or_create_prim(conn_id, is_new_flow=is_new_flow)
            prim.iterations += 1
            # c,r= await self.agent.get_action(obs)
            c,r= self.agent.get_action(observation, prim, conn_id)
            self.last_active_time = time.time()
            if self.connection_id in connections:
                connections[self.connection_id]['last_active_time'] = self.last_active_time
            print(f"python getAction conn_id={conn_id}, iterations:{prim.iterations}, "
                  f"pre_cwnd_rate:{prim.pre_cwnd_rate}, cwnd:{c}, rate:{r}\n")
            return ccp_capnp.Action.new_message(cwnd=int(c), rate=int(r))
        except Exception as e:
            traceback_str = traceback.format_exc()
            print(f"An error occurred in getAction() conn_id={conn_id}: {e}\n{traceback_str}")
            # 可以选择将 traceback_str 写入到日志文件中
            with open("error_log.txt", "a") as log_file:
                log_file.write(traceback_str)
            return ccp_capnp.Action.new_message(cwnd=10, rate=1000000)


connections = {}
max_connections = 20

def clear_agent_prims(agent, reason):
    try:
        if not hasattr(agent, 'conn_prims'):
            return
        flow_ids = list(agent.conn_prims.keys())
        if hasattr(agent, 'unregister_prim'):
            for fid in flow_ids:
                agent.unregister_prim(fid)
        else:
            agent.conn_prims.clear()
        print(f"[standalone cleanup] {reason}; cleared {len(flow_ids)} flow prims")
    except Exception as e:
        print(f"[standalone cleanup] failed to clear conn_prims: {e}")

class CustomTwoPartyServer:
    def __init__(self, server, connection_id):
        self.server = server
        self.connection_id = connection_id
        self.last_active_time = time.time()  # Set the initial active time

    def update_activity(self):
        """Update the last active time when an activity occurs."""
        self.last_active_time = time.time()

    async def close(self):
        """Close the connection"""
        await self.server.close()

async def manage_connection_timeout(connection_id, server, timeout=60):
    while True:
        await asyncio.sleep(timeout)
        if connection_id in connections:
            current_time = time.time()
            last_active_time = connections[connection_id]['last_active_time']
            if current_time - last_active_time > timeout:
                print(f"Connection {connection_id} timed out. Closing...")
                await server.close()
                break

async def new_connection(stream, agent):
    connection_id = len(connections) + 1

    if len(connections) >= max_connections:
        print(f"Connection rejected: Maximum connections reached. ID = {connection_id}")
        return 

    if not connections:
        clear_agent_prims(agent, f"new RPC connection {connection_id} starting")

    server = capnp.TwoPartyServer(stream, bootstrap=CcpRlAgentImpl(agent, connection_id))
    connections[connection_id] = {'server': server, 'last_active_time': time.time()}
    print(f"New connection established: Connection ID = {connection_id}")

    # 启动超时管理任务
    timeout_task = asyncio.create_task(manage_connection_timeout(connection_id, server))

    try:
        await server.on_disconnect()
    finally:
        print(f"Connection closed: Connection ID = {connection_id}")
        clear_agent_prims(agent, f"RPC connection {connection_id} closed")
        if connection_id in connections:
            del connections[connection_id]
        if not timeout_task.done():
            timeout_task.cancel()

async def run_server(addr, agent, ready_event=None):
    host, port = addr.split(":")
    async with capnp.kj_loop():
        server = await capnp.AsyncIoStream.create_server(
            functools.partial(new_connection,agent=agent),
            host,
            port,
            family=socket.AF_INET
        )
        print(f"Standalone RPC server listening on {host}:{port}", flush=True)
        if ready_event is not None:
            ready_event.set()
        await server.serve_forever()

def start_server(addr, agent, ready_event=None):
    # import debugpy
    # debugpy.listen(5678)  # 监听 5678 端口
    # print("子进程调试器已启动，等待连接...")
    # time.sleep(10)  # 给调试器附加的时间
    try:
        asyncio.run(run_server(addr=addr, agent=agent, ready_event=ready_event))
    except Exception:
        traceback.print_exc()
        if ready_event is not None:
            ready_event.set()
        raise

def start_client(addr):
    server_addr = "127.0.0.1:"+addr.split(':')[1]
    print("Trying to connect RPC server in %s" %(server_addr))
    path_to_dtcc_client = os.path.join(os.getcwd(),"../iccp-rust/dtcc/target/debug/dtcc")
    print("DTCC client is in %s" %(path_to_dtcc_client))
    process = sh.Popen("sudo " + path_to_dtcc_client +" --ipc=netlink --addr="+server_addr+" --init_cwnd=10 --report_interval_ms=10",
                        shell=True, stdout=sh.PIPE, stderr=sh.STDOUT, text=True)
    print(f"The PID of the subprocess is: {process.pid}")
    # trace_process = sh.Popen("sudo capnp_trace attach -f -r/home/jxl/record/r --verbose 127.0.0.1:4826 "+str(process.pid),
    #                          shell=True, stdout=sh.PIPE, stderr=sh.STDOUT, text=True)
    threading.Thread(target=log_reader, args=(process.stdout,)).start()
    # threading.Thread(target=log_reader, args=(trace_process.stdout,)).start()
    return process

def kill_old_server(port):
    try:
        result = sh.run(["lsof", "-i", f":{port}"], capture_output=True, text=True)
        if result.returncode == 0:
            lines = result.stdout.splitlines()
            for line in lines[1:]:  # 跳过标题行
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
    server_ready = multiprocessing.Event()
    server_process = multiprocessing.Process(target=start_server, args=(addr, agent, server_ready))
    server_process.daemon = False
    server_process.start()
    print("RPC server PID is %d "%server_process.pid)

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
        return server_process, False
    # start RPC client in RUST
    print("Waiting to start RPC client RUST process.............................\n")
    # client_process = start_client(addr)

    sleep(1.0)  # spawn has delay
    print("End of RPC server init \n")
    #TODO:add monitor the socket connection

    if server_process.is_alive():
        print("RPC server are running successfully.\n")
        return server_process,True
    else:
        print("Failed to start RPC server.\n")
        return server_process,False
    
def log_reader(stream):
    for line in stream:
        print('RUST: '+line, end='')

class CCEnv(gym.Env):
    metadata = {'render.modes': ['human']}
    def __init__(self,name='TCP',rl_channel_addr="",agent=None,params=None, config=None,
                 for_init_only=False, use_normalizer=False,id=0,num_flows=1,env_bw = 48,
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
        self.rpc_ms = rpc_ms
        self.batch_size = batch_size
        self.arch = arch
        self.rpc_timeout_ms = rpc_timeout_ms
        self.resp_timeout_ms = resp_timeout_ms
        self.bp_timeout = bp_timeout

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
            sp, ec = init_ccprlagent_env(self.rl_channel_addr,self.agent)
        else:
            print("Agent is not ready!\n")
                     # Keep the main thread alive to allow the server thread to continue running
        # TODO:delete!
        # print("Main thread is alive. Press Ctrl+C to exit.")
        # try:
        #     while True:
        #         time.sleep(1)
        # except KeyboardInterrupt:
        #     print("Main thread interrupted, exiting.")
        return sp, ec

    def render(self, mode='human'):
        # print("INFO:render!\n")
        pass

    def seed(self, seed=None):
        self.np_random, seed = seeding.np_random(seed)
        return [seed]
