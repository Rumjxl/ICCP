import os
# JXL：CPU的多线程有冲突，须设置
os.environ["OMP_NUM_THREADS"] = "1"
os.environ['TF_NUM_INTEROP_THREADS'] = '1'
os.environ['TF_NUM_INTRAOP_THREADS'] = '1'
os.environ["CUDA_VISIBLE_DEVICES"] = ""
import argparse
import torch
import gym
import sys
import sysv_ipc
from decision_transformer.models.decision_transformer import DecisionTransformer
import decision_transformer.envs.timestep as ts
import yaml
import numpy as np
import tracemalloc
import subprocess as sh
import time
from time import sleep
import threading
import signal
import faulthandler
import math
import random
from dtcc_agent import DTCCAgent
from llm_agent import LLMAgent
from orca_agent import ORCAAgent

from plm_special.plm_utils import load_plm
from plm_special.config import cfg
from plm_special.models.low_rank import peft_model
from plm_special.models.rl_policy import OfflineRLPolicy

from orca_special.onlinerl_policy import OnlineRLPolicy

from sage_special.sageactor import SagePolicy
from sage_agent import SageAgent

from aurora_special.ppo import PPO
from aurora_agent import AuroraAgent
import tensorflow as tf
# MAX_CWND = 80
faulthandler.enable()

    
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '0'

RUNMODE = 0  #  0:eval

def str2bool(value):
    if isinstance(value, bool):
        return value
    value = value.lower()
    if value in ('yes', 'true', 't', '1', 'y'):
        return True
    if value in ('no', 'false', 'f', '0', 'n'):
        return False
    raise argparse.ArgumentTypeError('Boolean value expected.')

def configure_ccp_schema(model_type):
    if model_type == 'orca':
        os.environ["DTCC_CCP_SCHEMA"] = "ccp_orca.capnp"
    else:
        os.environ["DTCC_CCP_SCHEMA"] = "ccp_dtcc.capnp"
    print(f"[dtcc.py] DTCC_CCP_SCHEMA={os.environ['DTCC_CCP_SCHEMA']}", flush=True)

def load_model(args,model,load_model_path,device):
    if args.model_type =='dt':
        checkpoint = torch.load(load_model_path,map_location=device, weights_only=True)
        model.load_state_dict(checkpoint['state_dict'])
    elif args.model_type == 'plm':
        if args.rank > 0:
            # load lora weights
            model.plm.load_adapter(load_model_path, adapter_name='default')
            # load other modules except plm
            model.modules_except_plm.load_state_dict(torch.load(os.path.join(load_model_path, 'modules_except_plm.bin'),map_location=device))
        else:
            # lora is disabled, load whole model
            model.load_state_dict(torch.load(os.path.join(load_model_path, 'model.bin'),map_location=device))
    return model
import torch
def run_eval(args):
    if args.device == 'gpu':
        device = torch.device("cuda")
    elif args.device == 'cpu':
        os.environ["CUDA_VISIBLE_DEVICES"] = ""
        # evaluate only on CPU
        torch.cuda.set_device(-1)
        print(torch.cuda.is_available())
        device = torch.device("cpu")

    #load config
    config_rl_module_path = os.path.dirname(os.path.realpath(__file__))

    with open(os.path.join(config_rl_module_path, "config-rl-eval.yaml"), 'r') as fs:
        config = yaml.load(fs,Loader=yaml.FullLoader)

    # load env
    env = CCEnv(config=config['tcpspec'], agent=None, rl_channel_addr=args.rl_channel_addr,
                for_init_only=False, id=args.id, env_bw=args.bw, num_flows=args.flows,
                rpc_ms=args.rpc_ms, batch_size=args.batch_size, arch=args.arch,
                rpc_timeout_ms=args.rpc_timeout, resp_timeout_ms=args.resp_timeout,
                bp_timeout=args.bp_timeout)

    configA=dict()
    configA['state_dim'] = config['tcpspec']['obs_dim']
    configA['act_dim'] = 1
    configA['action_max'] = config['tcpspec']['action_max']
    configA['action_version'] = config['tcpspec']['action_version']
    configA['max_length'] = config['max_window_size']
    configA['max_ep_len'] = config['num_training_step']
    configA['embed_dim'] = 128
    configA['n_layer'] = 4
    configA['n_head'] = 4
    configA['activation_function'] = 'relu'
    configA['resid_dropout'] = 0.1
    configA['attn_dropout'] = 0.1
    configA['state_mean'] = torch.from_numpy(np.array(config['state_mean'])).to(device=device)
    configA['state_std'] = torch.from_numpy(np.array(config['state_std'])).to(device=device)
    configA['reward_mean'] = torch.tensor(config['reward_mean'],device=device,dtype=torch.float32).reshape(1,1)
    configA['reward_std'] = torch.tensor(config['reward_std'],device=device,dtype=torch.float32).reshape(1,1)
    configA['lr'] = 1e-4
    configA['weight_decay'] = 1e-4
    configA['ep_return'] = args.offline_return
    configA['scale'] = config['reward_scale'] if 'reward_scale' in config else 100
    configA['short_win'] = 10
    configA['mid_win'] = 200
    configA['long_win'] = 1000

    if args.model_type =='dt':
        model = DecisionTransformer(
                state_dim=configA['state_dim'],
                act_dim=configA['act_dim'],
                max_length=configA['max_length'],
                max_ep_len=configA['max_ep_len'],
                hidden_size=configA['embed_dim'],
                n_layer=configA['n_layer'],
                n_head=configA['n_head'],
                n_inner=4*configA['embed_dim'],
                activation_function=configA['activation_function'],
                n_positions=1024,
                resid_pdrop=configA['resid_dropout'],
                attn_pdrop=configA['attn_dropout'],
        )
        if args.load_file != '':
            load_file = os.path.join(config_rl_module_path,'dt_ckpt',args.load_file)
        model = load_model(args,model,load_file,device)
        agent = DTCCAgent(model,env.observation_space,env.action_space,configA,device,env.env_bw,args.batch,arch=args.arch)
        if args.hybrid_enable:
            print("Hybrid mode is enabled, change the agent_client")
            agent.clientapp = "iccp/target/debug/iccp"

    elif args.model_type == 'sage':
        device = tf.device(args.device)
        with open(os.path.join(config_rl_module_path, "config-sage-eval.yaml"), 'r') as fs:
            configS = yaml.load(fs,Loader=yaml.FullLoader)
        configS['action_dim'] = 1
        configS['state_dim'] = config['tcpspec']['obs_dim']
        configS['action_version'] = config['tcpspec']['action_version']
        configS['short_win'] = 10
        configS['mid_win'] = 200
        configS['long_win'] = 1000
        model = SagePolicy(configS)
        action_shape = model.action_spec.shape
        model.make_networks(
            action_shape=action_shape,
            vmin=config['offline_config']['vmin'], vmax=config['offline_config']['vmax'],
            num_atoms=config['offline_config']['num_atoms'],
            p_lstm_size=config['offline_config']['policy_lstm_size'],
            c_lstm_size=config['offline_config']['critic_lstm_size'],
            p_enc_size=config['offline_config']['p_enc_size'],
            c_enc_size=config['offline_config']['c_enc_size'],
            p_mlp_size=config['offline_config']['p_mlp_size'],
            c_mlp_size=config['offline_config']['c_mlp_size'],
            p_mlp_depth=config['offline_config']['p_mlp_depth'],
            c_mlp_depth=config['offline_config']['c_mlp_depth'],
            nw_type=config['offline_config']['type']
        )
        model.load_weights(os.path.join(config_rl_module_path,'sage_ckpt'))
        model.init_actor()
        agent = SageAgent(model,env.observation_space,env.action_space,configS,device,env.env_bw,args.batch,arch=args.arch)
        if args.hybrid_enable:
            print("Hybrid mode is enabled, change the agent_client")
            agent.clientapp = "iccp/target/debug/iccp"
    
    elif args.model_type == 'plm':
        configP=dict()
        configP=configA.copy()
        plm, *_ = load_plm(args.plm_type,
                           os.path.join(cfg.plm_dir, args.plm_type, args.plm_size), 
                        device_input_side=args.device, device_output_side=args.device_out, device_middle_side=args.device_mid)
        if args.plm_type != 'llama':
            plm = plm.to(args.device)
        if args.rank != -1:
            plm = peft_model(plm, args.plm_type, rank=args.rank)
        plm_embed_size = cfg.plm_embed_sizes[args.plm_type][args.plm_size]
        model = OfflineRLPolicy(state_feature_dim=configP['state_dim'], plm=plm, plm_embed_size=plm_embed_size, 
                                           max_length=configP['max_length'], max_ep_len=configP['max_ep_len'], device=args.device)
        load_dir = cfg.plm_ft_dir
        model = load_model(args,model,load_dir,device)
        agent = LLMAgent(model,env.observation_space,env.action_space,configP,device,env.env_bw,use_batch=args.batch)
    elif args.model_type == 'orca':
        device = tf.device(args.device)
        # tf.config.threading.set_intra_op_parallelism_threads(1)
        # tf.config.threading.set_inter_op_parallelism_threads(1)
        with open(os.path.join(config_rl_module_path, "config-orca-eval.yaml"), 'r') as fs:
            configO= yaml.load(fs,Loader=yaml.FullLoader)
        s_dim= configO['state_dim']
        a_dim= configO['action_dim']
        if not configO['use_TCP']:
            configO['state_dim'] = s_dim
        if configO['recurrent']:# JXL: recurrent=TRUE
            s_dim = s_dim * configO['rec_dim']
        if configO['use_hard_target'] == True:
            configO['tau'] = 1.0
        configO['s_dim'] = s_dim
        model = OnlineRLPolicy(s_dim, a_dim,device=device ,h1_shape=configO['h1_shape'],
                        h2_shape=configO['h2_shape'],batch_size=configO['batch_size'],stddev=configO['stddev'],mem_size=configO['memsize'],gamma=configO['gamma'],
                        lr_c=configO['lr_c'],lr_a=configO['lr_a'],tau=configO['tau'],PER=configO['PER'],CDQ=configO['CDQ'],
                        LOSS_TYPE=configO['LOSS_TYPE'],noise_type=configO['noise_type'],noise_exp=configO['noise_exp'])        
        # TODO：load_model
        model.load_weights('./orca_ckpt')
        agent = ORCAAgent(model,s_dim,a_dim,configO,device,is_eval=args.orca_eval,use_batch=args.batch,arch=args.arch)

        # dtypes = [tf.float32, tf.float32, tf.float32, tf.float32, tf.float32]
        # shapes = [[s_dim], [a_dim], [1], [s_dim], [1]]
        # queue = tf.FIFOQueue(10000, dtypes, shapes, shared_name="rp_buf")
    elif args.model_type == 'aurora':
        device = tf.device(args.device)
        configAU = dict()
        configAU['history_length'] = 10
        model = PPO(s_dim=30, a_dim=1, device=device)
        model.load_weights(os.path.join(config_rl_module_path,'aurora_ckpt'))
        agent = AuroraAgent(model,configAU,device)

    env.agent = agent

    # run ccp-agent
    if args.standalone:
        sp,ec = env.run_ccp_agent()
        return ec
    else:
        sp,cp,ec = env.run_ccp_agent()
        return sp,cp,ec,env

def stream_output(stream):
    for line in stream:
        print('IPERF: '+line, end='')

def setup_mahimahi(args):
    trace_path = args.trace_path
    # start Mahimahi
    config_dict = {}

    config_dict["mmdelay"] = "mm-delay"
    config_dict["mmlink"] = "mm-link"
    config_dict["delay"] = int(args.linkdelay)

    config_dict["uplinktrace"] = os.path.join(args.trace_path,args.uplinktracefile)
    config_dict["downlinktrace"] = os.path.join(args.trace_path,args.downlinktracefile)

    config_dict["workloadSender"] = "./clientsender-test.sh"

    start_mahimahi_cmd = "sudo sysctl -w net.ipv4.ip_forward=1 && "
    start_mahimahi_cmd += \
            "%(mmdelay)s %(delay)d %(mmlink)s %(uplinktrace)s %(downlinktrace)s  \
            --uplink-queue=droptail --downlink-queue=droptail --uplink-queue-args=\"packets=40\" --downlink-queue-args=\"packets=40\"\
            %(workloadSender)s "% config_dict    
    print(start_mahimahi_cmd)
    # sh.run(start_mahimahi_cmd,shell=True)
    # sleep(1.0)
    # TODO:wait for the container end,then end the process
    mahimahi_container = sh.Popen(start_mahimahi_cmd, shell=True,stdout=sh.PIPE, stderr=sh.STDOUT, text=True)
    
    mahimahi_container.wait()  # Wait for the Mahimahi container to finish

def run_client_app(self):
    sh.Popen("sudo ./clientsender-test.sh", stdout=sh.PIPE, stderr=sh.PIPE,shell=True)

def log_reader(stream):
    for line in stream:
        print('TEST: '+line.decode('utf-8'), end='')

def run_server_app():
    sh.run("sudo pkill -9 iperf",shell=True)
    sleep(1.0)
    sh.run("sudo chmod a+x serverreceiver-test.sh",shell=True)
    server_process = sh.Popen("sudo ./serverreceiver-test.sh",stdout=sh.PIPE, stderr=sh.STDOUT,shell=True)
    threading.Thread(target=log_reader, args=(server_process.stdout,)).start()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--rl_channel_addr', type=str, default='127.0.0.1:4826')
    parser.add_argument('--base_path', type=str, required=False, default='')
    parser.add_argument('--eval', action='store_true', default=False,
                        help='(Deprecated 9/23)default is  %(default)s')
    parser.add_argument('--mode', type=int,  default=0)
    parser.add_argument('--flows', type=int, default=1)
    parser.add_argument('--bw', type=int,  default=48)
    parser.add_argument('--id', type=int,  default=0)
    parser.add_argument('--device', type=str, default='cpu')
    parser.add_argument('--model_type',type=str,default='dt')
    parser.add_argument('--load_file', type=str, default='sfdandc_4token_best_checkpoint_iter_17.pkl')#23999step seems better now #train len 50:14g_dt_tanh_len50_3_999.pkl
    parser.add_argument('--offline_return', type=float, default=600.0)#47500.0,40000.0,1250.0 #for tanh reward max return 1900+, set 1800 or 10000 seems no much difference

    parser.add_argument('--arch', type=str, default='lotus', choices=['lotus', 'portus'],
                        help='Rust 客户端架构: lotus (单TCP多流) 或 portus (每流独立TCP)')

    parser.add_argument('--plm_type', type=str, default='llama')
    parser.add_argument('--plm_size', type=str, default='base')
    parser.add_argument('--rank', type=int, help='rank of low-rank matrices. if set to -1, low-rank matrices will not be enabled', default=-1)
    parser.add_argument('--device_out', action='store', dest='device_out', help='device (cuda or cpu) to place the split of model near the output')
    parser.add_argument('--device_mid', action='store', dest='device_mid', help='device (cuda or cpu) to place the split of model between the input and output')

    parser.add_argument('--orca_eval', action='store_true', default=True)

    parser.add_argument('--linkdelay',type=int,default=5)
    parser.add_argument('--trace_path',type=str,default='./traces')
    parser.add_argument('--uplinktracefile',type=str,default='wired48')
    parser.add_argument('--downlinktracefile',type=str,default='wired48')
    parser.add_argument('--standalone', nargs='?', const=True, type=str2bool, default=False,
                        help='if True, run only the Python RPC server without starting the Rust client')

    parser.add_argument('--hybrid_enable', '--hybrid_alone', action='store_true',
                        dest='hybrid_enable', default=False)

    parser.add_argument('--batch', action='store_true', default=False)
    parser.add_argument('--batch_size', type=int, default=0,
                        help='BatchProcessor 的批大小（0 = 根据 flows/rpc_ms 自动推断）')
    parser.add_argument('--rpc_ms', type=float, default=5.0,
                        help='Agent 推理延迟经验值（毫秒），用于自动计算 batch_size')
    parser.add_argument('--rpc_timeout', type=int, default=0,
                        help='Rust lotus agent_task RPC timeout（毫秒）；0 = 使用旧逻辑 max(rpc_ms, mtp) * 2')
    parser.add_argument('--resp_timeout', type=int, default=0,
                        help='Rust lotus flow 等待 Python 响应 timeout（毫秒）；0 = rpc_timeout + 10')
    parser.add_argument('--bp_timeout', type=int, default=0,
                        help='BatchProcessor 收集请求的等待超时（毫秒）；0 = 按安全量自动计算(mtp*(1-1/n_flows)*0.7)')
    args = parser.parse_args()
    if args.hybrid_enable and args.arch != 'lotus':
        parser.error('--hybrid_enable 当前只支持 --arch lotus')

    if args.mode == 0:
        if args.standalone:
            configure_ccp_schema(args.model_type)
            from decision_transformer.envs.cc_standalone import CCEnv
            print("[TCPACTOR.PY][EVAL] Actor %s is starting ..." % (args.id))
            if args.hybrid_enable:
                print("[TCPACTOR.PY][EVAL] hybrid_alone/standalone mode: Python RPC server only; start ICCP separately.")
            ec = run_eval(args)
            print("*****************************Waiting for connections****************************")
        else:
            configure_ccp_schema(args.model_type)
            from decision_transformer.envs.cc import CCEnv
            print("[TCPACTOR.PY][TRAIN] Actor %s is starting ..." % (args.id))
            sp,cp,ec,env = run_eval(args)
            cleanup_done = [False]

            def _cleanup(sp, cp, env):
                if cleanup_done[0]:
                    print("Cleanup already completed, skipping.")
                    return
                cleanup_done[0] = True

                print("Cleaning up subprocesses...")

                def _stop_rpc_server():
                    try:
                        sp.terminate()
                        sp.join(timeout=3)
                        if sp.is_alive():
                            sp.kill()
                            sp.join(timeout=2)
                    except Exception:
                        pass

                # 先尝试停止全局 BatchProcessor（如果已创建）
                try:
                    import decision_transformer.envs.cc as cc_module
                    if hasattr(cc_module, 'batch_processor') and cc_module.batch_processor is not None:
                        try:
                            cc_module.batch_processor.stop()
                        except Exception:
                            pass
                except Exception:
                    pass

                # 先通知 monitor_process 停止
                if hasattr(env, '_monitor_stop_event'):
                    env._monitor_stop_event.set()
                # 终止 Rust client 进程
                agent_grace_sec = float(os.environ.get("DTCC_AGENT_CLIENT_GRACE_SEC", "0"))
                terminate_grace_sec = float(os.environ.get("DTCC_AGENT_CLIENT_TERM_SEC", "3"))
                if cp is None:
                    print("Rust agent_client was not started.")
                elif cp.poll() is None:
                    if agent_grace_sec > 0:
                        print(f"Waiting up to {agent_grace_sec:.1f}s for Rust agent_client to exit cleanly...")
                        try:
                            cp.wait(timeout=agent_grace_sec)
                            print("Rust agent_client exited cleanly.")
                        except sh.TimeoutExpired:
                            pass

                    if cp.poll() is None:
                        print("Rust agent_client still running; sending SIGTERM...")
                        try:
                            os.killpg(os.getpgid(cp.pid), signal.SIGTERM)
                        except Exception:
                            cp.terminate()

                        # Once Rust has been asked to stop, close the RPC server promptly
                        # so no post-test reports trigger more Python inference.
                        _stop_rpc_server()

                        try:
                            cp.wait(timeout=terminate_grace_sec)
                            print("Rust agent_client exited after SIGTERM.")
                        except Exception:
                            print("Rust agent_client did not exit after SIGTERM; sending SIGKILL.")
                            try:
                                os.killpg(os.getpgid(cp.pid), signal.SIGKILL)
                            except Exception:
                                cp.kill()
                            try:
                                cp.wait(timeout=2)
                            except Exception:
                                pass
                else:
                    print(f"Rust agent_client already exited with code {cp.returncode}.")

                _stop_rpc_server()
                print("Subprocess killed.")

            def _sigterm_handler(signum, frame):
                print("SIGTERM received, exiting...")
                _cleanup(sp, cp, env)
                sys.exit(0)

            signal.signal(signal.SIGTERM, _sigterm_handler)

            def _sigusr1_handler(signum, frame):
                """收到 SIGUSR1 信号时清空所有流的历史推理状态（用于跨轮实验重置）。"""
                print("[SIGUSR1] resetting all conn_prims for next experiment round")
                try:
                    env.agent.conn_prims.clear()
                    print("[SIGUSR1] conn_prims cleared")
                except Exception as e:
                    print(f"[SIGUSR1] failed to clear conn_prims: {e}")

            signal.signal(signal.SIGUSR1, _sigusr1_handler)

            # iperf -s
            # run_server_app()
            # mahimahi and iperf -c
            # setup_mahimahi(args)
            try:
                while True:
                    sleep(1)
            except KeyboardInterrupt:
                print("Main process killed.")
            finally:
                _cleanup(sp, cp, env)

            print("*****************************END of PYTHON****************************")
        

        
        
