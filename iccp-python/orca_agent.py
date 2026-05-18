import torch
import tensorflow as tf
import numpy as np
import os
import time
from orca_special.utils import OU_Noise, ReplayBuffer, G_Noise, Prioritized_ReplayBuffer

EXPLORE = 4000
STDDEV = 0.1
NSTEP = 0.3
import decision_transformer.envs.timestep as ts
from utils.dualqueue import dq
import numpy as np
import sys
import math
from utils.cc_agent import CCAgent
import os
import collections


MIN_CWND = 4
MAX_CWND = 10000

class Prims:
    def __init__(self, config, device):
        # 基础参数
        self.iterations = np.int64(0)
        self.pre_cwnd_rate = 1.0
        self.pre_cwnd_packets = 10

        # 状态相关
        self.max_bw = 0.0
        self.max_cwnd = 0.0
        self.max_smp = 0.0
        self.min_del = 9999999.0
        self.pre_pkt_lost = 0
        self.s0 = np.zeros([config['state_dim']],dtype=np.float32)
        self.s0_rec_buffer = np.zeros([config['s_dim']],dtype=np.float32)
        self.a= np.zeros([config['action_dim']],dtype=np.float32)
        # self.rewards = torch.ones(0, device=device, dtype=torch.float32)
        # self.target_return = torch.tensor(config['ep_return'], device=device, dtype=torch.float32).reshape(1, 1)
        # self.timesteps = torch.tensor(0, device=device, dtype=torch.long).reshape(1, 1)
        # self.actions = torch.zeros((0, config['act_dim']), device=device, dtype=torch.float32)

class Moving_Win():
    def __init__(self,win_size):
        self.queue_main = collections.deque(maxlen=win_size)
        self.queue_aux = collections.deque(maxlen=win_size)
        self.length = 0
        self.avg = 0.0
        self.size = win_size
        self.total_samples=0

    def push(self,sample_value,sample_num):
        if self.length<self.size:
            self.queue_main.append(sample_value)
            self.queue_aux.append(sample_num)
            self.length=self.length+1
            self.avg=(self.avg*self.total_samples+sample_value*sample_num)
            self.total_samples+=sample_num
            if self.total_samples>0:
                self.avg=self.avg/self.total_samples
            else:
                self.avg=0.0
        else:
            pop_value=self.queue_main.popleft()
            pop_num=self.queue_aux.popleft()
            self.queue_main.append(sample_value)
            self.queue_aux.append(sample_num)
            self.avg=(self.avg*self.total_samples+sample_value*sample_num-pop_value*pop_num)
            self.total_samples=self.total_samples+(sample_num-pop_num)
            if self.total_samples>0:
                self.avg=self.avg/self.total_samples
            else:
                self.avg=0.0

    def get_avg(self):
        return self.avg

    def get_length(self):
        return self.length

class Normalizer():
    def __init__(self, config):
        self.config = config
        self.n = 1e-5
        num_inputs = self.config['input_dim']
        self.mean = np.zeros(num_inputs)
        self.mean_diff = np.zeros(num_inputs)
        self.var = np.zeros(num_inputs)
        self.dim = num_inputs
        self.min = np.zeros(num_inputs)


    def observe(self, x):
        self.n += 1
        last_mean = np.copy(self.mean)
        self.mean += (x-self.mean)/self.n
        self.mean_diff += (x-last_mean)*(x-self.mean)
        self.var = self.mean_diff/self.n

    def normalize(self, inputs):
        obs_std = np.sqrt(self.var)
        a=np.zeros(self.dim)
        if self.n > 2:
            a=(inputs - self.mean)/obs_std
            for i in range(0,self.dim):
                if a[i] < self.min[i]:
                    self.min[i] = a[i]
            return a
        else:
            return np.zeros(self.dim)

    def normalize_delay(self,delay):
        obs_std = math.sqrt(self.var[0])
        if self.n > 2:
            return (delay - self.mean[0])/obs_std
        else:
            return 0

    def stats(self):
        return self.min

    def save_stats(self):
        dic={}
        dic['n']=self.n
        dic['mean'] = self.mean.tolist()
        dic['mean_diff'] = self.mean_diff.tolist()
        dic['var'] = self.var.tolist()
        dic['min'] = self.min.tolist()
        import json
        with open(os.path.join(self.config['train_dir'], 'stats.json'), 'w') as fp:
                json.dump(dic, fp)

        print("--------save stats at{}--------".format(self.config['train_dir']))
        # logger.info("--------save stats at{}--------".format(self.config['train_dir']))



    def load_stats(self, file='stats.json'):
        import json
        if os.path.isfile(os.path.join(self.config['train_dir'], file)):
            # print("Stats exist!, load", self.config.task)
            with open(os.path.join(self.config['train_dir'], file), 'r') as fp:
                history_stats = json.load(fp)
                print(history_stats)
            self.n = history_stats['n']
            self.mean = np.asarray(history_stats['mean'])
            self.mean_diff = np.asarray(history_stats['mean_diff'])
            self.var = np.asarray(history_stats['var'])
            self.min = np.asarray(history_stats['min'])
            return True
        else:
            print("stats file is missing when loading")
            return False


def create_input_op_shape(obs, tensor):
    input_shape = [x or -1 for x in tensor.shape.as_list()]
    return np.reshape(obs, input_shape)



class ORCAAgent(object):
    def __init__(self,model,observation_space,action_space,config,device,is_eval,use_batch,*args,**kwargs):

        # JXL：what is orca's onlineRLmodel, need abstract
        self.model = model
        self.device = device
        self.config = config
        self.observation_space=observation_space
        self.action_space=action_space
        # print(self.model)
        # self.model.eval()#TODO:may learn
        # self.model.to(device=self.device)
        self.arch = kwargs.get('arch', 'lotus')
        if self.arch == 'portus':
            self.clientapp = "orca-portus/target/debug/orca"
        else:
            self.clientapp = "orca/target/debug/orca"
        self.mtp=10
        self.evaluation = is_eval
        self.use_normalizer = self.config['use_normalizer']
        if self.use_normalizer == True:
            self.normalizer=Normalizer(config)
        else:
            self.normalizer=None
        self.use_batch = use_batch
        # cc.py now manages per-flow Prims for both batched and non-batched
        # inference, so this mapping must always exist.
        self.conn_prims = {}
        
    def prims_init(self):
        return Prims(self.config, self.device)

    def calculate_target_cwnd_rate(self,alpha):
        target_cwnd_rate = alpha / 100
        return target_cwnd_rate

    def _orca_observation_fields(self, obs, prims):
        """Return ORCA-style fields from ORCA, DTCC-lotus, or DTCC-portus observations."""
        if hasattr(obs, 'avgrtt'):
            return (
                obs.avgrtt,
                obs.deliveryRate,
                obs.cnt,
                obs.timeDelta,
                obs.sndCwnd,
                obs.pacingRate,
                obs.loss,
                obs.srtt,
                obs.minrtt,
            )

        time_delta_s = max(obs.timeDelta / 1000000.0, 1e-6)
        loss_delta = obs.loss - prims.pre_pkt_lost if obs.loss > prims.pre_pkt_lost else 0
        prims.pre_pkt_lost = obs.loss

        delivered = obs.delivered if getattr(obs, 'delivered', 0) > 0 else obs.bytesAcked / max(obs.sndMss, 1)
        delivery_rate_bps = obs.deliveryRate * 8
        pacing_rate_bps = obs.bytesSent * 8 / time_delta_s if obs.bytesSent > 0 else delivery_rate_bps
        loss_bytes = loss_delta * max(obs.sndMss, 1)

        return (
            obs.rtt,
            delivery_rate_bps,
            delivered,
            obs.timeDelta,
            obs.sndCwnd,
            pacing_rate_bps,
            loss_bytes,
            obs.rtt,
            obs.minrtt,
        )

    def get_state_reward(self,obs,prims):
        # print(f"DEBUG: avgrtt:{obs.avgrtt}, srtt_ms: {obs.srtt}, cnt:{obs.cnt}, loss:{obs.loss},delivery_rate:{obs.deliveryRate},pacing_rate:{obs.pacingRate}\n")
        avgrtt, delivery_rate, cnt, time_delta, snd_cwnd, pacing_rate_raw, loss, srtt, minrtt = \
            self._orca_observation_fields(obs, prims)
        if avgrtt <= 0 or srtt <= 0 or snd_cwnd <= 0:
            return None, None
        state = np.zeros(1)

        d = np.float32(avgrtt/1000.0)
        thr = delivery_rate
        samples = cnt
        delta_t = time_delta/1000000.0
        cwnd = snd_cwnd
        prims.pre_cwnd_packets = cwnd
        pacing_rate = pacing_rate_raw
        loss_rate = loss/max(delta_t, 1e-6)
        srtt_ms=srtt/1000.0
        min_rtt = minrtt/1000.0

        s0 = np.array([d,thr,samples,delta_t,cwnd,pacing_rate,loss_rate,srtt_ms,min_rtt],dtype=np.float32)
        if self.config['use_normalizer'] == True:
            if self.evaluation!=True:
                self.normalizer.observe(s0)
            s0 = self.normalizer.normalize(s0)
            min_ = self.normalizer.stats()
        else:
            min_ = s0-s0
        d_n=d-min_[0]
        thr_n=thr
        thr_n_min=thr-min_[1]
        samples_n=samples
        samples_n_min=samples -min_[2]
        delta_t_n=delta_t
        delta_t_n_min=delta_t-min_[3]
        cwnd_n_min=cwnd-min_[4]
        pacing_rate_n_min=pacing_rate-min_[5]
        loss_rate_n_min=loss_rate-min_[6]
        srtt_ms_min=srtt_ms-min_[7]
        min_rtt_min=min_rtt-min_[8]
        if self.use_normalizer==False:
                thr_n=thr_n
                thr_n_min=thr_n_min
                samples_n_min=samples_n_min
                cwnd_n_min=cwnd_n_min
                loss_rate_n_min=loss_rate_n_min
                d_n=d_n
        if prims.max_bw<thr_n_min:
            prims.max_bw=thr_n_min
        if prims.max_cwnd<cwnd_n_min:
            prims.max_cwnd=cwnd_n_min
        if prims.max_smp<samples_n_min:
            prims.max_smp=samples_n_min
        if prims.min_del>d_n:
            prims.min_del=d_n

        if srtt_ms_min > 0 and min_rtt_min > 0 and min_rtt_min*(self.config['delay_margin_coef'])<srtt_ms_min:
            delay_metric=(min_rtt_min*(self.config['delay_margin_coef']))/srtt_ms_min
        else:
            delay_metric=1

        if prims.max_bw != 0:
            reward  = (thr_n_min-5*loss_rate_n_min)/prims.max_bw*delay_metric
        else:
            reward = 0.0

        if prims.max_bw!=0:
            state[0]=thr_n_min/prims.max_bw
            tmp=pacing_rate_n_min/prims.max_bw
            if tmp>10:
                tmp=10
            state=np.append(state,[tmp])
            state=np.append(state,[5*loss_rate_n_min/prims.max_bw])
        else:
            state[0]=0
            state=np.append(state,[0])
            state=np.append(state,[0])
        state=np.append(state,[samples/cwnd if cwnd > 0 else 0])
        state=np.append(state,[delta_t_n])
        state=np.append(state,[min_rtt_min/srtt_ms_min if srtt_ms_min > 0 else 0])
        state=np.append(state,[delay_metric])
        
        # with open("./log/iccp-orca-cwnd-log.txt", "a") as log_file:
        #     log_file.write(" ".join(f"{x:.7f}" for x in state))
        #     log_file.write("      ")
        #     log_file.write(f"{obs.sndCwnd:.1f}")
        #     log_file.write("      ")
        #     log_file.write(f"{reward:.7f}")
        #     log_file.write("\n")
        return np.float32(state), np.float32(reward)

# TODO:TEST-const agent
    # def get_action(self,obs):
    #     print(self.get_state(obs))
    #     print("---------------------------------------------------------")
    #     print(self.get_reward(obs))
    #     return 1.0 , 0
    def map_action(self,action):
        out = math.pow(4, action)
        out *= 100
        out = int(out)
        return out
    
    # TODO:here ccaagent.get_action(): somehow like env.get_state_reward() ->self.get_state_reward
    # + agent.get_action()  -> model(orcaagent).get_action(state)
    # + [agent.step() i.e. feedback]
    def single_predict(self,obs,prims):
        state_dim = self.config['state_dim']
        act_dim = self.config['action_dim']
        iterations = prims.iterations

        # first action
        # print("DEBUG:iteration:{}".format(iterations))
        if iterations == 1:
            s0, _ = self.get_state_reward(obs,prims)
            if s0 is None:
                print("DEBUG: get_state_reward failed")
                return 0, 0
            prims.s0_rec_buffer = np.zeros([self.observation_space])
            prims.s0_rec_buffer[-1*state_dim:] = s0
            if self.config['recurrent']:
                action = self.model.get_action(prims.s0_rec_buffer,training = not self.evaluation)
            else:
                action = self.model.get_action(s0,training = not self.evaluation)
            prims.s0 = s0
            prims.a = action[0][0]
        else:
            s1, r= self.get_state_reward(obs,prims)
            terminal = 0
            if s1 is None:
                return 0,0
            else:
                s1_rec_buffer = np.concatenate((prims.s0_rec_buffer[state_dim:],s1))
            if self.config['recurrent']:
                # print(s1_rec_buffer)
                action = self.model.get_action(s1_rec_buffer, training = not self.evaluation)
            else:
                action = self.model.get_action(s1, training = not self.evaluation)

            # JXL:down is to create feedback
            # JXL:down is to train 
            if not self.evaluation:
                print("DEBUG:feedback and train")
                _ , _ = self.model.actor_train_step(
                    prims.s0_rec_buffer if self.config['recurrent'] else prims.s0,
                    action[0][0],
                    np.array([r]),
                    s1_rec_buffer if self.config['recurrent'] else s1,
                    np.array([terminal],np.float)
                )

            prims.s0 = s1
            prims.a = action[0][0]
            if self.config['recurrent']:
                prims.s0_rec_buffer = s1_rec_buffer

        # JXL:down is to map and write action
        a = action
        alpha = self.map_action(a[0][0])
        target_cwnd_rate= self.calculate_target_cwnd_rate(alpha)
        temp_cwnd_packets = prims.pre_cwnd_packets * target_cwnd_rate
        # prims.pre_cwnd_packets = temp_cwnd_packets
        target_cwnd_packets = 0xFFFFFFFF if temp_cwnd_packets >= 0xFFFFFFFF else temp_cwnd_packets
        if target_cwnd_packets < MIN_CWND: 
            target_cwnd_packets = MIN_CWND
            # prims.pre_cwnd_packets = MIN_CWND
        if target_cwnd_packets > MAX_CWND:
            target_cwnd_packets = MAX_CWND
            # prims.pre_cwnd_packets = MAX_CWND
        prims.pre_cwnd_rate = target_cwnd_rate
        # print(f"DEBUG: alpha:{alpha}, modified_cwnd_rate: {target_cwnd_rate}, cwnd_packets = {target_cwnd_packets}\n")
        return target_cwnd_packets, 0
    
    def register_prim(self, conn_id, prim):
        self.conn_prims[conn_id] = prim

    def unregister_prim(self, conn_id):
        try:
            if conn_id in self.conn_prims:
                del self.conn_prims[conn_id]
        except Exception:
            pass
    
    def batch_predict(self, states, rewards, conn_ids):
        if not self.use_batch:
            raise RuntimeError("批量预测在单连接处理模式下不可用")
        # 确保所有输入有相同批次大小
        state_dim = self.config['state_dim']
        act_dim = self.config['action_dim']
        batch_size = states.shape[0]
        if batch_size == 0:
            return np.zeros(0, dtype=np.float32), np.zeros(0, dtype=np.float32)
        if len(rewards) != batch_size or len(conn_ids) != batch_size:
            raise ValueError(f"输入维度不一致: states={batch_size}, rewards={len(rewards)}, conn_ids={len(conn_ids)}")
        # 准备结果列表
        cwnd_list = np.zeros(batch_size, dtype=np.float32)
        rate_list = np.zeros(batch_size, dtype=np.float32)
        # 准备批量推理的输入数据
        batch_input = []
        prims_list = []
        
        for i, conn_id in enumerate(conn_ids):
            if conn_id not in self.conn_prims:
                self.conn_prims[conn_id] = Prims(self.config, self.device)
            prims = self.conn_prims[conn_id]
            prims_list.append(prims)
            # 根据是否首次迭代准备输入
            if prims.iterations == 1:
                # 首次迭代使用初始状态缓冲区
                prims.s0_rec_buffer = np.zeros([self.config['s_dim']], dtype=np.float32)
                prims.s0_rec_buffer[-1*state_dim:] = states[i]
                input_data = prims.s0_rec_buffer if self.config['recurrent'] else states[i]
            else:
                prev_input_data = prims.s0_rec_buffer if self.config['recurrent'] else prims.s0
                # 非首次迭代更新状态缓冲区
                if self.config['recurrent']:
                    # 合并历史状态和新状态
                    input_data = np.concatenate((prims.s0_rec_buffer[state_dim:], states[i]))
                else:
                    input_data = states[i]
                
                # 如果是训练模式且非首次迭代，更新模型
                if not self.evaluation:
                    self.model.actor_train_step(
                        prev_input_data,
                        prims.a,
                        np.array([rewards[i]]),
                        input_data,
                        np.array([0])  # 终止状态
                    )
                if self.config['recurrent']:
                    prims.s0_rec_buffer = input_data  # 更新状态缓冲区
            
            # 更新历史状态
            prims.s0 = states[i]
            batch_input.append(input_data)
        
        # 转换为numpy数组进行批量推理
        batch_input = np.array(batch_input)
        # print("Batch predict:{}".format(batch_input))
        if not self.model or not hasattr(self.model, 'actor'):
            print("WARNING: Model or get_action is invalid, returning default action")
            return np.full(batch_size, MIN_CWND, dtype=np.float32), rate_list

        infer_start = time.time()
        batch_tensor = tf.convert_to_tensor(batch_input, dtype=tf.float32)
        actions = self.model.actor(batch_tensor, training=not self.evaluation).numpy()
        infer_duration_ms = (time.time() - infer_start) * 1000
        # 确保动作数组转换为一维
        if actions.ndim == 2 and actions.shape[1] == 1:
            # 如果是二维单列数组，转换为一维
            actions = actions.squeeze(axis=1)
                
        # 如果仍然不是一维，尝试展平
        if actions.ndim > 1:
            actions = actions.ravel()
            
        # 确保结果是一维数组
        assert actions.ndim == 1, "Actions must be 1-dimensional after processing"
        
        # 处理批次大小不匹配
        if actions.shape[0] != batch_size:
            # 创建与目标形状兼容的填充值
            if actions.size > 0:
                # 获取第一个样本的动作值作为默认值
                default_val = actions[0].item() if isinstance(actions[0], np.ndarray) else actions[0]
                default_val = float(default_val)  # 确保是标量
            else:
                default_val = 0.0
            
            # 创建形状兼容的填充数组
            actions = np.full(batch_size, default_val)

        for i in range(batch_size):
            prims = prims_list[i]
            
            if i < len(actions):
                action_val = actions[i]
            else:
                action_val = 0.0
            prims.a = action_val
            
            # 计算目标cwnd
            alpha = self.map_action(action_val)
            target_cwnd_rate = self.calculate_target_cwnd_rate(alpha)
            
            # 计算目标cwnd并限制范围
            target_cwnd_packets = prims.pre_cwnd_packets * target_cwnd_rate
            target_cwnd_packets = max(MIN_CWND, min(MAX_CWND, target_cwnd_packets))
            
            # 更新Prims状态
            prims.pre_cwnd_rate = target_cwnd_rate
            prims.pre_cwnd_packets = target_cwnd_packets
            
            # 添加到结果列表
            cwnd_list[i] = target_cwnd_packets
            rate_list[i] = 0  # 目前不支持rate设置

        print(f"[batch_predict] N={batch_size:2d} "
              f"infer_ms={infer_duration_ms:.2f} "
              f"per_item_ms={infer_duration_ms/batch_size:.2f} "
              f"success={batch_size}")
        
        return cwnd_list, rate_list

    def get_action(self, obs, prim, conn_id):
        """为单个连接获取动作"""
        if self.use_batch and conn_id is None:
            raise ValueError("conn_id must be provided")
        
        state, reward = self.get_state_reward(obs, prim)
        if state is None:
            return prim.pre_cwnd_packets, 0
        
        if self.use_batch:
            # 批量处理路径
            state_array = np.expand_dims(state, axis=0)
            reward_array = np.array([reward], dtype=np.float32)
            actions, _ = self.batch_predict(state_array,reward_array, [conn_id])
            return actions[0], 0
        else:
            return self.single_predict(obs, prim)
