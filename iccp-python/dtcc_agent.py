import torch
from decision_transformer.models.decision_transformer import DecisionTransformer
import decision_transformer.envs.timestep as ts
from utils.dualqueue import dq
import numpy as np
import sys
import math
from utils.cc_agent import CCAgent

# 定义常量
ESTIMATE = 0
ACCURACY = 10000.0
# 定义常量
ESTIMATE = 0
ACCURACY = 10000.0
MIN_CWND = 4
MAX_CWND = 20000
USEC_PER_SEC = 1_000_000.0

RTG_5TH_PERCENTILE = 2439.53
RTG_95TH_PERCENTILE = 34487.80
RTG_LOWER_BOUND = RTG_5TH_PERCENTILE
RTG_UPPER_BOUND = RTG_95TH_PERCENTILE

class FixedRTGScaler:
    def __init__(self, q5, q95):
        self.scale_factor = 2.0 / (q95 - q5)
        self.offset = - (q5 + q95) * 0.5 * self.scale_factor

    def scale(self, raw_rtg):
        scaled = raw_rtg * self.scale_factor + self.offset
        return np.tanh(scaled) * 1.5
    
    def inverse_scale(self, scaled_rtg):
        # 反函数更精确
        unscaled = (scaled_rtg / 1.5)  # 反 tanh
        return (unscaled - self.offset) / self.scale_factor
    
    def update_rtg(self, reward, current_rtg):
        """
        更新目标 RTG
        """
        # 反缩放当前 RTG
        raw_rtg = self.inverse_scale(current_rtg)
        
        # 更新原始 RTG
        new_raw_rtg = raw_rtg - reward
        
        # 重新缩放
        return self.scale(new_raw_rtg)

class InferenceRTGScaler:
    def __init__(self, global_q5, global_q95, window_size=50):
        """
        global_q5: 训练集 RTG 的 5% 分位数
        global_q95: 训练集 RTG 的 95% 分位数
        window_size: 滑动窗口大小
        """
        self.global_q5 = global_q5
        self.global_q95 = global_q95
        self.window_size = window_size
        self.rtg_history = []  # 存储历史 RTG 值
        self.scaled_history = []  # 存储缩放后的 RTG 值
        
    def scale(self, raw_rtg):
        """
        推理时自适应缩放单个 RTG 值
        """
        # 更新历史记录
        self.rtg_history.append(raw_rtg)
        if len(self.rtg_history) > self.window_size:
            self.rtg_history.pop(0)
        
        # 计算窗口内的分位数（如果有足够数据）
        if len(self.rtg_history) >= 5:  # 最小数据点要求
            window_q5 = np.percentile(self.rtg_history, 5)
            window_q95 = np.percentile(self.rtg_history, 95)
        else:
            # 使用全局分位数作为默认值
            window_q5 = self.global_q5
            window_q95 = self.global_q95
        
        # 动态缩放参数
        scale_factor = 2.0 / (window_q95 - window_q5 + 1e-6)
        offset = - (window_q5 + window_q95) * 0.5 * scale_factor

        # 应用缩放
        scaled = raw_rtg * scale_factor + offset
        
        # 软截断防止极端值
        scaled = np.tanh(scaled) * 1.5
        
        # 保存缩放后的值
        self.scaled_history.append(scaled)
        return scaled
    
    def inverse_scale(self, scaled_rtg):
        """
        将缩放后的 RTG 转换回原始空间（用于目标更新）
        """
        if not self.scaled_history:
            return scaled_rtg * (self.global_q95 - self.global_q5) / 2.0 + (self.global_q5 + self.global_q95) / 2.0

        # 使用最近的有效缩放参数
        last_raw = self.rtg_history[-1]
        last_scaled = self.scaled_history[-1]
        
        # 近似反函数
        if last_scaled != 0:
            scale_factor = last_scaled / last_raw
            return scaled_rtg / scale_factor
        return scaled_rtg
    
    def update_rtg(self, reward, current_rtg):
        """
        更新目标 RTG
        """
        # 反缩放当前 RTG
        raw_rtg = self.inverse_scale(current_rtg)
        
        # 更新原始 RTG
        new_raw_rtg = raw_rtg - reward
        
        # 重新缩放
        return self.scale(new_raw_rtg)

class Prims:
    def __init__(self, config, device):
        # 基础参数
        self.iterations = np.int64(0)
        self.pre_cwnd_rate = 1.0
        self.pre_cwnd_packets = 10
        # RTT 和吞吐量
        self.rtt_s = dq(config['short_win'])
        self.rtt_m = dq(config['mid_win'])
        self.rtt_l = dq(config['long_win'])
        self.thr_s = dq(config['short_win'])
        self.thr_m = dq(config['mid_win'])
        self.thr_l = dq(config['long_win'])

        # RTT 变化率和方差
        self.rtt_rate_s = dq(config['short_win'])
        self.rtt_rate_m = dq(config['mid_win'])
        self.rtt_rate_l = dq(config['long_win'])
        self.rtt_var_s = dq(config['short_win'])
        self.rtt_var_m = dq(config['mid_win'])
        self.rtt_var_l = dq(config['long_win'])

        # 流量相关
        self.inflight_s = dq(config['short_win'])
        self.inflight_m = dq(config['mid_win'])
        self.inflight_l = dq(config['long_win'])
        self.lost_s = dq(config['short_win'])
        self.lost_m = dq(config['mid_win'])
        self.lost_l = dq(config['long_win'])

        # 其他监控数据
        self.pre_bytes_sent = 0
        self.pre_pkt_lost = 0
        self.total_bytes_acked_pre = 0
        self.pre_dr_w_mbps = 0
        self.pre_dr_w_max = 48.0
        self.loss_db = dq(100)
        self.sent_db = dq(100)
        self.sent_dt = dq(100)
        self.dlv_db = dq(100)
        self.sending_rates = dq(5000)
        self.dr_w = dq(200)
        self.uack_db = dq(100)

        # 状态相关
        self.states = torch.zeros((1, config['state_dim']), device=device, dtype=torch.float32)
        self.rewards = torch.ones(0, device=device, dtype=torch.float32)
        self.target_return = torch.tensor(0, device=device, dtype=torch.float32).reshape(1, 1)
        # self.target_return = torch.tensor(config['ep_return'] / config['scale'], device=device, dtype=torch.float32).reshape(1, 1)
        self.timesteps = torch.tensor(0, device=device, dtype=torch.long).reshape(1, 1)
        self.actions = torch.zeros((0, config['act_dim']), device=device, dtype=torch.float32)

        # attentions ckpt
        self.attention_buffer = {}

class DTCCAgent(CCAgent):
    def __init__(self,model,observation_space,action_space,config,device,env_bw,use_batch,*args,**kwargs):
        self.observation_space=observation_space
        self.action_space=action_space
        self.device=device
        self.env_bw = env_bw
        self.config = config
        self.model = model
        self.arch = kwargs.get('arch', 'lotus')
        self.mtp=10
        if self.arch == 'portus':
            self.clientapp="dtcc-portus/target/debug/dtcc"
        else:
            self.clientapp="dtcc/target/debug/dtcc"
        self.optimizer = torch.optim.AdamW(
        self.model.parameters(),
        lr=config['lr'],
        weight_decay=config['weight_decay'],
        )
        # print(self.model)
        self.model.eval()
        self.model.to(device=self.device)
        # self.prims = None
        self.max_window_size = config['max_length']
        self.use_batch = use_batch
        # if self.use_batch:
        #     self.conn_prims = {}
        self.conn_prims = {}
        # self.rtg_scaler = InferenceRTGScaler(
        #     global_q5=RTG_5TH_PERCENTILE,
        #     global_q95=RTG_95TH_PERCENTILE,
        #     window_size=50
        # )
        self.rtg_scaler = FixedRTGScaler(
            q5=RTG_5TH_PERCENTILE,
            q95=RTG_95TH_PERCENTILE
        )

    def prims_init(self):
        return Prims(self.config, self.device)

    def reset_timestep(self,state):
        return ts.restart(state)

    def _convert_timestep(self, ts):
        return ts._replace(discount=np.array(ts.discount, copy=False, dtype=np.float32))

    def get_timestep(self,state,reward):
        return self._convert_timestep(ts.transition(reward=reward, observation=state))

    def check_values(self, o, r):
        for i in range(len(o)):
            if math.isnan(o[i]) or math.isinf(o[i]):
                with open('wrongvalues.txt', 'a') as f:
                    f.write("[Actor: "+str(self.id)+"] NONE/INF signal detected: index="+str(i)+" input: "+str(o))
                sys.stderr.write("[Actor: "+str(self.id)+"] NONE signal detected: index="+str(i)+" input: "+str(o))
                o[i] = 0
        
        if math.isnan(r) or math.isinf(r):
            with open('wrongvalues.txt', 'a') as f:
                f.write("[Actor: "+str(self.id)+"] NONE/INF reward detected: index="+str(r))
            r = 0
        
        return o, r
    
    # jxl:TODO1020 back cwnd or back cwnd_ratio?
    def calculate_target_cwnd_rate(self,alpha):
        if ESTIMATE:
            tmp_target_cwnd = ACCURACY * alpha
            alpha_tmp = round(tmp_target_cwnd)
            target_cwnd_rate = alpha_tmp / ACCURACY
        else:
            target_cwnd_rate = alpha
        return target_cwnd_rate

    def get_state_reward(self,obs,prims):
        # TODO:scale and normalize to input state
        duration_us = obs.duration if obs.duration > 0 else obs.timeDelta
        time_delta = duration_us / USEC_PER_SEC
        if time_delta <= 0:
            return None, None
        bytes_acked_ = obs.bytesAcked
        if bytes_acked_ <= 0 and obs.delivered <= 0:
            return None, None

        # Kernel reports are ACK-clocked: in real networks a nominal 10ms report
        # can arrive after 30-50ms. Convert all count-like observations to rates
        # over the real report duration before feeding the policy.
        canonical_duration_us = max(float(self.mtp) * 1000.0, 1.0)
        duration_scale = canonical_duration_us / max(float(duration_us), 1.0)
        acked_rate = bytes_acked_ / time_delta
        sent_rate = obs.bytesSent / time_delta
        delivered_bytes = (
            obs.deliveryRate * time_delta
            if obs.deliveryRate > 0
            else obs.delivered * obs.sndMss
        )
        loss_bytes = obs.loss * obs.sndMss
        bytes_sent_norm = obs.bytesSent * duration_scale
        delivered_bytes_norm = delivered_bytes * duration_scale
        loss_bytes_norm = loss_bytes * duration_scale
        dt = duration_us
        min_rtt_us = obs.minrtt

        s_db = bytes_sent_norm
        prims.sent_db.add(s_db)
        # print(f"DEBUG: obs.bytesSent: {obs.bytesSent}, obs.bytesAcked: {obs.bytesAcked}, obs.delivered: {obs.delivered}, delta_time: {time_delta}\n")
        if obs.rtt > 0:
            rtt_rate = min_rtt_us/obs.rtt
            sending_rate = sent_rate
            prims.sending_rates.add(sending_rate)
            max_sending_rate = prims.sending_rates.max()
            acked_rate = acked_rate/max_sending_rate
        else:
            rtt_rate = 0.0

        prims.rtt_s.add(obs.rtt/100000.0)
        prims.rtt_m.add(obs.rtt/100000.0)
        prims.rtt_l.add(obs.rtt/100000.0)
        prims.thr_s.add(obs.deliveryRate/125000.0/100)
        prims.thr_m.add(obs.deliveryRate/125000.0/100)
        prims.thr_l.add(obs.deliveryRate/125000.0/100)
        prims.rtt_rate_s.add(rtt_rate)
        prims.rtt_rate_m.add(rtt_rate)
        prims.rtt_rate_l.add(rtt_rate)
        prims.rtt_var_s.add(obs.rttvar/1000.0)
        prims.rtt_var_m.add(obs.rttvar/1000.0)
        prims.rtt_var_l.add(obs.rttvar/1000.0)
        prims.inflight_s.add(obs.unacked/1000.0)
        prims.inflight_m.add(obs.unacked/1000.0)
        prims.inflight_l.add(obs.unacked/1000.0)
        prims.lost_s.add(obs.loss/100.0)
        prims.lost_m.add(obs.loss/100.0)
        prims.lost_l.add(obs.loss/100.0)

        l_db = loss_bytes_norm
        prims.pre_pkt_loss = obs.loss
        prims.loss_db.add(l_db)
        prims.sent_dt.add(canonical_duration_us)
        dt_sum = prims.sent_dt.sum()
        l_w_mbps = 8 * prims.loss_db.sum() / dt_sum
        # 10/17TODO: add dr_w_mbps, need add obs.delivered
        # print(f"DEBUG: previous prims.sent_db.sum: {prims.dlv_db.sum()}\n")
        prims.dlv_db.add(delivered_bytes_norm)
        
        dr_w_mbps = 8 * prims.dlv_db.sum() / dt_sum
        # print(f"DEBUG: after prims.sent_db.sum: {prims.dlv_db.sum()}, obs.delivered: {obs.delivered}, obs.deliveryrate:{8*obs.deliveryRate/1000000.0},dlv: {dr_w_mbps},sr:{8*sending_rate/1000000.0}\n")
        if prims.pre_dr_w_mbps > 0.0:
            dr_ratio = dr_w_mbps / prims.pre_dr_w_mbps
        else:
            dr_ratio = dr_w_mbps

        prims.dr_w.add(dr_w_mbps)
        dr_w_max = prims.dr_w.max()
        if dr_w_max == 0: dr_w_max = 1

        if prims.pre_dr_w_max > 0: dr_w_max_ratio = dr_w_max / prims.pre_dr_w_max
        else: dr_w_max_ratio = dr_w_max

        cwnd_bits = obs.sndCwnd * obs.sndMss * 8
        if cwnd_bits == 0:
            cwnd_bits += 1
        # prims.pre_cwnd_packets = int(obs.sndCwnd)
        prims.uack_db.add(obs.unacked * obs.sndMss)
        ua_db_tmp = prims.uack_db.avg()
        s_db_tmp =prims.sent_db.sum()
        if s_db_tmp > 0:
            cwnd_unacked_rate = ua_db_tmp/s_db_tmp
        else:
            cwnd_unacked_rate = ua_db_tmp
        
        if obs.rtt > 0: time_delta = 1000000.0*time_delta/min_rtt_us
        else: time_delta = 10
        # cwnd_rate = round(math.log2(prims.pre_cwnd_rate)*1000/1000.0) if prims.pre_cwnd_rate >0.0 else np.log2(0.0001)
        if prims.pre_cwnd_rate > 0.0:
            cwnd_rate = round(math.log2(prims.pre_cwnd_rate) * 1000) / 1000
        else:
            cwnd_rate = math.log2(0.0001)
        state = np.zeros(self.config['state_dim'],)
        state[0] = obs.rtt/100000.0 
        state[1] = obs.rttvar/1000.0
        state[2] = obs.deliveryRate/125000.0/100
        state[3] = obs.castate
        state[4] = prims.rtt_s.get_avg()
        state[5] = prims.rtt_s.get_min()
        state[6] = prims.rtt_s.get_max()
        state[7] = prims.rtt_m.get_avg()
        state[8] = prims.rtt_m.get_min()
        state[9] = prims.rtt_m.get_max()
        state[10] = prims.rtt_l.get_avg()
        state[11] = prims.rtt_l.get_min()
        state[12] = prims.rtt_l.get_max()
        state[13] = prims.thr_s.get_avg()
        state[14] = prims.thr_s.get_min()
        state[15] = prims.thr_s.get_max()
        state[16] = prims.thr_m.get_avg()
        state[17] = prims.thr_m.get_min()
        state[18] = prims.thr_m.get_max()
        state[19] = prims.thr_l.get_avg()
        state[20] = prims.thr_l.get_min()
        state[21] = prims.thr_l.get_max()
        state[22] = prims.rtt_rate_s.get_avg()
        state[23] = prims.rtt_rate_s.get_min()
        state[24] = prims.rtt_rate_s.get_max()
        state[25] = prims.rtt_rate_m.get_avg()
        state[26] = prims.rtt_rate_m.get_min()
        state[27] = prims.rtt_rate_m.get_max()
        state[28] = prims.rtt_rate_l.get_avg()
        state[29] = prims.rtt_rate_l.get_min()
        state[30] = prims.rtt_rate_l.get_max()
        state[31] = prims.rtt_var_s.get_avg()
        state[32] = prims.rtt_var_s.get_min()
        state[33] = prims.rtt_var_s.get_max()
        state[34] = prims.rtt_var_m.get_avg()
        state[35] = prims.rtt_var_m.get_min()
        state[36] = prims.rtt_var_m.get_max()
        state[37] = prims.rtt_var_l.get_avg()
        state[38] = prims.rtt_var_l.get_min()
        state[39] = prims.rtt_var_l.get_max()
        state[40] = prims.inflight_s.get_avg()
        state[41] = prims.inflight_s.get_min()
        state[42] = prims.inflight_s.get_max()
        state[43] = prims.inflight_m.get_avg()
        state[44] = prims.inflight_m.get_min()
        state[45] = prims.inflight_m.get_max()
        state[46] = prims.inflight_l.get_avg()
        state[47] = prims.inflight_l.get_min()
        state[48] = prims.inflight_l.get_max()
        state[49] = prims.lost_s.get_avg()
        state[50] = prims.lost_s.get_min()
        state[51] = prims.lost_s.get_max()
        state[52] = prims.lost_m.get_avg()
        state[53] = prims.lost_m.get_min()
        state[54] = prims.lost_m.get_max()
        state[55] = prims.lost_l.get_avg()
        state[56] = prims.lost_l.get_min()
        state[57] = prims.lost_l.get_max()
        # state[58] = elapsed_time
        state[58] = time_delta
        state[59] = rtt_rate
        state[60] = l_w_mbps/100
        state[61] = acked_rate
        state[62] = dr_ratio
        state[63] = dr_w_max * min_rtt_us / cwnd_bits
        state[64] = dr_w_mbps / 100
        state[65] = cwnd_unacked_rate
        state[66] = dr_w_max_ratio
        state[67] = dr_w_max / 100
        state[68] = cwnd_rate
        # TODO: state_69 is pre cwnd_rate
        prims.pre_dr_w_mbps = dr_w_mbps
        # 控制前期的最大值
        if (dr_w_mbps - prims.pre_dr_w_max) / prims.pre_dr_w_max > 0.1:
           is_probing = True
        else:
           is_probing = False
        prims.pre_dr_w_max = dr_w_max

        signof_bw = 1
        rtt_rate_tmp = 1 if rtt_rate >= 0.8 else rtt_rate
        if dr_w_mbps < 2 * l_w_mbps: signof_bw = -1
        # check bw_true?
        # bw_true = dr_w_mbps
        # bw_current_max = self.env_bw
        # if dr_w_mbps > bw_current_max:
        #     bw_true = bw_current_max * 0.9

        # reward = signof_bw *((dr_w_mbps - 2 * l_w_mbps)* (dr_w_mbps - 2 * l_w_mbps))/(bw_current_max*bw_current_max)*rtt_rate_tmp
        # reward0 = signof_bw *(25*(bw_true - 2 * l_w_mbps)* (bw_true - 2 * l_w_mbps))/(bw_current_max*bw_current_max)*rtt_rate_tmp

        # 使用历史最大吞吐量作为参考基准
        if is_probing:
            bw_ref = max(prims.pre_dr_w_max, 48.0)
        else:
            bw_ref = max(prims.pre_dr_w_max, 1.0)  
        # 计算相对带宽利用率
        bw_util = min(dr_w_mbps / bw_ref, 1.0)  # 限制在0-1范围内
        # 计算带宽效率（吞吐量 - 2倍丢包率）
        bw_eff = max(bw_util - (2 * l_w_mbps / bw_ref), 0)
        # 新的奖励函数
        reward0 = signof_bw * 25 * bw_eff * rtt_rate_tmp

        reward0 = np.trunc(reward0*1e7)/1e7
        reward = np.tanh(float(reward0/25))
    
        # print(f"INFO:reward: {reward} bw_true: {bw_true} rtt_rate: {rtt_rate_tmp}, dt_sum: {dt_sum} dlv_sum:{prims.dlv_db.sum()} obs_dlv: {obs.deliveryRate}\n")
        state, reward = self.check_values(state,reward)
        state = np.trunc(state * 1e7) / 1e7
        
        # with open("./log/iccp-dtcc-cwnd-log.txt", "a") as log_file:
        #     log_file.write(" ".join(f"{x:.7f}" for x in state))
        #     log_file.write("     ")
        #     log_file.write(f"{reward0:.7f}")
        #     log_file.write("     ")
        #     log_file.write(f"{obs.sndCwnd:.1f}")
        #     log_file.write("\n")
        return np.float32(state), np.float32(reward0)

# TODO:TEST-const agent
    # def get_action(self,obs):
    #     print(self.get_state(obs))
    #     print("---------------------------------------------------------")
    #     print(self.get_reward(obs))
    #     return 1.0 , 0
    def map_action(self,action):
        # beta = 0.8
        # noise = random.gauss(0, 1) 
        # action += beta * noise
        if self.config['action_version']==9:
            m_action = math.pow(2,round(action,3))   
        else:
            m_action = action

        return m_action

    def process_reward(self, reward, rewards_max=171.42, REWARD_SCALE_FACTOR=20.0, MREWARD_SCALE_FACTOR=10.0):
        if reward > 0:
            return min(reward, rewards_max * 0.95) / REWARD_SCALE_FACTOR
        else:
            abs_neg = min(abs(reward), 10000.0)
            return -np.log1p(abs_neg) / MREWARD_SCALE_FACTOR

# TODO:abstract!
    def single_predict(self,obs,prim):
        state_dim = self.config['state_dim']
        act_dim = self.config['act_dim']
        iterations = prim.iterations
        with torch.no_grad():
            # while iterations < 100000:
            # while iterations < self.config['max_ep_len']:
                # first action
                # print("INFO:iteration:{}".format(iterations))
            if iterations == 1:
                state0 , _ = self.get_state_reward(obs,prim)
                if state0 is None:
                    return prim.pre_cwnd_packets,0
                timestep = self.reset_timestep(state0)
                prim.states = torch.from_numpy(timestep.observation).reshape(1, state_dim).to(device=self.device, dtype=torch.float32)
                prim.rewards = torch.cat([prim.rewards, torch.zeros(1, device=self.device)])
                prim.actions = torch.cat([prim.actions, torch.zeros((1, act_dim), device=self.device)], dim=0)

                scaled_return = self.rtg_scaler.scale(RTG_95TH_PERCENTILE)
                prim.target_return = torch.tensor(scaled_return, device=self.device, dtype=torch.float32).reshape(1, 1)
            else:
                state0 , reward0 = self.get_state_reward(obs,prim)
                if state0 is None:
                    return prim.pre_cwnd_packets,0
                next_timestep = self.get_timestep(state0,reward0)
                # TODO:check states, rewards
                if len(prim.states) > self.max_window_size:
                    start_index = len(prim.states) - self.max_window_size
                    prim.states = prim.states.narrow(0,start_index,self.max_window_size)
                    prim.actions = prim.actions.narrow(0,start_index,self.max_window_size)
                    prim.rewards = prim.rewards.narrow(0,start_index,self.max_window_size)
                    prim.target_return = prim.target_return.narrow(1,start_index,self.max_window_size)
                    prim.timesteps = prim.timesteps.narrow(1,start_index,self.max_window_size)
                # JXL：timessteps discount

                reward = next_timestep.reward
                prim.rewards[-1] = torch.tensor(self.process_reward(reward),dtype=torch.float32)
                # pred_return = prim.target_return[0,-1] - reward
                # pred_return = prim.target_return[0,-1] - reward / self.config['scale']
                pred_return = torch.tensor(self.rtg_scaler.update_rtg(reward, prim.target_return[0, -1].item()), dtype=torch.float32)

                cur_state = torch.from_numpy(next_timestep.observation).to(device=self.device).reshape(1, state_dim)
                prim.states = torch.cat([prim.states, cur_state], dim=0)
                prim.target_return = torch.cat(
                    [prim.target_return, pred_return.reshape(1, 1)], dim=1)
                prim.timesteps = torch.cat(
                    [prim.timesteps, torch.ones((1, 1), device=self.device, dtype=torch.long) * (iterations+1)], dim=1)
                # decide next action
                prim.actions = torch.cat([prim.actions, torch.zeros((1, act_dim), device=self.device)], dim=0)
                prim.rewards = torch.cat([prim.rewards, torch.zeros(1, device=self.device)])

            action, attentions =self.model.get_action(
                (prim.states.to(dtype=torch.float32)-self.config['state_mean'])/self.config['state_std'],
                prim.actions.to(dtype=torch.float32),
                prim.rewards.to(dtype=torch.float32),
                prim.target_return.to(dtype=torch.float32),
                prim.timesteps.to(dtype=torch.long),
            )

            prim.actions[-1] = action
            a = action.detach().cpu().numpy()
            alpha = self.map_action(a[0])

            # NaN/Inf 防御：模型输出异常时保持上次 cwnd 不变
            if not math.isfinite(alpha):
                print(f"[single_predict] WARNING: action alpha is {alpha} at iter {iterations}, "
                      f"keeping pre_cwnd_packets={prim.pre_cwnd_packets}")
                return max(MIN_CWND, min(MAX_CWND, math.ceil(prim.pre_cwnd_packets))), 0

            target_cwnd_rate= self.calculate_target_cwnd_rate(alpha)
            temp_cwnd_packets = prim.pre_cwnd_packets * target_cwnd_rate

            # 防止 NaN 污染 pre_cwnd_packets
            if not math.isfinite(temp_cwnd_packets):
                print(f"[single_predict] WARNING: temp_cwnd_packets NaN/Inf at iter {iterations}, "
                      f"keeping pre_cwnd_packets={prim.pre_cwnd_packets}")
                return max(MIN_CWND, min(MAX_CWND, math.ceil(prim.pre_cwnd_packets))), 0

            prim.pre_cwnd_packets = temp_cwnd_packets
            temp_cwnd_packets = math.ceil(temp_cwnd_packets)
            target_cwnd_packets = 0xFFFFFFFF if temp_cwnd_packets >= 0xFFFFFFFF else temp_cwnd_packets
            if target_cwnd_packets < MIN_CWND: 
                target_cwnd_packets = MIN_CWND
                prim.pre_cwnd_packets = MIN_CWND
            if target_cwnd_packets > MAX_CWND:
                target_cwnd_packets = MAX_CWND
                prim.pre_cwnd_packets = MAX_CWND
            prim.pre_cwnd_rate = target_cwnd_rate
            
            # save attention
            step_key = f"step_{iterations}"
            prim.attention_buffer[step_key] = {
                "attentions": {
                    f"layer_{i}": attn.cpu().numpy()
                    for i, attn in enumerate(attentions)
                },
                "action": a,  # 当前动作
                "return": prim.target_return[0, -1].item()  # 当前回报
            }
            if iterations < 1000 and iterations % 100 == 0:
                # torch.save(prim.attention_buffer, f"./infer_logs/attentions_{iterations}.pth")
                prim.attention_buffer = {}
            return target_cwnd_packets, 0

    def register_prim(self, conn_id, prim):
        if self.use_batch: 
            # 将 prim 注册到 conn_prims 映射，供批量推理使用
            try:
                self.conn_prims[conn_id] = prim
            except Exception:
                pass

    def unregister_prim(self, conn_id):
        """在连接关闭时移除 conn_prims 中的条目，避免内存泄漏。"""
        try:
            if conn_id in self.conn_prims:
                del self.conn_prims[conn_id]
        except Exception:
            pass

    def batch_predict(self, states: np.ndarray, rewards: np.ndarray, conn_ids: list):
        """
        批量预测方法 —— 真正的一次性批量推理。
        将所有流的历史序列 pad 到相同长度，拼成 (N, T, *) 张量，
        一次调用 model.forward 完成所有流的推理，推理时间 ≈ 单流推理时间。
        """
        if not self.use_batch:
            raise RuntimeError("批量预测在单连接处理模式下不可用")

        state_dim  = self.config['state_dim']
        act_dim    = self.config['act_dim']
        n          = len(conn_ids)
        states_np  = states   # shape (n, state_dim)，已由 _process_batch 计算好
        rewards_np = rewards  # shape (n,)

        # ── Step 1: 更新各流 prim 的历史序列（纯 CPU/tensor 操作，无推理）──────
        prims_list = []
        for i, conn_id in enumerate(conn_ids):
            if conn_id not in self.conn_prims:
                self.conn_prims[conn_id] = Prims(self.config, self.device)
            prim = self.conn_prims[conn_id]
            prims_list.append(prim)

            state     = torch.from_numpy(states_np[i]).to(self.device, dtype=torch.float32).unsqueeze(0)  # (1, S)
            reward_val = torch.tensor(float(rewards_np[i]), device=self.device, dtype=torch.float32)

            if prim.iterations == 1 or prim.actions.size(0) == 0:
                # 首次调用：直接赋值（不 cat），确保各序列长度都是 1
                prim.states       = state                                                           # (1, S)
                prim.rewards      = torch.zeros(1, device=self.device)                             # (1,)
                prim.actions      = torch.zeros((1, act_dim), device=self.device)                  # (1, A)
                prim.timesteps    = torch.tensor([[1]], device=self.device, dtype=torch.long)       # (1, 1)
                scaled_return     = self.rtg_scaler.scale(RTG_95TH_PERCENTILE)
                prim.target_return = torch.tensor([[scaled_return]], device=self.device, dtype=torch.float32)  # (1, 1)
            else:
                # 滑动窗口裁剪
                if prim.states.size(0) >= self.max_window_size:
                    start = prim.states.size(0) - self.max_window_size + 1
                    prim.states        = prim.states[start:]
                    prim.actions       = prim.actions[start:]
                    prim.rewards       = prim.rewards[start:]
                    prim.target_return = prim.target_return[:, start:]
                    prim.timesteps     = prim.timesteps[:, start:]

                # 填入上一步的奖励
                if prim.rewards.size(0) == 0:
                    prim.rewards = torch.zeros(1, device=self.device)
                prim.rewards[-1] = reward_val

                # 更新 target_return
                pred_return = prim.target_return[0, -1] - reward_val / self.config['scale']

                # 追加新时间步
                prim.states        = torch.cat([prim.states, state], dim=0)
                prim.target_return = torch.cat([prim.target_return, pred_return.reshape(1, 1)], dim=1)
                prim.timesteps     = torch.cat([
                    prim.timesteps,
                    torch.tensor([[prim.iterations + 1]], device=self.device, dtype=torch.long)
                ], dim=1)
                prim.actions       = torch.cat([prim.actions, torch.zeros((1, act_dim), device=self.device)], dim=0)
                prim.rewards       = torch.cat([prim.rewards, torch.zeros(1, device=self.device)])

        # ── Step 2: 把 N 条流的序列 pad 到相同长度，拼成 (N, T, *) 张量 ──────
        max_len = min(
            max(p.states.size(0) for p in prims_list),
            self.max_window_size
        )

        batch_states        = torch.zeros((n, max_len, state_dim),  device=self.device)
        batch_actions       = torch.zeros((n, max_len, act_dim),    device=self.device)
        batch_rewards       = torch.zeros((n, max_len, 1),          device=self.device)
        batch_returns_to_go = torch.zeros((n, max_len, 1),          device=self.device)
        batch_timesteps     = torch.zeros((n, max_len),             device=self.device, dtype=torch.long)
        batch_attention_mask = torch.zeros((n, max_len),            device=self.device, dtype=torch.long)

        state_mean = self.config['state_mean']  # (S,) tensor
        state_std  = self.config['state_std']   # (S,) tensor

        for i, prim in enumerate(prims_list):
            T = prim.states.size(0)
            t = min(T, max_len)          # 实际填入长度（不超过 max_len）
            start = T - t                # 若超过 max_len，取最后 t 步

            norm_states = (prim.states[start:] - state_mean) / state_std   # (t, S)

            batch_states[i, max_len - t:]        = norm_states
            batch_actions[i, max_len - t:]       = prim.actions[start:]
            batch_rewards[i, max_len - t:, 0]    = prim.rewards[start:]
            batch_returns_to_go[i, max_len - t:] = prim.target_return[0, start:].unsqueeze(-1)
            batch_timesteps[i, max_len - t:]     = prim.timesteps[0, start:]
            batch_attention_mask[i, max_len - t:] = 1

        # ── Step 3: 一次 forward 完成所有流的推理 ────────────────────────────
        # inference_mode 比 no_grad 更快（跳过 view tracking 和 version counter 更新）
        import time
        infer_start = time.time()
        with torch.inference_mode():
            _, action_preds, _, _, _ = self.model.forward(
                batch_states,
                batch_actions,
                batch_returns_to_go,
                batch_rewards,
                batch_timesteps,
                attention_mask=batch_attention_mask,
            )
        infer_duration_ms = (time.time() - infer_start) * 1000
        # action_preds: (N, T, act_dim)；取最后一步
        last_actions = action_preds[:, -1, :]   # (N, act_dim)

        # ── Step 4: 将推理结果写回各流 prim，计算 cwnd ────────────────────────
        result_actions = np.zeros(n, dtype=np.float32)
        result_rates   = np.zeros(n, dtype=np.float32)

        for i, (prim, conn_id) in enumerate(zip(prims_list, conn_ids)):
            action = last_actions[i]              # (act_dim,) tensor
            prim.actions[-1] = action             # 写回 prim

            action_np = action.detach().cpu().numpy()
            alpha = self.map_action(action_np[0])

            if not math.isfinite(alpha):
                print(f"[batch_predict] WARNING: action alpha is nan for conn {conn_id}, "
                      f"keeping pre_cwnd_packets={prim.pre_cwnd_packets}")
                result_actions[i] = max(MIN_CWND, min(MAX_CWND, math.ceil(prim.pre_cwnd_packets)))
                continue

            target_cwnd_rate   = self.calculate_target_cwnd_rate(alpha)
            temp_cwnd_packets  = prim.pre_cwnd_packets * target_cwnd_rate

            if not math.isfinite(temp_cwnd_packets):
                print(f"[batch_predict] WARNING: temp_cwnd_packets NaN/Inf for conn {conn_id}, "
                      f"keeping pre_cwnd_packets={prim.pre_cwnd_packets}")
                result_actions[i] = max(MIN_CWND, min(MAX_CWND, math.ceil(prim.pre_cwnd_packets)))
                continue

            prim.pre_cwnd_packets = temp_cwnd_packets
            target_cwnd_packets   = max(MIN_CWND, min(MAX_CWND, math.ceil(temp_cwnd_packets)))

            prim.pre_cwnd_rate   = target_cwnd_rate
            result_actions[i]    = target_cwnd_packets

        # ── 诊断日志：batch_predict 完成 ────────────────────────
        print(f"[batch_predict] N={n:2d} "
              f"max_len={max_len:3d} "
              f"infer_ms={infer_duration_ms:.2f} "
              f"per_item_ms={infer_duration_ms/n:.2f} "
              f"success={n}")
        
        return result_actions, result_rates

    def get_action(self, obs, prim, conn_id):
        """为单个连接获取动作"""
        if self.use_batch and conn_id is None:
            raise ValueError("conn_id must be provided")
        
        if self.use_batch:
            state, reward = self.get_state_reward(obs, prim)
            if state is None:
                return prim.pre_cwnd_packets, 0
            # 批量处理路径
            state_array = np.expand_dims(state, axis=0)
            reward_array = np.array([reward], dtype=np.float32)
            actions, _ = self.batch_predict(state_array,reward_array, [conn_id])
            return actions[0], 0
        else:
            return self.single_predict(obs, prim)
