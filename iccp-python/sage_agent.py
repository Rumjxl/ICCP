import tensorflow as tf
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

def compute_dynamic_rtg(rewards, gamma=0.95, alpha=0.3):
        """带近期增强的时间衰减RTG计算"""
        n = len(rewards)
        # 基础RTG计算 (逆序累积和)
        base_rtg = np.cumsum(rewards[::-1])[::-1]
        
        # 创建时间衰减权重
        time_weights = np.array([gamma**i for i in range(n)])
        # 增强最近3步的权重
        time_weights[:min(3, n)] *= 1.0 + alpha
        
        return base_rtg * time_weights

class RTGProcessor:
    def __init__(self, scale=100,gamma=0.95, alpha=0.3):
        self.scale = scale
        self.gamma = gamma
        self.alpha = alpha
    
    def process(self, rewards):
        """生成并校准RTG"""
        rtg = compute_dynamic_rtg(rewards, self.gamma, self.alpha)
        # 动态标准化
        if len(self.recent_rtg) > 3:
            mean = np.mean(self.recent_rtg)
            std = np.std(self.recent_rtg) + 1e-5
            rtg = (rtg - mean) / std
        return rtg / self.scale

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
        self.pre_dr_w_max = 1.0
        self.loss_db = dq(100)
        self.sent_db = dq(100)
        self.sent_dt = dq(100)
        self.dlv_db = dq(100)
        self.sending_rates = dq(5000)
        self.dr_w = dq(200)
        self.uack_db = dq(100)

        self.a =np.zeros([config['action_dim']],dtype=np.float32)
        self.lstm_state = None


class SageAgent(CCAgent):
    def __init__(self,model,observation_space,action_space,config,device,env_bw,use_batch,*args,**kwargs):
        self.observation_space=observation_space
        self.action_space=action_space
        self.device=device
        self.env_bw = env_bw
        self.config = config
        self.model = model
        self.arch = kwargs.get('arch', 'lotus')
        if self.arch == 'portus':
            self.clientapp="dtcc-portus/target/debug/dtcc"
        else:
            self.clientapp="dtcc/target/debug/dtcc"
        self.mtp=20
        self.rtgprocessor = RTGProcessor(scale=100, gamma=0.95, alpha=0.3)

        # print(self.model)
        self.use_batch = use_batch
        # cc.py manages per-flow Prims for both batched and non-batched
        # inference, so this mapping must always exist.
        self.conn_prims = {}

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
        time_delta = obs.timeDelta / 1000000.0
        
        bytes_acked_ = obs.bytesAcked
        if bytes_acked_ <= 0 and obs.delivered <= 0:
            return None, None
        acked_rate = bytes_acked_ /  time_delta
        dt = obs.duration
        min_rtt_us = obs.minrtt

        s_db = obs.bytesSent
        prims.sent_db.add(s_db)
        # print(f"DEBUG: obs.bytesSent: {obs.bytesSent}, obs.bytesAcked: {obs.bytesAcked}, obs.delivered: {obs.delivered}\n")
        if obs.rtt > 0:
            rtt_rate = min_rtt_us/obs.rtt
            sending_rate = s_db / time_delta
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

        l_db = (obs.loss - prims.pre_pkt_lost) * obs.sndMss if obs.loss > prims.pre_pkt_lost else 0
        prims.pre_pkt_loss = obs.loss
        prims.loss_db.add(l_db)
        prims.sent_dt.add(dt)
        dt_sum = prims.sent_dt.sum()
        l_w_mbps = 8 * prims.loss_db.sum() / dt_sum
        # 10/17TODO: add dr_w_mbps, need add obs.delivered
        # print(f"DEBUG: previous prims.sent_db.sum: {prims.dlv_db.sum()}\n")
        prims.dlv_db.add(obs.delivered*obs.sndMss)
        
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
        prims.pre_dr_w_max = dr_w_max

        signof_bw = 1
        rtt_rate_tmp = 1 if rtt_rate >= 0.8 else rtt_rate
        if dr_w_mbps < 2 * l_w_mbps: signof_bw = -1
        # check bw_true?
        bw_true = dr_w_mbps
        bw_current_max = self.env_bw
        if dr_w_mbps > bw_current_max:
            bw_true = bw_current_max * 0.9
        reward = signof_bw *(25*(dr_w_mbps - 2 * l_w_mbps)* (dr_w_mbps - 2 * l_w_mbps))/(bw_current_max*bw_current_max)*rtt_rate_tmp
        # reward0 = signof_bw *(25*(bw_true - 2 * l_w_mbps)* (bw_true - 2 * l_w_mbps))/(bw_current_max*bw_current_max)*rtt_rate_tmp
        # reward0 = np.trunc(reward0*1e7)/1e7
        # reward = np.tanh(float(reward0/25))
        # print(f"INFO:reward: {reward} bw_true: {bw_true} rtt_rate: {rtt_rate_tmp}, dt_sum: {dt_sum} dlv_sum:{prims.dlv_db.sum()} obs_dlv: {obs.deliveryRate}\n")
        state, reward = self.check_values(state,reward)
        state = np.trunc(state * 1e7) / 1e7
        
        # with open("./log/iccp-sage-cwnd-log.txt", "a") as log_file:
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
        # beta = 0.8
        # noise = random.gauss(0, 1) 
        # action += beta * noise
        if self.config['action_version']==9:
            m_action = math.pow(2,round(action,3))   
        else:
            m_action = action

        return m_action
    
# TODO:abstract!
    def single_predict(self,obs,prims):
        state_dim = self.config['state_dim']
        act_dim = self.config['action_dim']
        iterations = prims.iterations

        # first action
        print("INFO:iteration:{}".format(iterations))
        if iterations == 1:
            s0, _ = self.get_state_reward(obs,prims)
            if s0 is None:
                print("INFO: get_state_reward failed")
                return 0, 0
            action = self.model.get_action(s0)
        else:
            s1, r= self.get_state_reward(obs,prims)
            terminal = 0
            if s1 is None:
                return 0,0
            action = self.model.get_action(s1)

        # JXL:down is to map and write action
        a = action
        prims.a = a[0]
        alpha = self.map_action(a[0])
        target_cwnd_rate= self.calculate_target_cwnd_rate(alpha)
        temp_cwnd_packets = prims.pre_cwnd_packets * target_cwnd_rate
        prims.pre_cwnd_packets = temp_cwnd_packets
        temp_cwnd_packets = math.ceil(temp_cwnd_packets)
        target_cwnd_packets = 0xFFFFFFFF if temp_cwnd_packets >= 0xFFFFFFFF else temp_cwnd_packets
        if target_cwnd_packets < MIN_CWND: 
            target_cwnd_packets = MIN_CWND
            prims.pre_cwnd_packets = MIN_CWND
        if target_cwnd_packets > MAX_CWND:
            target_cwnd_packets = MAX_CWND
            prims.pre_cwnd_packets = MAX_CWND
        prims.pre_cwnd_rate = target_cwnd_rate
        return target_cwnd_packets, 0

    def register_prim(self, conn_id, prim):
        try:
            self.conn_prims[conn_id] = prim
        except Exception:
            pass

    def unregister_prim(self, conn_id):
        try:
            if conn_id in self.conn_prims:
                del self.conn_prims[conn_id]
        except Exception:
            pass

    def batch_predict(self, states: np.ndarray, rewards: np.ndarray, conn_ids: list):
        """
        真正聚合来自不同 conn_id 的批量推理。
        Sage 使用 TF RecurrentActor (DeepRNN/GRU)，因此需要：
        1) 收集各流独立的 RNN 隐状态
        2) 把所有观测值拼成 (N, obs_dim) 批量
        3) 一次前向传播得到所有动作和新隐状态
        4) 将新隐状态写回各流 prim
        """
        if not self.use_batch:
            raise RuntimeError("批量预测在单连接处理模式下不可用")

        n = len(conn_ids)
        if n == 0:
            return np.zeros(0, dtype=np.float32), np.zeros(0, dtype=np.float32)

        if len(states) != n:
            raise ValueError(f"states batch size {len(states)} != conn_ids size {n}")

        state_dim = self.config['state_dim']
        act_dim = self.config['action_dim']
        network = self.model.actor._network          # eval_policy (snt.DeepRNN)
        initial_lstm_state = network.initial_state(1)
        expected_lstm_leaves = tf.nest.flatten(initial_lstm_state)

        def _is_valid_lstm_state(lstm_state):
            try:
                tf.nest.assert_same_structure(initial_lstm_state, lstm_state)
            except (TypeError, ValueError):
                return False

            lstm_leaves = tf.nest.flatten(lstm_state)
            if len(lstm_leaves) != len(expected_lstm_leaves):
                return False

            for leaf, expected_leaf in zip(lstm_leaves, expected_lstm_leaves):
                leaf_shape = tf.TensorShape(getattr(leaf, 'shape', None))
                expected_shape = tf.TensorShape(expected_leaf.shape)
                if leaf_shape.rank != expected_shape.rank:
                    return False
                if leaf_shape.rank is None or leaf_shape[0] != 1:
                    return False
                if leaf_shape[1:] != expected_shape[1:]:
                    return False

            return True

        prims_list = []
        for i, conn_id in enumerate(conn_ids):
            if conn_id not in self.conn_prims:
                prim = Prims(self.config, self.device)
                self.conn_prims[conn_id] = prim
            prims_list.append(self.conn_prims[conn_id])

        # ── Step 1: 收集 / 初始化各流的 RNN 隐状态 ──────────────────────
        lstm_states = []
        for i, prim in enumerate(prims_list):
            if prim.lstm_state is None or not _is_valid_lstm_state(prim.lstm_state):
                prim.lstm_state = network.initial_state(1)
            lstm_states.append(prim.lstm_state)

        # ── Step 2: 构建批量输入 ─────────────────────────────────────────
        # states: (N, state_dim) numpy → TF tensor (N, state_dim)
        batch_obs = tf.constant(states, dtype=tf.float32)          # (N, S)

        # 将 N 个单流 RNN 状态拼接成批量状态，同时保留 Sonnet 的嵌套结构。
        # Sage 的 eval_policy 是外层 DeepRNN([policy_network, ...])，而
        # policy_network 本身也是 DeepRNN([encoder, GRU, ...])，所以状态形如
        # ((tensor[1, hidden],),)。直接按下标 concat 会把内层 tuple 打平，
        # 导致 DeepRNN 在 prev_state[0] 上取到错误对象并触发 out-of-bounds。
        try:
            batched_state = tf.nest.map_structure(
                lambda *xs: tf.concat(xs, axis=0),
                *lstm_states
            )
        except (TypeError, ValueError) as exc:
            # 如果历史运行留下了旧版本错误结构，丢弃该 batch 的旧隐状态并重建。
            print(f"[batch_predict] WARNING: invalid cached RNN state, resetting batch state: {exc}")
            lstm_states = [network.initial_state(1) for _ in prims_list]
            for prim, lstm_state in zip(prims_list, lstm_states):
                prim.lstm_state = lstm_state
            batched_state = tf.nest.map_structure(
                lambda *xs: tf.concat(xs, axis=0),
                *lstm_states
            )

        # ── Step 3: 一次前向推理 ─────────────────────────────────────────
        import time as _time
        infer_start = _time.time()
        policy_output, new_batched_state = network(batch_obs, batched_state)
        # policy_output: Distribution 或 Tensor, shape (N, act_dim)
        if isinstance(policy_output, tf.Tensor):
            batch_actions = policy_output
        else:
            batch_actions = policy_output.sample()
        infer_duration_ms = (_time.time() - infer_start) * 1000

        batch_actions_np = batch_actions.numpy()                   # (N, act_dim)

        # ── Step 4: 拆分隐状态并写回各流 prim ────────────────────────────
        for i, prim in enumerate(prims_list):
            prim.lstm_state = tf.nest.map_structure(
                lambda x: x[i:i + 1],
                new_batched_state
            )

        # ── Step 5: 映射动作并计算 cwnd ─────────────────────────────────
        result_actions = np.zeros(n, dtype=np.float32)
        result_rates   = np.zeros(n, dtype=np.float32)

        for i, prim in enumerate(prims_list):
            action = batch_actions_np[i]                           # (act_dim,)
            prim.a = action
            alpha = self.map_action(action[0])

            if not math.isfinite(alpha):
                result_actions[i] = max(MIN_CWND, min(MAX_CWND, math.ceil(prim.pre_cwnd_packets)))
                continue

            target_cwnd_rate  = self.calculate_target_cwnd_rate(alpha)
            temp_cwnd_packets = prim.pre_cwnd_packets * target_cwnd_rate

            if not math.isfinite(temp_cwnd_packets):
                result_actions[i] = max(MIN_CWND, min(MAX_CWND, math.ceil(prim.pre_cwnd_packets)))
                continue

            prim.pre_cwnd_packets = temp_cwnd_packets
            target_cwnd_packets   = max(MIN_CWND, min(MAX_CWND, math.ceil(temp_cwnd_packets)))
            prim.pre_cwnd_rate    = target_cwnd_rate
            result_actions[i]     = target_cwnd_packets

        print(f"[batch_predict] N={n:2d} "
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
            state_array = np.expand_dims(state, axis=0)
            reward_array = np.array([reward], dtype=np.float32)
            actions, _ = self.batch_predict(state_array, reward_array, [conn_id])
            return actions[0], 0
        else:
            return self.single_predict(obs, prim)
