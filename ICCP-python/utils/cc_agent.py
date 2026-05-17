import torch
from decision_transformer.models.decision_transformer import DecisionTransformer
import decision_transformer.envs.timestep as ts
from utils.dualqueue import dq
import numpy as np
import sys
import math

# 定义常量
ESTIMATE = 0
ACCURACY = 10000.0
# 定义常量
ESTIMATE = 0
ACCURACY = 10000.0
MIN_CWND = 4
MAX_CWND = 20000

class Prims:
    def __init__(self, config, device):
        # priminative 
        self.iterations = np.int64(0)
        self.pre_cwnd_rate = 1.0
        self.pre_cwnd_packets = 10
        # RTT 和吞吐量


class CCAgent(object):
    def __init__(self,model,observation_space,action_space,config,device,env_bw,*args,**kwargs):
        self.observation_space=observation_space
        self.action_space=action_space
        self.device=device
        self.env_bw = env_bw
        self.config = config
        self.model = model
        self.optimizer = torch.optim.AdamW(
        self.model.parameters(),
        lr=config['lr'],
        weight_decay=config['weight_decay'],
        )
        self.model.eval()
        self.model.to(device=self.device)

    def reset_timestep(self,state):
        return ts.restart(state)

    def _convert_timestep(self, ts):
        return ts._replace(discount=np.array(ts.discount, copy=False, dtype=np.float32))

    def get_timestep(self,state,reward):
        return self._convert_timestep(ts.transition(reward=reward, observation=state))

    def get_state_reward(self,obs,prims):
        raise NotImplementedError
    
    def map_action(self,action):
        raise NotImplementedError
    
    def get_action(self,obs,prims):
        raise NotImplementedError