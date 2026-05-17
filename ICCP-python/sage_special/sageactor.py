
from acme.agents.tf import actors
import sys
from acme import wrappers
from acme.tf import utils as tf2_utils
from acme import specs
from acme.tf import networks as acme_networks
from sage_special.unplug_networks import ControlNetwork
import sonnet as snt
import tensorflow as tf
from typing import Dict, Optional, Tuple, Set, Sequence
import numpy as np
import os


class SagePolicy(object):
    def __init__(self, config):
        self.config = config
        self.observation_spec = specs.BoundedArray(shape=(config['tcpspec']['obs_dim'],), dtype='float32', name='observation',minimum=[-1e6], maximum=[1e6])
        if config['tcpspec']['action_version']==9:
            self.action_spec = specs.BoundedArray(shape=(1,), dtype='float32', name='action', minimum=[-self.config['tcpspec']['action_max']], maximum=[self.config['tcpspec']['action_max']])
        else:
            self.action_spec = specs.BoundedArray(shape=(1,), dtype='float32', name='action', minimum=[0.], maximum=[1500.])
        self.policy_network = None
        self.eval_policy = None
        self.actor = None

    def init_actor(self):
        if self.policy_network is not None:
            print("Policy network is inited")
            self.eval_policy = snt.DeepRNN([
            self.policy_network,
            acme_networks.StochasticMeanHead(),
            acme_networks.ClipToSpec(self.action_spec),
            ])
            self.actor = actors.RecurrentActor(policy_network=self.eval_policy)
            dummy_observation = np.float32(np.ones((self.config['tcpspec']['obs_dim'],)))
            self.actor.select_action(dummy_observation)
            print("Actor is inited")
            self.actor._state = self.actor._network.initial_state(1)
            self.actor._state = None

    def make_networks(
            self,
        action_shape : Tuple[int],
        act_fn: str = "tanh",
        policy_lstm_sizes: Sequence[int] = None,
        critic_lstm_sizes: Sequence[int] = None,
        num_components: int = 5,
        vmin: float = 0.,
        vmax: float = 100.,
        num_atoms: int = 51,
        nw_type: int = 1909,

        p_enc_size: int = 256,
        p_mlp_size: int = 256,
        p_mlp_depth: int = 2,
        c_enc_size: int = 256,
        c_mlp_size: int = 256,
        c_mlp_depth: int = 2,
        p_lstm_size: int = 256,
        c_lstm_size: int = 256,
        lstm_depth: int = 1,
    ):
        action_size = np.prod(action_shape, dtype=int)
        actor_head = acme_networks.MultivariateGaussianMixture(
            num_components=num_components, num_dimensions=action_size)

        if nw_type == 1909:
            if act_fn == "tanh":
                act = tf.nn.tanh
            elif act_fn == "leaky":
                act = tf.nn.leaky_relu
            else:
                act = tf.nn.relu

            policy_lstm_sizes = [p_lstm_size for i in range(lstm_depth)]
            critic_lstm_sizes = [c_lstm_size for i in range(lstm_depth)]

            actor_neck = acme_networks.LayerNormAndResidualMLP(
                hidden_size=p_mlp_size, num_blocks=p_mlp_depth)
            actor_encoder = ControlNetwork(
                proprio_encoder_size=p_enc_size, activation=act)
            actor_encoder2 = ControlNetwork(
                proprio_encoder_size=p_enc_size, activation=act)
            policy_lstms = [snt.GRU(s) for s in policy_lstm_sizes]
            policy_network = snt.DeepRNN(
                [actor_encoder] + policy_lstms + [actor_encoder2] + [actor_neck] + [actor_head])

            critic_encoder = ControlNetwork(
                proprio_encoder_size=c_enc_size, activation=act)
            critic_encoder2 = ControlNetwork(
                proprio_encoder_size=c_enc_size, activation=act)
            critic_neck = acme_networks.LayerNormAndResidualMLP(
                hidden_size=c_mlp_size, num_blocks=c_mlp_depth)
            distributional_head = acme_networks.DiscreteValuedHead(
                vmin=vmin, vmax=vmax, num_atoms=num_atoms)
            critic_lstms = [snt.GRU(s) for s in critic_lstm_sizes]
            critic_network = acme_networks.CriticDeepRNN(
                [critic_encoder] + critic_lstms + [critic_encoder2] + [critic_neck] + [distributional_head])

        else:
            raise NotImplementedError

        self.policy_network = policy_network
        
    
    def load_weights(self, load_path: str):
        if os.path.exists(load_path):
            assert tf.train.latest_checkpoint(
                            load_path) != None, "no checkpoint is loaded, please choose the ckpt directory"
            checkpointer_rl = tf.train.Checkpoint(policy=self.policy_network)
            checkpointer_rl.restore(
                tf.train.latest_checkpoint(load_path)).expect_partial()
        else:
            print(f"Path {load_path} does not exist. No weights loaded.")

    def get_action(self, observation):
        # print(observation)
        a = self.actor.select_action(observation)
        return a      