import tensorflow as tf
from orca_special.acc import Actor,Critic
from orca_special.utils import OU_Noise,G_Noise,ReplayBuffer, Prioritized_ReplayBuffer
import numpy as np
import os
# model=TD3
EXPLORE = 4000
STDDEV = 0.1
NSTEP = 0.3

def create_input_op_shape(obs, tensor):
    input_shape = [x or -1 for x in tensor.shape.as_list()]
    return np.reshape(obs, input_shape)

class OnlineRLPolicy(object):
    def __init__(self,
                 s_dim, 
                 a_dim,
                 device,
                h1_shape,
                h2_shape,
                gamma=0.995, 
                batch_size=8, 
                lr_a=1e-4, 
                lr_c=1e-3, 
                tau=1e-3, 
                mem_size=1e5,
                action_scale=1.0, 
                action_range=(-1.0, 1.0),
                noise_type=3, 
                noise_exp=50000, 
                summary=None,
                stddev=0.1, 
                PER=False, 
                alpha=0.6, 
                CDQ=True, 
                LOSS_TYPE='HUBERT',
                **kwargs):
        self.PER = PER
        self.CDQ = CDQ
        self.LOSS_TYPE = LOSS_TYPE
        self.lr_a = lr_a
        self.lr_c = lr_c
        self.s_dim = s_dim
        self.a_dim = a_dim
        self.gamma = gamma
        self.noise_type = noise_type
        self.noise_exp = noise_exp
        self.action_range = action_range
        self.h1_shape=h1_shape
        self.h2_shape=h2_shape
        self.stddev=stddev
        self.tau=tau
        self.batch_size = batch_size


        self.actor = Actor(s_dim, a_dim, h1_shape, h2_shape,action_scale=action_scale)
        self.critic1 = Critic(s_dim, a_dim, h1_shape, h2_shape,name='critic1')
        self.critic2 = Critic(s_dim, a_dim, h1_shape, h2_shape,name='critic2')
        self.target_actor = Actor(s_dim, a_dim, h1_shape, h2_shape,action_scale=action_scale,name='target_actor')
        self.target_critic1 = Critic(s_dim, a_dim, h1_shape, h2_shape,name='target_critic1')
        self.target_critic2 = Critic(s_dim, a_dim, h1_shape, h2_shape,name='target_critic2')
    
        self.actor_optimizer = tf.keras.optimizers.Adam(lr_a)
        self.critic_optimizer = tf.keras.optimizers.Adam(lr_c)

        self.device = device

        self.train_dir = './orca_special/train_dir'

        if not self.PER:
            self.rp_buffer = ReplayBuffer(int(mem_size), s_dim, a_dim, batch_size=batch_size)
        else:
            self.rp_buffer = Prioritized_ReplayBuffer(int(mem_size), s_dim, a_dim, batch_size=batch_size, alpha=alpha)


        if noise_type == 1:
            self.actor_noise = OU_Noise(mu=np.zeros(a_dim), sigma=float(self.stddev) * np.ones(a_dim),dt=1,exp=self.noise_exp)
        elif noise_type == 2:
            ## Gaussian with gradually decay
            self.actor_noise = G_Noise(mu=np.zeros(a_dim), sigma=float(self.stddev) * np.ones(a_dim), explore =self.noise_exp)
        elif noise_type == 3:
            ## Gaussian without gradually decay
            self.actor_noise = G_Noise(mu=np.zeros(a_dim), sigma=float(self.stddev) * np.ones(a_dim), explore = None,theta=0.1)
        elif noise_type == 4:
            ## Gaussian without gradually decay
            self.actor_noise = G_Noise(mu=np.zeros(a_dim), sigma=float(self.stddev) * np.ones(a_dim), explore = EXPLORE,theta=0.1,mode="step",step=NSTEP)
        elif noise_type == 5:
            self.actor_noise = None
        else:
            self.actor_noise = OU_Noise(mu=np.zeros(a_dim), sigma=float(self.stddev) * np.ones(a_dim),dt=0.5)
        
    @tf.function
    def train_step(self, batch_samples, weights=None):
        s0, a, r, s1, terminal = batch_samples
        
        # Critic 网络更新
        with tf.GradientTape(persistent=True) as tape:
            target_actions = self.target_actor(s1, training=True)
            target_q1 = self.target_critic1(s1, target_actions)
            target_q2 = self.target_critic2(s1, target_actions)
            target_q = tf.minimum(target_q1, target_q2)
            y = r + self.gamma * (1 - terminal) * target_q
            
            current_q1 = self.critic1(s0, a, training=True)
            current_q2 = self.critic2(s0, a, training=True)
            
            if self.LOSS_TYPE == 'HUBER':
                critic_loss1 = tf.reduce_mean(tf.keras.losses.Huber()(y, current_q1))
                critic_loss2 = tf.reduce_mean(tf.keras.losses.Huber()(y, current_q2))
            else:
                critic_loss1 = tf.reduce_mean(tf.square(y - current_q1))
                critic_loss2 = tf.reduce_mean(tf.square(y - current_q2))
                
            if self.PER:
                critic_loss1 *= tf.reduce_mean(weights)
                critic_loss2 *= tf.reduce_mean(weights)
                
        critic_grad1 = tape.gradient(critic_loss1, self.critic1.trainable_variables)
        critic_grad2 = tape.gradient(critic_loss2, self.critic2.trainable_variables)
        self.critic_optimizer.apply_gradients(zip(critic_grad1, self.critic1.trainable_variables))
        self.critic_optimizer.apply_gradients(zip(critic_grad2, self.critic2.trainable_variables))
        
        # Actor 网络更新
        with tf.GradientTape() as tape:
            actions = self.actor(s0, training=True)
            critic_value = self.critic(s0, actions)
            actor_loss = -tf.reduce_mean(critic_value)
            
        actor_grad = tape.gradient(actor_loss, self.actor.trainable_variables)
        self.actor_optimizer.apply_gradients(zip(actor_grad, self.actor.trainable_variables))
        
        # 目标网络更新
        self.soft_update(self.target_actor.variables, self.actor.variables)
        self.soft_update(self.target_critic1.variables, self.critic1.variables)
        self.soft_update(self.target_critic2.variables, self.critic2.variables)
        
        return critic_loss1 + critic_loss2, actor_loss

    @tf.function
    def actor_train_step(self, s0, a, r, s1, terminal, weights=None):
        s0_tensor = tf.convert_to_tensor(s0, dtype=tf.float32)
        action_tensor = tf.convert_to_tensor([a], dtype=tf.float32)
        reward_tensor = tf.convert_to_tensor([r], dtype=tf.float32)
        s1_tensor = tf.convert_to_tensor(s1, dtype=tf.float32)
        terminal_tensor = tf.convert_to_tensor([terminal], dtype=tf.float32)

        # Critic 网络更新
        with tf.GradientTape(persistent=True) as tape:
            target_actions = self.target_actor(s1_tensor, training=True)
            target_q1 = self.target_critic(s1_tensor, target_actions)
            target_q2 = self.target_critic2(s1_tensor, target_actions)
            target_q = tf.minimum(target_q1, target_q2)
            y = reward_tensor + self.gamma * (1 - terminal_tensor) * target_q
            
            current_q1 = self.critic(s0_tensor, action_tensor, training=True)
            current_q2 = self.critic2(s0_tensor, action_tensor, training=True)
            
            if self.LOSS_TYPE == 'HUBER':
                critic_loss1 = tf.reduce_mean(tf.keras.losses.Huber()(y, current_q1))
                critic_loss2 = tf.reduce_mean(tf.keras.losses.Huber()(y, current_q2))
            else:
                critic_loss1 = tf.reduce_mean(tf.square(y - current_q1))
                critic_loss2 = tf.reduce_mean(tf.square(y - current_q2))
                
            if self.PER:
                critic_loss1 *= tf.reduce_mean(weights)
                critic_loss2 *= tf.reduce_mean(weights)
                
        critic_grad1 = tape.gradient(critic_loss1, self.critic1.trainable_variables)
        critic_grad2 = tape.gradient(critic_loss2, self.critic2.trainable_variables)
        self.critic_optimizer.apply_gradients(zip(critic_grad1, self.critic1.trainable_variables))
        self.critic_optimizer.apply_gradients(zip(critic_grad2, self.critic2.trainable_variables))
        
        # Actor 网络更新
        with tf.GradientTape() as tape:
            actions = self.actor(s0_tensor, training=True)
            critic_value = self.critic(s0, actions)
            actor_loss = -tf.reduce_mean(critic_value)
            
        actor_grad = tape.gradient(actor_loss, self.actor.trainable_variables)
        self.actor_optimizer.apply_gradients(zip(actor_grad, self.actor.trainable_variables))
        
        # 目标网络更新
        self.soft_update(self.target_actor.variables, self.actor.variables)
        self.soft_update(self.target_critic1.variables, self.critic1.variables)
        self.soft_update(self.target_critic2.variables, self.critic2.variables)
        
        return critic_loss1 + critic_loss2, actor_loss

    def soft_update(self, target_vars, source_vars):
        for t, s in zip(target_vars, source_vars):
            t.assign(t * (1.0 - self.tau) + s * self.tau)

    # @tf.function(autograph=False)
    def get_action(self, s, add_noise=True, training=True):
        # print(s)
        s = tf.convert_to_tensor([s], dtype=tf.float32)
        action = self.actor(s, training=training) 
        if add_noise:
            # 生成与 action 形状相同的噪声
            noise = tf.random.normal(
                shape=tf.shape(action),
                mean=0.0,
                stddev=self.stddev,
                dtype=tf.float32
            )
            action += noise
            
            # 使用 TensorFlow 的裁剪操作
            action = tf.clip_by_value(
                action,
                self.action_range[0],
                self.action_range[1]
            )
        return action

    def save_model(self, save_dir):
        ckp_a = tf.train.Checkpoint(model=self.actor)
        ckp_a.save(save_dir+'/actor/model')
        ckp_c1 = tf.train.Checkpoint(model=self.critic1)
        ckp_c1.save(save_dir+'/critic1/model')
        ckp_c2 = tf.train.Checkpoint(model=self.critic2)
        ckp_c2.save(save_dir+'/critic2/model')


    def load_weights(self, save_dir):
        ckp_a = tf.train.Checkpoint(model=self.actor)
        ckp_a.restore(tf.train.latest_checkpoint(save_dir+'/actor'))
        ckp_c1 = tf.train.Checkpoint(model=self.critic1)
        ckp_c1.restore(tf.train.latest_checkpoint(save_dir+'/critic1'))
        ckp_c2 = tf.train.Checkpoint(model=self.critic2)
        ckp_c2.restore(tf.train.latest_checkpoint(save_dir+'/critic2'))
        print("load model done")

