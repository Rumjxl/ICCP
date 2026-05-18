import os
import time
import tensorflow as tf
import numpy as np
from keras.layers import Dense, BatchNormalization
from keras.models import Model


tf.keras.backend.set_floatx('float32')

# @tf.function
class Actor(tf.keras.Model):
    def __init__(self, s_dim, a_dim, h1_shape, h2_shape, action_scale=1.0, name='actor'):
        super(Actor, self).__init__(name=name)
        self.s_dim = s_dim
        self.a_dim = a_dim
        self.action_scale = action_scale
        self.h1_shape = h1_shape
        self.h2_shape = h2_shape

        self.fc1 = tf.keras.layers.Dense(h1_shape, activation=tf.nn.leaky_relu)
        self.bn1 = tf.keras.layers.BatchNormalization()
        self.fc2 = tf.keras.layers.Dense(h2_shape, activation=tf.nn.leaky_relu)
        self.bn2 = tf.keras.layers.BatchNormalization()
        self.output_layer = tf.keras.layers.Dense(a_dim, activation=tf.nn.tanh)

    def call(self, inputs, training=False):
        # print(f"in actor: inputs type :{type(inputs)} inputs shape:{inputs.shape} h1_shape:{self.h1_shape} h2_shape:{self.h2_shape}")
        # print(f"in actor: inputs:{inputs}")
        x = self.fc1(inputs)
        # print(f"in actor: inputs fc1")
        x = self.bn1(x, training=training)
        # print(f"in actor: inputs bn1")
        x = self.fc2(x)
        # print(f"in actor: inputs fc2")
        x = self.bn2(x, training=training)
        # print(f"in actor: inputs bn2")
        output = self.output_layer(x)
        # print(f"in actor: inputs output_layer")
        return output * self.action_scale
    
# @tf.function
class Critic(tf.keras.Model):
    def __init__(self, s_dim, a_dim, h1_shape, h2_shape,name='critic'):
        super(Critic, self).__init__(name=name)
        self.s_dim = s_dim
        self.a_dim = a_dim

        self.h1_shape = h1_shape
        self.h2_shape = h2_shape

        self.fc1 = tf.keras.layers.Dense(h1_shape, activation=tf.nn.leaky_relu)
        self.fc2 = tf.keras.layers.Dense(h2_shape, activation=tf.nn.leaky_relu)
        self.output_layer = tf.keras.layers.Dense(1)

    def call(self, inputs, actions):
        x = self.fc1(inputs)
        x = self.fc2(tf.concat([x, actions], axis=-1))
        output = self.output_layer(x)
        return output


    