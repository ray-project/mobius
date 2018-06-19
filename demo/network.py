import tensorflow as tf
import ray


class SimpleNetwork(object):
    def __init__(self):
        # Seed TensorFlow to make the script deterministic.
        tf.set_random_seed(0)
        # Define the inputs.
        self.x_data = tf.placeholder(tf.float32, [None, 1])
        self.y_data = tf.placeholder(tf.float32, [None, 1])

        # Define the weights and computation.
        w = tf.Variable(tf.random_uniform([1], -1.0, 1.0))
        b = tf.Variable(tf.zeros([1]))
        self.y = w * self.x_data + b
        # Define the loss.
        self.loss = tf.reduce_mean(tf.square(self.y - self.y_data))
        optimizer = tf.train.GradientDescentOptimizer(0.2)
        self.grads = optimizer.compute_gradients(self.loss)
        self.train = optimizer.apply_gradients(self.grads)
        # Define the weight initializer and session.
        init = tf.global_variables_initializer()
        self.sess = tf.Session()
        # Additional code for setting and getting the weights
        self.variables = ray.experimental.TensorFlowVariables(
            self.loss, self.sess)
        # Return all of the data needed to use the network.
        self.sess.run(init)

    # Define a remote function that trains the network for one step and
    # returns the new weights.
    def step(self, weights, x, y):
        # Set the weights in the network.
        self.set_weights(weights)
        # Do one step of training.
        self.sess.run(self.train, {self.x_data: x, self.y_data: y})
        # Return the new weights.
        return self.variables.get_weights()

    def set_weights(self, weights):
        return self.variables.set_weights(weights)

    def get_weights(self):
        return self.variables.get_weights()

    def inference(self, x):
        return self.sess.run(self.y, {self.x_data: x})
