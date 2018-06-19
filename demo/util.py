import numpy
import time


def now_millis():
    return int(round(time.time() * 1000))


def generate_linear_x_y_data(num_data, a=0.1, b=0.3, seed=0):
    # Seed numpy to make the script deterministic.
    numpy.random.seed(seed)
    x = numpy.random.rand(num_data)
    y = x * a + b
    return x, y
