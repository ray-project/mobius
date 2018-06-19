import argparse
import math
import time
import util

from datetime import datetime
from kafka import KafkaClient
from kafka.producer import SimpleProducer

parser = argparse.ArgumentParser(description="Online learning service")

parser.add_argument("--kafka_server", default="localhost:9092", type=str,
                    help="Kafka server")
parser.add_argument("--kafka_topic", default="kafkaesque", type=str,
                    help="Kafka topic")
parser.add_argument("--num_to_send", default=100000, type=int,
                    help="Number of messages to send", )
parser.add_argument("--sleep_every_N", default=1000, type=int,
                    help="Number of messages to send per second")
parser.add_argument("--target_a", default=0.1, type=float,
                    help="Target 'a' to simulate for y = ax + b")
parser.add_argument("--target_b", default=0.3, type=float,
                    help="Target 'b' to simulate for y = ax + b")


def publish_training_data(kafka_server, kafka_topic, num_to_send, sleep_every_N, target_a, target_b):
    kafka = KafkaClient(kafka_server)
    producer = SimpleProducer(kafka)

    for c in range(num_to_send):
        x, y = util.generate_linear_x_y_data(1, target_a, target_b, c)

        # print "%d %.20f %.20f" % (int(round(time.time() * 1000)), x[0], y[0])
        producer.send_messages(
            kafka_topic,
            "%d %.5f %.5f %.20f %.20f" % (util.now_millis(), target_a, target_b, x[0], y[0]))
        if (c + 1) % sleep_every_N == 0:
            time.sleep(1)


if __name__ == "__main__":
    args = parser.parse_args()
    publish_training_data(args.kafka_server, args.kafka_topic,
                          args.num_to_send, args.sleep_every_N,
                          args.target_a, args.target_b)
