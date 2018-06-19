import argparse
import network
import online_learning
import ray
import util

from kafka import KafkaConsumer

parser = argparse.ArgumentParser(description="Online learning service")

parser = argparse.ArgumentParser(description="Online learning service")

parser.add_argument("--kafka_server", default="localhost:9092", type=str,
                    help="Kafka server")
parser.add_argument("--kafka_topic", default="kafkaesque", type=str,
                    help="Kafka topic")
parser.add_argument("--batch_size", default=100, type=int,
                    help="Batch size")
parser.add_argument("--max_batch_wait_millis", default=100, type=int,
                    help="Max wait millis per batch")
parser.add_argument("--verbose", default="False", type=str,
                    help="detailed message if true")


if __name__ == "__main__":
    ray.init()
    args = parser.parse_args()
    remote_network = ray.remote(network.SimpleNetwork)
    train_actor = remote_network.remote()
    infer_actor = remote_network.remote()
    verbose = "true" == args.verbose.lower()
    online_learning.train(
        train_actor, infer_actor,
        args.kafka_server, args.kafka_topic,
        args.batch_size, args.max_batch_wait_millis, verbose)
