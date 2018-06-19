import argparse
import grpc
import ray
import time

from concurrent import futures

# import the generated classes
import proto
from proto import mobius_pb2, mobius_pb2_grpc

import network
import online_learning
import util

parser = argparse.ArgumentParser(description="Online learning service")

parser.add_argument("--port", default=50051, type=int,
                    help="Port for RPC service")
parser.add_argument("--redis_address", default=None, type=str,
                    help="Redis service address")
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


# create a class to define the server functions
# derived from mobius_pb2_grpc.MobiusServicer
class MobiusServicer(mobius_pb2_grpc.MobiusServicer):
    def __init__(self, driver, verbose):
        self._driver = driver
        self._verbose = verbose

    # mobius.Infer is exposed here
    # the request and response are of the data types
    # generated as mobius_pb2.InferRequest and mobius_pb2.InferResponse.
    def Infer(self, request, context):
        x_data = [[x] for x in request.x]
        if self._verbose:
            print x_data
        y_data = self._driver.infer(x_data)
        if self._verbose:
            print y_data

        response = mobius_pb2.InferResponse()
        for ele in y_data:
            response.y.extend(ele)
        if self._verbose:
            print response
        return response


if __name__ == "__main__":
    args = parser.parse_args()
    if args.redis_address:
        ray.init(redis_address=args.redis_address)
    else:
        ray.init()

    verbose = "true" == args.verbose.lower()
    print verbose
    remote_network = ray.remote(network.SimpleNetwork)
    driver = online_learning.Driver(
        remote_network, args.kafka_server, args.kafka_topic,
        args.batch_size, args.max_batch_wait_millis, verbose)
    train_actor = remote_network.remote()

    # create a gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    # use the generated function `add_MobiusServicer_to_server`
    # to add the defined class to the created server
    mobius_pb2_grpc.add_MobiusServicer_to_server(
        MobiusServicer(driver, verbose), server)

    # listen on port 50051
    print('Starting server. Listening on port %d.' % args.port)
    server.add_insecure_port('[::]:%d' % args.port)
    server.start()

    print('Starting training')
    driver.start()

    # since server.start() will not block,
    # a sleep-loop is added to keep alive
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)
