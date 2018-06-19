import argparse
import grpc
import numpy
import time
import util

from proto import mobius_pb2, mobius_pb2_grpc

parser = argparse.ArgumentParser(description="Online learning service")

parser.add_argument("--rpc_endpoint", default="localhost:50051", type=str,
                    help="RPC endpoint")
parser.add_argument("--target_a", default=0.1, type=float,
                    help="Target 'a' to simulate for y = ax + b")
parser.add_argument("--target_b", default=0.3, type=float,
                    help="Target 'b' to simulate for y = ax + b")
parser.add_argument("--batch_size", default=100, type=int,
                    help="Batch size for inference")
parser.add_argument("--num_batch", default=100, type=int,
                    help="Number of batches to send")
parser.add_argument("--batch_interval_seconds", default=1, type=int,
                    help="Interval seconds between batches")


if __name__ == "__main__":
    args = parser.parse_args()
    # open a gRPC channel
    channel = grpc.insecure_channel(args.rpc_endpoint)
    # create a stub (client)
    stub = mobius_pb2_grpc.MobiusStub(channel)

    for i in range(args.num_batch):
        x, y = util.generate_linear_x_y_data(
            args.batch_size, args.target_a, args.target_b,
            util.now_millis() / 1000)
        # create a valid request message
        request = mobius_pb2.InferRequest(x=x)
        # make the call
        response = stub.Infer(request)

        A = numpy.array(y)
        B = numpy.array(response.y)
        mean_error = ((A - B) ** 2).mean()
        # print response
        print 'batch', i, 'mean squared error', mean_error

        time.sleep(args.batch_interval_seconds)
