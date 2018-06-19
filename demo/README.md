## Prerequisites
- Additional packages needed to run the demo code.
- Need to disable the main thread check manually. It will be located at line
1038 of file python2.7/site-packages/ray/worker.py (or similar path depending
on the OS and python version). There are ongoing efforts to support multiple
threading, hopefully it will be supported natively soon.

Generate proto file
> $ bash generate_proto.sh

Start local zookeeper service
> $ zkServer start

Start kafka service
> $ kafka-server-start /usr/local/etc/kafka/server.properties


Start RPC server
> $ python server_rpc.py --kafka_server=localhost:9092 --kafka_topic=kafkaesque

Start gateway server (easiest integration as go gateway generates most of the code)
Required if you'd like to send the request with PostMate.
> $ go run server_gateway.go

Stream training data
> $ python publish_msg.py --kafka_server=localhost:9092 --kafka_topic=kafkaesque --num_to_send=10000 --sleep_every_N=100 --target_a=1 --target_b=3

Client script to send inference request and compute the mean squared error.
> $ python client.py --num_batch=100 --target_a=1 --target_b=3
