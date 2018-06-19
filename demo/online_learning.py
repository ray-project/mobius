from kafka import KafkaConsumer
import multiprocessing
import ray
import util


def train(train_actor, infer_actor, kafka_server, kafka_topic, batch_size,
          max_batch_wait_millis, verbose):
    weights = ray.get(train_actor.get_weights.remote())

    consumer = KafkaConsumer(bootstrap_servers=kafka_server)
    consumer.subscribe([kafka_topic])
    iteration = 0

    while 1:
        start = util.now_millis()
        x_data = []
        y_data = []

        while len(x_data) < batch_size:
            max_wait = max_batch_wait_millis - (util.now_millis() - start)
            if max_wait <= 0:
                break
            msg = consumer.poll(timeout_ms=max_wait)

            if len(msg) > 0:
                for _, records in msg.iteritems():
                    for record in records:
                        if not record.value:
                            continue

                        parts = record.value.split()
                        if len(parts) != 5:
                            continue

                        x, y = float(parts[3]), float(parts[4])
                        x_data.append([x])
                        y_data.append([y])

        if len(x_data) > 0:
            weights_id = ray.put(weights)
            new_weights_id = train_actor.step.remote(
                weights_id, x_data, y_data)
            weights = ray.get(new_weights_id)

            iteration += 1
            if iteration % 10 == 0:
                millis = int(parts[0])
                delay = util.now_millis() - millis
                print("Training latency: {}".format(delay))
                print("Training iteration {}: weights are {}".format(
                      iteration, weights))

            # TODO: move this to separate module and decouple from the
            #       training process.
            if verbose:
                print("Update weights for infer")
            infer_actor.set_weights.remote(new_weights_id)


class Driver(object):
    def __init__(self, remote_network, kafka_server, kafka_topic, batch_size,
                 max_batch_wait_millis, verbose):
        self._network = remote_network
        self._train_actor = remote_network.remote()
        self._infer_actor = remote_network.remote()
        self._kafka_server = kafka_server
        self._kafka_topic = kafka_topic
        self._batch_size = batch_size
        self._max_batch_wait_millis = max_batch_wait_millis
        self._verbose = verbose

    def start(self):
        # Start training process
        # TODO: load model from persistent storage.
        # TODO: switch model with separate process.
        # self._train_process = multiprocessing.Process(
        #   . . target=train,
        #  . . .args=(self._train_actor, self._infer_actor, kafka_server,
        #             kafka_topic, batch_size, max_batch_wait_millis))
        # self._train_process.start()
        train(self._train_actor, self._infer_actor, self._kafka_server,
              self._kafka_topic, self._batch_size, self._max_batch_wait_millis,
              self._verbose)

    def infer(self, x_data):
        return ray.get(self._infer_actor.inference.remote(x_data))
