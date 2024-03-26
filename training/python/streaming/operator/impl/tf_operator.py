import logging
import pickle
import threading
from concurrent.futures import ThreadPoolExecutor
from operator import methodcaller
from streaming.operator.checkpoint_listener import CheckpointListener
from streaming.operator.impl.batch_operator import BatchOperator
from streaming.operator.reader.tf_operator_reader import TFOperatorReader

logger = logging.getLogger(__name__)


class TFOperator(BatchOperator, CheckpointListener):
    """
    The Tensorflow operator
    """

    def __init__(self):
        self._reader = None
        # Create last checkpoint state with empty dict, so training operator
        # can update executor state by itself.
        self._last_checkpoint_state = {}
        self._async_cp_threadpool = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="TF-Operator-Checkpoint-Thread")
        self._worker_parallelism_index = None
        self._trainer_thread_started = False

    def init(self, config):
        # set reader config
        logger.info("TF operator Initialized with config: {}".format(config))
        self._reader = TFOperatorReader(config)
        index_key = "worker_parallelism_index"
        self._worker_parallelism_index = int(
            config[index_key]) if index_key in config else 0

    # should override by penrose
    def init_and_run(self, config):
        self.init(config)
        logger.info("TF operator finished basic initiation.")

    def set_key_value_state(self, key_value_state):
        self.key_value_state = key_value_state

    def set_named_handler(self, func):
        self._named_handler = func

    def destroy(self):
        pass

    def get_all_actor_names(self):
        return self._named_handler()

    def _train(self):
        """
        operator should override this method
        :return:
        """
        raise NotImplementedError('function _train not implemented!')

    def _get_consumed_offset(self):
        """
        operator should override this method
        :return: operator's consumed offset, return None by default
        """
        return None

    def on_checkpoint_complete(self, checkpoint_id):
        """
        Action on checkpoint complete
        :param checkpoint_id:
        :return:
        """
        logger.info("Tf operator on checkpoint complete, checkpoint id: {}."
                    .format(checkpoint_id))
        logger.debug("Tf operator last checkpoint state: {}"
                     .format(self._last_checkpoint_state))
        if self._last_checkpoint_state is None or len(
                self._last_checkpoint_state) == 0:
            logger.warning("Tf operator last checkpoint state is None.")
            return
        self._reader.clear_expired_data(
            self._last_checkpoint_state["reader_consumed_offset"])

    def __gen_tf_operator_cp_key(self, checkpoint_id):
        return "checkpoint-{}-state-{}".format(checkpoint_id,
                                               self._worker_parallelism_index)

    def save_checkpoint(self, checkpoint_id):
        # 1. save current consume offset before cp sink!
        logger.info("Tf operator({}) saving checkpoint, checkpoint id: {}, "
                    "getting last checkpoint state.".format(
                        self._worker_parallelism_index, checkpoint_id))

        self._last_checkpoint_state.update(self._reader.get_state())
        self._reader.set_can_be_notified_to_clear()

        logger.info("Tf operator saving checkpoint, checkpoint id: {}.".format(
            checkpoint_id))
        logger.debug("Tf operator last checkpoint state: {}".format(
            self._last_checkpoint_state))
        state_data = pickle.dumps(self._last_checkpoint_state)
        try:
            self.key_value_state.put(
                self.__gen_tf_operator_cp_key(checkpoint_id), state_data)
        except BaseException as e:
            logger.warning(
                "Put state checkpoint id {} failed, exception {}.".format(
                    checkpoint_id, e))
        return state_data

    def save_checkpoint_async(self, checkpoint_id):
        # return future
        logger.info(
            "Tf operator({}) submitting async checkpoint task, checkpoint is: "
            "{}.".format(self._worker_parallelism_index, checkpoint_id))
        f = self._async_cp_threadpool.submit(self.save_checkpoint,
                                             checkpoint_id)
        logger.info(
            "Tf operator({}) save_checkpoint_async return future: {}.".format(
                self._worker_parallelism_index, f))
        return f

    def load_checkpoint(self, checkpoint_id):
        logger.info(
            "Tf operator load checkpoint, id: {}.".format(checkpoint_id))
        # user operator should use state to keep reader's offset from now on
        if checkpoint_id <= 0:
            return
        try:
            state_data = self.key_value_state.get(
                self.__gen_tf_operator_cp_key(checkpoint_id))
            self._last_checkpoint_state = pickle.loads(state_data)
            self._last_checkpoint_state["operator_consumed_offset"] = \
                self._get_consumed_offset()
            self._reader.load_checkpoint(self._last_checkpoint_state)
        except BaseException as e:
            logger.warning("Get state checkpoint id {} failed, {}.".format(
                checkpoint_id, e))

    def delete_checkpoint(self, checkpoint_id):
        logger.info("Delete tf operator state : {}.".format(checkpoint_id))
        try:
            self.key_value_state.remove(
                self.__gen_tf_operator_cp_key(checkpoint_id))
        except BaseException as e:
            logger.warning(
                "Remove tf operator state failed, exception {}.".format(e))

    def forward_command(self, message):
        pass

    # should override by penrose
    def run(self):
        if not self._trainer_thread_started:
            logger.info("Starting training thread!")
            t = threading.Thread(target=self._train)
            t.setDaemon(True)
            t.start()
            logger.info("Training thread started!")
            self._trainer_thread_started = True

    def process(self, msg):
        self._reader.put_data(msg)


class MockOperator(TFOperator):
    def __init__(self):
        super(MockOperator, self).__init__()
        self._batch_size = 5
        self._epoch = 1
        self._need_rollback = False
        self._rollback_offset = 0
        self._consumed_offset = None

    def init(self, config):
        super(MockOperator, self).init(config)

        if "batch_size" in config:
            self._batch_size = config["batch_size"]
        if "epoch" in config:
            self._epoch = config["epoch"]
        if "rollback_offset" in config:
            self._need_rollback = True
            self._rollback_offset = config["rollback_offset"]

    def set_epoch(self, epoch):
        self._epoch = epoch

    def _train(self):
        i = 0
        while i < self._epoch:
            data_list, flag = self._reader.fetch_data(self._batch_size)
            logger.info("Training with data: {}".format(data_list))
            i += 1

        if self._need_rollback:
            self._reader.seek_by_offset(self._rollback_offset)
            data_list, flag = self._reader.fetch_data(self._batch_size)
            logger.info("Rollback, training with data: {}".format(data_list))
            i += 1

    def set_consumed_offset(self, offset):
        self._consumed_offset = offset

    def _get_consumed_offset(self):
        return self._consumed_offset


class Mock4RescaleOperator(TFOperator):
    def __init__(self):
        super(Mock4RescaleOperator, self).__init__()
        self._batch_size = 5
        self._epoch = 1
        self._need_rollback = False
        self._rollback_offset = 0
        self._consumed_offset = None

    def init(self, config):
        super(Mock4RescaleOperator, self).init(config)

        if "batch_size" in config:
            self._batch_size = config["batch_size"]
        if "epoch" in config:
            self._epoch = config["epoch"]
        if "rollback_offset" in config:
            self._need_rollback = True
            self._rollback_offset = config["rollback_offset"]

    def set_epoch(self, epoch):
        self._epoch = epoch

    def forward_command(self, message):
        command_func = message.message.get("func_name", None)
        if command_func is None:
            raise ValueError(f"Can't find function {command_func}.")
        return methodcaller(
            command_func,
            *message.message.get("func_args", ()))(self)

    def is_ready_rescaling(self):
        return True

    def _train(self):
        i = 0
        while i < self._epoch:
            data_list, flag = self._reader.fetch_data(self._batch_size)
            logger.debug("Training with data: {}".format(data_list))

        if self._need_rollback:
            self._reader.seek_by_offset(self._rollback_offset)
            data_list, flag = self._reader.fetch_data(self._batch_size)
            logger.info("Rollback, training with data: {}".format(data_list))
            i += 1

    def set_consumed_offset(self, offset):
        self._consumed_offset = offset

    def _get_consumed_offset(self):
        return self._consumed_offset


class MockGenerateOperator(TFOperator):
    def __init__(self):
        super(MockGenerateOperator, self).__init__()
        self._batch_size = 5
        self._epoch = 1

    def init(self, config):
        super(MockGenerateOperator, self).init(config)

        if "batch_size" in config:
            self._batch_size = config["batch_size"]
        if "epoch" in config:
            self._epoch = config["epoch"]

    def _train(self):
        i = 0
        for data in self._reader.iterator(self._batch_size):
            i += 1
            logger.info("Training with data: {}".format(data))
            if i >= self._epoch:
                break
