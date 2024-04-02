import logging
import json
import threading
import ray
from ray.streaming.function import SinkFunction
from streaming.operator.reader.eval_reader import EvalReader

logger = logging.getLogger(__name__)


class EvalSinkFunction(SinkFunction):
    def __init__(self):
        pass

    def open(self, runtime_context):
        self.job_config = runtime_context.get_job_config()
        self._op_config = runtime_context.get_config()
        self.task_index = runtime_context.get_task_index()
        self.task_id = runtime_context.get_task_id()
        self.parallelism = runtime_context.get_parallelism()
        self.metric = runtime_context.get_metric()
        self.job_id = ray.get_runtime_context().actor_id.job_id.hex()
        logging.info(
            "Open eval function. op config : {}, job config : {}.".format(
                self._op_config, runtime_context.get_job_config()
            )
        )
        self.optimize_config()
        logger.info("Initializing operator with config: {}".format(self._op_config))
        self._reader = EvalReader(self._op_config)
        self._evaluate_thread_started = False
        logger.info("Operator begin finish.")

    def optimize_config(self):
        for k, v in self._op_config.items():
            try:
                if isinstance(v, str) and v.find("{") != -1 and v.find("}") != -1:
                    self._op_config[k] = json.loads(v)
            except Exception:
                pass

    def get_all_actor_names(self):
        task_index = 0
        global_index = self.task_id - self.task_index
        actor_names = [
            f"{self.job_id}-{self.job_config['StreamingOpName']}-"
            + f"{task_index + i}|{global_index+i}"
            for i in range(0, self.parallelism)
        ]
        return actor_names

    def sink(self, record):
        logging.debug(f"Sink record : {record}")
        self._reader.put_data(record)
        if not self._evaluate_thread_started:
            logger.info("Starting evaluating thread!")
            t = threading.Thread(target=self._evaluate)
            t.setDaemon(True)
            t.start()
            logger.info("Evaluating thread started!")
            self._evaluate_thread_started = True

    def _evaluate(self):
        """
        evaluator should override this method
        :return:
        """
        raise NotImplementedError("function _evaluate not implemented!")

    def save_checkpoint(self, checkpoint_id):
        pass

    def load_checkpoint(self, checkpoint_id):
        pass
