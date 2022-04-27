import logging
import json
import ray
from ray.streaming.function import SinkFunction
from streaming.operator.util import OperatorModuleManager

logger = logging.getLogger(__name__)

OPERATOR_MODULE_NAME = "operator_module_name"
OPERATOR_CLASS_NAME = "operator_class_name"
OPERATOR_MODULE_PATH = "operator_module_path"
# runtime
WORKER_PARALLELISM_INDEX = "worker_parallelism_index"
ACTOR_WAS_RECONSTRUCTED = "ACTOR_RECONSTRUCTED"


class TFSinkFunction(SinkFunction):
    def __init__(self):
        pass

    def open(self, runtime_context):
        self.job_config = runtime_context.get_job_config()
        self.task_id = runtime_context.get_task_id()
        self.task_index = runtime_context.get_task_index()
        self.job_id = ray.get_runtime_context().actor_id.job_id.hex()
        self.parallelism = runtime_context.get_parallelism()
        self.metric = runtime_context.get_metric()
        self._op_config = runtime_context.get_config()
        logging.info(
            "Open tf function. op config : {}, job config : {}.".format(
                self._op_config, runtime_context.get_job_config()))
        self._operator_module_manager = self.__init_operator_module_manager()
        module_name = self._op_config[OPERATOR_MODULE_NAME]
        class_name = self._op_config[OPERATOR_CLASS_NAME]
        try:
            self._operator = self._operator_module_manager.load_single_module(
                module_name, class_name)
            self._operator.set_master_actor(
                runtime_context.get_controller_actor_handler())
        except Exception as e:
            ray.report_event(
                ray.EventSeverity.ERROR, "TF_OPERATOR_ERROR",
                f"Loading operator exception {e}")
            raise RuntimeError(f"Loading operator exception : {e}.")

        self.optimize_config()

        logger.info("Initializing operator with config: {}".format(
            self._op_config))

        self._op_config[
            WORKER_PARALLELISM_INDEX] = runtime_context.get_task_index()
        was_reconstructed = False
        try:
            was_reconstructed = \
                ray.get_runtime_context().was_current_actor_reconstructed
            logger.info(
                "Get ray actor reconstructed {}".format(was_reconstructed))
        except Exception as e:
            was_reconstructed = False
            logger.info("Get ray actor reconstructed exception, {}".format(e))

        logger.info("Operator actor list : {}".format(
            self.get_all_actor_names()))
        logger.info("Operator begin init.")
        self._operator.set_named_handler(self.get_all_actor_names)
        self._operator.set_key_value_state(
            runtime_context.get_key_value_state())
        self._operator.init_and_run({
            **self._op_config,
            **{
                ACTOR_WAS_RECONSTRUCTED: was_reconstructed
            },
            **self.job_config
        })
        logger.info("Operator init finished.")

    def optimize_config(self):
        for k, v in self._op_config.items():
            try:
                if isinstance(v,
                              str) and v.find("{") != -1 and v.find("}") != -1:
                    self._op_config[k] = json.loads(v)
            except Exception:
                pass

    def forward_command(self, message):
        return self._operator.forward_command(message)

    # united distributed controller function start.
    def on_prepare(self, msg):
        return self._operator.on_prepare(msg)

    def on_commit(self):
        return self._operator.on_commit()

    def on_disposed(self):
        return self._operator.on_disposed()

    def on_cancel(self):
        return self._operator.on_cancel()

    # united distributed controller function end.

    def sink(self, record):
        logging.debug(f"Sink record : {record}")
        self._operator.process(record)

    def get_all_actor_names(self):
        task_index = 0
        global_index = self.task_id - self.task_index
        actor_names = [
            f"{self.job_id}-{self.job_config['StreamingOpName']}-" +
            f"{task_index + i}|{global_index+i}"
            for i in range(0, self.parallelism)
        ]
        return actor_names

    def save_checkpoint(self, checkpoint_id):
        return self._operator.save_checkpoint(checkpoint_id)

    def load_checkpoint(self, checkpoint_id):
        return self._operator.load_checkpoint(checkpoint_id)

    def delete_checkpoint(self, checkpoint_id):
        return self._operator.delete_checkpoint(checkpoint_id)

    def __init_operator_module_manager(self):
        self._operator_module_path = self._op_config[OPERATOR_MODULE_PATH]
        logger.info("Initializing operator module from path {}".format(
            self._operator_module_path))
        return OperatorModuleManager(self._operator_module_path)
