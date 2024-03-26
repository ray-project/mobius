from streaming.operator.base_operator import BaseOperator
from streaming.operator.reader.tf_operator_reader import TFOperatorReader


class BatchOperator(BaseOperator):
    """
    Batch operator
    """

    def __init__(self):
        self._reader = TFOperatorReader()

    def init(self, config):
        pass

    def run(self):
        pass

    def init_and_run(self, config):
        pass

    def destroy(self):
        pass

    def save_checkpoint(self, checkpoint_id):
        pass

    def save_checkpoint_async(self, checkpoint_id):
        pass

    def save_checkpoint(self, checkpoint_id, barrier_id):
        pass

    def save_checkpoint_async(self, checkpoint_id, barrier_id):
        pass

    def load_checkpoint(self, checkpoint_id):
        pass

    def process(self, msg):
        self._reader.put_data(msg)
