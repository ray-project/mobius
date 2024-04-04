import logging

from streaming.operator.constant.operator_constants import OperatorConstants

from .circular_buffer import CircularBuffer
from .stream_reader import StreamReader

logger = logging.getLogger(__name__)


class EvalReader(StreamReader):
    """
    Eval Operator Data Reader
    """

    def __init__(self, config):

        max_size = (
            int(config[OperatorConstants.READER_MAX_SLOT_SIZE])
            if OperatorConstants.READER_MAX_SLOT_SIZE in config
            else OperatorConstants.DEFAULT_QUEUE_MAX_SLOT_SIZE
        )

        max_bytes = (
            int(config[OperatorConstants.READER_MAX_BYTES])
            if OperatorConstants.READER_MAX_BYTES in config
            else OperatorConstants.DEFAULT_QUEUE_MAX_BYTES
        )

        super().__init__()

        self.__data_buffer = CircularBuffer(max_size=max_size, max_bytes=max_bytes)

    def put_data(self, msg):
        self.__data_buffer.put(msg)

    def fetch_data(self, batch_size, **options):
        """
        Fetch training input data from buffer (A BlockingQueue)
        :param batch_size: The required batch size
        :param options: Other options
        :return:  List of training input data
        """
        batch_data = list(
            map(
                lambda x: str(x, encoding="utf-8"),
                self.__data_buffer.get_list(batch_size),
            )
        )
        return batch_data

    def get_offset(self):
        pass

    def seek_by_offset(self, start_offset):
        pass

    def clear_expired_data(self, expired_offset):
        pass

    def iterator(self, batch_size, **options):
        while True:
            batch_data = self.fetch_data(batch_size)
            yield batch_data

    def __str__(self):
        return "EvalReader"
