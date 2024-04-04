import logging
import threading
import time
import os

from streaming.operator.constant.operator_constants import OperatorConstants

# from ray.streaming.constants import StreamingConstants

from .ring_buffer import RingBuffer
from .stream_reader import StreamReader

logger = logging.getLogger(__name__)


class TFOperatorReader(StreamReader):
    """
    TF Operator Data Reader
    """

    def __init__(self, config):
        start_offset = (
            int(config[OperatorConstants.READER_START_OFFSET])
            if OperatorConstants.READER_START_OFFSET in config
            else OperatorConstants.DEFAULT_QUEUE_START_OFFSET
        )

        max_slot_size = (
            int(config[OperatorConstants.READER_MAX_SLOT_SIZE])
            if OperatorConstants.READER_MAX_SLOT_SIZE in config
            else OperatorConstants.DEFAULT_QUEUE_MAX_SLOT_SIZE
        )

        max_bytes = (
            int(config[OperatorConstants.READER_MAX_BYTES])
            if OperatorConstants.READER_MAX_BYTES in config
            else OperatorConstants.DEFAULT_QUEUE_MAX_BYTES
        )

        log_interval = (
            int(config[OperatorConstants.READER_LOG_INTERVAL])
            if OperatorConstants.READER_LOG_INTERVAL in config
            else OperatorConstants.DEFAULT_LOG_DETAIL_INTERVAL
        )

        force_clear = (
            config[OperatorConstants.READER_FORCE_CLEAR_WHEN_FULL_AND_ALL_CONSUMED]
            if OperatorConstants.READER_FORCE_CLEAR_WHEN_FULL_AND_ALL_CONSUMED in config
            else OperatorConstants.DEFAULT_FORCE_CLEAR
        )

        super().__init__(start_offset)

        self.__data_buffer = RingBuffer(
            start_offset=start_offset,
            max_slot_size=max_slot_size,
            max_bytes=max_bytes,
            log_interval=log_interval,
            force_clear=force_clear,
        )

        self._consumed_offset = start_offset
        self._stop_event = threading.Event()

    def put_data(self, msg):
        if not self._stop_event.is_set():
            self.__data_buffer.put(msg)
        else:
            self._stop_event.clear()
            raise Exception(
                "Stop event is set, raise exception to rollback now."
                if not self._stop_reason
                else self._stop_reason
            )

    def fetch_data(self, batch_size, **options):
        """
        Fetch training input data from buffer (A BlockingQueue)
        :param batch_size: The required batch size
        :param options: Other options
        :return:  List of training input data
        """

        batch_data = []

        while len(batch_data) < batch_size:
            item = self.__data_buffer.get()
            if item is None:
                time.sleep(0.01)
                continue
            # Just for tensorflow data where buffer item type is byte
            batch_data.append((self._consumed_offset, str(item, encoding="utf-8")))

        # Update consume offset(The checkpoint state info)
        self._consumed_offset += batch_size
        return batch_data, False

    def iterator(self, batch_size, **options):
        batch_data = []
        last_fetched_data_ts = time.time()
        data_num = 0
        while True:
            item = self.__data_buffer.get()
            if item is None:
                time.sleep(0.1)
                if time.time() > last_fetched_data_ts + 20:
                    logger.info(
                        f"No data fetched 20s, buffer info : {self.__data_buffer}, batch_data {batch_data}, data num {data_num}"
                    )
                    last_fetched_data_ts = time.time()
                continue
            data_num += 1
            batch_data.append(str(item, encoding="utf-8"))
            if len(batch_data) % 100 == 0:
                logger.debug("in iterator batch data size is: %d" % len(batch_data))
            if batch_size == len(batch_data):
                logger.debug("in iterator batch data size is: %d" % batch_size)

                yield batch_data
                self._consumed_offset += len(batch_data)
                batch_data = []

    def seek_by_offset(self, start_offset):
        """
        seek data by specified start offset
        when request start offset is illegal, use current cursor as start offset
        :param start_offset:
        :return:
        """
        try:
            self.__data_buffer.seek_by_offset(start_offset)
        except ValueError as v:
            logger.warning(
                "Seek by offset caught value error: {}, "
                "use current cursor as start offset.".format(v)
            )

    def stop_reader(self, reason=None):
        """
        Report stop reader reason for upper level.
        """
        logger.info("Tf operator stop, reason : {}.".format(reason))
        try:
            self._stop_reason = reason
            self._stop_event.set()
            # Close it to awake ring buffer.
            self.__data_buffer.close()
        except Exception as e:
            logger.warning("Stop reader and make exception event failed {}.".format(e))
            os._exit(-1)
        logger.info("Finish to stop reader, emit exception.")

    def clear_expired_data(self, expired_offset):
        self.__data_buffer.clear_expired_data(expired_offset)

    def set_can_be_notified_to_clear(self):
        self.__data_buffer.set_can_be_notified_to_clear()

    def get_cursor(self):
        return self.__data_buffer.get_cursor()

    def get_head(self):
        return self.__data_buffer.get_head()

    def get_offset(self):
        return self._consumed_offset

    def get_tail(self):
        return self.__data_buffer.get_tail()

    def get_state(self):
        return self.__data_buffer.get_state(self._consumed_offset)

    def load_checkpoint(self, checkpoint_state):
        if not self.__is_operator_consumed_offset_legal(checkpoint_state):
            logger.info(
                "operator_consumed_offset is illegal, use reader_consumed_offset: {}".format(
                    checkpoint_state["reader_consumed_offset"]
                )
            )
            checkpoint_state["operator_consumed_offset"] = checkpoint_state[
                "reader_consumed_offset"
            ]
        self._consumed_offset = checkpoint_state["operator_consumed_offset"]
        logger.info("Tf operator consumed_offset is: {}".format(self._consumed_offset))
        self.__data_buffer.load_checkpoint(checkpoint_state)
        logger.debug(
            "Tf operator finished load checkpoint, current state is: {}".format(
                self.get_state()
            )
        )

    def __is_operator_consumed_offset_legal(self, checkpoint_state):
        reader_consumed_offset = checkpoint_state["reader_consumed_offset"]
        operator_consumed_offset = checkpoint_state["operator_consumed_offset"]
        real_head = checkpoint_state["real_head"]
        real_tail = checkpoint_state["real_tail"]
        logger.info("operator_consumed_offset is: {}".format(operator_consumed_offset))
        # if operator_consumed_offset is valid, use it as reader offset to keep
        # exactly once
        if (
            operator_consumed_offset is not None
            and real_head
            <= reader_consumed_offset
            <= operator_consumed_offset
            <= real_tail
        ):
            logger.info("operator_consumed_offset is legal.")
            return True
        else:
            return False

    def __str__(self):
        return "TfOperatorReader"
