import logging
import threading
import time

from streaming.operator.constant.operator_constants import OperatorConstants
from ray.streaming import utils

logger = logging.getLogger(__name__)


class CircularBuffer:
    def __init__(
        self,
        max_size=OperatorConstants.DEFAULT_CIRCULAR_BUFFER_MAX_SIZE,
        max_bytes=OperatorConstants.DEFAULT_CIRCULAR_BUFFER_MAX_SIZE,
        log_interval=OperatorConstants.DEFAULT_LOG_DETAIL_INTERVAL,
    ):
        self.max_size = max_size
        self.max_bytes = max_bytes
        self.queue = [None] * max_size
        self.remain_bytes = max_bytes - utils.getsize(self.queue)
        if self.remain_bytes < 0:
            raise ValueError(
                "Used bytes of {} items > max bytes {}".format(max_size, max_bytes)
            )
        self.head = 0
        self.tail = 0
        self.is_empty = True
        self.lost_data_count = 0
        self.last_log_detail_time = time.time()
        self.log_detail_interval_in_secs = log_interval
        self._lock = threading.Condition()

    def empty(self):
        return self.is_empty

    def get(self):
        self._lock.acquire()
        data = self._get()
        self._lock.release()
        return data

    def get_list(self, num):
        self._lock.acquire()
        res = []
        while num > 0:
            num -= 1
            res.append(self._get())
            if self.empty() and num > 0:
                self._lock.wait()
        self._lock.release()
        return res

    def _get(self):
        if self.empty():
            self._lock.wait()
        data = self.queue[self.head]
        self.queue[self.head] = None
        self.remain_bytes += utils.getsize(data) - OperatorConstants.NONE_BYTES
        self.head = self._add_cursor(self.head)
        if self.head == self.tail:
            self.is_empty = True
        return data

    def put(self, data):
        self._lock.acquire()
        used_bytes = utils.getsize(data)
        if used_bytes > self.max_bytes:
            raise RuntimeError(
                "The bytes of this \
                item is out of queue bytes limit."
            )
        while not self._can_put(used_bytes):
            self._pop_front()
            self.lost_data_count += 1
            if self.need_log():
                logger.info(f"Threw away {self.lost_data_count} pieces of data.")
                self.last_log_detail_time = time.time()

        self.remain_bytes += OperatorConstants.NONE_BYTES - used_bytes
        self.queue[self.tail] = data
        self.tail = self._add_cursor(self.tail)
        self.is_empty = False
        self._lock.notify()
        self._lock.release()

    def get_size(self):
        self._lock.acquire()
        if self.empty():
            size = 0
        else:
            size = (self.tail + self.max_size - self.head - 1) % self.max_size + 1
        self._lock.release()
        return size

    def get_bytes_size(self):
        self._lock.acquire()
        bytes_size = self.max_bytes - self.remain_bytes
        self._lock.release()
        return bytes_size

    def clear(self):
        self._lock.acquire()
        self.queue = [None] * self.max_size
        self.remain_bytes = self.max_bytes - utils.getsize(self.queue)
        self.head = 0
        self.tail = 0
        self.is_empty = True
        self._lock.release()

    def _pop_front(self):
        self._get()

    def _can_put(self, used_bytes):
        if self.head == self.tail and not self.empty():
            return False
        if used_bytes - OperatorConstants.NONE_BYTES > self.remain_bytes:
            return False
        return True

    def _add_cursor(self, cursor):
        return (cursor + 1) % self.max_size

    def need_log(self):
        if self.log_detail_interval_in_secs > 0:
            return (
                time.time() - self.last_log_detail_time
                > self.log_detail_interval_in_secs
            )
        else:
            return True
