from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import sys
import time
from queue import Queue, Empty, Full

from streaming.operator.constant.operator_constants import OperatorConstants
from ray.streaming import utils

logger = logging.getLogger(__name__)


class RingBuffer(Queue):
    """
    A ring buffer extend Queue by override _init, _put, _get and _qsize method

    Note that item in ringbuffer must to bytes
    """

    def __init__(
        self,
        max_slot_size=OperatorConstants.DEFAULT_QUEUE_MAX_SLOT_SIZE,
        max_bytes=OperatorConstants.DEFAULT_QUEUE_MAX_BYTES,
        start_offset=OperatorConstants.DEFAULT_QUEUE_START_OFFSET,
        log_interval=OperatorConstants.DEFAULT_RINGBUFFER_LOG_DETAIL_INTERVAL,
        force_clear=OperatorConstants.DEFAULT_FORCE_CLEAR,
        report_interval=OperatorConstants.DEFAULT_LOG_DETAIL_INTERVAL,
    ):

        logger.info(
            "Ringbuffer initialized with max_slot_size:{}, "
            "max_byte:{}, start_offset:{}, log_interval:{}".format(
                max_slot_size, max_bytes, start_offset, log_interval
            )
        )

        super(RingBuffer, self).__init__(maxsize=max_slot_size)

        self.max_slot_size = max_slot_size
        self.max_bytes = max_bytes  # the max memory in bytes for ringbuffer
        self.queue = [None] * max_slot_size
        self.used_bytes = utils.getsize(self.queue)
        if self.used_bytes > max_bytes:
            raise ValueError(
                "Used bytes {} of {} items > max bytes {}".format(
                    self.used_bytes, max_slot_size, max_bytes
                )
            )

        self.start_offset = start_offset
        self.cursor = start_offset % max_slot_size  # the reader cursor
        self.head = self.cursor  # the oldest index
        self.tail = self.cursor  # the newest index

        # the real index without mod
        self.real_cursor = start_offset
        self.real_head = start_offset
        self.real_tail = start_offset

        self.is_full = False
        self.is_empty = True
        self.last_report_time = time.time()
        self.last_log_detail_time = time.time()
        self.report_metrics_interval_in_secs = report_interval
        self.log_detail_interval_in_secs = log_interval
        self.force_clear = force_clear
        self.can_be_notified_to_clear = True  # whether can be notified to clear or not
        self._closed = False

    def _init(self, max_slot_size: int):
        pass

    def put(self, item, block=True, timeout=None):
        """Put an item into the queue.

        If optional args 'block' is true and 'timeout' is None (the default),
        block if necessary until a free slot is available. If 'timeout' is
        a non-negative number, it blocks at most 'timeout' seconds and raises
        the Full exception if no free slot was available within that time.
        Otherwise ('block' is false), put an item on the queue if a free slot
        is immediately available, else raise the Full exception ('timeout'
        is ignored in that case).
        """
        if self._closed:
            raise RuntimeError("Ringbuffer is closed.")
        with self.not_full:
            if self.maxsize > 0:
                if not block:
                    if self.is_full:
                        raise Full
                elif timeout is None:
                    # Wait no full with timeout but return directly
                    # when this reader is closed.
                    while self.is_full and not self._closed:
                        self.not_full.wait(1)
                elif timeout < 0:
                    raise ValueError("'timeout' must be a non-negative number")
                else:
                    endtime = time.time() + timeout
                    while self.is_full:
                        remaining = endtime - time.time()
                        if remaining <= 0.0:
                            raise Full
                        self.not_full.wait(remaining)
            self._put(item)
            self.unfinished_tasks += 1
            self.not_empty.notify()
            self.is_empty = False

    def _put(self, data):
        """
        Add data to queue and increase the newest data index
        :param data(bytes): the data(in bytes type) to put
        :return:
        """
        commit_data_size = sys.getsizeof(data)
        # Put data into queue , update used bytes and tail pointer
        self.queue[self.tail] = data
        self.used_bytes += commit_data_size
        self.real_tail += 1
        self.tail = self.real_tail % self.max_slot_size

        if self.need_report_metrics():
            self.report_metrics()

        if self.need_log():
            msg = "Increase used bytes from {} to {}".format(
                self.used_bytes, self.used_bytes + commit_data_size
            )
            self.log_ring_buffer_detail("put", msg)

        if self.tail == self.head:
            logger.warning(
                "Queue is full, tail: {}, head: {}, unfinished tasks: {}".format(
                    self.head, self.tail, self.unfinished_tasks
                )
            )
            self.is_full = True
            self.log_ring_buffer_detail("put", "queue is full")

        if self.used_bytes > self.max_bytes:
            logger.warning(
                "Queue is full, max bytes: {}, used bytes: {}, "
                "commit data bytes: {}.".format(
                    self.max_bytes, self.used_bytes, commit_data_size
                )
            )
            self.is_full = True
            self.log_ring_buffer_detail("put", "queue is full")

    def get(self, block=True, timeout=None):
        """Retrieve an item from the queue.

        If optional args 'block' is true and 'timeout' is None (the default),
        block if necessary until an item is available. If 'timeout' is
        a non-negative number, it blocks at most 'timeout' seconds and raises
        the Empty exception if no item was available within that time.
        Otherwise ('block' is false), return an item if one is immediately
        available, else raise the Empty exception ('timeout' is ignored
        in that case).
        """
        if self._closed:
            raise RuntimeError("Ringbuffer is closed.")

        with self.not_empty:
            if not block:
                if self.is_empty:
                    raise Empty
            elif timeout is None:
                while self.is_empty:
                    self.not_empty.wait()
            elif timeout < 0:
                raise ValueError("'timeout' must be a non-negative number")
            else:
                endtime = time.time() + timeout
                while self.is_empty:
                    remaining = endtime - time.time()
                    if remaining <= 0.0:
                        raise Empty
                    self.not_empty.wait(remaining)
            item = self._get()
            # the queue still can be full.
            # self.not_full.notify()
            return item

    def _get(self):
        """
        Get data from queue and move the cursor
        :return:
        """
        try:
            data = self.queue[self.cursor]

            if self.need_report_metrics():
                self.report_metrics()

            if self.need_log():
                self.log_ring_buffer_detail("get")

            if data is None:
                # Data is not ready at current cursor
                # self.log_ring_buffer_detail("get", "data is none")
                return None

            self.real_cursor += 1
            self.cursor = self.real_cursor % self.max_slot_size
            # Queue is empty
            if self.cursor == self.tail:
                self.is_empty = True
                self.log_ring_buffer_detail("get", "queue is empty")
                # Queue is full and all data are consumed
                if self.is_full and self.force_clear:
                    self.log_ring_buffer_detail(
                        "get",
                        "queue is full and all data are consumed, "
                        "force clear buffer starting",
                    )
                    self.queue = [None] * self.max_slot_size
                    self.used_bytes = utils.getsize(self.queue)
                    self.is_full = False
                    self.not_full.notify()
                    self.can_be_notified_to_clear = False
                    self.real_head = self.real_cursor
                    self.log_ring_buffer_detail("get", "force clear buffer finished")
            return data
        except Exception as e:
            logger.error("Data is not ready, cursor: {}".format(self.cursor), e)
            return None

    def _qsize(self):
        """
        Get queue size (DO NOT USE THIS METHOD TO JUDGE FULL OR EMPTY ! )
        :return: queue size
        """
        if self.tail > self.head:
            return self.tail - self.head
        elif self.tail < self.head:
            return self.max_slot_size - (self.head - self.tail)
        else:
            if self.is_full:
                return self.maxsize
            else:
                return 0

    def seek_by_offset(self, start_offset):
        """
        Seek by specified cursor
        :param start_offset:
        :return:
        """
        with self.mutex:
            logger.info("Start to reset start offset to {}".format(start_offset))

            if not (self.real_head <= start_offset <= self.real_tail):
                raise ValueError(
                    "Seek error, specified cursor {} is not in "
                    "[{}, {}]".format(start_offset, self.real_head, self.real_tail)
                )

            self.start_offset = start_offset
            self.real_cursor = start_offset
            self.cursor = self.real_cursor % self.max_slot_size

            logger.info("Finished resetting start offset to {}".format(start_offset))

    def clear_expired_data(self, expired_offset):
        """
        Set the oldest data index
        :param expired_offset:
        :return:
        """
        with self.mutex:
            self.log_ring_buffer_detail("clear")

            if not self.can_be_notified_to_clear:
                msg = (
                    "can_be_notified_to_clear=False, do not clear for this"
                    " time, waiting for next checkpoint"
                )
                self.log_ring_buffer_detail("clear", msg)
                return

            self.log_ring_buffer_detail(
                "clear",
                "Start to clear expired data, "
                "expired offset: {}".format(expired_offset),
            )

            if expired_offset < self.real_head:
                msg = (
                    "expired_offset {} < real_head {}, reader consumed offset has not bean updated after "
                    "force clearing, do not clear this time".format(
                        expired_offset, self.real_head
                    )
                )
                self.log_ring_buffer_detail("clear", msg)
                return
            if expired_offset > self.real_cursor:
                msg = "expired_offset {} > real cursor {}, illegal value, do not clear this time.".format(
                    expired_offset, self.real_cursor
                )
                self.log_ring_buffer_detail("clear", msg)
                return

            old_head = self.head

            self.real_head = expired_offset
            self.head = self.real_head % self.max_slot_size
            self.log_ring_buffer_detail(
                "clear",
                "Setting head to {}, old head is:" " {}".format(self.head, old_head),
            )
            if old_head == self.head:
                if self.is_full and self.real_cursor == self.real_tail:
                    # queue is full and all data are consumed
                    self.queue = [None] * self.max_slot_size
                    self.used_bytes = utils.getsize(self.queue)
                    self.is_full = False
                else:
                    pass
            elif old_head < self.head:
                for i in range(old_head, self.head):
                    self.used_bytes -= sys.getsizeof(self.queue[i])
                    self.queue[i] = None

                if self.used_bytes < self.max_bytes:
                    self.is_full = False
            else:
                for i in range(old_head, self.max_slot_size):
                    self.used_bytes -= sys.getsizeof(self.queue[i])
                    self.queue[i] = None
                for i in range(0, self.head):
                    self.used_bytes -= sys.getsizeof(self.queue[i])
                    self.queue[i] = None

                if self.used_bytes < self.max_bytes:
                    self.is_full = False

            self.log_ring_buffer_detail(
                "clear",
                "Finished clearing expired data,"
                " expired offset: {}".format(expired_offset),
            )

            if not self.is_full:
                self.not_full.notify()

    def get_cursor(self):
        return self.cursor

    def get_head(self):
        return self.head

    def get_tail(self):
        return self.tail

    def log_ring_buffer_detail(self, operation, extra=None):
        pattern = (
            "[{}] ringbuffer max size:{}, used size:{}, real head:{}, "
            "real tail:{}, real cursor:{}, max bytes:{} used bytes:{}, "
            "is full:{}, is empty:{}, can_be_notified_to_clear:{}, extra msg:{}"
        )
        detail = pattern.format(
            operation,
            self.max_slot_size,
            self._qsize(),
            self.real_head,
            self.real_tail,
            self.real_cursor,
            self.max_bytes,
            self.used_bytes,
            self.is_full,
            self.is_empty,
            self.can_be_notified_to_clear,
            extra,
        )
        logger.debug(detail)
        self.last_log_detail_time = time.time()
        # Report ringbuffer metrics
        # self.report_metrics()

    def report_metrics(self):
        self.last_report_time = time.time()

    def need_log(self):
        if self.log_detail_interval_in_secs > 0:
            return (
                time.time() - self.last_log_detail_time
                > self.log_detail_interval_in_secs
            )
        else:
            return True

    def need_report_metrics(self):
        if self.report_metrics_interval_in_secs > 0:
            return (
                time.time() - self.last_report_time
                > self.report_metrics_interval_in_secs
            )
        return True

    def set_can_be_notified_to_clear(self):
        """
        This will be called after checkpoint finish
        :return:
        """
        with self.mutex:
            self.can_be_notified_to_clear = True
            self.log_ring_buffer_detail(
                "checkpoint", "set can_be_notified_to_clear to True"
            )

    def get_state(self, reader_consumed_offset):
        """
        :param reader_consumed_offset: tf operator reader consumed offset (int)
        :return: dict
        """
        with self.mutex:
            state = {
                "real_head": self.real_head,
                "real_tail": self.real_tail,
                "real_cursor": self.real_cursor,
                "reader_consumed_offset": reader_consumed_offset,
                "unconsumed_data": [],
            }
            # save unconsumed data to checkpoint state
            for i in range(state["reader_consumed_offset"], state["real_tail"]):
                state["unconsumed_data"].append(self.queue[i % self.maxsize])
            return state

    def __reset_start_offset(self, start_offset):
        self.start_offset = start_offset

        self.head = start_offset % self.max_slot_size
        self.tail = self.head
        self.cursor = self.head

        # the real index without mod
        self.real_head = start_offset
        self.real_tail = start_offset
        self.real_cursor = start_offset

    def __reset_cursor(self, real_cursor):
        self.real_cursor = real_cursor
        self.cursor = real_cursor % self.max_slot_size

    def load_checkpoint(self, state):
        with self.mutex:
            self.log_ring_buffer_detail("load_checkpoint", "load checkpoint start")

            # reset pointer
            logger.info(
                "Reset start offset to: {}".format(state["reader_consumed_offset"])
            )
            self.__reset_start_offset(state["reader_consumed_offset"])

            logger.info("Reset cursor to: {}".format(state["operator_consumed_offset"]))
            self.__reset_cursor(state["operator_consumed_offset"])

            # load data from state
            recover_start = state["reader_consumed_offset"]
            recover_end = state["real_tail"]
            logger.info(
                "Load checkpoint and reput data from {} to {}.".format(
                    recover_start, recover_end
                )
            )
            logger.info(
                f"Before load checkpoint, state info : real_head {state['real_head']},\
                  real_tail {state['real_tail']}, real_cursor {state['real_cursor']}, \
                  reader_consumed_offset {state['reader_consumed_offset']}, \
                  current real_head {self.real_head}, read_tail {self.real_tail}, \
                  real cursor {self.real_cursor} head {self.head}, tail {self.tail}, \
                  cursor {self.cursor}, unfinished task {self.unfinished_tasks}"
            )
            none_value_num_in_state = 0
            for i in range(recover_start, recover_end):
                value = state["unconsumed_data"][i - recover_start]
                if value:
                    self._put(value)
                    self.unfinished_tasks += 1
                else:
                    none_value_num_in_state += 1

            # putted data into queue, we need to notify not empty condition
            if recover_end > recover_start:
                self.is_empty = False
                self.not_empty.notify()
            self.log_ring_buffer_detail("load_checkpoint", "load checkpoint finish")
            logger.info(
                f"After load checkpoint, none value num : {none_value_num_in_state} {self}"
            )

    def close(self):
        self._closed = True
        logger.info("Close ring buffer.")
        if self.is_full:
            logger.info("Notify not full.")
            try:
                self.not_full.notify()
            except Exception as e:
                logger.info("Ignore not full notify exception {}.".format(e))
        if self.is_empty:
            logger.info("Notify not empty.")
            try:
                self.not_empty.notify()
            except Exception as e:
                logger.info("Ignore not empty notify exception {}.".format(e))

    def __str__(self):
        return f"RingBuffer real_head {self.real_head}, read_tail {self.real_tail}, \
                  real cursor {self.real_cursor} head {self.head}, tail {self.tail}, \
                  cursor {self.cursor}, unfinished task {self.unfinished_tasks}"

    def __repr__(self):
        return self.__str__()
