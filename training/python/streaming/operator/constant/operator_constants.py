from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class OperatorConstants:
    """
    Training constants
    """

    NONE_BYTES = 8

    # Stream reader
    READER_START_OFFSET = "reader_start_offset"
    READER_MAX_SLOT_SIZE = "reader_max_slot_size"
    READER_MAX_BYTES = "reader_max_bytes"
    READER_LOG_INTERVAL = "reader_log_interval_in_secs"
    READER_FORCE_CLEAR_WHEN_FULL_AND_ALL_CONSUMED = (
        "reader_force_clear_when_full_and_all_consumed"
    )

    DEFAULT_QUEUE_START_OFFSET = 0
    DEFAULT_QUEUE_MAX_SLOT_SIZE = 100000
    DEFAULT_QUEUE_MAX_BYTES = 1024 * 1024 * 1024
    DEFAULT_LOG_DETAIL_INTERVAL = 10
    DEFAULT_FORCE_CLEAR = True
    DEFAULT_RINGBUFFER_LOG_DETAIL_INTERVAL = 60 * 60

    DEFAULT_CIRCULAR_BUFFER_MAX_SIZE = 100000
    DEFAULT_CIRCULAR_BUFFER_MAX_BYTES = 1024 * 1024 * 1024
    DEFAULT_CIRCULAR_BUFFER_WAIT_TIME = 0.2
