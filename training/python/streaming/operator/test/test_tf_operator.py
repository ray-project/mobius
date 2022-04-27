from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pickle
from random import random
from time import sleep

from ray.streaming.constants import StreamingConstants
from ray.streaming.runtime.state_backend import StateBackendFactory
from streaming.operator.impl.tf_operator import (
    MockOperator,
    MockGenerateOperator,
    TFOperator,
)


def test_process():
    tf_operator = MockOperator()
    tf_operator.init({})
    tf_operator.run()
    assert tf_operator._reader.get_cursor() == 0

    # Put data into buffer and start training thread
    tf_operator.process(b"a")
    sleep(0.5)
    assert tf_operator._reader.get_cursor() == 1

    # Put data into buffer and start training thread
    tf_operator.process(b"b")
    sleep(0.5)
    assert tf_operator._reader.get_cursor() == 2

    tf_operator.process(b"c")
    tf_operator.process(b"d")
    tf_operator.process(b"e")
    sleep(0.5)
    assert tf_operator._reader.get_cursor() == 5


def test_generate():
    tf_operator = MockGenerateOperator()
    tf_operator.init({})
    tf_operator.run()
    assert tf_operator._reader.get_cursor() == 0

    # Put data into buffer and start training thread
    tf_operator.process(b"a")
    sleep(0.5)
    assert tf_operator._reader.get_cursor() == 1

    # Put data into buffer and start training thread
    tf_operator.process(b"b")
    sleep(0.5)
    assert tf_operator._reader.get_cursor() == 2

    tf_operator.process(b"c")
    tf_operator.process(b"d")
    tf_operator.process(b"e")
    sleep(0.5)
    assert tf_operator._reader.get_cursor() == 5


def test_save_checkpoint():
    key_value_state = StateBackendFactory.get_state_backend(
        StreamingConstants.CP_STATE_BACKEND_PANGU)
    key_value_state.init({
        "cp_state_backend_type": "cp_state_backend_pangu",
        "cp_pangu_root_dir": "file:///tmp/state_backend/"
    })
    tf_operator = MockOperator()
    tf_operator.init({"batch_size": 5, "epoch": 2})
    tf_operator.set_key_value_state(key_value_state)
    tf_operator.run()
    [tf_operator.process(c.encode(encoding='utf-8')) for c in "abcdefgh"]
    # Leave a little bit time for consumer
    sleep(0.5)
    assert tf_operator._reader.get_head() == 0

    # Save checkpoint
    tf_operator.save_checkpoint(1)

    assert tf_operator._last_checkpoint_state["reader_consumed_offset"] == 5
    assert tf_operator._reader.get_offset() == 5
    assert tf_operator._reader.get_head() == 0
    assert tf_operator._reader.get_cursor() == 8
    assert tf_operator._reader.get_tail() == 8

    tf_operator.on_checkpoint_complete(1)
    assert tf_operator._reader.get_head() == 5

    [tf_operator.process(c.encode(encoding='utf-8')) for c in "ijk"]
    sleep(0.5)
    assert tf_operator._reader.get_offset() == 10
    assert tf_operator._reader.get_cursor() == 10


def test_save_and_load_checkpoint():
    key_value_state = StateBackendFactory.get_state_backend(
        StreamingConstants.CP_STATE_BACKEND_DFS)
    key_value_state.init({
        StreamingConstants.CP_STATE_BACKEND_TYPE:
        StreamingConstants.CP_STATE_BACKEND_DFS,
        StreamingConstants.CP_DFS_ROOT_DIR:
        StreamingConstants.CP_DFS_ROOT_DIR_DEFAULT + "/{}".format(random())
    })
    tf_operator = MockOperator()
    tf_operator.init({"batch_size": 5, "epoch": 2})
    tf_operator.set_key_value_state(key_value_state)
    tf_operator.run()
    [tf_operator.process(c.encode(encoding='utf-8')) for c in "abcdefgh"]
    # Leave a little bit time for consumer
    sleep(0.5)
    assert tf_operator._reader.get_head() == 0

    # Save checkpoint
    tf_operator.save_checkpoint(1)

    assert tf_operator._last_checkpoint_state["reader_consumed_offset"] == 5
    assert tf_operator._reader.get_offset() == 5
    assert tf_operator._reader.get_head() == 0
    assert tf_operator._reader.get_cursor() == 8
    assert tf_operator._reader.get_tail() == 8

    tf_operator.on_checkpoint_complete(1)
    assert tf_operator._reader.get_head() == 5

    [tf_operator.process(c.encode(encoding='utf-8')) for c in "ijk"]
    sleep(0.5)
    assert tf_operator._reader.get_offset() == 10
    assert tf_operator._reader.get_cursor() == 10

    tf_operator = MockOperator()
    tf_operator.init({"batch_size": 5, "epoch": 2})
    tf_operator.set_key_value_state(key_value_state)
    tf_operator.load_checkpoint(1)
    assert tf_operator._last_checkpoint_state["reader_consumed_offset"] == 5
    assert tf_operator._reader.get_offset() == 5
    assert tf_operator._reader.get_head() == 5
    assert tf_operator._reader.get_cursor() == 5
    assert tf_operator._reader.get_tail() == 8
    current_state = tf_operator._reader.get_state()
    print(current_state)

    tf_operator.delete_checkpoint(1)


def test_save_checkpoint_async():
    key_value_state = StateBackendFactory.get_state_backend(
        StreamingConstants.CP_STATE_BACKEND_PANGU)
    key_value_state.init({
        "cp_state_backend_type": "cp_state_backend_pangu",
        "cp_pangu_root_dir": "file:///tmp/state_backend/"
    })
    tf_operator = MockOperator()
    tf_operator.init({})
    tf_operator.set_key_value_state(key_value_state)
    tf_operator.run()

    assert len(tf_operator._last_checkpoint_state) == 0

    [tf_operator.process(c.encode(encoding='utf-8')) for c in "abcde"]

    # Leave a little bit time for consumer
    sleep(0.5)
    # Return Future
    f = tf_operator.save_checkpoint_async(1)
    assert 5 == pickle.loads(f.result()).get("reader_consumed_offset")
    assert tf_operator._last_checkpoint_state["reader_consumed_offset"] == 5


def test_seek_by_offset_normal():
    tf_operator = MockOperator()
    tf_operator.init({"rollback_offset": 2})

    assert len(tf_operator._last_checkpoint_state) == 0

    [tf_operator.process(c.encode(encoding='utf-8')) for c in "abcde"]

    # Leave a little bit time for consumer
    sleep(0.5)

    [tf_operator.process(c.encode(encoding='utf-8')) for c in "xyz"]


def test_seek_by_offset_abnormal():
    tf_operator = MockOperator()
    tf_operator.init({"rollback_offset": 12})

    assert len(tf_operator._last_checkpoint_state) == 0

    [tf_operator.process(c.encode(encoding='utf-8')) for c in "abcdefghijk"]

    # Leave a little bit time for consumer
    sleep(0.5)

    [tf_operator.process(c.encode(encoding='utf-8')) for c in "xyz"]


class MockAbortOperator(TFOperator):
    def __init__(self):
        super(MockAbortOperator, self).__init__()
        self._batch_size = 1
        self._epoch = 1

    def init(self, config):
        super(MockAbortOperator, self).init(config)

        if "batch_size" in config:
            self._batch_size = config["batch_size"]
        if "epoch" in config:
            self._epoch = config["epoch"]

    def _train(self):
        i = 0
        for data in self._reader.iterator(self._batch_size):
            i += 1
            if i >= self._epoch:
                break
        self._reader.stop_reader("Stop")
        print("Stop reader")
        # try to sleep and wait test finished
        sleep(2.0)


def test_catch_exception_in_main_thread():
    tf_operator = MockAbortOperator()
    tf_operator.init({})
    tf_operator.run()

    # Put data into buffer and start training thread
    tf_operator.process(b"thread")
    sleep(0.5)
    assert tf_operator._reader.get_cursor() == 1

    catch_exception = None
    try:
        tf_operator.process(b"b")
    except Exception as e:
        catch_exception = e
    assert catch_exception is not None
