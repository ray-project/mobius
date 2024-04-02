import os
import sys
import threading
from queue import Full, Empty

import psutil

from time import sleep

import pytest
from streaming.operator.reader.ring_buffer import RingBuffer
from ray.streaming import utils


def debug(q, return_msg):
    print(return_msg)
    print("Buffer is: {}".format(q.queue))
    print(
        "head: {}, tail:{}, cursor: {}, size:{}\n".format(
            q.head, q.tail, q.cursor, q._qsize()
        )
    )


def pull(q: RingBuffer):
    for i in range(1, 100):
        print("Pulling from queue: {}".format(q.get()))

        # Adjust interval to block
        sleep(0.01)

        if i % 20 == 0:
            print("Clearing expired offset: {}".format(i))
            q.clear_expired_data(i)
            assert q.head == i % q.maxsize

        if i == 90:
            print("Reset start offset to 85")
            q.seek_by_offset(85)
            assert q.cursor == 85 % q.maxsize


def push(q):
    for i in range(1, 100):
        print("Pushing {} into queue starting".format(i))
        q.put(i)
        print("Pushing {} into queue finished".format(i))


def test_buffer_operation_block():
    q = RingBuffer(30, force_clear=False)

    producer = threading.Thread(target=push, args=(q,))
    producer.start()

    consumer = threading.Thread(target=pull, args=(q,))
    consumer.start()

    producer.join()
    consumer.join()


def test_buffer_operation_non_block():
    # Initialize ring buffer with capacity == 5
    q = RingBuffer(5, force_clear=False)

    debug(q, q.put(1))
    assert (
        q._qsize() == 1
        and not q.is_full
        and q.head == 0
        and q.tail == 1
        and q.cursor == 0
    )

    debug(q, q.put(2))
    assert (
        q._qsize() == 2
        and not q.is_full
        and q.head == 0
        and q.tail == 2
        and q.cursor == 0
    )

    debug(q, q.put(3))
    assert (
        q._qsize() == 3
        and not q.is_full
        and q.head == 0
        and q.tail == 3
        and q.cursor == 0
    )

    debug(q, q.put(4))
    assert (
        q._qsize() == 4
        and not q.is_full
        and q.head == 0
        and q.tail == 4
        and q.cursor == 0
    )

    debug(q, q.put(5))
    assert (
        q._qsize() == 5 and q.is_full and q.head == 0 and q.tail == 0 and q.cursor == 0
    )

    # Buffer is full, blocking until timeout
    with pytest.raises(Full):
        q.put(6, block=True, timeout=0.5)

    # Get operation does not pop data, so the buffer is still full
    debug(q, q.get())
    assert (
        q._qsize() == 5 and q.is_full and q.head == 0 and q.tail == 0 and q.cursor == 1
    )

    debug(q, q.get())
    assert (
        q._qsize() == 5 and q.is_full and q.head == 0 and q.tail == 0 and q.cursor == 2
    )

    debug(q, q.get())
    assert (
        q._qsize() == 5 and q.is_full and q.head == 0 and q.tail == 0 and q.cursor == 3
    )

    debug(q, q.get())
    assert (
        q._qsize() == 5 and q.is_full and q.head == 0 and q.tail == 0 and q.cursor == 4
    )

    debug(q, q.get())
    assert (
        q._qsize() == 5 and q.is_full and q.head == 0 and q.tail == 0 and q.cursor == 0
    )

    # All data are consumed so the queue is both full and empty for now
    assert q.is_empty
    assert q.is_full

    # Buffer is empty
    with pytest.raises(Empty):
        debug(q, q.get_nowait())

    # Buffer is still full, can not put
    with pytest.raises(Full):
        q.put_nowait(6)

    # Clear expired data to free buffer memory, set head from 0 to 2 (index 0 and 1 are expired)
    q.clear_expired_data(2)
    assert (
        q._qsize() == 3
        and not q.is_full
        and q.head == 2
        and q.tail == 0
        and q.cursor == 0
    )

    debug(q, q.put(6))
    assert (
        q._qsize() == 4
        and not q.is_full
        and q.head == 2
        and q.tail == 1
        and q.cursor == 0
    )

    # Buffer is full again
    debug(q, q.put(7))
    assert (
        q._qsize() == 5 and q.is_full and q.head == 2 and q.tail == 2 and q.cursor == 0
    )

    # Can not put when buffer is full
    with pytest.raises(Full):
        q.put_nowait(8)

    debug(q, q.get())
    assert (
        q._qsize() == 5 and q.is_full and q.head == 2 and q.tail == 2 and q.cursor == 1
    )

    debug(q, q.get())
    assert (
        q._qsize() == 5 and q.is_full and q.head == 2 and q.tail == 2 and q.cursor == 2
    )

    # Empty again
    with pytest.raises(Empty):
        q.get(block=False, timeout=1)


def get_process_memory():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss


def test_python_list_memory():
    assert 64 == sys.getsizeof([])
    assert 64 == utils.getsize([])

    assert 64 + 8 == sys.getsizeof([None])
    assert 64 + 8 + 16 == utils.getsize([None])

    assert 64 + 8 * 5 == sys.getsizeof([None] * 5)
    assert 64 + 8 * 5 + 16 == utils.getsize([None] * 5)

    # []->64 + item pointer-> 8*5 + None->16 + bytes->33 + 'a'->1
    assert 64 + 8 * 5 + 16 + 33 + 1 == utils.getsize([None, b"a", None, None, None])

    # []->64 + item pointer-> 8*5 + bytes->33 + 'a'->1
    assert 64 + 8 * 5 + 33 + 1 == utils.getsize([b"a", b"a", b"a", b"a", b"a"])

    assert 64 + 8 == sys.getsizeof([b"a"])

    # []->64 + item pointer->8 + bytes->33
    assert 64 + 8 + 33 == utils.getsize([b""])
    # []->64 + item pointer->8 + bytes->33 + 'a'->1
    assert 64 + 8 + 34 == utils.getsize([b"a"])
    # []->64 + item pointer->8 + bytes->33 + 'ab'->2
    assert 64 + 8 + 35 == utils.getsize([b"ab"])

    last_memory_in_bytes = get_process_memory()
    # Ring buffer with 10000000 same b'a' takes 76MB
    q = [b"a"] * 10000000
    assert 64 + 8 * 10000000 + 33 + 1 == utils.getsize(q)
    assert int(utils.getsize(q) / 1024 / 1024) == 76  # 76MB
    # Check increased memory of current process
    assert int((get_process_memory() - last_memory_in_bytes) / 1024 / 1024) == 76


def test_ring_buffer_memory():
    q = RingBuffer(max_slot_size=10, max_bytes=10240, force_clear=False)

    last_used_bytes = q.used_bytes
    last_mem_size = utils.getsize(q.queue)

    print("\n")
    for i in range(1, 10):
        q.put(str(i).encode(encoding="utf-8"))

        # Count used bytes directly
        print(
            "#{} Used byte: {}, diff byte: {}".format(
                i, q.used_bytes, q.used_bytes - last_used_bytes
            )
        )

        # Use memory utils to count memory size
        print(
            "#{} Used mem:  {}, diff mem:  {}\n".format(
                i, utils.getsize(q.queue), utils.getsize(q.queue) - last_mem_size
            )
        )

        last_used_bytes = q.used_bytes
        last_mem_size = utils.getsize(q.queue)
        assert last_mem_size == last_used_bytes


def test_put():
    q = RingBuffer(max_slot_size=2, force_clear=False)

    item = 0
    q.put(item, block=False)
    assert q.get() == item

    item = 1
    q.put(item, block=True, timeout=0.2)
    assert q.get() == item

    # No clear operation, ringbuffer is both full and empty(all data ared consumed)
    assert q.is_full is True
    assert q.is_empty is True

    with pytest.raises(Full):
        q.put_nowait(1)

    with pytest.raises(Full):
        q.put(1, timeout=0.2)

    assert q.is_empty is True

    with pytest.raises(Empty):
        q.get_nowait()
    with pytest.raises(Full):
        q.put(1, timeout=0.2)


def test_get():
    q = RingBuffer(max_slot_size=2, force_clear=False)

    item = 0
    q.put(item)
    assert q.get() == item

    item = 1
    q.put(item)
    assert q.get(timeout=0.2) == item

    assert q.is_full

    with pytest.raises(Full):
        q.put_nowait(1)

    q.clear_expired_data(2)
    assert q.head == q.tail == q.cursor == 0

    with pytest.raises(Empty):
        q.get_nowait()

    with pytest.raises(Empty):
        q.get(timeout=0.2)


def test_get_force_clear():
    q = RingBuffer(max_slot_size=2, force_clear=True)

    item = 0
    q.put(item)
    assert q.get() == item

    item = 1
    q.put(item)

    with pytest.raises(Full):
        q.put_nowait(1)

    assert q.get(timeout=0.2) == item

    assert q.head == q.tail == q.cursor == 0
    assert not q.is_full
    assert q.is_empty

    with pytest.raises(Empty):
        q.get_nowait()

    item = 2
    q.put(item)
    assert q.get() == item


def test_init_exceed_mem_limit():
    with pytest.raises(ValueError):
        RingBuffer(max_slot_size=5000, max_bytes=1024)


def test_put_reach_memory_limit():
    buffer = RingBuffer(max_slot_size=40, max_bytes=471, force_clear=False)
    assert 64 + 8 * 40 + 16 == utils.getsize(buffer.queue)  # 400Bytes

    # add 35Bytes
    buffer.put(b"10", timeout=1)
    assert (
        buffer.used_bytes == 435
        and utils.getsize(buffer.queue) == 435
        and not buffer.is_full
    )
    # add 35Bytes
    buffer.put(b"11", timeout=1)
    assert (
        buffer.used_bytes == 470
        and utils.getsize(buffer.queue) == 470
        and not buffer.is_full
    )
    # add 35Bytes, can exceed little bit
    buffer.put(b"12", timeout=1)
    assert (
        buffer.used_bytes == 505
        and utils.getsize(buffer.queue) == 505
        and buffer.is_full
    )

    with pytest.raises(Full):
        buffer.put(b"13", timeout=1)

    assert buffer.is_full

    assert b"10" == buffer.get()
    assert b"11" == buffer.get()
    assert b"12" == buffer.get()

    assert buffer.is_full

    assert buffer.cursor == 3

    print("\nBefore clear###\n")
    print(utils.getsize(buffer.queue))
    print(buffer.used_bytes)

    buffer.clear_expired_data(2)

    print("\nAfter clear###\n")
    print(utils.getsize(buffer.queue))
    print(buffer.used_bytes)

    assert not buffer.is_full
    # Freed 2 item 35Bytes for each one, so memory decreased 70Bytes (505 - 70 == 435)
    assert buffer.used_bytes == 435
    assert utils.getsize(buffer.queue) == 435


def test_clear():
    buffer = RingBuffer(max_slot_size=10)
    for i in range(0, 10):
        buffer.put(bytes(i))
    assert buffer.is_full

    for i in range(0, 10):
        buffer.get()
    assert buffer.is_empty

    # force cleared, can_be_notified_to_clear == False
    assert not buffer.can_be_notified_to_clear

    assert buffer.real_head == 10
    assert buffer.real_tail == 10
    assert buffer.real_cursor == 10

    # mock checkpoint signal, set can_be_notified_to_clear to True
    buffer.set_can_be_notified_to_clear()

    # If expected offset < real head, clear operator will not work
    expired_offset = 8
    buffer.clear_expired_data(expired_offset)
    assert buffer.real_head == 10

    buffer.put(b"1")
    buffer.put(b"2")
    buffer.put(b"3")

    buffer.get()
    buffer.get()

    expired_offset = 12
    buffer.clear_expired_data(expired_offset)

    assert buffer.real_head == 12
    assert buffer.real_tail == 13
    assert buffer.real_cursor == 12


def test_seek_by_offset():
    buffer = RingBuffer(max_slot_size=10)
    for i in range(0, 10):
        buffer.put(bytes(i))
    assert buffer.is_full

    for i in range(0, 9):
        buffer.get()

    assert buffer.real_head == 0
    assert buffer.real_tail == 10
    assert buffer.real_cursor == 9

    buffer.seek_by_offset(5)

    assert buffer.real_head == 0
    assert buffer.real_tail == 10
    assert buffer.real_cursor == 5

    with pytest.raises(ValueError):
        buffer.seek_by_offset(12)


def test_load_checkpoint():
    state = {
        "real_head": 8,
        "real_tail": 10,
        "real_cursor": 8,
        "reader_consumed_offset": 8,
        "operator_consumed_offset": 8,
        "unconsumed_data": [1, 2],
    }
    buffer = RingBuffer(max_slot_size=10)
    buffer.load_checkpoint(state)
    print(buffer.queue)
    for i in range(0, 8):
        print(f"Put buffer {i + 3}")
        buffer.put(i + 3)
    print("Put all buffers {}".format(buffer.queue))
    result = 0
    for i in range(0, 10):
        print(f"Get buffer {i + 1}")
        result += buffer.get()
    buffer.put(0)
    result += buffer.get()
    assert result == 55


class ProducerThread(threading.Thread):
    def __init__(self, buffer, num):
        threading.Thread.__init__(self)
        self.buffer = buffer
        self.num = num

    def run(self):
        print("starting " + self.name)
        for i in range(0, self.num):
            sleep(0.01)
            self.buffer.put(i)


class ConsumerThread(threading.Thread):
    def __init__(self, buffer, num):
        threading.Thread.__init__(self)
        self.buffer = buffer
        self.num = num
        self.sum = 0

    def run(self):
        print("starting " + self.name)
        for i in range(0, self.num):
            self.sum += self.buffer.get()

    def get_sum(self):
        return self.sum


def test_producer_consumer():
    state = {
        "real_head": 8,
        "real_tail": 10,
        "real_cursor": 8,
        "reader_consumed_offset": 8,
        "operator_consumed_offset": 8,
        "unconsumed_data": [1, 2],
    }
    buffer = RingBuffer(max_slot_size=10)
    buffer.load_checkpoint(state)
    print(buffer.queue)
    thread1 = ProducerThread(buffer, 100)
    thread2 = ConsumerThread(buffer, 102)
    thread1.start()
    sleep(1.0)
    thread2.start()
    thread1.join()
    thread2.join()
    assert thread2.get_sum() == 4953


def test_load_none_value():
    state = {
        "real_head": 8,
        "real_tail": 10,
        "real_cursor": 8,
        "reader_consumed_offset": 8,
        "operator_consumed_offset": 8,
        "unconsumed_data": [None, 2],
    }
    buffer = RingBuffer(max_slot_size=10)
    buffer.load_checkpoint(state)
    print(buffer.queue)
    thread1 = ProducerThread(buffer, 100)
    thread2 = ConsumerThread(buffer, 101)
    thread1.start()
    sleep(1.0)
    thread2.start()
    thread1.join()
    thread2.join()

    assert thread2.get_sum() == 4952
