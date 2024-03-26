import threading
from time import sleep
import time

from streaming.operator.reader.circular_buffer import CircularBuffer


def test_queue_get_put():
    q = CircularBuffer(10)
    # test 'get' and 'put'
    a = []
    for i in range(1, 11):
        q.put(i)
        a.append(i)
    b = []
    q.put(1)
    for i in range(1, 11):
        b.append(q.get())
    a.append(1)
    assert a[1:] == b

    # test get_size
    q.put(1)
    assert q.get_size() == 1

    # test empty
    q._pop_front()
    assert q.empty() is True

    # test thread safe
    def get_one_data():
        curr_time1 = time.time()
        data = q.get()
        curr_time2 = time.time()
        used_time = curr_time2 - curr_time1
        assert data == 2
        assert used_time >= 0.5

    def put_one_data():
        sleep(0.5)
        q.put(2)

    threads = []
    threads.append(threading.Thread(target=get_one_data))
    threads.append(threading.Thread(target=put_one_data))
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # test get multi data
    b = []
    for i in range(1, 11):
        q.put(i)
        b.append(i)
    a = q.get_list(10)
    assert a == b
    q.clear()
    assert q.empty()
    assert q.get_size() == 0
