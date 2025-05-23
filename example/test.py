# from shmqueue3 import ShmQueue, ShmPool, ShmElem
from ctypes import c_uint8, c_int64, c_int32, c_double, c_float, Structure
from multiprocessing import Event, Process
import time
from functools import reduce
import sys

sys.path.append('.')
from shmqueue3 import ShmQueue, ShmPool, ShmElem

# define element structure
class Element(Structure):
    _pack_ = 4
    _fields_ = [('size', c_uint8),
                ('values', c_float*256)]

# proc to allocate, fill and enqueue elements until told to stop

def producer(out_queue: ShmQueue, stop_event: Event):
    
    done = False
    elem = ShmElem()
    pool = out_queue.pool
    count = 0

    try:
        while not done:
            e = pool.allocate(elem, timeout=1.0)
            if e is not None:
                e.contents.size = 16
                e.contents.values[:16] = [ i for i in range(16) ]

                if not stop_event.is_set():
                    if out_queue.put(e, timeout=1.0):
                        count += 1
                    else:
                        print('producer put timeout, queue size {}'.format(out_queue.available()))
            else:
                print('producer allocate timeout')

            done = stop_event.is_set()

        elem.free_if_valid()
    except Exception as ex:
        print('exception in producer {}'.format(ex))
    print('producer ending, count = {}'.format(count))



if __name__ == '__main__':

    # create a pool of elements in shared memory
    pool = ShmPool(elem_type=Element,size=1024)

    # create a queue that uses the pool
    queue = ShmQueue(pool, size=1024)

    # event to tell producer to stop
    stop_event = Event()

    print('starting producer')
    proc = Process(target=producer, args=(queue, stop_event))
    proc.start()


    start_time = time.monotonic()

    take_elem = ShmElem()
    take_done = False
    sum = 0.0
    count = 0

    while not take_done:
        e = queue.take(take_elem, timeout=1.0)
        if e is not None:
            sum = reduce(lambda a,b: a + b, e.contents.values[:e.contents.size], sum)
            count += 1
            e.free_if_valid()

        take_done = (time.monotonic() - start_time) >= 10.0

    stop_event.set()

    proc.join()

    print('sum = {}, count = {}'.format(sum, count))