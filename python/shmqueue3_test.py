from multiprocessing import Value, RawValue, RawArray, Process, Event, Semaphore, Condition
from ctypes import Structure, c_bool, c_uint8, c_int32, c_int64, c_float, c_double, addressof, sizeof, string_at, byref, pointer
import os
# os.environ['OPENBLAS_NUM_THREADS'] = '1'

import numpy as np
import cProfile
import time

from shmqueue3 import ShmPool, ShmElem, ShmQueue

def set_priority(prio=0):
    pass
    # try:
    #     import psutil

    #     process = psutil.Process()
    #     process.nice(prio)
    # except Exception as e:
    #     print('exception while trying to set priority: {}'.format(e))

    # print('final nice value is {}'.format(process.nice()))

def set_realtime():
    pass
    # os.sched_setscheduler(0, os.SCHED_RR)
    # pass
    # try:
    #     import psutil

    #     process = psutil.Process()
    #     process.nice(-20)
    # except Exception as e:
    #     print('exception while trying to set priority: {}'.format(e))

    # print('final nice value is {}'.format(process.nice()))

def display_nice():
    try:
        import psutil

        process = psutil.Process()
        print('nice value is {}'.format(process.nice()))

    except Exception as e:
        print('exception while trying to get priority: {}'.format(e))



ARRAY_SIZE = 8192

ARRAY = c_int32 * ARRAY_SIZE

TS_SIZE = 16
TS_ARRAY = c_double * TS_SIZE

class Element(Structure):
    _fields_ = [('offset', c_int64),
                ('length', c_int32),
                ('next_time_idx', c_int32),
                ('times', TS_ARRAY),
                ('values', ARRAY)]
    
def init_ts(elem):
    elem.ptr.contents.next_time_idx = 0

def update_ts(elem):
        if elem.ptr.contents.next_time_idx < TS_SIZE:
            elem.ptr.contents.times[elem.ptr.contents.next_time_idx] = time.time_ns() * 0.001
            elem.ptr.contents.next_time_idx += 1

def get_ts(elem):
    rv = list([ elem.ptr.contents.times[i] for i in range(elem.ptr.contents.next_time_idx)])
    return rv


def producer(out_queue: ShmQueue, stop_event: Event):
    print('producer starting')
    # set_priority(10)
    #set_realtime()
    # display_nice()
    done = False
    count = 0
    pool = out_queue.pool
    first_ts = time.time()
    last_ts = first_ts
    elem = ShmElem()

    array_data = np.arange(ARRAY_SIZE, dtype=np.int32)

    try:
        while not done:
            e = pool.allocate(elem, timeout=1.0)
            if e is not None:
                # fill the elem
                # init_ts(elem)
                # update_ts(elem)
                # ary = np.frombuffer(elem.ptr.contents.values, np.int32, ARRAY_SIZE)
                # ary[:] = array_data
                # update_ts(elem)

                while not stop_event.is_set():
                    if out_queue.put(e, timeout=1.0):
                        if count == 0:
                            first_ts = time.time()
                        else:
                            last_ts = time.time()
                        count += 1
                        break
                    else:
                        print('producer put timeout, queue size {}'.format(out_queue.available()))
            else:
                print('producer allocate timeout')

            done = stop_event.is_set()

        elem.free_if_valid()
    except Exception as ex:
        print('exception in producer {}'.format(ex))
    print('producer ending, count = {}'.format(count))
    elapsed_sec = last_ts - first_ts
    time_per_item = elapsed_sec / count
    print('producer time per item {:.3f} usec'.format(time_per_item * 1e6))

def filter(in_queue: ShmQueue, out_queue: ShmQueue, stop_event: Event):
    print('filter starting')
    # set_priority(11)
    #set_realtime()
    # display_nice()
    done = False
    count = 0
    first_ts = time.time()
    last_ts = first_ts

    elem = ShmElem()

    try:
        while not done:
            e = in_queue.take(elem, timeout=1.0)
            if e is not None:
                # update_ts(elem)
                # ary = np.frombuffer(elem.ptr.contents.values, np.int32, ARRAY_SIZE)
                # ary[:] += 1
                # update_ts(elem)

                while not stop_event.is_set():
                    if out_queue.put(e, timeout=1.0):
                        if count == 0:
                            first_ts = time.time()
                        else:
                            last_ts = time.time()
                        count += 1
                        break
            else:
                print('filter: take timeout, in queue size {}'.format(in_queue.available()))
            done = stop_event.is_set()

        elem.free_if_valid()
    except Exception as ex:
        print('exception in filter {}'.format(ex))
    print('filter ending, count = {}'.format(count))
    elapsed_sec = last_ts - first_ts
    time_per_item = elapsed_sec / count
    print('filter time per item {:.3f} usec'.format(time_per_item * 1e6))

def consumer(in_queue: ShmQueue, stop_event: Event):
    print('consumer starting')
    #set_realtime()
    # set_priority(12)
    # display_nice()
    done = False
    count = 0
    first_ts = time.time()
    last_ts = first_ts
    
    sum : np.int64 = 0

    elem = ShmElem()
    try:
        while not done:
            e = in_queue.take(elem, timeout=1.0)
            if e is not None:
                # update_ts(elem)
                # ary = np.frombuffer(elem.ptr.contents.values, np.int32, ARRAY_SIZE)
                # sum += np.int64(np.sum(ary))
                # update_ts(elem) 

                if count == 0:
                    first_ts = time.time()
                else:
                    last_ts = time.time()
                count += 1

                # if count > 0 and (count % 10000) == 0:
                #     tsa = np.array(get_ts(elem))
                #     dtsa = np.diff(tsa) #[1::2]
                #     print('{}: ts count {} dtsa {}'.format(count, len(tsa), ','.join(['{:.3f}'.format(dt) for dt in dtsa])))

                e.free_if_valid()

            done = stop_event.is_set()

        elem.free_if_valid()
    except Exception as ex:
        print('exception in consumer {}'.format(ex))
    print('consumer ending, count = {}'.format(count))
    elapsed_sec = last_ts - first_ts
    time_per_item = elapsed_sec / count
    print('consumer time per item {:.3f} usec'.format(time_per_item * 1e6))
    print('final sum {}'.format(sum))


def banger(queue: ShmQueue, stop_event: Event):
    print('producer starting')
    # set_priority(10)
    #set_realtime()
    # display_nice()
    done = False
    count = 0
    pool = queue.pool
    first_ts = time.time()
    last_ts = first_ts
    elem = ShmElem()

    fill_max = 16
    fill_count = 0

    try:
        while not done:
            while not done and fill_count < fill_max:
                e = pool.allocate(elem, timeout=1.0)
                if e is not None:

                    while not stop_event.is_set():
                        if queue.put(e, timeout=1.0):
                            fill_count += 1
                            count += 1
                            break
                        else:
                            print('producer put timeout, queue size {}'.format(queue.available()))
                else:
                    print('producer allocate timeout')

                done = stop_event.is_set()
            while not done and fill_count > 0:
                e = queue.take(elem, timeout=1.0)
                if e is not None:
                # update_ts(elem)
                    count += 1

                    fill_count -= 1
                    e.free()

                done = stop_event.is_set()

        elem.free_if_valid()
    except Exception as ex:
        print('exception in producer {}'.format(ex))
    print('producer ending, count = {}'.format(count))
    elapsed_sec = last_ts - first_ts
    time_per_item = elapsed_sec / count
    print('producer time per item {:.3f} usec'.format(time_per_item * 1e6))

if __name__ == '__main__':
    set_realtime()
    pool_size = 1024

    pool = ShmPool(elem_type=Element,size=pool_size)

    state = pool.array.__dict__.copy()

    print('state = {}'.format(state))

    queue_size = 32 #(pool_size-20) // 2
    qa = ShmQueue(pool, size=queue_size)
    qb = ShmQueue(pool, size=queue_size)
    qc = ShmQueue(pool, size=queue_size)

    stop_event = Event()

    procs = []
    # procs.append(Process(target=banger, args=(qa, stop_event)))
    procs.append(Process(target=producer, args=(qa, stop_event)))
    procs.append(Process(target=filter, args=(qa, qb, stop_event)))
    procs.append(Process(target=filter, args=(qb, qc, stop_event)))
    #procs.append(Process(target=filter, args=(qa, qc, stop_event)))
    #procs.append(Process(target=filter, args=(qa, qc, stop_event)))
   
    procs.append(Process(target=consumer, args=(qc, stop_event)))
    # procs.append(Process(target=consumer, args=(qa, stop_event)))

    print('starting procs')
    for proc in procs:
        proc.start()

    now = time.time()
    deadline = now + 4.0

    while time.time() < deadline:
        time.sleep(1.0)
        # print('pool free {}, qa {} qb {} qc {}'.format(pool.available(), qa.available(), qb.available(), qc.available()))

    print('setting stop_event')
    stop_event.set()

    for proc in procs:
        proc.join()

    # print('procs joined. final pool available {}, qa {} qb {} qc {}. exiting'.format(pool.available(), qa.available(),
                                                                                    #  qb.available(), qc.available()))

    del qa, qb, qc


    print('after del: final pool available {}. exiting'.format(pool.available()))
