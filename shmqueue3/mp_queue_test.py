from multiprocessing import Manager, Value, RawValue, RawArray, Process, Event, Semaphore, Condition, freeze_support
from ctypes import Structure, c_bool, c_uint8, c_int32, c_int64, c_float, c_double, addressof, sizeof, string_at, byref, pointer
import os
# os.environ['OPENBLAS_NUM_THREADS'] = '1'

import numpy as np
import cProfile
import time
from queue import Empty, Full
import sys

from multiprocessing import Queue as Queue

ARRAY_SIZE = 1024

ARRAY = c_int32 * ARRAY_SIZE

TS_SIZE = 16
TS_ARRAY = c_double * TS_SIZE

class Element(Structure):
    _fields_ = [('offset', c_int64),
                ('length', c_int32),
                ('next_time_idx', c_int32),
                ('times', TS_ARRAY),
                ('values', ARRAY)]

def clear_queue(q: Queue, name = None):
    count = 0
    while not q.empty():
        count += 1
        q.get(False)
    print(f'queue {name if name is not None else ""} cleared of {count} items')


def producer(out_queue: Queue, stop_event: Event, quit_count: Value):
    print('producer starting')

    done = False
    count = 0
    
    first_ts = time.time()
    last_ts = first_ts
    

    array_data = np.arange(ARRAY_SIZE, dtype=np.int32)

    try:
        while not done:
        
            e = Element()
            if e is not None:
                # fill the elem
                # init_ts(elem)
                # update_ts(elem)
                ary = np.frombuffer(e.values, np.int32, ARRAY_SIZE)
                ary[:] = array_data
                # update_ts(elem)
                # time.sleep(0.01)

                while not done:
                    try:
                        out_queue.put(e, timeout=1.0)
                        if count == 0:
                            first_ts = time.time()
                        else:
                            last_ts = time.time()
                        count += 1
                        break
                    except Full:
                        done = stop_event.is_set()
            else:
                print('producer allocate timeout')

            done = stop_event.is_set()

    except Exception as ex:
        print('exception in producer {}'.format(ex))
    print('producer ending, count = {}'.format(count))
    elapsed_sec = last_ts - first_ts
    time_per_item = elapsed_sec / count if count > 0 else 0.0
    print('producer time per item {:.3f} usec'.format(time_per_item * 1e6))

    clear_queue(out_queue, 'producer')

    with quit_count.get_lock():
        quit_count.value += 1

    return 0


def filter(in_queue: Queue, out_queue: Queue, stop_event: Event, quit_count: Value):
    print('filter starting')

    done = False
    count = 0
    first_ts = time.time()
    last_ts = first_ts


    try:
        while not done:
            
            e = in_queue.get(timeout=1.0)
            if e is not None:
                # update_ts(elem)
                # ary = np.frombuffer(elem.ptr.contents.values, np.int32, ARRAY_SIZE)
                # ary[:] += 1
                # update_ts(elem)

                while not done:
                    try:
                        out_queue.put(e, timeout=1.0)
                        if count == 0:
                            first_ts = time.time()
                        else:
                            last_ts = time.time()
                        count += 1
                        break
                    except Full:
                        done = stop_event.is_set()
            else:
                print('filter: take timeout')

            done = stop_event.is_set()
                

    except Exception as ex:
        print('exception in filter "{}"'.format(ex))
    print('filter ending, count = {}'.format(count))
    elapsed_sec = last_ts - first_ts
    time_per_item = elapsed_sec / count if count > 0 else 0.0
    print('filter time per item {:.3f} usec'.format(time_per_item * 1e6))

    clear_queue(out_queue, 'filter')

    with quit_count.get_lock():
        quit_count.value += 1


    return 0


def consumer(in_queue: Queue, stop_event: Event, quit_count: Value):
    print('consumer starting')

    done = False
    count = 0
    first_ts = time.time()
    last_ts = first_ts
    
    sum : np.int64 = 0

    try:
        while not done:
            try:
                e = in_queue.get(timeout=1.0)
                if e is not None:

                    if count == 0:
                        first_ts = time.time()
                    else:
                        last_ts = time.time()
                    count += 1
            except Empty:
                pass

            done = stop_event.is_set()

    except Exception as ex:
        print('exception in consumer {}'.format(ex))
    print('consumer ending, count = {}'.format(count))
    elapsed_sec = last_ts - first_ts
    time_per_item = elapsed_sec / count if count > 0 else 0.0
    print('consumer time per item {:.3f} usec'.format(time_per_item * 1e6))
    print('consumer final sum {}'.format(sum))

    with quit_count.get_lock():
        quit_count.value += 1


    return 0


if __name__ == '__main__':
    freeze_support()
    pool_size = 1024


    queue_size = 32 #(pool_size-20) // 2
    qa = Manager().Queue(maxsize=queue_size)
    qb = Manager().Queue(maxsize=queue_size)
    qc = Manager().Queue(maxsize=queue_size)

    stop_event = Event()

    quit_count = Value(c_int32, 0)

    procs = []
    # procs.append(Process(target=banger, args=(qa, stop_event)))
    procs.append(Process(name="producer", group=None, target=producer, args=(qa, stop_event, quit_count)))
    procs.append(Process(name="filter1", group=None, target=filter, args=(qa, qb, stop_event, quit_count)))
    procs.append(Process(name="filter2", group=None, target=filter, args=(qb, qc, stop_event, quit_count)))
    #procs.append(Process(target=filter, args=(qa, qc, stop_event)))
    #procs.append(Process(target=filter, args=(qa, qc, stop_event)))
   
    procs.append(Process(name="consumer", group=None, target=consumer, args=(qc, stop_event, quit_count)))
    # procs.append(Process(target=consumer, args=(qa, stop_event)))

    print('starting procs')
    for proc in procs:
        proc.start()

    

    now = time.time()
    deadline = now + 10.0

    while time.time() < deadline:
        time.sleep(1.0)
        # print('pool free {}, qa {} qb {} qc {}'.format(pool.available(), qa.available(), qb.available(), qc.available()))

    print('setting stop_event')
    stop_event.set()

    time.sleep(2)
    # for q in [ qa, qb, qc ]:
    #     q.close()

    print('waiting for procs')
    any_running = True
    while any_running:
        any_running = False
        alive_count = 0
        for proc in procs:
            alive_count = alive_count + (1 if proc.is_alive() else 0)
        with quit_count.get_lock():
            qc = quit_count.value

        print(f'alive_count {alive_count} quit_count {qc}')
        for proc in procs:
            if proc.is_alive():
                proc.join(1.0)
                if proc.is_alive():
                    print(f'{time.time()}: proc {proc.name} is still alive')
                    any_running = True
        

    # print('procs joined. final pool available {}, qa {} qb {} qc {}. exiting'.format(pool.available(), qa.available(),
                                                                                    #  qb.available(), qc.available()))

    print('all procs joined')
