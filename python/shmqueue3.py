from multiprocessing import Value, RawValue, RawArray, Process, Event, Semaphore, Condition
from ctypes import Structure, c_bool, c_uint8, c_int32, c_int64, c_float, addressof, sizeof, string_at, byref, pointer
import cProfile
import os
import time

# USE_NATIVE_WAIT = True

# def wait_for(cond, pred, timeout=None, resolution=0.01): 
#     if USE_NATIVE_WAIT:
#         rv = cond.wait_for(pred, timeout=timeout)
#         return rv
#     else:
#         if timeout is not None:
#             tstart = time.time()
#             tuntil = tstart + timeout
#             while time.time() < tuntil:
#                 if pred():
#                     return True
#                 else:
#                     cond.wait(resolution)

#             return False
#         else:
#             while True:
#                 if pred():
#                     return True
#                 else:
#                     cond.wait(resolution)


class PoolState(Structure):
    _fields_ = [('stack_index', c_int32)]


class ShmPool:

    def __init__(self, elem_type=None, elem_size=None, size=16):
        #print('pool init pid {}'.format(os.getpid()))
        if elem_type is not None:
            self.elem_size = sizeof(elem_type)
            self.elem_type = elem_type
        elif elem_size is not None:
            self.elem_size = elem_size
            self.elem_type = c_uint8 * elem_size
        else:
            raise Exception('elem_type or elem_size may not both be None')

        self.size = size

        self.array = RawArray(self.elem_type, self.size)
        self.free_queue = RawArray(c_int32, self.size)
        self.free_queue[:] = [ i for i in range(self.size) ]
        self.state = RawValue(PoolState, self.size)

        self.not_empty = Condition()

    #def __del__(self):
        #print('pool delete {}, pid {}'.format(self, os.getpid()))

    def pointer_for_index(self, idx):
        return pointer(self.array[idx])
    
    def pred_not_empty(self):
        return self.state.stack_index > 0

    def allocate(self, elem=None, timeout=None):
        alloc_idx = -1

        with self.not_empty:
            # if self.state.stack_index > 0 or wait_for(self.not_empty,
            #                                         #   lambda: self.state.stack_index > 0,
            #                                           self.pred_not_empty,
            #                                           timeout=timeout):
            if self.state.stack_index > 0 or self.not_empty.wait_for(self.pred_not_empty, timeout=timeout):
                self.state.stack_index -= 1
                alloc_idx = self.free_queue[self.state.stack_index]

        if alloc_idx >= 0:
            #print('pool alloc {}'.format(alloc_idx))
            if elem is None:
                elem = ShmElem(self, alloc_idx, pointer(self.array[alloc_idx]))
            else:
                elem.init(self, alloc_idx, pointer(self.array[alloc_idx]))
            return elem
        else:
            return None

    def dump_avail(self):
        with self.not_empty:
            print('pool: {} free elems'.format(self.state.stack_index))
            for i in range(self.state.stack_index):
                print('{}: {}'.format(i, self.free_queue[i]))

    def free(self, idx):
        #print('pool free idx {}'.format(idx))
        if idx < 0:
            raise Exception('pool free: invalid index {}'.format(idx))
        with self.not_empty:
            if self.state.stack_index == self.size:
                #self.dump_avail()
                raise Exception('trying to free {} when more elements then should exist!'.format(idx))

            #print('Pool free stack_index {}, item idx {}'.format(self.state.stack_index, idx))
            self.free_queue[self.state.stack_index] = idx
            self.state.stack_index += 1
            # self.not_empty.notify()
            if self.state.stack_index == 1:
                self.not_empty.notify()

    def available(self):
        with self.not_empty:
            return self.state.stack_index


class ShmElem:

    def __init__(self, pool=None, qidx=-1, ptr=None):
        self.pool = pool
        self.qidx = qidx
        self.ptr = ptr
        self.valid = ptr is not None

    def init(self, pool, qidx, ptr):
        self.pool = pool
        self.qidx = qidx
        self.ptr = ptr
        self.valid = ptr is not None

    def contents(self):
        if self.valid:
            return self.ptr.contents
        else:
            raise Exception('invalid elem')

    def free(self):
        if self.valid:
            self.pool.free(self.qidx)
            self.valid = False
        else:
            raise Exception('can''t free invalid elem')

    def free_if_valid(self):
        if self.valid:
            self.pool.free(self.qidx)
            self.valid = False

    def is_valid(self):
        return self.valid

    def __del__(self):
        self.free_if_valid()


class QueueState(Structure):
    _fields_ = [('head', c_int32),
                ('size', c_int32)]


class ShmQueue:

    def __init__(self, pool, size=None):
        self.pool = pool
        self.size = size if size is not None else pool.size

        self.queue = RawArray(c_int32, self.size)
        self.state = Value(QueueState, 0, 0)

        self.not_empty = Condition(lock=self.state.get_lock())
        self.not_full = Condition(lock=self.state.get_lock())

    def __del__(self):
       #self.clear()
        pass

    def pred_not_empty(self):
        return self.state.size > 0
    
    def pred_not_full(self):
        return self.state.size < self.size
    
    def clear(self):
        with self.state.get_lock():
            if self.state.size > 0 and self.pool is not None:
                for i in range(self.state.size):
                    idx = (self.state.head + i) % self.size
                    self.pool.free(self.queue[idx])
                self.state.size = 0

            self.not_full.notify()

    
    def put_or_replace_oldest(self, elem):
        """
        Enqueue an item, automatically dequeing the head if the queue is currently full. 
        This call always succeeds immediately.

        Returns True if a dequeue was required.
        """
        if elem is None or not elem.valid:
            raise Exception('put of invalid elem')

        replaced = False
        with self.not_empty:
            if self.state.size < self.size:
                tail = (self.state.head + self.state.size) % self.size
                self.queue[tail] = elem.qidx
                self.state.size += 1

                elem.valid = False
                # self.not_empty.notify()
                if self.state.size == 1:
                    self.not_empty.notify()
            else: 
                # queue is full, dequeue the next item to make room
                qidx = self.queue[self.state.head]
                self.state.head = (self.state.head + 1) % self.size
                # self.state.size -= 1
                self.pool.free(qidx) # free the item back to the pool
                # then enqueue 
                tail = (self.state.head + self.state.size - 1) % self.size
                self.queue[tail] = elem.qidx


                elem.valid = False
                replaced = True

        return replaced

    def put(self, elem, blocking = True, timeout=None):
        if elem is None or not elem.valid:
            raise Exception('put of invalid elem')

        success = False
        with self.not_full:
            # if self.state.size < self.size or (blocking and wait_for(self.not_full,
            #                                         #    lambda: self.state.size < self.size,
            #                                         self.pred_not_full,
            #                                         timeout=timeout)):
            if self.state.size < self.size or (blocking and self.not_full.wait_for(self.pred_not_full, timeout=timeout)):
                tail = (self.state.head + self.state.size) % self.size
                self.queue[tail] = elem.qidx
                self.state.size += 1 # !viztracer: log_var("self.state.size", self.state.size)

                elem.valid = False
                success = True
                # self.not_empty.notify()
                if self.state.size == 1:
                    self.not_empty.notify()

        if success:
            return True
        else:
            return False

    def take(self, elem = None, blocking=True, timeout=None):
        if elem.valid:
            raise Exception('take on valid element')

        qidx = -1

        with self.not_empty: # 
            # if self.state.size > 0 or (blocking and wait_for(self.not_empty,
            #                                 #    lambda: self.state.size > 0,
            #                                    self.pred_not_empty,
            #                                    timeout=timeout)):
            if self.state.size > 0 or (blocking and self.not_empty.wait_for(self.pred_not_empty, timeout = timeout)):
                qidx = self.queue[self.state.head]
                self.state.head = (self.state.head + 1) % self.size
                if self.state.size == self.size:
                    self.state.size -= 1 # !viztracer: log_var("self.state.size", self.state.size)
                    self.not_full.notify()
                else:
                    self.state.size -= 1 # !viztracer: log_var("self.state.size", self.state.size)
                
                # self.not_full.notify()
                # if self.state.size == self.size - 1:
                #     self.not_full.notify()

        if qidx != -1:
            if elem is None:
                elem = ShmElem(self.pool, qidx, self.pool.pointer_for_index(qidx))
            else:
                elem.init(self.pool, qidx, self.pool.pointer_for_index(qidx))
            return elem
        else:
            return None  # timeout

    def available(self):
        with self.state.get_lock():
            return self.state.size
