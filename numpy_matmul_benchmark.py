import time
import sys
import numpy as np


class Timing():
    def __init__(self, name):
        self._name = name
        self._last_start_time = 0
        self.ct = 0
        self.cumulative_time = 0
        self.min_time = sys.float_info.max

    def start(self):
        self._last_start_time = time.time()

    def finish(self):
        end_time = time.time()
        elapsed_time = end_time - self._last_start_time
        self.cumulative_time += elapsed_time
        self.ct += 1
        if elapsed_time < self.min_time:
            self.min_time = elapsed_time

    def __str__(self):
        return '{}: avg {} min {}'.format(self._name,
            self.cumulative_time/self.ct, self.min_time)

def benchmark(dim, iterations):
    t = Timing('initialize ones')
    for _ in range(iterations):
        t.start()
        a = np.ones((dim,dim))
        t.finish()
    print t

    t = Timing('initialize random')
    for _ in range(iterations):
        t.start()
        b = np.random.rand(dim,dim)
        t.finish()
    print t

    t = Timing('add')
    for _ in range(iterations):
        t.start()
        c = np.add(a, b)
        t.finish()
    print t

    t = Timing('mul')
    for _ in range(iterations):
        t.start()
        d = np.dot(a, c)
        t.finish()
    print t


if __name__ == '__main__':
    benchmark(2500, 10)
