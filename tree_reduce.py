import time
import argparse
import ray
import numpy as np


parser = argparse.ArgumentParser(description="Run tree reduce")
parser.add_argument("--workers", default=2, type=int, help="The number of Ray workers")

data_size = 10 ** 5
num_data = 4000

@ray.remote
def load_data():
  return np.random.normal(size=data_size)

@ray.remote
def aggregate(x, y):
  return x + y


def tree_reduce(num_workers):
    ray.init(num_workers=num_workers, start_ray_local=True)

    start = time.time()
    data = [load_data.remote() for _ in range(num_data)]
    while len(data) > 1:
      data.append(aggregate.remote(data.pop(0), data.pop(0)))

    #while len(data) > 1:
    #  data = [aggregate.remote(data.pop(0), data.pop(0))] + data

    ray.get(data[0])
    end = time.time()
    print "{}".format(end - start)

if __name__ == '__main__':
    args = parser.parse_args()
    num_workers = args.workers
    tree_reduce(num_workers)
