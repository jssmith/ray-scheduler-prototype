import time
import sys
import numpy as np
import argparse

import ray
import ray.array.remote as ra
import ray.array.distributed as da


parser = argparse.ArgumentParser(description="Run example of distributed matrix multipication.")
parser.add_argument("--size", default=100, type=int, help="The size of the matrix")
parser.add_argument("--workers", default=2, type=int, help="The number of Ray workers")
parser.add_argument("--block-size", default=10, type=int, help="The matrix block size")


def mat_mul(size, num_workers, block_size):
    # Start Ray.
    ray.init(start_ray_local=True, num_workers=num_workers)
    ray.register_class(da.DistArray)
    # Allocate two size x size arrays of all one's, and multiply them. Block on
    # the result.
    print "Matrix multiply with size {}, block size {}".format(size,
            block_size)
    start = time.time()
    a = da.ones.remote([size, size], block_size=block_size)
    b = da.ones.remote([size, size], block_size=block_size)
    c = da.dot.remote(a, b)
    c = da.assemble.remote(c)
    print c.id
    c_get = ray.get(c)
    end = time.time()
    print c_get
    print "Took {} seconds".format(end - start)
    return ray

if __name__ == '__main__':
    args = parser.parse_args()
    size = args.size
    num_workers = args.workers
    block_size = args.block_size
    mat_mul(size, num_workers, block_size)
