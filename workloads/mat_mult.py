import ray
import numpy as np
import sys
import argparse

import ray.array.remote as ra
import ray.array.distributed as da


parser = argparse.ArgumentParser(description="Run example of distributed matrix multipication.")
parser.add_argument("--size", default=100, type=str, help="The size of the matrix")


def mat_mul(args):
    ray.init(start_ray_local=True, num_workers=1)

    #size=args[1]
    size = args.size
    print size
    a=da.zeros.remote([size, size], "float")
    b=da.zeros.remote([size, size], "float")
    c=da.dot.remote(a, b)
    c_get = ray.get(c)


if __name__ == '__main__':
    args = parser.parse_args()
    #mat_mul(sys.argv)
    mat_mul(args)
