import ray
import time
import ray.array.remote as ra
import sys


@ray.remote
def sleep_function():
	sleep(100)
	return 0
  
@ray.remote
def trivial_function(a):
	for i in range(20000000):
		pass
	return a+1


def sleep_test():
	ray.init(start_ray_local=True, num_workers=3)
	elapsed_times = []
	start_time = time.time()
	#ray.init(start_ray_local=True, num_workers=3)
	tot_time_start = time.time()
	print "init"
	print time.time()	
	end_time = time.time()
	elapsed_times.append(end_time - start_time)
	start_time = time.time()
	v1 = trivial_function.remote(1)
	end_time = time.time()
	elapsed_times.append(end_time - start_time)
	start_time = time.time()
	v2 = trivial_function.remote(2)
	end_time = time.time()
	elapsed_times.append(end_time - start_time)
	start_time = time.time()
	#sleep_function.remote()
	end_time = time.time()
	elapsed_times.append(end_time - start_time)
	ray.get(v1)
	ray.get(v2)
	print "last get"
	print time.time()
	tot_time_finish = time.time()
	total_time =  tot_time_finish - tot_time_start
	print "Time passed init {}, trivial_function first {}, trivial function second {}, sleep {}, total time {}".format(elapsed_times[0], elapsed_times[1], elapsed_times[2], elapsed_times[3], total_time)
	return total_time



if __name__ == "__main__":
	sleep_test()
