import ray
import rnn
import time
import ray.array.remote as ra
import sys
import getopt
import numpy as np

def rnn_ray(argv):

  #num_workers = 1
  scale = 10
  num_steps = 10
  try:
    opts, args = getopt.getopt(argv, "hw:s:n:", ["workers=","scale=","num_steps="])
  except getopt.GetoptError:
    print 'rnn_ray_loop -w <num_workers> -s <scale> -n <num_steps>'
    sys.exit(2)
  for opt, arg in opts:
    if opt == '-h':
      print 'rnn_ray_loop -w <num_workers>'
      sys.exit()
    elif opt in ("-w", "--workers"):
      num_of_workers = int(arg)
    elif opt in ("-s", "--scale"):
      scale = int(arg)
    elif opt in ("-n", "--num_steps"):
      print "num steps is {}".format(arg)
      num_steps = int(arg)
 
  ray.init(start_ray_local=True, num_workers=num_of_workers)
  start_time = time.time()
  scale = scale*5
  batch_size = scale - 1

  xdim = scale * 10
  h1dim = (scale + 1) * 10
  h2dim = (scale + 2) * 10
  h3dim = (scale + 3) * 10
  h4dim = (scale + 4) * 10
  h5dim = (scale + 5) * 10
  ydim = (2 * scale + 6) * 10

  ray.reusables.net_vars = ray.Reusable(lambda : rnn.net_initialization(scale,num_steps,batch_size,xdim,h1dim,h2dim,h3dim,h4dim,h5dim,ydim), rnn.net_reinitialization)
  res = ray_rnn_int.remote(num_of_workers, scale, num_steps, batch_size, xdim, h1dim, h2dim, h3dim, h4dim, h5dim, ydim)
  ray.get(res)


@ray.remote
def ray_rnn_int(num_of_workers, scale, num_steps, batch_size, xdim, h1dim, h2dim, h3dim, h4dim, h5dim, ydim):
#  for _ in range(1):
    start_time = time.time()
#    scale = scale*5
#    batch_size = scale - 1

#    xdim = scale * 10
#    h1dim = (scale + 1) * 10
#    h2dim = (scale + 2) * 10
#    h3dim = (scale + 3) * 10
#    h4dim = (scale + 4) * 10
#    h5dim = (scale + 5) * 10
#    ydim = (2 * scale + 6) * 10

#    ray.reusables.net_vars = ray.Reusable(lambda : rnn.net_initialization(scale,num_steps,batch_size,xdim,h1dim,h2dim,h3dim,h4dim,h5dim,ydim), rnn.net_reinitialization)

    h1 = ra.zeros.remote([batch_size, h1dim])
    h2 = ra.zeros.remote([batch_size, h2dim])
    h3 = ra.zeros.remote([batch_size, h3dim])
    h4 = ra.zeros.remote([batch_size, h4dim])
    h5 = ra.zeros.remote([batch_size, h5dim])
    inputs = [ra.random.normal.remote([batch_size, xdim]) for _ in range(num_steps)]

    # Run distributed RNN
    elapsed_time_6_layers = []

    start_time = time.time()
    outputs = []
    for t in range(num_steps):
        h1 = rnn.first_layer.remote(inputs[t], h1)
        h2 = rnn.second_layer.remote(h1, h2)
        h3 = rnn.third_layer.remote(h2, h3)
        h4 = rnn.fourth_layer.remote(h3, h4)
        h5 = rnn.fifth_layer.remote(h4, h5)
        outputs.append(rnn.sixth_layer.remote(h5))
    for t in range(num_steps):
        ray.get(outputs[t])
    end_time = time.time()
    elapsed_time_6_layers.append(end_time - start_time)


    elapsed_time_6_layers = np.sort(elapsed_time_6_layers)

    elapsed_time_6_layers_average = sum(elapsed_time_6_layers) 

    #ray.visualize_computation_graph(view=True) 

    print ""
    print "Number of workers = {}.".format(num_of_workers)
    print "Scale = {}.".format(scale)


    #print "Time required for 6 layer RNN:"
    #print "    Average: {}".format(elapsed_time_6_layers_average)
    #print "    90th precentile: {}".format(elapsed_time_6_layers[8])   
    #print "    Worst: {}".format(elapsed_time_6_layers[9]) 
    end_time = time.time()
    print "Distributed RNN, 6 layer, elapsed_time = {} seconds.".format(end_time - start_time)
    return end_time-start_time

    # print "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}".format(num_of_workers,scale,num_steps, elapsed_time_1_layers_average, elapsed_time_1_layers[8], elapsed_time_1_layers[9],
                                                                                                        #elapsed_time_2_layers_average, elapsed_time_2_layers[8], elapsed_time_2_layers[9],
                                                                                                        #elapsed_time_3_layers_average, elapsed_time_3_layers[8], elapsed_time_3_layers[9],
                                                                                                        #elapsed_time_4_layers_average, elapsed_time_4_layers[8], elapsed_time_4_layers[9],
                                                                                                        #elapsed_time_5_layers_average, elapsed_time_5_layers[8], elapsed_time_5_layers[9],
                                                                                                        #elapsed_time_6_layers_average, elapsed_time_6_layers[8], elapsed_time_6_layers[9]) 



if __name__ == "__main__":
  rnn_ray(sys.argv[1:])
