import rnn_ray_loop


for w in range(10): 
  for s in range(10):
    print "running ray_rnn with {} workers and scale {}, giving a load of {}".format(w+1, s+1, (w+1)/(s+1))
    rnn_ray_loop.rnn_ray(["-w","{}".format(3+1),"-s","{}".format(s+1)])
