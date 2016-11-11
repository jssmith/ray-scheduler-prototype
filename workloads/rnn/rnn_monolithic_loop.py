import rnn
import tensorflow as tf
import numpy as np
import time
import sys
import getopt


def rnn_ray_monolithic(argv):

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


  # Run monolithic RNN

  scale = scale*5
  batch_size = scale - 1

  xdim = scale * 10
  h1dim = (scale + 1) * 10
  h2dim = (scale + 2) * 10
  h3dim = (scale + 3) * 10
  h4dim = (scale + 4) * 10
  h5dim = (scale + 5) * 10
  ydim = (2 * scale + 6) * 10

  W1_p = tf.Variable(tf.truncated_normal([xdim, h1dim]))
  W1_h = tf.Variable(tf.truncated_normal([h1dim, h1dim]))

  W2_p = tf.Variable(tf.truncated_normal([h1dim, h2dim]))
  W2_h = tf.Variable(tf.truncated_normal([h2dim, h2dim]))

  W3_p = tf.Variable(tf.truncated_normal([h2dim, h3dim]))
  W3_h = tf.Variable(tf.truncated_normal([h3dim, h3dim]))

  W4_p = tf.Variable(tf.truncated_normal([h3dim, h4dim]))
  W4_h = tf.Variable(tf.truncated_normal([h4dim, h4dim]))

  W5_p = tf.Variable(tf.truncated_normal([h4dim, h5dim]))
  W5_h = tf.Variable(tf.truncated_normal([h5dim, h5dim]))

  W6_p = tf.Variable(tf.truncated_normal([h5dim, ydim]))

  h1_mono = tf.Variable(tf.zeros([batch_size, h1dim]))
  h2_mono = tf.Variable(tf.zeros([batch_size, h2dim]))
  h3_mono = tf.Variable(tf.zeros([batch_size, h3dim]))
  h4_mono = tf.Variable(tf.zeros([batch_size, h4dim]))
  h5_mono = tf.Variable(tf.zeros([batch_size, h5dim]))
  inputs_monolithic = [tf.placeholder(tf.float32, [batch_size, xdim]) for _ in range(num_steps)]
  y_monolithic = []


  elapsed_time_1_layers = []
  elapsed_time_2_layers = []
  elapsed_time_3_layers = []
  elapsed_time_4_layers = []
  elapsed_time_5_layers = []
  elapsed_time_6_layers = []
  for _ in range(10):
    for t in range(num_steps):
      h1_mono = tf.matmul(inputs_monolithic[t], W1_p) + tf.matmul(h1_mono, W1_h)
      h2_mono = tf.matmul(h1_mono, W2_p) + tf.matmul(h2_mono, W2_h)
      h3_mono = tf.matmul(h2_mono, W3_p) + tf.matmul(h3_mono, W3_h)
      h4_mono = tf.matmul(h3_mono, W4_p) + tf.matmul(h4_mono, W4_h)
      h5_mono = tf.matmul(h4_mono, W5_p) + tf.matmul(h5_mono, W5_h)
      y_monolithic.append(tf.matmul(h5_mono, W6_p))

    init = tf.initialize_all_variables()
    sess = tf.Session()
    sess.run(init)

    inputs = [np.random.normal(size=[batch_size, xdim]) for _ in range(num_steps)]
    feed_dict = dict(zip(inputs_monolithic, inputs))

    start_time = time.time()
    outputs = sess.run(h1_mono, feed_dict=feed_dict)
    end_time = time.time()
    elapsed_time_1_layers.append(end_time - start_time)
    print "Monolithic RNN, 1 layer, elapsed_time = {} seconds.".format(end_time - start_time)

    start_time = time.time()
    outputs = sess.run(h2_mono, feed_dict=feed_dict)
    end_time = time.time()
    elapsed_time_2_layers.append(end_time - start_time)
    print "Monolithic RNN, 2 layer, elapsed_time = {} seconds.".format(end_time - start_time)

    start_time = time.time()
    outputs = sess.run(h3_mono, feed_dict=feed_dict)
    end_time = time.time()
    elapsed_time_3_layers.append(end_time - start_time)
    print "Monolithic RNN, 3 layer, elapsed_time = {} seconds.".format(end_time - start_time)


    start_time = time.time()
    outputs = sess.run(h4_mono, feed_dict=feed_dict)
    end_time = time.time()
    elapsed_time_4_layers.append(end_time - start_time)
    print "Monolithic RNN, 4 layer, elapsed_time = {} seconds.".format(end_time - start_time)

    start_time = time.time()
    outputs = sess.run(h5_mono, feed_dict=feed_dict)
    end_time = time.time()
    elapsed_time_5_layers.append(end_time - start_time)
    print "Monolithic RNN, 5 layer, elapsed_time = {} seconds.".format(end_time - start_time)

    start_time = time.time()
    outputs = sess.run(y_monolithic, feed_dict=feed_dict)
    end_time = time.time()
    elapsed_time_6_layers.append(end_time - start_time)
    print "Monolithic RNN, 6 layer, elapsed_time = {} seconds.".format(end_time - start_time)

  elapsed_time_1_layers = np.sort(elapsed_time_1_layers)
  elapsed_time_2_layers = np.sort(elapsed_time_2_layers)
  elapsed_time_3_layers = np.sort(elapsed_time_3_layers)
  elapsed_time_4_layers = np.sort(elapsed_time_4_layers)
  elapsed_time_5_layers = np.sort(elapsed_time_5_layers)
  elapsed_time_6_layers = np.sort(elapsed_time_6_layers)

  elapsed_time_1_layers_average = sum(elapsed_time_1_layers) / 10
  elapsed_time_2_layers_average = sum(elapsed_time_2_layers) / 10
  elapsed_time_3_layers_average = sum(elapsed_time_3_layers) / 10
  elapsed_time_4_layers_average = sum(elapsed_time_4_layers) / 10
  elapsed_time_5_layers_average = sum(elapsed_time_5_layers) / 10
  elapsed_time_6_layers_average = sum(elapsed_time_6_layers) / 10


  print ""
  print "Monolitic: Number of workers is irrelevant, but still generated for consistency with Ray result formats = {}.".format(num_of_workers)
  print "Scale = {}.".format(scale)
  print "Load measure (scale/num_workers) = {}.".format(scale/num_of_workers)
  print "Time required for 1 layer RNN:"
  print "    Average: {}".format(elapsed_time_1_layers_average)
  print "    90th precentile: {}".format(elapsed_time_1_layers[8])   
  print "    Worst: {}".format(elapsed_time_1_layers[9]) 

  print "Time required for 2 layer RNN:"
  print "    Average: {}".format(elapsed_time_2_layers_average)
  print "    90th precentile: {}".format(elapsed_time_2_layers[8])   
  print "    Worst: {}".format(elapsed_time_2_layers[9]) 

  print "Time required for 3 layer RNN:"
  print "    Average: {}".format(elapsed_time_3_layers_average)
  print "    90th precentile: {}".format(elapsed_time_3_layers[8])   
  print "    Worst: {}".format(elapsed_time_3_layers[9]) 

  print "Time required for 4 layer RNN:"
  print "    Average: {}".format(elapsed_time_4_layers_average)
  print "    90th precentile: {}".format(elapsed_time_4_layers[8])   
  print "    Worst: {}".format(elapsed_time_4_layers[9]) 

  print "Time required for 5 layer RNN:"
  print "    Average: {}".format(elapsed_time_5_layers_average)
  print "    90th precentile: {}".format(elapsed_time_5_layers[8])   
  print "    Worst: {}".format(elapsed_time_5_layers[9]) 

  print "Time required for 6 layer RNN:"
  print "    Average: {}".format(elapsed_time_6_layers_average)
  print "    90th precentile: {}".format(elapsed_time_6_layers[8])   
  print "    Worst: {}".format(elapsed_time_6_layers[9]) 

  print "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}".format(num_of_workers,scale,num_steps, elapsed_time_1_layers_average, elapsed_time_1_layers[8], elapsed_time_1_layers[9],
                                                                                                        elapsed_time_2_layers_average, elapsed_time_2_layers[8], elapsed_time_2_layers[9],
                                                                                                        elapsed_time_3_layers_average, elapsed_time_3_layers[8], elapsed_time_3_layers[9],
                                                                                                        elapsed_time_4_layers_average, elapsed_time_4_layers[8], elapsed_time_4_layers[9],
                                                                                                        elapsed_time_5_layers_average, elapsed_time_5_layers[8], elapsed_time_5_layers[9],
                                                                                                        elapsed_time_6_layers_average, elapsed_time_6_layers[8], elapsed_time_6_layers[9]) 

if __name__ == "__main__":
  rnn_ray_monolithic(sys.argv[1:])
