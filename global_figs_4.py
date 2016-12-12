import os
import sys

import gen_global_4 as gen_global

from plot_workloads import drawplots_fn, drawplots_generic, drawplots_relative

def usage():
    print "Usage: global_figs.py"

def global_figs():
    output_path = 'poster_figs'
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    experiment_name_graph2_matmult = gen_global.experiment_name_poster_synmatmul
    drawplots_fn(experiment_name_graph2_matmult, lambda x: x['max_cache_depth_size'] * x['num_nodes'],
        'max_cache_depth_size', 'Total Cache Size Needed [Bytes]',
        lambda x: True,
        'Global Schedulers - Synthetic Matrix Multiplication 16,000x16,000',
        'poster_figs/syn_matmult_global_cache_total.png')

    experiment_name_graph2_rnn = gen_global.experiment_name_poster_rnn
    drawplots_fn(experiment_name_graph2_rnn, lambda x: x['max_cache_depth_size'] * x['num_nodes'],
        'max_cache_depth_size', 'Total Cache Size Needed [Bytes]',
        lambda x: True,
        'Global Schedulers - RNN',
        'poster_figs/rnn_global_cache_total.png')

    experiment_name_graph2_rlpong = gen_global.experiment_name_poster_rlpong
    drawplots_fn(experiment_name_graph2_rlpong, lambda x: x['max_cache_depth_size'] * x['num_nodes'],
        'max_cache_depth_size', 'Total Cache Size Needed [Bytes]',
        lambda x: True,
        'Global Schedulers - RLPong',
        'poster_figs/rlpong_global_cache_total.png')


    experiment_name_graph2_matmult = gen_global.experiment_name_poster_synmatmul
    drawplots_fn(experiment_name_graph2_matmult, lambda x: x['max_cache_depth_size'],
        'max_cache_depth_size', 'Max Cache Depth Used [Bytes]',
        lambda x: True,
        'Global Schedulers - Synthetic Matrix Multiplication 16,000x16,000',
        'poster_figs/syn_matmult_global_cache_host.png')

    experiment_name_graph2_rnn = gen_global.experiment_name_poster_rnn
    drawplots_fn(experiment_name_graph2_rnn, lambda x: x['max_cache_depth_size'],
        'max_cache_depth_size', 'Max Cache Depth Used [Bytes]',
        lambda x: True,
        'Global Schedulers - RNN',
        'poster_figs/rnn_global_cache_host.png')

    experiment_name_graph2_rlpong = gen_global.experiment_name_poster_rlpong
    drawplots_fn(experiment_name_graph2_rlpong, lambda x: x['max_cache_depth_size'],
        'max_cache_depth_size', 'Max Cache Depth Used [Bytes]',
        lambda x: True,
        'Global Schedulers - RLPong',
        'poster_figs/rlpong_global_cache_host.png')


if __name__ == '__main__':
    if len(sys.argv) != 1:
        usage()
        sys.exit(1)
    global_figs()