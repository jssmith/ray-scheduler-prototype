import os
import sys

# import gen_global
import gen_global_2 as gen_global

from plot_workloads import drawplots_fn, drawplots_generic, drawplots_relative

def usage():
    print "Usage: global_figs.py"

def global_figs():
    output_path = 'poster_figs'
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    for network_slowdown in [1, 10, 100, 1000]:
        object_transfer_time_cost = .00000001 * network_slowdown
        experiment_name_graph2_matmult = gen_global.experiment_name_poster_synmatmul
        drawplots_fn(experiment_name_graph2_matmult, lambda x: x['job_completion_time'],
            'job_completion_time', 'Job Completion Time [seconds]',
            lambda x: x['object_transfer_time_cost'] == str(object_transfer_time_cost),
            'Global Schedulers - Synthetic Matrix Multiplication 16,000x16,000 - slow {}'.format(network_slowdown),
            'poster_figs/syn_matmult_global_slow_{}.png'.format(network_slowdown))

        experiment_name_graph2_rnn = gen_global.experiment_name_poster_rnn
        drawplots_fn(experiment_name_graph2_rnn, lambda x: x['job_completion_time'],
            'job_completion_time', 'Job Completion Time [seconds]',
            lambda x: x['object_transfer_time_cost'] == str(object_transfer_time_cost),
            'Global Schedulers - RNN - slow {}'.format(network_slowdown),
            'poster_figs/rnn_global_slow_{}.png'.format(network_slowdown))

        experiment_name_graph2_rlpong = gen_global.experiment_name_poster_rlpong
        drawplots_fn(experiment_name_graph2_rlpong, lambda x: x['job_completion_time'],
            'job_completion_time', 'Job Completion Time [seconds]',
            lambda x: x['object_transfer_time_cost'] == str(object_transfer_time_cost),
            'Global Schedulers - RLPong - slow {}'.format(network_slowdown),
            'poster_figs/rlpong_global_slow_{}.png'.format(network_slowdown))



if __name__ == '__main__':
    if len(sys.argv) != 1:
        usage()
        sys.exit(1)
    global_figs()