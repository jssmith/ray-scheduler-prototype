import os
import sys

from plot_workloads import drawplots_fn, drawplots_generic

def usage():
    print "Usage: poster_figs.py"

def poster_figs():
    output_path = 'poster_figs'
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    # global scheduler job completion time vs nodes
    #  - Workloads: RNN (combined ?x, ?delay), RLPong (combined ?x, ?delay)
    #  - Synthetic Matrix Multiplication
    experiment_name_graph2_matmult = 'graphs-dec4-d'
    drawplots_fn(experiment_name_graph2_matmult, lambda x: x['job_completion_time'],
        'job_completion_time', 'Job Completion Time [seconds]',
        lambda x: not x['scheduler'].endswith('local'),
        'Global Schedulers - Synthetic Matrix Multiplication 16,000x16,000',
        'poster_figs/syn_matmult_global.pdf')
    drawplots_fn(experiment_name_graph2_matmult, lambda x: x['job_completion_time'],
        'job_completion_time', 'Job Completion Time [seconds]',
        lambda x: x['scheduler'].endswith('local'),
        'Local Schedulers - Synthetic Matrix Multiplication 16,000x16,000',
        'poster_figs/syn_matmult_local.pdf')

    experiment_name_graph2_rnn = 'graphs-dec4-e'
    drawplots_fn(experiment_name_graph2_rnn, lambda x: x['job_completion_time'],
        'job_completion_time', 'Job Completion Time [seconds]',
        lambda x: not x['scheduler'].endswith('local'),
        'Global Schedulers - RNN',
        'poster_figs/rnn_global.pdf')
    drawplots_fn(experiment_name_graph2_rnn, lambda x: x['job_completion_time'],
        'job_completion_time', 'Job Completion Time [seconds]',
        lambda x: x['scheduler'].endswith('local'),
        'Local Schedulers - RLPong',
        'poster_figs/rnn_local.pdf')

    experiment_name_graph2_rlpong = 'graphs-dec4-f'
    drawplots_fn(experiment_name_graph2_rlpong, lambda x: x['job_completion_time'],
        'job_completion_time', 'Job Completion Time [seconds]',
        lambda x: not x['scheduler'].endswith('local'),
        'Global Schedulers - RNN',
        'poster_figs/rlpong_global.pdf')
    drawplots_fn(experiment_name_graph2_rlpong, lambda x: x['job_completion_time'],
        'job_completion_time', 'Job Completion Time [seconds]',
        lambda x: x['scheduler'].endswith('local'),
        'Local Schedulers - RLPong',
        'poster_figs/rlpong_local.pdf')

    experiment_name_threshold = 'graphs-dec4-threshold-b'
    drawplots_generic(experiment_name_threshold,
        lambda x: x['env']['RAY_SCHED_THRESHOLD1L'], 't1l', 'Threshold 1L',
        lambda x: x['job_completion_time'], 'job_completion_time', 'Job Completion Time [seconds]',
        lambda x: not x['scheduler'].startswith('trivial'),
        'Threshold - RLPong',
        'poster_figs/rlpong_threshold.pdf')

if __name__ == '__main__':
    if len(sys.argv) != 1:
        usage()
        sys.exit(1)
    poster_figs()