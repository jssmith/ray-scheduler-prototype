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


    # Figure 3
    experiment_name_graph3_matmult = experiment_name_graph2_matmult
    drawplots_fn(experiment_name_graph3_matmult, lambda x: x['job_completion_time'],
        'job_completion_time', 'Job Completion Time [seconds]',
        lambda x: x['scheduler'] in set(['trivial', 'trivial_local']),
        'Global vs Local - Synthetic Matrix Multiplication 16,000x16,000',
        'poster_figs/syn_matmult_trivial_globalvlocal.pdf')

    experiment_name_graph3_matmult = experiment_name_graph2_matmult
    include_schedulers = set(['transfer_aware', 'transfer_aware_local',
        'transfer_aware_threshold_local', 'location_aware',
        'location_aware_local', 'location_aware_threshold_local'])
    drawplots_fn(experiment_name_graph3_matmult, lambda x: x['job_completion_time'],
        'job_completion_time', 'Job Completion Time [seconds]',
        lambda x: x['scheduler'] in include_schedulers,
        'Global vs Local - Synthetic Matrix Multiplication 16,000x16,000',
        'poster_figs/syn_matmult_nontrivial_globalvlocal.pdf')

    experiment_name_graph3_rlpong = experiment_name_graph2_rlpong
    drawplots_fn(experiment_name_graph3_rlpong, lambda x: x['job_completion_time'],
        'job_completion_time', 'Job Completion Time [seconds]',
        lambda x: x['scheduler'] in set(['trivial', 'trivial_local']),
        'Global vs Local - RLPong',
        'poster_figs/rlpong_trivial_globalvlocal.pdf')

    include_schedulers = set(['transfer_aware', 'transfer_aware_local',
        'transfer_aware_threshold_local', 'location_aware',
        'location_aware_local', 'location_aware_threshold_local'])
    drawplots_fn(experiment_name_graph3_rlpong, lambda x: x['job_completion_time'],
        'job_completion_time', 'Job Completion Time [seconds]',
        lambda x: x['scheduler'] in include_schedulers,
        'Global vs Local - RLPong',
        'poster_figs/rlpong_nontrivial_globalvlocal.pdf')

    experiment_name_graph3_rnn = experiment_name_graph2_rlpong
    drawplots_fn(experiment_name_graph3_rnn, lambda x: x['job_completion_time'],
        'job_completion_time', 'Job Completion Time [seconds]',
        lambda x: x['scheduler'] in set(['trivial', 'trivial_local']),
        'Global vs Local - RNN',
        'poster_figs/rnn_trivial_globalvlocal.pdf')

    include_schedulers = set(['transfer_aware', 'transfer_aware_local',
        'transfer_aware_threshold_local', 'location_aware',
        'location_aware_local', 'location_aware_threshold_local'])
    drawplots_fn(experiment_name_graph3_rnn, lambda x: x['job_completion_time'],
        'job_completion_time', 'Job Completion Time [seconds]',
        lambda x: x['scheduler'] in include_schedulers,
        'Global vs Local - RNN',
        'poster_figs/rnn_nontrivial_globalvlocal.pdf')


if __name__ == '__main__':
    if len(sys.argv) != 1:
        usage()
        sys.exit(1)
    poster_figs()