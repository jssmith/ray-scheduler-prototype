import os
import sys

from plot_workloads import drawplots_fn, drawplots_generic, drawplots_relative

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
        'poster_figs/syn_matmult_global.png')
    # drawplots_fn(experiment_name_graph2_matmult, lambda x: x['job_completion_time'],
    #     'job_completion_time', 'Job Completion Time [seconds]',
    #     lambda x: x['scheduler'].endswith('local'),
    #     'Local Schedulers - Synthetic Matrix Multiplication 16,000x16,000',
    #     'poster_figs/syn_matmult_local.png')

    experiment_name_graph2_rnn = 'graphs-dec4-e'
    drawplots_fn(experiment_name_graph2_rnn, lambda x: x['job_completion_time'],
        'job_completion_time', 'Job Completion Time [seconds]',
        lambda x: not x['scheduler'].endswith('local'),
        'Global Schedulers - RNN',
        'poster_figs/rnn_global.png')
    # drawplots_fn(experiment_name_graph2_rnn, lambda x: x['job_completion_time'],
    #     'job_completion_time', 'Job Completion Time [seconds]',
    #     lambda x: x['scheduler'].endswith('local'),
    #     'Local Schedulers - RLPong',
    #     'poster_figs/rnn_local.png')

    # drawplots_fn(experiment_name_graph2_rnn, lambda x: x['job_completion_time'],
    #     'job_completion_time', 'Job Completion Time [seconds]',
    #     lambda x: True,
    #     'Local and Global Schedulers - RLPong',
    #     'poster_figs/rnn_local_global.png')


    experiment_name_graph2_rlpong = 'graphs-dec4-f'
    drawplots_fn(experiment_name_graph2_rlpong, lambda x: x['job_completion_time'],
        'job_completion_time', 'Job Completion Time [seconds]',
        lambda x: not x['scheduler'].endswith('local'),
        'Global Schedulers - RNN',
        'poster_figs/rlpong_global.png')
    # drawplots_fn(experiment_name_graph2_rlpong, lambda x: x['job_completion_time'],
    #     'job_completion_time', 'Job Completion Time [seconds]',
    #     lambda x: x['scheduler'].endswith('local'),
    #     'Local Schedulers - RNN',
    #     'poster_figs/rlpong_local.png')

    experiment_name_threshold = 'graphs-dec4-threshold-b'
    drawplots_generic(experiment_name_threshold,
        lambda x: x['env']['RAY_SCHED_THRESHOLD1L'], 't1l', 'Threshold 1L',
        lambda x: x['job_completion_time'], 'job_completion_time', 'Job Completion Time [seconds]',
        lambda x: True,
        'Threshold - RLPong',
        'poster_figs/rlpong_threshold.png')

# TODO - add this for Matrix, RNN
#      - do just a single one, rather than combined
#      - greater granularity - 0.5 increment

    # Figure 3
    def plot_relative(experiment_name, ref_scheduler, plot_schedulers, name, output_path):
        drawplots_relative(experiment_name_graph2_matmult, lambda x: x['num_nodes'],
            'num_nodes', 'Number of Nodes',
            ref_scheduler,
            plot_schedulers,
            'Local / Global - {}'.format(name),
            output_path)

    scheduler_combinations = [
        ('trivial', ['trivial_local', 'trivial_threshold_local']),
        (['transfer_aware_local', 'transfer_aware_threshold_local'])]
    workloads = [
        (experiment_name_graph2_matmult, 'syn_matmult', 'Synthetic Matrix Multiplication 16,000x16,000'),
        (experiment_name_graph2_rnn, 'rnn', 'RNN'),
        (experiment_name_graph2_rlpong, 'rlpong', 'RLPong')]

    for ref_scheduler, plot_schedulers in scheduler_combinations:
        for experiment_name, workload, workload_desc in workloads:
            plot_relative(experiment_name, ref_scheduler, plot_schedulers,
                workload_desc,
                'poster_figs/{}_{}_localvglobal.png'.format(workload, ref_scheduler))


if __name__ == '__main__':
    if len(sys.argv) != 1:
        usage()
        sys.exit(1)
    poster_figs()