import os
import sys

import poster_gen

from plot_workloads import drawplots_fn, drawplots_generic, drawplots_relative

def usage():
    print "Usage: poster_figs.py"

def poster_figs():
    output_path = 'poster_figs'
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    experiment_name_graph2_matmult = poster_gen.experiment_name_poster_synmatmul
    drawplots_fn(experiment_name_graph2_matmult, lambda x: x['job_completion_time'],
        'job_completion_time', 'Job Completion Time [seconds]',
        lambda x: not x['scheduler'].endswith('local'),
        'Global Schedulers - Synthetic Matrix Multiplication 16,000x16,000',
        'poster_figs/syn_matmult_global.png')

    experiment_name_graph2_rnn = poster_gen.experiment_name_poster_rnn
    drawplots_fn(experiment_name_graph2_rnn, lambda x: x['job_completion_time'],
        'job_completion_time', 'Job Completion Time [seconds]',
        lambda x: not x['scheduler'].endswith('local'),
        'Global Schedulers - RNN',
        'poster_figs/rnn_global.png')

    experiment_name_graph2_rlpong = poster_gen.experiment_name_poster_rlpong
    drawplots_fn(experiment_name_graph2_rlpong, lambda x: x['job_completion_time'],
        'job_completion_time', 'Job Completion Time [seconds]',
        lambda x: not x['scheduler'].endswith('local'),
        'Global Schedulers - RLPong',
        'poster_figs/rlpong_global.png')

    experiment_name_threshold = poster_gen.experiment_name_t1l
    threshold_schedulers = set(['trivial_threshold_local','transfer_aware_threshold_local', 'location_aware_threshold_local'])
    def plot_threshold(experiment_name, name, description, tracefile):
        print "threshold plot {} {}".format(description, tracefile)
        drawplots_generic(experiment_name,
            lambda x: x['env']['RAY_SCHED_THRESHOLD1L'], 't1l', 'Threshold 1L',
            lambda x: x['job_completion_time'], 'job_completion_time', 'Job Completion Time [seconds]',
            lambda x: x['scheduler'] in threshold_schedulers and x['tracefile'] == tracefile,
            'Threshold - {}'.format(description),
            'poster_figs/{}_threshold.png'.format(name))
    plot_threshold(experiment_name_threshold, 'rnn', 'RNN', poster_gen.trace_single_rnn)
    plot_threshold(experiment_name_threshold, 'rlpong', 'RLPong', poster_gen.trace_single_rlpong)
    plot_threshold(experiment_name_threshold, 'synmatmul', 'Synthetic Matrix Multiplication', poster_gen.trace_single_synmatmul)

    # Figure 3
    def plot_relative(experiment_name, ref_scheduler, plot_schedulers, name, output_path):
        drawplots_relative(experiment_name, lambda x: x['num_nodes'],
            'num_nodes', 'Number of Nodes',
            ref_scheduler,
            plot_schedulers,
            'Local / Global - {}'.format(name),
            output_path)

    scheduler_combinations = [
        ('trivial', ['trivial_local', 'trivial_threshold_local']),
        ('transfer_aware', ['transfer_aware_local', 'transfer_aware_threshold_local'])]
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