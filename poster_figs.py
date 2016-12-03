import os
import sys

from plot_workloads import drawplots_fn

def usage():
    print "Usage: poster_figs.py"

def poster_figs():
    output_path = 'poster_figs'
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    # global scheduler job completion time vs nodes
    #  - Workloads: RNN (combined ?x, ?delay), RLPong (combined ?x, ?delay)
    #  - Synthetic Matrix Multiplication
    experiment_name_graph2 = 'graphs-2-a'
    drawplots_fn(experiment_name_graph2, lambda x: x['job_completion_time'],
        'job_completion_time', 'Job Completion Time [seconds]',
        lambda x: not x['scheduler'].endswith('local'),
        'Global Schedulers - Synthetic Matrix Multiplication 16,000x16,000',
        'poster_figs/syn_matmult_global.pdf')
    drawplots_fn(experiment_name_graph2, lambda x: x['job_completion_time'],
        'job_completion_time', 'Job Completion Time [seconds]',
        lambda x: x['scheduler'].endswith('local'),
        'Local Schedulers - Synthetic Matrix Multiplication 16,000x16,000',
        'poster_figs/syn_matmult_local.pdf')


if __name__ == '__main__':
    if len(sys.argv) != 1:
        usage()
        sys.exit(1)
    poster_figs()