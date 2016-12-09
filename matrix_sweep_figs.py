import os
import sys

from gen_matrix_sweep import experiment_name_matrix_sweep, get_block_dims
from plot_workloads import drawplots_fn

def usage():
    print "Usage: matrix_sweep_figs.py"


def matrix_sweep_figs():
    output_path = 'poster_figs'
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    block_dims = get_block_dims(250, 8000)

    for block_dim in block_dims:
        drawplots_fn(experiment_name_matrix_sweep, lambda x: x['job_completion_time'],
            'job_completion_time', 'Job Completion Time [seconds]',
            lambda x: x['tracefile'].find('syn_mat_mul_8_{}_combined'.format(block_dim)) >= 0,
            'Global Schedulers - Synthetic Matrix Multiplication block dim {}'.format(block_dim),
            'poster_figs/syn_matmult_global_8_{}.png'.format(block_dim))



if __name__ == '__main__':
    if len(sys.argv) != 1:
        usage()
        sys.exit(1)
    matrix_sweep_figs()
