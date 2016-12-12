from subprocess import call
from sweep_queue import sweep_queue
from ec2config import s3_cp, s3_bucket
import synmultiply

version_prefix = 'dec8a'

min_nodes = 3
max_nodes = 20
nodes_step = 1


schedulers = ['trivial', 'transfer_aware', 'location_aware', 'trivial_priority']

experiment_name_matrix_sweep = '{}-matrixsweep'.format(version_prefix)

def usage():
    print "Usage: gen_matrix_sweep.py"

def synthesize(num_blocks, block_dim):
    call(['python', 'synmultiply.py', str(num_blocks), str(block_dim)])
    single_trace_fn = synmultiply.output_filename(num_blocks, block_dim)
    combined_trace_fn = 'traces/sweep/trace_syn_mat_mul_{}_{}_combined_2_0-dec8.json.gz'.format(num_blocks, block_dim)
    call(['python', 'combine_traces.py',
        '--trace-filename', single_trace_fn,
        '--repetitions', '2',
        '--offset', '0',
        '--output-filename', combined_trace_fn])
    s3_cp(single_trace_fn, s3_bucket + '/' + single_trace_fn)
    s3_cp(combined_trace_fn, s3_bucket + '/' + combined_trace_fn)
    return single_trace_fn, combined_trace_fn



def get_block_dims(min, max, mul=2):
    block_dims = []
    dim = min
    while dim <= max:
        block_dims.append(dim)
        dim *= mul
    return block_dims

def queue_matrix_sweeps():
    block_dims = get_block_dims(250, 8000)
    print 'Block dimensions', block_dims
    for block_dim in block_dims:
        single_trace_fn, combined_trace_fn = synthesize(8, block_dim) 
        sweep_queue(min_nodes, max_nodes, nodes_step, schedulers,
            experiment_name_matrix_sweep,
            combined_trace_fn)

if __name__ == '__main__':
    if len(sys.argv) != 1:
        usage()
        sys.exit(1)
    queue_matrix_sweeps()
