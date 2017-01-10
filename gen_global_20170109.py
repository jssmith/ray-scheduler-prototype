from sweep_queue import sweep_queue

version_prefix = 'jan9a'

min_nodes = 3
max_nodes = 20
nodes_step = 1


schedulers = ['trivial', 'transfer_aware', 'location_aware', 'trivial_priority']

trace_matmul = 'traces-ng/sweep/trace_mat-mult_t5_s3.json'
trace_rnn = 'traces-ng/sweep/trace_rnn_t6_s5.json'
trace_rlpong = 'traces-ng/sweep/trace_rl-pong_t4_s1.json'
trace_array = 'traces-ng/distributed-array-test.json'
trace_parallel_tree_reduce = 'traces-ng/trace-parallel-tree-reduce.json'
trace_sequential_tree_reduce = 'traces-ng/trace-sequential-tree-reduce.json'


experiment_name_rise_global = '{}-global-cache-synmatmul'.format(version_prefix)

def queue_basic_sweeps():
        sweep_queue(min_nodes, max_nodes, nodes_step, schedulers,
            experiment_name_rise_global,
            trace_matmul)
        sweep_queue(min_nodes, max_nodes, nodes_step, schedulers,
            experiment_name_rise_global,
            trace_rnn)
        sweep_queue(min_nodes, max_nodes, nodes_step, schedulers,
            experiment_name_rise_global,
            trace_rlpong)
        sweep_queue(min_nodes, max_nodes, nodes_step, schedulers,
            experiment_name_rise_global,
            trace_array)
        sweep_queue(min_nodes, max_nodes, nodes_step, schedulers,
            experiment_name_rise_global,
            trace_parallel_tree_reduce)
        sweep_queue(min_nodes, max_nodes, nodes_step, schedulers,
            experiment_name_rise_global,
            trace_sequential_tree_reduce)


if __name__ == '__main__':
    queue_basic_sweeps()
