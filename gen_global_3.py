from sweep_queue import sweep_queue

version_prefix = 'dec12g'

min_nodes = 3
max_nodes = 20
nodes_step = 1


schedulers = ['trivial', 'transfer_aware', 'location_aware', 'trivial_priority', 'trivial_df_priority']

trace_synmatmul = 'traces/sweep/trace_syn_mat_mul_8_2000.json.gz'
trace_rnn = 'traces/sweep/trace_rnn_t30_s100.json.gz'
trace_rlpong = 'traces/sweep/trace_rl-pong_t3_s2.json.gz'

experiment_name_poster_synmatmul = '{}-global-cache-synmatmul'.format(version_prefix)
experiment_name_poster_rnn = '{}-global-cache-rnn'.format(version_prefix)
experiment_name_poster_rlpong = '{}-global-cache-rlpong'.format(version_prefix)

def queue_basic_sweeps():
        sweep_queue(min_nodes, max_nodes, nodes_step, schedulers,
            experiment_name_poster_synmatmul,
            trace_synmatmul)
        sweep_queue(min_nodes, max_nodes, nodes_step, schedulers,
            experiment_name_poster_rnn,
            trace_rnn)
        sweep_queue(min_nodes, max_nodes, nodes_step, schedulers,
            experiment_name_poster_rlpong,
            trace_rlpong)

if __name__ == '__main__':
    queue_basic_sweeps()
