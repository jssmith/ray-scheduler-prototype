from sweep_queue import sweep_queue

version_prefix = 'dec8a'

min_nodes = 3
max_nodes = 20
nodes_step = 1


schedulers = ['trivial', 'transfer_aware', 'location_aware', 'trivial_priority']


trace_combined_synmatmul = 'traces/sweep/trace_syn_mat_mul_8_2000_combined-10-0.5-dec4.json.gz'
trace_combined_rnn = 'traces/sweep/trace_rnn_t30_s100_combined-10-0.5-dec4.json.gz'
trace_combined_rlpong = 'traces/sweep/trace_rl-pong_t4_s1_combined-10-0.5-dec4.json.gz'

experiment_name_poster_synmatmul = '{}-global-synmatmul'.format(version_prefix)
experiment_name_poster_rnn = '{}-global-rnn'.format(version_prefix)
experiment_name_poster_rlpong = '{}-global-rlpong'.format(version_prefix)

def queue_basic_sweeps():
    sweep_queue(min_nodes, max_nodes, nodes_step, schedulers,
        experiment_name_poster_synmatmul,
        trace_combined_synmatmul)
    sweep_queue(min_nodes, max_nodes, nodes_step, schedulers,
        experiment_name_poster_rnn,
        trace_combined_rnn)
    sweep_queue(min_nodes, max_nodes, nodes_step, schedulers,
        experiment_name_poster_rlpong,
        trace_combined_rlpong)

if __name__ == '__main__':
    queue_basic_sweeps()
