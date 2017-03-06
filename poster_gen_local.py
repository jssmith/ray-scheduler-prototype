from sweep_queue import sweep_queue
from sweep_threshold_queue import sweep_threshold_queue

version_prefix = 'dec9g'

min_nodes = 3
max_nodes = 20
nodes_step = 1


# schedulers = ['transfer_aware', 'transfer_aware_local', 'transfer_aware_threshold_local']

# schedulers = ['trivial', 'trivial_local', 'trivial_threshold_local',
#               'transfer_aware', 'transfer_aware_local', 'transfer_aware_threshold_local',
#               'location_aware', 'location_aware_local', 'location_aware_threshold_local']

threshold_schedulers = ['trivial_threshold_local', 'transfer_aware_threshold_local', 'location_aware_threshold_local']

trace_single_synmatmul = 'traces/sweep/trace_syn_mat_mul_8_2000.json.gz'
trace_single_rnn = 'traces/sweep/trace_rnn_t30_s100.json.gz'
trace_single_rlpong = 'traces/sweep/trace_rl-pong_t3_s2.json.gz'

trace_combined_synmatmul = 'traces/sweep/trace_syn_mat_mul_8_2000_combined-10-0.5-dec4.json.gz'
trace_combined_rnn = 'traces/sweep/trace_rnn_t30_s100_combined-10-0.5-dec4.json.gz'
trace_combined_rlpong = 'traces/sweep/trace_rl-pong_t4_s1_combined-10-0.5-dec4.json.gz'

experiment_name_poster_synmatmul = '{}-poster-synmatmul'.format(version_prefix)
experiment_name_poster_rnn = '{}-poster-rnn'.format(version_prefix)
experiment_name_poster_rlpong = '{}-poster-rlpong'.format(version_prefix)

experiment_name_t1l = '{}-poster-thresholdsweep'.format(version_prefix)

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

def queue_threshold_sweeps():
    t1l_range = []
    t1l = 0
    t1l_step = 0.5
    while t1l <= 16:
        t1l_range.append(t1l)
        t1l += t1l_step


    sweep_threshold_queue(threshold_schedulers, t1l_range, experiment_name_t1l, trace_single_synmatmul)
    sweep_threshold_queue(threshold_schedulers, t1l_range, experiment_name_t1l, trace_single_rnn)
    sweep_threshold_queue(threshold_schedulers, t1l_range, experiment_name_t1l, trace_single_rlpong)

if __name__ == '__main__':
    queue_basic_sweeps()
    # queue_threshold_sweeps()
