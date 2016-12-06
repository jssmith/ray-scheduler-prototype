import sys
import ec2config

import plot_cdfs

from collections import defaultdict

tracefile_to_workload = {
    'traces/sweep/trace_rl-pong_t4_s1_combined-10-0.5-dec4.json.gz': 'RLPong',
    'traces/sweep/trace_rnn_t30_s100_combined-10-0.5-dec4.json.gz': 'RNN',
    'traces/sweep/trace_syn_mat_mul_8_2000_combined-10-0.5-dec4.json.gz': 'Synthetic Matrix Multiplication 16,000 x 16,000'
}

include_schedulers = set(['trivial', 'location_aware', 'transfer_aware'])

def gen_cdfs(experiment_name):
    sdb_conn = ec2config.sdb_connect()
    dom = sdb_conn.get_domain(ec2config.sdb_sweep_domain)
    query = "select * from `{}` where experiment_name like'{}%' and num_nodes='4'".format(
        ec2config.sdb_sweep_domain, experiment_name, experiment_name)

    rs = dom.select(query)

    to_plot = defaultdict(list)
    for r in rs:
        # selected workloads. Skip those for which env set (threshold sweep)
#        print r
        if r['tracefile'] in tracefile_to_workload and 'log_fn' in r:
            if 'env' in r and r['env'] != u'{}':
                print 'skipping', r
                continue
            workload = tracefile_to_workload[r['tracefile']]
            scheduler = r['scheduler']
            log_fn = r['log_fn']
            print workload, scheduler, log_fn
            if scheduler in include_schedulers:
                to_plot[workload].append((scheduler, 'sweep/' + r['experiment_name'] + '/' + log_fn))
    
    print to_plot
    for workload, data in to_plot.items():
        plot_cdfs.build_submit_phase0_cdf_multi_scheduler(workload, data,
            'poster_figs/{}-cdf'.format(workload))


if __name__ == '__main__':
    if len(sys.argv) != 2:
        usage()
        sys.exit(1)
    experiment_name = sys.argv[1]
    gen_cdfs(experiment_name)
    