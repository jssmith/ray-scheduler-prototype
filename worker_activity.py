import re
import sys

import ec2config

from matplotlib.backends.backend_pdf import PdfPages
from sim_plots import plot_worker_activity
from analyze_basic import analyze_distn

from textwrap import wrap

def usage():
    print "Usage: worker_activity.py experiment_name filters"

def get_simulator_runs(experiment_name, filters):
    sdb_conn = ec2config.sdb_connect()
    dom = sdb_conn.get_domain(ec2config.sdb_sweep_domain)
    query = "select * from `{}` where experiment_name like '{}%' and end_time is not null".format(
        ec2config.sdb_sweep_domain, experiment_name, experiment_name)
    for filter in filters:
        query += " and {}='{}'".format(filter[0], filter[1])
    rs = dom.select(query)
    simulator_runs = []
    for r in list(rs):
        simulator_runs.append(dict(r.items()))
    sdb_conn.close()
    return simulator_runs


def worker_activity(args):
    experiment_name = args[1]
    filters = []
    filter_terms = set()
    for arg in args[2:]:
        m = re.search('^(.*)=(.*)$', arg)
        filter_terms.add(m.group(1))
        filters.append((m.group(1), m.group(2)))
    simulator_runs = get_simulator_runs(experiment_name, filters)
    lim_num_runs = 10
    if len(simulator_runs) > lim_num_runs:
        print "have more than {} runs".format(lim_num_runs)

    print "plotting {} runs".format(len(simulator_runs))

    title_keys = ['scheduler', 'object_transfer_time_cost', 'num_nodes', 'tracefile']
    with PdfPages('output') as pdf:
        for r in simulator_runs:
            log_filename = 'sweep/{}/{}'.format(r['experiment_name'], r['log_fn'])
            stats = analyze_distn(log_filename)
            title = " ".join(map(lambda x: r[x], filter(lambda x: x not in filter_terms, title_keys)))
            title = "\n".join(wrap(title, 40))
            plot_worker_activity(stats['worker_activity'], pdf, r['tracefile'])

if __name__ == '__main__':
    if len(sys.argv) < 2:
        usage()
        sys.exit(1)
    worker_activity(sys.argv)