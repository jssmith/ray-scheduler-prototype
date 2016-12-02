import sys
import numpy as np
import math
import json

import matplotlib.pyplot as plt
import matplotlib.cm as cm
import itertools

def usage():
    print "Usage: plot_workloads_nodes.py experiment_name"

def drawplots(experiment_name):
    json_filename = '{}.json'.format(experiment_name)
    with open(json_filename, 'rb') as f:
        plot_data = json.load(f)

    def unique_values(data, key):
        return sorted(set(map(lambda obs: obs[key], data)))

    all_num_nodes = unique_values(plot_data, 'num_nodes')
    all_schedulers = unique_values(plot_data, 'scheduler')

    colors = itertools.cycle(cm.rainbow(np.linspace(0, 1, len(all_schedulers))))
    scheduler_colors = {}
    for scheduler in all_schedulers:
        color = colors.next()
        scheduler_colors[scheduler] = colors.next()

    for workload in unique_values(plot_data, 'tracefile'):
        workload_name = workload.replace('.json','').replace('.gz','').replace('.pdf','').replace('/','-').replace('traces/sweep/','')
        fig = plt.figure(figsize=(16,8), dpi=100)
        sp = fig.add_subplot(1, 1, 1)
        workload_data = filter(lambda x: x['tracefile'] == workload, plot_data)
        for scheduler in unique_values(workload_data, 'scheduler'):
            x = {}
            for data in filter(lambda x: x['scheduler'] == scheduler, workload_data):
                x[data['num_nodes']] = data['job_completion_time']
            job_completion_time = []
            for num_nodes in all_num_nodes:
                if num_nodes in x:
                    job_completion_time.append(x[num_nodes])
                else:
                    job_completion_time.append(None)
            sp.plot(all_num_nodes, job_completion_time,c=scheduler_colors[scheduler], label=scheduler)

        sp.set_xlabel('Number of Nodes')
        sp.set_ylabel('Job Completion Time [seconds]')
        sp.set_title('Workload {}'.format(workload_name))
        sp.legend(shadow=True, fancybox=True, prop={'size':8})
        fig.savefig('fig-{}.pdf'.format(workload_name))


if __name__ == '__main__':
    if len(sys.argv) != 2:
        usage()
        sys.exit(1)
    drawplots(sys.argv[1])
