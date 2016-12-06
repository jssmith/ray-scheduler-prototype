import sys

import numpy as np
import matplotlib.pyplot as plt

from analyze_basic import analyze_distn
from plot_workloads import std_scheduler_colors

def usage():
    print "Usage: plot_cdfs.py log.gz output.pdf"

def build_submit_phase0_cdf(log_filename, output_filename):
    dist = analyze_distn(log_filename)
    plot_cdf(dist['submit_to_phase0_time'],
        'Scheduling Delay [seconds from submit to unblocked execution]',
        'Fraction of Tasks',
        output_filename)

def plot_cdf(data,
    x_variable_description,
    y_variable_description,
    output_filename):
    # print 'min {} max {}'.format(min(data), max(data))

    sorted_data = np.sort(np.asarray(data))
    yvals=np.arange(len(sorted_data))/float(len(sorted_data)-1)

    # plt.plot(sorted_data,yvals)
    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.semilogx(sorted_data,yvals)
    ax.set_xlabel(x_variable_description)
    ax.set_ylabel(y_variable_description)
    fig.savefig(output_filename)
    plt.close(fig)

def build_submit_phase0_cdf_multi_scheduler(workload_name, scheduler_inputs, output_filename):
    scheduler_dist = []
    for scheduler, log_filename in scheduler_inputs:
        scheduler_dist.append((scheduler, analyze_distn(log_filename)))

    plot_cdf_multi_scheduler(scheduler_dist,
        'Scheduling Delay [seconds from submit to unblocked execution]',
        'Fraction of Tasks',
        'Scheduling Delay - {} - 4 nodes'.format(workload_name),
        output_filename)

def plot_cdf_multi_scheduler(all_data,
    x_variable_description,
    y_variable_description,
    title,
    output_filename):
    fig_dpi = 300
    plt.rcParams.update({'font.size': 6})

    fig = plt.figure(figsize=(4,3), dpi=fig_dpi)
    ax = fig.add_subplot(111)
    for scheduler, data in all_data:
        sorted_data = np.sort(np.asarray(data['submit_to_phase0_time']))
        yvals=np.arange(len(sorted_data))/float(len(sorted_data)-1)
        if scheduler == 'trivial':
            linewidth = 4.0
        else:
            linewidth = 1.0
        ax.plot(sorted_data,yvals, c=std_scheduler_colors[scheduler], label=scheduler, linewidth=linewidth)

    ax.set_xlabel(x_variable_description, labelpad=2)
    ax.set_ylabel(y_variable_description, labelpad=2)
    ax.set_title(title)
    ax.legend(shadow=True, fancybox=True, prop={'size':6})
    print 'saving cdf to figure {}'.format(output_filename)
    fig.savefig(output_filename, dpi=fig_dpi)
    plt.close(fig)


if __name__ == '__main__':
    if len(sys.argv) != 3:
        usage()
        sys.exit(1)
    log_filename = sys.argv[1]
    output_filename = sys.argv[2]
    build_submit_phase0_cdf(log_filename, output_filename)