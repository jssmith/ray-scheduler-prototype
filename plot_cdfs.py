import sys

import numpy as np
import matplotlib.pyplot as plt

from analyze_basic import analyze_distn

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

if __name__ == '__main__':
    if len(sys.argv) != 3:
        usage()
        sys.exit(1)
    log_filename = sys.argv[1]
    output_filename = sys.argv[2]
    build_submit_phase0_cdf(log_filename, output_filename)