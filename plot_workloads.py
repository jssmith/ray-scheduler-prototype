import os
import sys
import numpy as np
import math
import json
import gzip

import matplotlib.pyplot as plt
import matplotlib.cm as cm
import itertools

def usage():
    print "Usage: plot_workloads_nodes.py experiment_name"

std_scheduler_colors = {
    u'transfer_aware': [ 0.5,  0. ,  1. ,  1. ],
    u'trivial_threshold_local': [  1.00000000e+00,   1.22464680e-16,   6.12323400e-17,1.00000000e+00],
    u'transfer_aware_threshold_local': [ 0.3       ,  0.95105652,  0.80901699,  1.        ],
    u'trivial_local': [ 1.        ,  0.58778525,  0.30901699,  1.        ],
    u'trivial': [ 0.7       ,  0.95105652,  0.58778525,  1.        ],
    u'transfer_aware_local': [ 0.1       ,  0.58778525,  0.95105652,  1.        ]}

std_scheduler_markers = {
    u'transfer_aware': 'o',
    u'trivial_threshold_local': '.',
    u'transfer_aware_threshold_local': '^',
    u'trivial_local': 'x',
    u'trivial': '+',
    u'transfer_aware_local': '*'}

def require_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

def drawplots(experiment_name, y_variable_name, y_variable_description, title=None, output_filename = None):
    drawplots_fn(experiment_name, lambda x: x[y_variable_name], y_variable_name, y_variable_description, lambda x: True, title, output_filename)

def drawplots_fn(experiment_name, y_variable_fn, y_variable_name, y_variable_description,
    filter_fn=lambda x: True, title=None, output_filename=None):
    drawplots_generic(experiment_name,
        lambda x: x['num_nodes'], 'num_nodes' , 'Number of Nodes',
        y_variable_fn, y_variable_name, y_variable_description,
        filter_fn, title, output_filename)

def drawplots_relative(experiment_name,
    x_variable_fn, x_variable_name, x_variable_description,
    ref_scheduler,
    compare_schedulers,
    title,
    output_filename,
    fig_dpi=300):
    json_filename = 'sweep-summaries/{}.json.gz'.format(experiment_name)
    with gzip.open(json_filename, 'rb') as f:
        plot_data = json.load(f)

    for x in plot_data:
        x['num_nodes'] = int(x['num_nodes'])

    ref_data_dict = {}
    for obs in filter(lambda x: x['scheduler'] == ref_scheduler, plot_data):
        ref_data_dict[x_variable_fn(obs)] = obs

    series_x = _unique_values(plot_data, x_variable_fn)

    include_plot_data = filter(lambda x: x['scheduler'] in compare_schedulers, plot_data)

    def y_variable_fn(obs):
        x_val = x_variable_fn(obs)
        if x_val in ref_data_dict:
            return float(obs['job_completion_time']) / float(ref_data_dict[x_val]['job_completion_time']) 
        else:
            return None

    def extra_fig_settings(fig, ax):
        ax.set_ylim([0, 5])

    _plot(include_plot_data,
        x_variable_fn, x_variable_name, x_variable_description,
        y_variable_fn,
        'job_completion_time_relative',
        'Relative Job Completion Time',
        series_x, std_scheduler_colors, std_scheduler_markers,
        title=title,
        output_filename=output_filename,
        fig_dpi=fig_dpi, extra_fig_settings=extra_fig_settings)


def drawplots_generic(experiment_name,
    x_variable_fn, x_variable_name, x_variable_description,
    y_variable_fn, y_variable_name, y_variable_description,
    filter_fn=lambda x: True, title=None, output_filename=None,
    fig_dpi=300):
    json_filename = 'sweep-summaries/{}.json.gz'.format(experiment_name)
    with gzip.open(json_filename, 'rb') as f:
        plot_data = json.load(f)

    require_dir('figs')

    plt.rcParams.update({'font.size': 6})

    for x in plot_data:
        x['num_nodes'] = int(x['num_nodes'])

    all_schedulers = _unique_values(plot_data, lambda x: x['scheduler'])
    series_x = _unique_values(plot_data, x_variable_fn)

    colors = itertools.cycle(cm.rainbow(np.linspace(0, 1, len(all_schedulers))))
    scheduler_colors = {}
    for scheduler in all_schedulers:
        scheduler_colors[scheduler] = colors.next()

    markers = itertools.cycle(('o', '*', '^', '+', 'x', '.'))
    scheduler_markers = {}
    for scheduler in all_schedulers:
        scheduler_markers[scheduler] = markers.next()

    _plot(filter(filter_fn, plot_data),
        x_variable_fn, x_variable_name, x_variable_description,
        y_variable_fn, y_variable_name, y_variable_description,
        series_x, scheduler_colors, scheduler_markers,
        title=title, output_filename=output_filename,
        fig_dpi=fig_dpi)

def _unique_values(data, fn):
    return sorted(set(map(fn, data)))

def _plot(plot_data,
    x_variable_fn, x_variable_name, x_variable_description,
    y_variable_fn, y_variable_name, y_variable_description,
    series_x, scheduler_colors, scheduler_markers,
    title=None, output_filename=None, fig_dpi=300,
    extra_fig_settings=None):

    all_wokloads = _unique_values(plot_data, lambda x: x['tracefile'])
    index = 0
    workload_figs = {}
    for workload in all_wokloads:
        workload_name = workload.replace('.json','').replace('.gz','').replace('.pdf','').replace('/','-').replace('traces/sweep/','')
        fig = plt.figure(figsize=(4,3), dpi=fig_dpi)
        sp = fig.add_subplot(111)
        workload_data = filter(lambda x: x['tracefile'] == workload, plot_data)
        for scheduler in _unique_values(workload_data, lambda x: x['scheduler']):
            series_map = {}
            for data in filter(lambda x: x['scheduler'] == scheduler, workload_data):
                series_map[x_variable_fn(data)] = y_variable_fn(data)
            series_y = []
            # Fill in missing values with None
            for x_value in series_x:
                if x_value in series_map:
                    series_y.append(series_map[x_value])
                else:
                    series_y.append(None)
            sp.plot(series_x, series_y, c=scheduler_colors[scheduler], marker=scheduler_markers[scheduler], label=scheduler)

        sp.set_xlabel(x_variable_description, labelpad=2)
        sp.set_ylabel(y_variable_description, labelpad=2)
        if title is not None:
            sp.set_title(title)
        else:
            sp.set_title('Workload {}'.format(workload_name))
        sp.legend(shadow=True, fancybox=True, prop={'size':6})
        if extra_fig_settings:
            extra_fig_settings(fig, sp)
        if output_filename is None:
            fig_fn = 'figs/{}/fig-{}-{}-{}.pdf'.format(experiment_name, workload_name, y_variable_name, x_variable_name)
            require_dir('figs/{}'.format(experiment_name))
        else:
            if len(all_wokloads) > 0:
                fig_fn = output_filename
            else:
                fig_fn = '{}-{}'.format(index, output_filename)
        print 'output to', fig_fn
        workload_figs[workload] = fig
        fig.savefig(fig_fn, dpi=fig_dpi)
        plt.close(fig)
        index += 1


if __name__ == '__main__':
    if len(sys.argv) != 2:
        usage()
        sys.exit(1)
    drawplots(sys.argv[1], 'job_completion_time', 'Job Completion Time [seconds]')
    drawplots(sys.argv[1], 'object_transfer_size', 'Object Transfer Size [bytes]')
    drawplots(sys.argv[1], 'object_transfer_time', 'Object Transfer Time [seconds]')
    drawplots(sys.argv[1], 'num_object_transfers', 'Number of Object Transfers')
    drawplots_fn(sys.argv[1], lambda x: x['submit_to_phase0_time'] / x['num_tasks'],
        'avg_submit_to_phase0_time', 'Average submit to phase0 time [seconds]')
    drawplots(sys.argv[1], 'num_tasks_scheduled_locally', 'Number of tasks shceduled locally')
