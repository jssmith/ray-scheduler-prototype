import sys
import numpy as np
import math

from mpl_toolkits.mplot3d import Axes3D
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import itertools
from scipy.interpolate import griddata

def drawplots(args):
    csv_filename = args[1]
    sim_sweep_csv = np.genfromtxt(csv_filename, delimiter=',', skip_header=1, dtype='|S32')

    #print sim_sweep_csv

    workload_types = np.unique(sim_sweep_csv[:,0])
    scheduler_types = np.unique(sim_sweep_csv[:,9])
    unique_num_tasks = np.unique(sim_sweep_csv[:,1])
    unique_total_task_durations = np.unique(sim_sweep_csv[:,2])
    unique_total_num_objects = np.unique(sim_sweep_csv[:,3])
    unique_total_object_sizes = np.unique(sim_sweep_csv[:,4])
    unique_norm_critical_path = np.unique(sim_sweep_csv[:,5])
    unique_num_nodes = np.unique(sim_sweep_csv[:,6])
    unique_workers_per_node = np.unique(sim_sweep_csv[:,7])
    unique_data_transfer_cost = np.unique(sim_sweep_csv[:,8])



    colors = itertools.cycle(cm.rainbow(np.linspace(0, 1, len(scheduler_types))))
    markers = itertools.cycle(('o', '*', '^', '+', ',', '.'))
    #markers = itertools.cycle(('o', '*', '^'))


    #characterize the schedulers compared to environment (assume workload parameters are constant)
    #assuming an RNN, rl-pong, alexnet workload of size (number of tasks) X
    #production graph: compare job completion time of different schedulers for RNN workloads data transfer cost 0.1 (2d plot: time vs. num_nodes)
    #production graph: compare job completion time of different schedulers for rl-pong workloads data transfer cost 0.1 (2d plot: time vs. num_nodes)
    #production graph: compare job completion time of different schedulers for alexnet workloads data transfer cost 0.1 (2d plot: time vs. num_nodes)

    fig5_scatter = plt.figure(figsize=(16,8), dpi=100)

    fig5_cont = plt.figure()
    fig5sp_scatter = {}
    fig5sp_cont = {}



 #generalization loop:
    fig5sp_height = int(math.sqrt(len(workload_types)))
    fig5sp_width = len(workload_types) / fig5sp_height + len(workload_types)%fig5sp_height
    for wload_ind in range(len(workload_types)):
        fig5sp_scatter[wload_ind] = fig5_scatter.add_subplot(fig5sp_height,fig5sp_width,wload_ind+1)
        fig5sp_cont[wload_ind] = fig5_cont.add_subplot(fig5sp_height,fig5sp_width,wload_ind+1)
        for sched in scheduler_types:
            color = colors.next()
            fig5data = sim_sweep_csv[np.logical_and(
                                     np.logical_and(
                                     sim_sweep_csv[:,0]==workload_types[wload_ind],
                                     sim_sweep_csv[:,9]==sched),
                                     sim_sweep_csv[:,10]!='-1')]
            fig5data_reduced = np.column_stack((fig5data[:,6], fig5data[:,10])).astype(np.float)

            fig5sp_scatter[wload_ind].scatter(fig5data_reduced[:,0], fig5data_reduced[:,1],c=color, marker=markers.next(), label=sched)

            fig5sp_cont[wload_ind].plot(fig5data_reduced[:,0], fig5data_reduced[:,1],c=color, label=sched)


        fig5sp_scatter[wload_ind].set_xlabel('Number of Nodes [a.u]')
        fig5sp_scatter[wload_ind].set_ylabel('Job Completion Time [seconds]')
        fig5sp_scatter[wload_ind].set_title('Job Completion Time vs. Number of Nodes: {} Workload'.format(workload_types[wload_ind]))
#        if wload_ind==0 :
        fig5sp_scatter[wload_ind].legend(shadow=True, fancybox=True, prop={'size':8})

        fig5sp_cont[wload_ind].set_xlabel('Number of Nodes [a.u]')
        fig5sp_cont[wload_ind].set_ylabel('Job Completion Time [seconds]')
        fig5sp_cont[wload_ind].set_title('Job Completion Time vs. Number of Nodes: {} Workload'.format(workload_types[wload_ind]))
#        if wload_ind==0 :
        fig5sp_cont[wload_ind].legend(shadow=True, fancybox=True, prop={'size':8})



    fig5_scatter.savefig('fig5-scatter.pdf')
    fig5_cont.savefig('fig5-cont.pdf')
    plt.show()


if __name__ == '__main__':
    drawplots(sys.argv)
