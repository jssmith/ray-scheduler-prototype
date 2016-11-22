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
    sim_sweep_csv = np.genfromtxt(csv_filename, delimiter=',', skip_header=1, dtype='|S15')

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


    #assuming cluster size of: 10 nodes, 3 workers per node
    #Exploratory graph: compare job completition time of different schedulers for different data transfer costs (3d plots: time vs. load shape vs. load size)
    fig1_scatter = plt.figure()
    fig1_cont = plt.figure()
    fig1sp_scatter = {}
    fig1sp_cont = {}
    fig1sp_height = int(math.sqrt(len(unique_data_transfer_cost)))
    fig1sp_width = len(unique_data_transfer_cost) / fig1sp_height + len(unique_data_transfer_cost)%fig1sp_height     
    for k in range(len(unique_data_transfer_cost)):
        fig1sp_scatter[k] = fig1_scatter.add_subplot(2,3,k+1, projection='3d')
        fig1sp_cont[k] = fig1_cont.add_subplot(2,3,k+1, projection='3d')
        for sched in scheduler_types:
            fig1data = sim_sweep_csv[np.logical_and( \
                                     np.logical_and( \
                                     sim_sweep_csv[:,9]==sched, \
                                     sim_sweep_csv[:,8]==unique_data_transfer_cost[k]), \
                                     sim_sweep_csv[:,10]!='-1')]
            
            #before job completion time normalization
            #fig1data_reduced = np.c_[np.column_stack((fig1data[:,5], fig1data[:,1])), fig1data[:,10]].astype(np.float)

            resource_capacity = np.multiply(fig1data[:,6].astype(np.float),fig1data[:,7].astype(np.float))
            critical_path = np.multiply(fig1data[:,10].astype(np.float),fig1data[:,5].astype(np.float))
            #Old Normalized Job Completion Time: Job Completion Time / (Total Task Durations / (number_of_nodes * workers_per_node))
            fig1data_reduced = np.c_[np.column_stack((fig1data[:,5], fig1data[:,1])), np.divide(fig1data[:,10].astype(np.float),np.divide(fig1data[:,2].astype(np.float), resource_capacity))].astype(np.float)
            #Normalized Job Complegtion Time: Job Completion Time / Critical Path Time
            #fig1data_reduced = np.c_[np.column_stack((fig1data[:,5], fig1data[:,1])), np.divide(fig1data[:,10].astype(np.float),critical_path)].astype(np.float)           


            fig1sp_scatter[k].scatter(fig1data_reduced[:,0],fig1data_reduced[:,1],fig1data_reduced[:,2], c=colors.next(), marker=markers.next(), label=sched)

            xi = np.linspace(math.floor(unique_norm_critical_path.astype(np.float).min()),math.ceil(unique_norm_critical_path.astype(np.float).max()),20)
            yi = np.linspace(math.floor(unique_num_tasks.astype(np.float).min()),math.ceil(unique_num_tasks.astype(np.float).max()),20)
            zi = griddata((fig1data_reduced[:,0], fig1data_reduced[:,1]), fig1data_reduced[:,2], (xi[None,:], yi[:,None]), method = 'cubic')
            X, Y = np.meshgrid(xi, yi)
            fig1sp_cont[k].plot_wireframe(X, Y, zi, rstride = 1, cstride = 1,color=colors.next(), label=sched)


        fig1sp_scatter[k].set_xlabel('Normalized Critical Path [a.u]')
        fig1sp_scatter[k].set_ylabel('Load (Total Number of Tasks) [a.u]')
        #fig1sp_cont[k].set_zlabel('Job Completion Time [seconds]')
        fig1sp_scatter[k].set_zlabel('Normalized Job Completion Time [a.u]')
        fig1sp_scatter[k].set_title('Job Completion Time vs. DAG Load Parmeter for Data Transfer Cost ' + unique_data_transfer_cost[k])
        fig1sp_scatter[k].legend(shadow=True, fancybox=True)


        fig1sp_cont[k].set_xlabel('Normalized Critical Path [a.u]')
        fig1sp_cont[k].set_ylabel('Load (Total Number of Tasks) [a.u]')
        #fig1sp_cont[k].set_zlabel('Job Completion Time [seconds]')
        fig1sp_cont[k].set_zlabel('Normalized Job Completion Time [a.u]')
        fig1sp_cont[k].set_title('Job Completion Time vs. DAG Load Parmeter for Data Transfer Cost' + unique_data_transfer_cost[k])
        if k==0 :
            fig1sp_cont[k].legend(shadow=True, fancybox=True)



    #characterize the schedulers compared to workloads (assume environment parameters are constant)
    #assuming cluster size of: 10 nodes, 3 workers per node, data transfer cost 0.1
    #production graph: compare job completion time of different schedulers for RNN workloads  (2d plots: time vs. load size)
    #production graph: compare job completion time of different schedulers for rl-pong workloads (2d plots: time vs. load size)
    #production graph: compare job completion time of different schedulers for alexnet workloads (2d plots: time vs. load size)
    num_nodes = 10
    workers_per_node = 3
    dtc = 10
    fig2 = plt.figure()
    fig2sp = {}
    fig2.suptitle('Job Completion Time for a {} Nodes Cluster, with {} Workers per Node, and Data Transfer Cost of {}'.format(num_nodes, workers_per_node, dtc))

    #generalization loop:
    fig2sp_height = int(math.sqrt(len(workload_types)))
    fig2sp_width = len(workload_types) / fig2sp_height + len(workload_types)%fig2sp_height
    for wload_ind in range(len(workload_types)):
        fig2sp[wload_ind] = fig2.add_subplot(fig2sp_height,fig2sp_width,wload_ind+1)
        for sched in scheduler_types:
            fig2data = sim_sweep_csv[np.logical_and( \
                                     np.logical_and( \
                                     np.logical_and( \
                                     np.logical_and( \
                                     np.logical_and( \
                                     sim_sweep_csv[:,0]==workload_types[wload_ind], \
                                     sim_sweep_csv[:,9]==sched), \
                                     sim_sweep_csv[:,6].astype(np.float)==num_nodes), \
                                     sim_sweep_csv[:,7].astype(np.float)==workers_per_node), \
                                     sim_sweep_csv[:,8].astype(np.float)==dtc), \
                                     sim_sweep_csv[:,10]!='-1')]
            fig2data_reduced = np.column_stack((fig2data[:,1], fig2data[:,10])).astype(np.float)
            fig2sp[wload_ind].scatter(fig2data_reduced[:,0], fig2data_reduced[:,1], c=colors.next(), marker=markers.next(), label=sched)

        fig2sp[wload_ind].set_xlabel('Job Load Size (number of tasks) [a.u]')
        fig2sp[wload_ind].set_ylabel('Job Completion Time [seconds]')
        fig2sp[wload_ind].set_title('Job Completion Time vs. Job Load Size: {} Workload'.format(workload_types[wload_ind]))
        fig2sp[wload_ind].legend(shadow=True, fancybox=True)






    #production graph: compare job completion time of different schedulers for different workloads of the same size (same number of tasks). (2d bar plot: time vs. workload type. assume all have total tasks num 200)
    fig3 = plt.figure()
    fig3_plt = fig3.add_subplot(111)
    N = len(workload_types)
    ind = np.arange(N)
    width = 0.2
    sched_bar = {}
    for sched_ind in range(len(scheduler_types)):
        fig3data = sim_sweep_csv[np.logical_and( \
                                 np.logical_and( \
                                 np.logical_and( \
                                 np.logical_and( \
                                 np.logical_and( \
                                 np.logical_and( \
                                 sim_sweep_csv[:,9]==scheduler_types[sched_ind], \
                                 sim_sweep_csv[:,6].astype(np.float)==num_nodes), \
                                 sim_sweep_csv[:,7].astype(np.float)==workers_per_node), \
                                 sim_sweep_csv[:,8].astype(np.float)==dtc), \
                                 sim_sweep_csv[:,1].astype(np.float) >200), \
                                 sim_sweep_csv[:,1].astype(np.float) <250), \
                                 sim_sweep_csv[:,10]!='-1')]
        #fig3data_reduced = np.column_stack((fig3data[:,1], fig3data[:,10])).astype(np.float)
        resource_capacity = np.multiply(fig3data[:,6].astype(np.float),fig3data[:,7].astype(np.float))
        #Old Normalized Job Completion Time: Job Completion Time / (Total Task Durations / (number_of_nodes * workers_per_node))
        #fig3data_reduced = np.column_stack((fig3data[:,1], np.divide(fig3data[:,10].astype(np.float),np.divide(fig3data[:,2].astype(np.float), resource_capacity)))).astype(np.float)
        #Critical Path Time = Normalized Critical Path * Total Task Durations
        critical_path = np.multiply(fig3data[:,10].astype(np.float),fig3data[:,5].astype(np.float))
        #Normalized Job Complegtion Time: Job Completion Time / Critical Path Time
        fig3data_reduced = np.column_stack((fig3data[:,1], np.divide(fig3data[:,10].astype(np.float),critical_path))).astype(np.float)
        sched_bar[sched_ind] = fig3_plt.bar(ind+sched_ind*width, fig3data_reduced[:,1], width, color=colors.next(), label=scheduler_types[sched_ind])
    fig3_plt.set_xticks(ind+len(scheduler_types)*width)
    fig3_plt.set_xticklabels(workload_types)
    #fig3_plt.set_ylabel('Job Completion Time [seconds]')

    fig3_plt.set_ylabel('Normalized Job Completion Time [a.u]')
    fig3_plt.set_title('Job Completion Time vs. Workloads and Schedulers')
    fig3_plt.legend(shadow=True, fancybox=True)

 


    #characterize the schedulers compared to environment (assume workload parameters are constant)
    #assuming an RNN, rl-pong, alexnet workload of size (number of tasks) X
    #production graph: compare job completion time of different schedulers for RNN workloads data transfer cost 0.1 (3d plot: time vs. num_nodes vs. num_workers_per_node)
    #production graph: compare job completion time of different schedulers for rl-pong workloads data transfer cost 0.1 (3d plot: time vs. num_nodes vs. num_workers_per_node)
    #production graph: compare job completion time of different schedulers for alexnet workloads data transfer cost 0.1 (3d plot: time vs. num_nodes vs. num_workers_per_node)

    fig4_scatter = plt.figure()
    fig4_cont = plt.figure()
    fig4sp_scatter = {}
    fig4sp_cont = {}
    fig4_scatter.suptitle('Job Completion Time for Workloads of size 200 Tasks')
    fig4_cont.suptitle('Job Completion Time for Workloads of size 200 Tasks')

    #generalization loop:
    fig4sp_height = int(math.sqrt(len(workload_types)))
    fig4sp_width = len(workload_types) / fig4sp_height + len(workload_types)%fig4sp_height
    for wload_ind in range(len(workload_types)):
        fig4sp_scatter[wload_ind] = fig4_scatter.add_subplot(fig4sp_height,fig4sp_width,wload_ind+1, projection='3d')
        fig4sp_cont[wload_ind] = fig4_cont.add_subplot(fig4sp_height,fig4sp_width,wload_ind+1, projection='3d')
        for sched in scheduler_types:
            fig4data = sim_sweep_csv[np.logical_and( \
                                     np.logical_and( \
                                     np.logical_and( \
                                     np.logical_and( \
                                     np.logical_and( \
                                     sim_sweep_csv[:,0]==workload_types[wload_ind], \
                                     sim_sweep_csv[:,9]==sched), \
                                     sim_sweep_csv[:,8].astype(np.float)==dtc), \
                                     sim_sweep_csv[:,1].astype(np.float) >200), \
                                     sim_sweep_csv[:,1].astype(np.float) <250), \
                                     sim_sweep_csv[:,10]!='-1')]
            fig4data_reduced = np.c_[np.column_stack((fig4data[:,6], fig4data[:,7])), fig4data[:,10]].astype(np.float)

            fig4sp_scatter[wload_ind].scatter(fig4data_reduced[:,0], fig4data_reduced[:,1], fig4data_reduced[:,2],c=colors.next(), marker=markers.next(), label=sched)

            xi = np.linspace(math.floor(unique_num_nodes.astype(np.float).min()),math.ceil(unique_num_nodes.astype(np.float).max()),20)
            yi = np.linspace(math.floor(unique_workers_per_node.astype(np.float).min()),math.ceil(unique_workers_per_node.astype(np.float).max()),20)
            zi = griddata((fig4data_reduced[:,0], fig4data_reduced[:,1]), fig4data_reduced[:,2], (xi[None,:], yi[:,None]), method = 'cubic')
            X, Y = np.meshgrid(xi, yi)
            fig4sp_cont[wload_ind].plot_wireframe(X, Y, zi, rstride = 1, cstride = 1,color=colors.next(), label=sched)


        fig4sp_scatter[wload_ind].set_xlabel('Number of Nodes [a.u]')
        fig4sp_scatter[wload_ind].set_ylabel('Number of Workers per Node [a.u]')
        fig4sp_scatter[wload_ind].set_zlabel('Job Completion Time [seconds]')
        fig4sp_scatter[wload_ind].set_title('Job Completion Time vs. Cluster Parameters: {} Workload'.format(workload_types[wload_ind]))
        fig4sp_scatter[wload_ind].legend(shadow=True, fancybox=True)

        fig4sp_cont[wload_ind].set_xlabel('Number of Nodes [a.u]')
        fig4sp_cont[wload_ind].set_ylabel('Number of Workers per Node [a.u]')
        fig4sp_cont[wload_ind].set_zlabel('Job Completion Time [seconds]')
        fig4sp_cont[wload_ind].set_title('Job Completion Time vs. Cluster Parameters: {} Workload'.format(workload_types[wload_ind]))
        if wload_ind==0 :
            fig4sp_cont[wload_ind].legend(shadow=True, fancybox=True)



    plt.show()


if __name__ == '__main__':
    drawplots(sys.argv)
