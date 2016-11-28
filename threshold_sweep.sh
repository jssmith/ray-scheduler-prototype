#!/bin/bash

trace_file="trace_rnn_t3_s2.json"
workload_name=`echo $trace_file | cut -d_ -f2`


db_delay=0.00000005
dtc=0.0000001
n=5
w=3

min_t1l_range=0
max_t1l_range=5
t1l_step=0.2
min_t1h_range=0
max_t1h_range=6
t1h_step=0.5
min_t2_range=0
max_t2_range=10
t2_step=1


#min_t1l_range=0
#max_t1l_range=0.1
#t1l_step=0.1
#min_t1h_range=0
#max_t1h_range=0.1
#t1h_step=0.1
#min_t2_range=1000
#max_t2_range=1100
#t2_step=100


threshold_sweep_output_file="threshold_sweep.csv"
if [ -f $threshold_sweep_out_file ] ; then
  rm $threshold_sweep_output_file
fi

#csv title
echo "workload,total_num_tasks,total_task_durations,total_num_objects,total_object_sizes,norm_critical_path,num_nodes,worker_per_node,data_transfer_cost,scheduler,t1l,t1h,t2total job completion" | paste -sd ',' >> $threshold_sweep_output_file


##############################rnn########################################
          for t1l in `seq $min_t1l_range $t1l_step $max_t1l_range`
          do
             for t1h in `seq $min_t1h_range $t1h_step $max_t1h_range`
             do
                for t2 in `seq $min_t2_range $t2_step $max_t2_range`
                do
                   if [ `bc<<<"$t1l < $t1h"` ] ; then
		           export RAY_SCHED_THRESHOLD1L=$t1l
		           export RAY_SCHED_THRESHOLD1H=$t1h
		           export RAY_SCHED_THRESHOLD2=$t2
		           echo $RAY_SCHED_THRESHOLD1L
		           echo $RAY_SCHED_THRESHOLD1H
		           echo $RAY_SCHED_THRESHOLD2
		           dot="$(cd "$(dirname "$0")"; pwd)"
		           echo running ray-scheduler-prototype on $workload_name trace with basic_threshold scheduling policy, 5 nodes, 2 workers per node, $dtc data transfer cost, and $db_delay db delay
		           sim_result=`python replaytrace.py $n $w $dtc $db_delay basic_threshold $dot/$trace_file 2>&1 | tail -n1` 
		           echo $sim_result
		           sim_time_result=`echo $sim_result | cut -d: -f1`
		           total_tasks_num=`echo $sim_result | cut -d: -f2`
		           total_task_durations=`echo $sim_result | cut -d: -f3`
		           total_num_objects=`echo $sim_result | cut -d: -f4`
		           total_object_sizes=`echo $sim_result | cut -d: -f5`
		           norm_critical_path=`echo $sim_result | cut -d: -f6`
		           echo $workload_name, $total_tasks_num, $total_task_durations, $total_num_objects, $total_object_sizes, $norm_critical_path, $n, $w, $dtc, basic_threshold, $t1l, $t1h, $t2, $sim_time_result | paste -sd ',' >> $threshold_sweep_output_file 
                   fi
                done
             done
          done

