#!/bin/bash


#part 1: run real ray with sweep over number of workload type, tasks, scale (these all need to be run with 1 worker, since they are just for generating a test
#part 2: generate trace for each of the previous runs 
#part 3: sweep over simulator (sweet over different schedulers, number of nodes, number of workers per node, data_transfer_cost

declare -a SCHEDULERS=("trivial" "location_aware" "transfer_aware" "delay" "trivial_local" "trivial_threshold_local" "location_aware_local" "transfer_aware_local" "transfer_aware_threshold_local") 

min_num_node_range=3
max_num_node_range=10
min_num_worker_range=2
max_num_worker_range=5

db_delay=0.0005

min_task_factor_range=1
max_task_factor_range=10
min_scale_factor_range=1
max_scale_factor_range=5


now=$(date +"%m_%d_%Y_%H_%M_%S")
sim_sweep_v_output_file="sim_sweep_verbose.csv"
if [ -f $sim_sweep_v_out_file ] ; then
  #rm $sim_sweep_v_output_file
  mv ${sim_sweep_v_output_file} ${sim_sweep_v_output_file}_${now}.csv
fi


sim_sweep_output_file="sim_sweep.csv"
if [ -f $sim_sweep_out_file ] ; then
  #rm $sim_sweep_output_file
  mv ${sim_sweep_output_file} ${sim_sweep_output_file}_${now}.csv
fi

#csv title
echo "workload,task_factor,scale,total_num_tasks,total_task_durations,total_num_objects,total_object_sizes,norm_critical_path,num_nodes,worker_per_node,total_workers,data_transfer_cost,scheduler,total job completion" | paste -sd ',' >> $sim_sweep_v_output_file
echo "workload,total_num_tasks,total_task_durations,total_num_objects,total_object_sizes,norm_critical_path,num_nodes,worker_per_node,data_transfer_cost,scheduler,total job completion" | paste -sd ',' >> $sim_sweep_output_file


dot="$(cd "$(dirname "$0")"; pwd)"
mkdir -p $dot/traces/sweep/


#******************external real Ray sweep******************************
###############################rnn########################################
#for t in `seq $min_task_factor_range $max_task_factor_range` #number of tasks factor (in the case of rnn this is num_steps_
#do
#    for s in `seq $min_scale_factor_range $max_scale_factor_range` #scale factor (object sizes)
#    do
#       rm -r /tmp/raylogs/*
#       echo running ray_rnn with 1 workers $t steps \(tasks factor\) and scale \(object size factor\) $((s*5))
#       python $dot/workloads/rnn/rnn_ray_loop.py -w 1 -s $s -n $t
#       #generate trace
#       echo generating trace of ray_rnn with 1 worker $t steps \(tasks factor\) and scale \(object size factor\) $((s*5))
#       python build_trace.py /tmp/raylogs
#       mv trace.json $dot/traces/sweep/trace_rnn_t${t}_s${s}.json
#    done
#done
#

##############################rl-pong########################################
#external real Ray sweep
#for t in `seq $min_task_factor_range $max_task_factor_range` #number of tasks factor (in the case of rnn this is num_steps_
#do
#    for s in `seq $min_scale_factor_range $max_scale_factor_range` #scale factor (object sizes)
#    do
#       rm -r /tmp/raylogs/*
#       echo running rl-pong with 1 workers $((t*10)) iterations \(tasks factor\) and scale \(object size factor\) $((s*5))
#       python $dot/workloads/rl_pong/driver.py --iterations $((t*10)) --workers 10
#       #generate trace
#       echo generating trace of rl-pong with 10 workers $((t*10)) iterations \(tasks factor\) and scale \(object size factor\) $((s*5))
#       python build_trace.py /tmp/raylogs
#       mv trace.json $dot/traces/sweep/trace_rl-pong_t${t}_s${s}.json
#    done
#done


##############################alexnet########################################
#for t in `seq $min_task_factor_range $max_task_factor_range` #number of tasks factor (in the case of rnn this is num_steps_
#do
#    for s in `seq 4 8` #scale factor (object sizes)
#    do
#       rm -r /tmp/raylogs/*
#       echo running alexnet with 1 workers $t iterations \(tasks factor\) and scale \(object size factor\) $s
#       python $dot/workloads/alexnet/driver.py --iterations $t --num_batches $s
#       #generate trace
#       echo generating trace of alexnet with $t iterations \(tasks factor\) and scale \(object size factor\) $s
#       python build_trace.py /tmp/raylogs
#       mv trace.json $dot/traces/sweep/trace_alexnet_t${t}_s${s}.json
#    done
#done


##############################matrix multipication########################################
#for t in `seq $min_task_factor_range $max_task_factor_range` #number of tasks factor (in the case of matrix multipication this is the size of the matrix)
#do
#    for s in `seq 1 3` #scale factor (object sizes)
#    do
#       rm -r /tmp/raylogs/*
#       echo running matrix multipication with 2 workers, matrix size $((t*100)) \(tasks factor\) and block size \(object size factor\) $((s*10))
#       python $dot/workloads/mat_mult.py --size $((t*100)) --block-size $((s*10))
#       #generate trace
#       echo generating trace of matrix multipication with matrix size $((t*100)) \(tasks factor\) and block size \(object size factor\) $((s*10))
#       python build_trace.py /tmp/raylogs
#       mv trace.json $dot/traces/sweep/trace_mat-mult_t${t}_s${s}.json
#    done
#done


#***************internal simulator sweep**********************************

for filename in $dot/traces/sweep/*.json; do
       workload_name=`echo $filename | cut -d_ -f2`
       t=`echo $filename | cut -d_ -f3`
       s=`echo $filename | cut -d_ -f4 | cut -d. -f1`
       for sched in ${SCHEDULERS[@]}
       do
          for n in `seq $min_num_node_range $max_num_node_range` #number of nodes
          do
             for w in `seq $min_num_worker_range $max_num_worker_range` #number of workers per node
             do
                for dtc_log in `seq -7 0` #data transfer cost as powers of 10: 0.00001, 0.0001, 0.001, 0.01, 0.1 etc.
                do
                   dtc=$(awk "BEGIN{print 10 ^ $dtc_log}")
                   echo running ray-scheduler-prototype on $filename trace with $sched scheduling policy, $n nodes, $w workers per node, $dtc data transfer cost, and $db_delay db delay
                   sim_result=`python replaytrace.py $n $w $dtc $db_delay $sched $filename 2>&1 | tail -n1`
                   sim_time_result=`echo $sim_result | cut -d: -f1`
                   total_tasks_num=`echo $sim_result | cut -d: -f2`
                   total_task_durations=`echo $sim_result | cut -d: -f3`
                   total_num_objects=`echo $sim_result | cut -d: -f4`
                   total_object_sizes=`echo $sim_result | cut -d: -f5`
                   norm_critical_path=`echo $sim_result | cut -d: -f6`
                   echo $workload_name, $t, $s, $total_tasks_num, $total_task_durations, $total_num_objects, $total_object_sizes, $norm_critical_path, $n, $w, $(( $n*$w )), $dtc, $sched, $sim_time_result | paste -sd ',' >> $sim_sweep_v_output_file 
                   echo $workload_name, $total_tasks_num, $total_task_durations, $total_num_objects, $total_object_sizes, $norm_critical_path, $n, $w, $dtc, $sched, $sim_time_result | paste -sd ',' >> $sim_sweep_output_file 
                done
             done
          done
       done
done


#############################################################################################################
#call plotting script
#`python ray_sched_plots.py $dot/sim_sweep.csv`
 
