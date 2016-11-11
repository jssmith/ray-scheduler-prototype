#!/bin/bash


timing_output_file="timing_monolitic.csv"
if [ -f $timing_out_file ] ; then
  rm $timing_output_file
fi


echo "workers,scale,num_steps,1 layer average time,1 layer 90th precntile,1 layer worst time," \
          "2 layers average time,2 layers 90th precntile,2 layers worst time," \
          "3 layers average time,3 layers 90th precntile,3 layers worst time," \
          "4 layers average time,4 layers 90th precntile,4 layers worst time," \
          "5 layers average time,5 layers 90th precntile,5 layers worst time," \
          "6 layers average time,6 layers 90th precntile,6 layers worst time" \
          | paste -sd ',' >> $timing_output_file

for k in `seq 1 2` 
do
  for n in `seq 1 5`
  do
    for s in `seq 1 5`
    do
       echo running ray_rnn_monolithic with $k workers $n steps and scale $((s*5))
       #python rnn_ray_loop.py -w $k -s $s
       python rnn_monolithic_loop.py -w $k -s $s -n $n | tail -n2 | head -n1 | paste -sd ',' >> $timing_output_file 
    done
  done
done
 
