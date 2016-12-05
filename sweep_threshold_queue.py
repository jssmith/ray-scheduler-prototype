import sys
import ec2config

from sweep_queue import enqueue

def sweep_threshold_queue(schedulers, t1l_range, experiment_name, tracefile):
    num_nodes = 4

    conn = ec2config.sqs_connect()
    queue = conn.get_queue(ec2config.sqs_sweep_queue)

    for scheduler in schedulers:
        for t1l in t1l_range:
            env = {
                'RAY_SCHED_THRESHOLD1L': t1l
            }
            enqueue(queue, num_nodes, scheduler, tracefile, experiment_name, env)

def usage():
    print 'Usage: sweep_threshold_queue.py experiment_name tracefile'
    print '    e.g., sweep_threshold_queue.py "trivial_threshold_local transfer_aware_threshold_local location_aware_threshold_local" my-experiment-1 trace.json'

if __name__ == '__main__':
    if len(sys.argv) == 4:
        schedulers = str.split(sys.argv[1])
        experiment_name = sys.argv[2]
        tracefile = sys.argv[3]
        sweep_threshold_queue(schedulers, range(0,16), experiment_name, tracefile)
    else:
        usage()
        sys.exit(-1)
