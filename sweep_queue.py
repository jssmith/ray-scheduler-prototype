import boto.sqs
import json
import math
import sys
import ec2config

from boto.sqs.message import Message

def enqueue(queue, num_nodes, scheduler, tracefile, experiment_name):
    m = Message()
    s = json.dumps({
            'scheduler': scheduler,
            'num_nodes': int(num_nodes),
            'num_workers_per_node': 4,
            'object_transfer_time_cost': .00000001,
            'db_message_delay': .001,
            'validate': 'false',
            'tracefile': str(tracefile),
            'experiment_name': str(experiment_name)
        })
    print s
    m.set_body(s)
    queue.write(m)


def queue_sweep(args):
    min_nodes = int(args[1])
    max_nodes = int(args[2])
    node_step_mult = float(args[3])
    schedulers = str.split(args[4])
    experiment_name = args[5]
    tracefile = args[6]

    node_cts = []
    nodes_f = float(min_nodes)
    nodes = min_nodes
    while nodes <= max_nodes:
        node_cts.append(nodes)
        nodes_f *= node_step_mult
        nodes = math.ceil(nodes_f)

    print 'Schedulers:', schedulers
    print 'Num nodes:', node_cts
    print 'Tracefile:', tracefile

    conn = ec2config.sqs_connect()
    queue = conn.get_queue(ec2config.sqs_sweep_queue)

    for scheduler in schedulers:
        for num_nodes in node_cts:
            enqueue(queue, num_nodes, scheduler, tracefile, experiment_name)


def usage():
    print 'Usage: queue_sweep.py min_nodes max_nodes node_step_mult schedulers experiment_name tracefile'
    print '    e.g., queue_sweep.py 1 16 2 "trivial location_aware" my-experiment-1 trace.json'

if __name__ == '__main__':
    if len(sys.argv) >= 7:
        queue_sweep(sys.argv)
    else:
        usage()
        sys.exit(-1)
