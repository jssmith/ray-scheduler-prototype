import boto.sqs
import json
import math
import sys
import ec2config
import json
import uuid
import hashlib

from boto.sqs.message import Message

def enqueue(queue, num_nodes, scheduler, tracefile, experiment_name, env={}, object_transfer_time_cost=.00000001, cache_policy='lru'):
    m = Message()
    s = json.dumps({
            'replay_id': new_id(),
            'scheduler': scheduler,
            'cache_policy': cache_policy,
            'num_nodes': int(num_nodes),
            'num_workers_per_node': 4,
            'object_transfer_time_cost': object_transfer_time_cost,
            'db_message_delay': .001,
            'validate': 'false',
            'tracefile': str(tracefile),
            'experiment_name': str(experiment_name),
            'env': env
        })
    print s
    m.set_body(s)
    queue.write(m)


def sweep_queue(min_nodes, max_nodes, nodes_step, schedulers, experiment_name, tracefile, object_transfer_time_cost=.00000001):
    node_cts = range(min_nodes, max_nodes + 1, nodes_step)

    print 'Schedulers:', schedulers
    print 'Num nodes:', node_cts
    print 'Tracefile:', tracefile

    conn = ec2config.sqs_connect()
    queue = conn.get_queue(ec2config.sqs_sweep_queue)

    for scheduler in schedulers:
        for num_nodes in node_cts:
            enqueue(queue, num_nodes, scheduler, tracefile, experiment_name, object_transfer_time_cost=object_transfer_time_cost)

def new_id():
    return hashlib.sha1(str(uuid.uuid1())).hexdigest()[:8]


def usage():
    print 'Usage: sweep_queue.py min_nodes max_nodes node_step_mult schedulers experiment_name tracefile'
    print '    e.g., sweep_queue.py 1 16 2 "trivial location_aware" my-experiment-1 trace.json'

if __name__ == '__main__':
    if len(sys.argv) >= 7:
        min_nodes = int(args[1])
        max_nodes = int(args[2])
        nodes_step = int(args[3])
        schedulers = str.split(args[4])
        experiment_name = args[5]
        tracefile = args[6]

        sweep_queue(min_nodes, max_nodes, nodes_step, schedulers, experiment_name, tracefile)
    else:
        usage()
        sys.exit(-1)
