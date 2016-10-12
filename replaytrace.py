import replaystate
from trivialscheduler import *
import json

import sys

def usage():
    print 'Usage: test_scheduler num_nodes num_workers_per_node transfer_time_cost scheduler input.json'

def run_tests(args):
    if len(args) != 6:
        usage()
        sys.exit(1)

    num_nodes = int(args[1])
    num_workers_per_node = int(args[2])
    transfer_time_cost = float(args[3])
    schedulers = {
        'trivial' : TrivialScheduler,
        'location_aware' : LocationAwareScheduler,
        'trivial_local' : TrivialLocalScheduler
    }
    scheduler_str = args[4]
    if scheduler_str not in schedulers.keys():
        usage()
        print 'Error - unrecognized scheduler'
        sys.exit(1)
    input_fn = args[5]
    print input_fn
    f = open(input_fn, 'r')
    computation = json.load(f, object_hook=replaystate.computation_decoder)
    f.close()

    system_time = replaystate.SystemTime()
    scheduler_db = replaystate.ReplaySchedulerDatabase(system_time, computation, num_nodes, num_workers_per_node, transfer_time_cost)
    scheduler = schedulers[scheduler_str](system_time, scheduler_db)
    scheduler.run()

if __name__ == '__main__':
    run_tests(sys.argv)