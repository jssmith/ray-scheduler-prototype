import replaystate
from trivialscheduler import TrivialScheduler
import json

import sys

def usage():
    print 'Usage: test_scheduler num_nodes num_workers_per_node location_aware input.json'

def run_tests(args):
    if len(args) != 5:
        usage()
        sys.exit(1)

    num_nodes = int(args[1])
    num_workers_per_node = int(args[2])
    location_aware_str = args[3]
    if location_aware_str == 'true':
        location_aware = True
    elif location_aware_str == 'false':
        location_aware = False
    else:
        usage()
        print 'Error - location_aware must be \'true\' or \'false\''
        sys.exit(1)
    input_fn = args[4]
    print input_fn
    f = open(input_fn, 'r')
    computation = json.load(f, object_hook=replaystate.computation_decoder)
    f.close()

    system_time = replaystate.SystemTime()
    scheduler_db = replaystate.ReplaySchedulerDatabase(system_time, computation, num_nodes, num_workers_per_node)
    scheduler = TrivialScheduler(system_time, scheduler_db, location_aware)
    scheduler.run()

if __name__ == '__main__':
    run_tests(sys.argv)