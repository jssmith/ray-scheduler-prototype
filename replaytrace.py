import replaystate
from trivialscheduler import TrivialScheduler
import json

import sys

def run_tests(args):
    if len(args) != 4:
        print 'Usage: test_scheduler num_nodes num_workers_per_node input.json'
        sys.exit(1)

    num_nodes = int(args[1])
    num_workers_per_node = int(args[2])
    input_fn = args[3]
    print input_fn
    f = open(input_fn, 'r')
    computation = json.load(f, object_hook=replaystate.computation_decoder)
    f.close()

    system_time = replaystate.SystemTime()
    scheduler_db = replaystate.ReplaySchedulerDatabase(system_time, computation, num_nodes, num_workers_per_node)
    scheduler = TrivialScheduler(system_time, scheduler_db)
    scheduler.run()

if __name__ == '__main__':
    run_tests(sys.argv)