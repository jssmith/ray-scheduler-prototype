import replaystate
from trivialscheduler import TrivialScheduler
import json

import sys

def run_tests(args):
    if len(args) != 2:
        print 'Usage: test_scheduler input.json'
        sys.exit(1)

    input_fn = args[1]
    print input_fn
    f = open(input_fn, 'r')
    computation = json.load(f, object_hook=replaystate.computation_decoder)
    f.close()

    system_time = replaystate.SystemTime()
    scheduler_db = replaystate.ReplaySchedulerDatabase(system_time, computation)
    scheduler = TrivialScheduler(system_time, scheduler_db)
    scheduler.run()

if __name__ == '__main__':
    run_tests(sys.argv)