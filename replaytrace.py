import replaystate
from trivialscheduler import *
import json

import sys
import logging
import imp

schedulers = {
    'TRIVIAL' : TrivialScheduler,
    'LOCATION_AWARE' : LocationAwareScheduler,
    'TRIVIAL_LOCAL' : TrivialLocalScheduler,
    'DELAY' : DelayScheduler,
    'TRANSFER_AWARE' : TransferCostAwareScheduler,
    'BASIC_THRESHOLD' : TrivialThresholdLocalScheduler,
    'TRANSFER_AWARE_LOCAL' : TransferCostAwareLocalScheduler
}

def usage():
    print("Usage: python replaytrace.py <config filename>; example config can "
          "be found in default_config.py")
    print("OR test_scheduler num_nodes num_workers_per_node "
          "object_transfer_time_cost db_message_delay scheduler input.json")


def simulate(computation, scheduler_type, system_time, logger, num_nodes, num_workers_per_node, object_transfer_time_cost, db_message_delay):
    object_store = replaystate.ObjectStoreRuntime(system_time, object_transfer_time_cost)
    scheduler_db = replaystate.ReplaySchedulerDatabase(system_time, logger, computation, num_nodes, num_workers_per_node, object_transfer_time_cost, db_message_delay)
    schedulers = scheduler_type(system_time, scheduler_db)
    global_scheduler = schedulers.get_global_scheduler(replaystate.EventLoop(system_time))
    local_runtimes = {}
    local_schedulers = {}
    for node_id in range(0, num_nodes):
        local_runtime = replaystate.NodeRuntime(system_time, object_store, logger, computation, node_id, num_workers_per_node)
        local_runtimes[node_id] = local_runtime
        local_event_loop = replaystate.EventLoop(system_time)
        local_schedulers[node_id] = schedulers.get_local_scheduler(local_runtime, local_event_loop)
    scheduler_db.schedule_root(0)
    system_time.advance_fully()
    num_workers_executing = 0
    for node_id, local_runtime in local_runtimes.items():
        num_workers_executing += local_runtime.num_workers_executing
    if num_workers_executing > 0:
        pylogger = logging.getLogger(__name__+'.simulate')
        pylogger.debug("failed to execute fully".format(
            num_workers_executing),
            extra={'timestamp':system_time.get_time()})
        print "{:.6f}: Simulation Error. Total Number of Tasks: {}, DAG Normalized Critical Path: {}, Total Tasks Durations: {}".format(system_time.get_time(), computation.total_num_tasks, computation.normalized_critical_path, computation.total_tasks_durations)
        print "-1: {} : {} : {} : {} : {}".format(system_time.get_time(), computation.total_num_tasks, computation.total_tasks_durations, computation.total_num_objects, computation.total_objects_size, computation.normalized_critical_path)
        return False
    else:
        print "{:.6f}: Simulation finished successfully. Total Number of Tasks: {}, DAG Normalized Critical Path: {}, Total Tasks Durations: {}".format(system_time.get_time(), computation.total_num_tasks, computation.normalized_critical_path, computation.total_tasks_durations)
        print "{:.6f}: {} : {} : {} : {} : {}".format(system_time.get_time(), computation.total_num_tasks, computation.total_tasks_durations, computation.total_num_objects, computation.total_objects_size, computation.normalized_critical_path)
        return True

def setup_logging():
    logging_format = '%(timestamp).6f %(name)s %(message)s'
    logging.basicConfig(format=logging_format)
    logging.getLogger().setLevel(logging.DEBUG)

def run_replay(num_nodes, num_workers_per_node, object_transfer_time_cost,
               db_message_delay, scheduler_name, trace_filename):
    scheduler_cls = schedulers.get(scheduler_name)
    if scheduler_cls is None:
        print 'Error - unrecognized scheduler'
        sys.exit(1)
    f = open(trace_filename, 'r')
    computation = json.load(f, object_hook=replaystate.computation_decoder)
    f.close()

    setup_logging()
    system_time = replaystate.SystemTime()
    logger = replaystate.PrintingLogger(system_time)
    simulate(computation, scheduler_cls, system_time, logger, num_nodes,
             num_workers_per_node, object_transfer_time_cost, db_message_delay)

def run_replay_from_sys_argv(args):
    num_nodes = int(args[1])
    num_workers_per_node = int(args[2])
    object_transfer_time_cost = float(args[3])
    db_message_delay = float(args[4])
    scheduler_name = args[5]
    trace_filename = args[6]
    run_replay(num_nodes, num_workers_per_node, object_transfer_time_cost,
               db_message_delay, scheduler_name, trace_filename)

def run_replay_from_config(config_filename):
    import default_config
    config = imp.load_source("config", config_filename)

    num_nodes = getattr(config, 'NUM_NODES', default_config.NUM_NODES)
    num_workers_per_node = getattr(config, 'NUM_WORKERS_PER_NODE',
                                   default_config.NUM_WORKERS_PER_NODE)
    object_transfer_time_cost = getattr(config, 'OBJECT_TRANSFER_TIME_COST',
                                        default_config.OBJECT_TRANSFER_TIME_COST)
    db_message_delay = getattr(config, 'DB_MESSAGE_DELAY',
                               default_config.DB_MESSAGE_DELAY)
    scheduler_name = getattr(config, 'SCHEDULER_NAME',
                             default_config.SCHEDULER_NAME)
    trace_filename = getattr(config, 'TRACE_FILENAME',
                             default_config.TRACE_FILENAME)
    run_replay(num_nodes, num_workers_per_node, object_transfer_time_cost,
               db_message_delay, scheduler_name, trace_filename)

if __name__ == '__main__':
    if len(sys.argv) == 2:
        run_replay_from_config(sys.argv[1])
    elif len(sys.argv) == 7:
        run_replay_from_sys_argv(sys.argv)
    else:
        usage()
        sys.exit(-1)
