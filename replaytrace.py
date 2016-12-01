import replaystate
from trivialscheduler import *
import json
import gzip

import sys
import imp

from helpers import TimestampedLogger
import statslogging

schedulers = {
    'trivial' : TrivialScheduler,
    'location_aware' : LocationAwareScheduler,
    'delay' : DelayScheduler,
    'transfer_aware' : TransferCostAwareScheduler,
    'trivial_local' : TrivialLocalScheduler,
    'trivial_threshold_local' : TrivialThresholdLocalScheduler,
    'transfer_aware_local' : TransferCostAwareLocalScheduler,
    'transfer_aware_threshold_local' : TransferCostAwareLocalScheduler,
    'location_aware_local' : LocationAwareLocalScheduler,
    'location_aware_threshold_local' : LocationAwareThresholdLocalScheduler
}

def usage():
    print("Usage: python replaytrace.py <config filename>; example config can "
          "be found in default_config.py")
    print("OR test_scheduler num_nodes num_workers_per_node "
          "object_transfer_time_cost db_message_delay scheduler "
          "enable_verification=<true|false> input.json")
    print("Available Schedulers: %s" %schedulers.keys())


def simulate(computation, scheduler_cls, event_simulation, logger, num_nodes,
             num_workers_per_node, object_transfer_time_cost, db_message_delay,
             global_scheduler_kwargs=None, local_scheduler_kwargs=None,
             enable_analysis=False):
    if global_scheduler_kwargs is None:
        global_scheduler_kwargs = {}
    if local_scheduler_kwargs is None:
        local_scheduler_kwargs = {}
    object_store = replaystate.ObjectStoreRuntime(event_simulation,
                                                  logger,
                                                  object_transfer_time_cost,
                                                  db_message_delay)
    scheduler_db = replaystate.ReplaySchedulerDatabase(event_simulation, logger, computation, num_nodes, num_workers_per_node, object_transfer_time_cost, db_message_delay)
    local_nodes = {}
    local_runtimes = {}
    for node_id in range(0, num_nodes):
        local_runtime = replaystate.NodeRuntime(event_simulation, object_store,
                                                logger, computation, node_id,
                                                num_workers_per_node)
        local_event_loop = replaystate.EventLoop(event_simulation)
        local_nodes[node_id] = (local_runtime, local_event_loop)
        local_runtimes[node_id] = local_runtime
    schedulers = scheduler_cls(replaystate.SystemTime(event_simulation), scheduler_db,
                               replaystate.EventLoop(event_simulation),
                               global_scheduler_kwargs, local_scheduler_kwargs,
                               local_nodes=local_nodes)
    global_scheduler = schedulers.get_global_scheduler()
    scheduler_db.schedule_root(0)
    event_simulation.advance_fully()
    num_workers_executing = 0
    for node_id, local_runtime in local_runtimes.items():
        num_workers_executing += local_runtime.num_workers_executing
    if enable_analysis:
        total_num_tasks, normalized_critical_path, total_tasks_durations, total_num_objects, total_objects_size = computation.analyze()
    else:
        total_num_tasks, normalized_critical_path, total_tasks_durations, total_num_objects, total_objects_size = [-1] * 5
    if num_workers_executing > 0:
        pylogger = TimestampedLogger(__name__+'.simulate', event_simulation)
        pylogger.debug("failed to execute fully".format(num_workers_executing))
        print "{:.6f}: Simulation Error. Total Number of Tasks: {}, DAG Normalized Critical Path: {}, Total Tasks Durations: {}".format(event_simulation.get_time(), total_num_tasks, normalized_critical_path, total_tasks_durations)
        print "-1: {} : {} : {} : {} : {}".format(event_simulation.get_time(), total_num_tasks, total_tasks_durations, total_num_objects, total_objects_size, normalized_critical_path)
        return False
    else:
        logger.job_ended()
        print "{:.6f}: Simulation finished successfully. Total Number of Tasks: {}, DAG Normalized Critical Path: {}, Total Tasks Durations: {}".format(event_simulation.get_time(), total_num_tasks, normalized_critical_path, total_tasks_durations)
        print "{:.6f}: {} : {} : {} : {} : {}".format(event_simulation.get_time(), total_num_tasks, total_tasks_durations, total_num_objects, total_objects_size, normalized_critical_path)
        return True

def run_replay(num_nodes, num_workers_per_node, object_transfer_time_cost,
               db_message_delay, scheduler_name, trace_filename,
               global_scheduler_kwargs, local_scheduler_kwargs,
               enable_verification=True):
    scheduler_cls = schedulers.get(scheduler_name)
    if scheduler_cls is None:
        print 'Error - unrecognized scheduler'
        sys.exit(1)
    if trace_filename.endswith('.gz'):
        f = gzip.open(trace_filename, 'rb')
    else:
        f = open(trace_filename, 'r')
    try:
      computation = json.load(f, object_hook=replaystate.computation_decoder)
    finally:
        f.close()
    if enable_verification:
        computation.verify()

    setup_logging()
    event_simulation = replaystate.EventSimulation()
    stats_logger = statslogging.StatsLogger(event_simulation)
    event_log_logger = statslogging.EventLogLogger(event_simulation)
    logger = statslogging.CompoundLogger([stats_logger, event_log_logger])
    simulate(computation, scheduler_cls, event_simulation, logger, num_nodes,
             num_workers_per_node, object_transfer_time_cost, db_message_delay,
             global_scheduler_kwargs, local_scheduler_kwargs)

def run_replay_from_sys_argv(args):
    num_nodes = int(args[1])
    num_workers_per_node = int(args[2])
    object_transfer_time_cost = float(args[3])
    db_message_delay = float(args[4])
    scheduler_name = args[5]
    enable_verification = args[6] == 'true'
    trace_filename = args[7]
    run_replay(num_nodes, num_workers_per_node, object_transfer_time_cost,
               db_message_delay, scheduler_name, trace_filename, {}, {},
               enable_verification=enable_verification)

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
    global_scheduler_kwargs = getattr(config, 'GLOBAL_SCHEDULER_KWARGS',
                                      default_config.GLOBAL_SCHEDULER_KWARGS)
    local_scheduler_kwargs = getattr(config, 'LOCAL_SCHEDULER_KWARGS',
                                     default_config.LOCAL_SCHEDULER_KWARGS)
    trace_filename = getattr(config, 'TRACE_FILENAME',
                             default_config.TRACE_FILENAME)
    enable_verification = getattr(config, 'ENABLE_VERIFICATION',
            default_config.ENABLE_VERIFICATION)
    run_replay(num_nodes, num_workers_per_node, object_transfer_time_cost,
               db_message_delay, scheduler_name, trace_filename,
               global_scheduler_kwargs, local_scheduler_kwargs,
               enable_verification=enable_verification)

if __name__ == '__main__':
    if len(sys.argv) == 2:
        run_replay_from_config(sys.argv[1])
    elif len(sys.argv) == 8:
        run_replay_from_sys_argv(sys.argv)
    else:
        usage()
        sys.exit(-1)
