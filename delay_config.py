# Cluster configuration.
NUM_NODES = 2
NUM_WORKERS_PER_NODE = 2

# Amount of delay to inject for object transfers.
OBJECT_TRANSFER_TIME_COST = 1000

# The pathname for the trace file to replay.
TRACE_FILENAME = "traces/test/delay_validation.json"

SCHEDULER_NAME = 'delay'
GLOBAL_SCHEDULER_KWARGS = {
        'delay': 2
        }
