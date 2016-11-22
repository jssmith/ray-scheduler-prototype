# Cluster configuration.
NUM_NODES = 1
NUM_WORKERS_PER_NODE = 1

# Amount of delay to inject for object transfers.
OBJECT_TRANSFER_TIME_COST = 0

# Amount of delay to inject for global database queries.
DB_MESSAGE_DELAY = 0

# Scheduler name. Must match a key in replaytrace.py.
SCHEDULER_NAME = "trivial"
# Keyword arguments for instantiating the schedulers.
GLOBAL_SCHEDULER_KWARGS = {}
LOCAL_SCHEDULER_KWARGS = {}

# The pathname for the trace file to replay.
TRACE_FILENAME = "traces/test/rnn_6layers_w3s2n1_attempt2.json"
