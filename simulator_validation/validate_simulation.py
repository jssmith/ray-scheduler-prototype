import os
import subprocess32 as subprocess
import simplejson as json

RAY_LOG_DIRECTORY = "/tmp/raylogs"
RAY_LOGS = "dump.rdb"
RUN_WORKLOAD_SCRIPT = "../tree_reduce.py"
BLOCK_SIZE = 100
NUM_BLOCKS = 10

BUILD_TRACE_SCRIPT = "../build_trace_ng.py"

REPLAY_TRACE_SCRIPT = "../replaytrace.py"
NUM_NODES = 1  # Single node.
DB_MESSAGE_DELAY = 0.001  # 1ms delay for Redis.
DATA_TRANSFER_COST = 0  # Single node, no cost for transferring data.
RAY_SCHEDULER = "trivial"  # Legacy Ray uses a trivial scheduler.

def get_trace_filename(num_workers):
    return "trace/simulation_{num_workers}.json".format(num_workers=num_workers)

def run_workload(num_workers):
    # Clear the log directory.
    if os.path.exists(RAY_LOGS):
        os.unlink(RAY_LOGS)

    # Run the workload.
    with open('out', 'w') as stdout_log:
        proc = subprocess.Popen(["python", RUN_WORKLOAD_SCRIPT,
                                 "--workers", str(num_workers),
                                 ],
                                stdout=stdout_log,
                                )
        proc.wait()
    with open('out', 'r') as stdout_log:
        for line in stdout_log.readlines():
            try:
                runtime = float(line)
                break
            except:
                continue

    # Build the trace.
    proc = subprocess.Popen(["python", BUILD_TRACE_SCRIPT,
                             get_trace_filename(num_workers)])
    proc.wait()
    return runtime

def run_simulation(num_workers, num_simulation_workers):
    trace_filename = get_trace_filename(num_workers)
    with open(os.devnull, 'w') as DEVNULL:
        with open('out', 'w') as stdout_log:
            proc = subprocess.Popen(["python", "../replaytrace.py",
                                     str(NUM_NODES),
                                     str(num_simulation_workers),
                                     str(DATA_TRANSFER_COST),
                                     str(DB_MESSAGE_DELAY),
                                     RAY_SCHEDULER,
                                     "lru",
                                     "true",
                                     trace_filename,
                                     ],
                                    stdout=stdout_log,
                                    stderr=DEVNULL,
                                    )
        proc.wait()
    with open('out', 'r') as f:
        lines = f.readlines()
    runtime = float(lines[-1].split(':')[0])
    return runtime

def generate_data(min_num_workers, max_num_workers, step_size):
    num_workers_range = range(min_num_workers, max_num_workers + 1, step_size)
    actual_runtimes = []
    simulation_results = {}
    print ("Generating results for workers in range {}".format(num_workers_range))
    for num_workers in num_workers_range:
        print ("Running workload for {} workers".format(num_workers))
        actual_runtime = run_workload(num_workers)
        print ("Finished workload for {} workers".format(num_workers))
        with open('output.csv', 'a') as f:
            f.write("{num_workers} {runtime}\n".format(
                num_workers=num_workers,
                runtime=actual_runtime
                ))
        actual_runtimes.append((num_workers, actual_runtime))
        simulation_runtimes = []
        for num_simulation_workers in num_workers_range:
            print ("Running {} worker simulation for {} workers".format(num_simulation_workers, num_workers))
            simulation_runtime = run_simulation(num_workers, num_simulation_workers)
            simulation_runtimes.append((num_simulation_workers, simulation_runtime))
            with open('output.csv', 'a') as f:
                f.write("{num_workers} {num_simulation_workers} {runtime}\n".format(
                    num_workers=num_workers,
                    num_simulation_workers=num_simulation_workers,
                    runtime=simulation_runtime
                    ))
        simulation_results[num_workers] = simulation_runtimes
        print ("Finished simulations for {} workers".format(num_workers))
    return actual_runtimes, simulation_results

if __name__ == '__main__':
    import sys
    max_num_workers = int(sys.argv[1])
    actual_runtimes, simulation_runtimes = generate_data(2, max_num_workers, 1)
    with open('output.json', 'w') as f:
        f.write(json.dumps({
            "actual": actual_runtimes,
            "simulation": simulation_runtimes,
            }))
