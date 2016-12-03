import os
import subprocess32 as subprocess
import simplejson as json

RAY_LOG_DIRECTORY = "/tmp/raylogs"
RUN_WORKLOAD_SCRIPT = "../workloads/rnn/rnn_ray_6_layers.py"
SCALE = 1
NUM_STEPS = 1

BUILD_TRACE_SCRIPT = "../build_trace.py"

REPLAY_TRACE_SCRIPT = "../replaytrace.py"
NUM_NODES = 1  # Single node.
DB_MESSAGE_DELAY = 0.001  # 1ms delay for Redis.
DATA_TRANSFER_COST = 0  # Single node, no cost for transferring data.
RAY_SCHEDULER = "trivial"  # Legacy Ray uses a trivial scheduler.

def get_trace_filename(num_workers):
    return "trace/simulation_{num_workers}.json".format(num_workers=num_workers)

def run_workload(num_workers):
    # Clear the log directory.
    if os.path.exists(RAY_LOG_DIRECTORY):
        for filename in os.listdir(RAY_LOG_DIRECTORY):
            filename = os.path.join(RAY_LOG_DIRECTORY, filename)
            os.unlink(filename)

    # Run the workload.
    proc = subprocess.Popen(["python", RUN_WORKLOAD_SCRIPT,
                             "-w", str(num_workers),
                             "-s", str(SCALE),
                             "-n", str(NUM_STEPS)],
                            stdout=subprocess.PIPE
                            )
    proc.wait()
    output = proc.stdout.read()
    output = output.split('\n')
    output = output[-3].split()
    runtime = float(output[-1])

    # Build the trace.
    proc = subprocess.Popen(["python", "../build_trace.py", RAY_LOG_DIRECTORY,
                             get_trace_filename(num_workers)])
    proc.wait()
    return runtime

def run_simulation(num_workers, num_simulation_workers):
    trace_filename = get_trace_filename(num_workers)
    with open(os.devnull, 'w') as DEVNULL:
        proc = subprocess.Popen(["python", "../replaytrace.py",
                                 str(NUM_NODES),
                                 str(num_simulation_workers),
                                 str(DATA_TRANSFER_COST),
                                 str(DB_MESSAGE_DELAY),
                                 RAY_SCHEDULER,
                                 "true",
                                 trace_filename,
                                 ],
                                stdout=subprocess.PIPE,
                                stderr=DEVNULL,
                                )
    proc.wait()
    output = proc.stdout.read()
    lines = output.split('\n')
    runtime = float(lines[-3].split(':')[0])
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
        actual_runtimes.append((num_workers, actual_runtime))
        simulation_runtimes = []
        print ("Running simulations for {} workers".format(num_workers))
        for num_simulation_workers in num_workers_range:
            simulation_runtime = run_simulation(num_workers, num_simulation_workers)
            simulation_runtimes.append((num_simulation_workers, simulation_runtime))
        simulation_results[num_workers] = simulation_runtimes
        print ("Finished simulations for {} workers".format(num_workers))
    return actual_runtimes, simulation_results

if __name__ == '__main__':
    import sys
    max_num_workers = int(sys.argv[1])
    actual_runtimes, simulation_runtimes = generate_data(2, max_num_workers, 2)
    with open('output.json', 'w') as f:
        f.write(json.dumps({
            "actual": actual_runtimes,
            "simulation": simulation_runtimes,
            }))
