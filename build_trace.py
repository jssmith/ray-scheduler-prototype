import os
import sys
import simplejson as json
from collections import defaultdict


ROOT_TASK_ID = "0"

def parse_json_dir(log_dir):
    event_logs = []
    for filename in os.listdir(log_dir):
        if not os.path.isfile(os.path.join(log_dir, filename)):
            continue
        if 'worker' not in filename:
            continue
        if filename.endswith('c++.log'):
            continue

        event_log = []
        with open(os.path.join(log_dir, filename)) as f:
            lines = f.readlines()
            for line in lines:
                event = json.loads(line.strip())
                event_log.append(event)
        event_logs.append(event_log)
    return event_logs

# Returns two dependency mappings. The first has key object id, value task uuid
# that produced it. The second has key task, value list of object ids that it
# depends on.
def build_dependencies(event_logs):
    task_dependencies = {}
    for event_log in event_logs:
        for event in event_log:
            if event['event'] != 'SCHEDULE':
                continue
            task_dependencies[event['taskId']] = event['dependsOn']
    return task_dependencies


def build_tasks(task_dependencies, event_log, task_roots):
    tasks = []
    phases = []

    is_driver = False
    task_id = None
    phase = 0
    depends_on = []
    submits = []
    creates = []
    cur_time = 0

    for event in event_log:
        event_type = event['event']
        if event_type == 'SCHEDULE':
            schedule = {'taskId': event['taskId']}
            if cur_time == 0:
                # This is the very first task scheduled, by the driver.
                offset = 0
            else:
                offset = event['time'] - cur_time
            schedule['timeOffset'] = offset
            submits.append(schedule)
        elif event_type == 'DRIVER_BEGIN':
            assert(task_id == None)
            cur_time = event['time']
            is_driver = True
            print "Program began at ", cur_time
        elif event_type == 'BEGIN':
            phases = []
            task_id = event['taskId']
            phase = 0
            depends_on = task_dependencies[task_id]
            submits = []
            creates = []
            cur_time = event['time']
        elif event_type == 'PHASE_END':
            phases.append({
                'phaseId': phase,
                'dependsOn': depends_on,
                'submits': submits,
                'duration': event['time'] - cur_time,
                'creates': creates,
                })
        elif event_type == 'PHASE_BEGIN':
            phase += 1
            depends_on = event['dependsOn']
            submits = []
            creates = []
            cur_time = event['time']
        elif event_type == 'END':
            phases.append({
                'phaseId': phase,
                'dependsOn': depends_on,
                'submits': submits,
                'duration': event['time'] - cur_time,
                'creates': creates,
                })
            tasks.append({
                'taskId': task_id,
                'phases': phases,
                'results': event['results'],
                })
        elif event_type == 'PUT':
            object_id = event['objectId']
            creates.append({
                'objectId': event['objectId'],
                'size': event['size'],
                'timeOffset': event['time'] - cur_time,
                })
        elif event_type == 'DRIVER_END':
            if not is_driver:
                continue
            phases.append({
                'phaseId': phase,
                'dependsOn': depends_on,
                'submits': submits,
                'duration': event['time'] - cur_time,
                'creates': creates,
                })
            break
        else:
            print "Found unexpected event type {0}".format(event_type)
            sys.exit(-1)

    # The task ID should not be set if this the driver program.
    if (task_id is None) and (event_log):
        task_id = ROOT_TASK_ID
        tasks.append({
            'taskId': task_id,
            'phases': phases,
            'results': [],
            })
        task_roots.append(task_id)

    return tasks

def dump_tasks(task_roots, tasks, trace_filename):
    # There should only be one driver program.
    if len(task_roots) == 0:
        print "Error: No task roots found."
        return
    if len(task_roots) > 1:
        print "Error: More than one task root."
        return
    # Write out the trace.
    with open(trace_filename, 'w') as f:
        f.write(json.dumps({
            'rootTask': task_roots[0],
            'tasks': tasks,
            }, sort_keys=True, indent=4, separators=(',', ': ')))


if __name__ == '__main__':
    import sys

    if len(sys.argv) < 2:
        print 'First argument must be directory with logs'
        sys.exit(1)

    log_dir = sys.argv[1]
    trace_filename = 'trace.json'
    if len(sys.argv) >= 3:
        trace_filename = sys.argv[2]
    print 'Dumping trace built from {0} directory to {1}'.format(log_dir,
                                                                 trace_filename)

    event_logs = parse_json_dir(log_dir)
    task_dependencies = build_dependencies(event_logs)
    task_roots, tasks = [], []
    for event_log in event_logs:
        tasks += build_tasks(task_dependencies, event_log, task_roots)
    dump_tasks(task_roots, tasks, trace_filename)
