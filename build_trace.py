import os
import simplejson as json


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
# that produced it. The second has key task, value list of task uuids that it
# depends on.
def build_dependencies(event_logs):
    object_dependencies = {}
    args = {}
    for event_log in event_logs:
        for event in event_log:
            if event['event'] != 'SCHEDULE':
                continue
            object_ids = event['returns']
            for object_id in object_ids:
                object_dependencies[object_id] = event['taskId']
            args[event['taskId']] = event['dependsOn']
    dependencies = {}
    for task in args:
        dependencies[task] = [object_dependencies[object_id] for object_id in args[task]]
    return object_dependencies, dependencies


def build_tasks(object_dependencies, task_dependencies, event_log):
    tasks = []
    phases = []

    task_id = None
    phase = 0
    depends_on = []
    schedules = []
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
            schedules.append(schedule)
        elif event_type == 'BEGIN':
            phases = []
            task_id = event['taskId']
            phase = 0
            depends_on = task_dependencies[task_id]
            schedules = []
            cur_time = event['time']
        elif event_type == 'PHASE_END':
            phases.append({
                'phaseId': phase,
                'dependsOn': depends_on,
                'schedules': schedules,
                'duration': event['time'] - cur_time,
                })
        elif event_type == 'PHASE_BEGIN':
            phase += 1
            depends_on = [object_dependencies[object_id] for object_id in
                          event['dependsOn']]
            schedules = []
            cur_time = event['time']
        elif event_type == 'END':
            phases.append({
                'phaseId': phase,
                'dependsOn': depends_on,
                'schedules': schedules,
                'duration': event['time'] - cur_time,
                })
            tasks.append({
                'taskId': task_id,
                'phases': phases,
                })

    if task_id is None:
        tasks.append({
            'taskId': 'root',
            'phases': phases,
            })

    return tasks

def dump_tasks(tasks, trace_filename):
    task_root = None
    for i, task in enumerate(tasks):
        if task['taskId'] == 'root':
            task_root = tasks.pop(i)
            break
    if task_root == None:
        print "Error: No task root found."
        return
    with open(trace_filename, 'w') as f:
        f.write(json.dumps({
            'taskRoot': task_root,
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
    obj_dependencies, task_dependencies = build_dependencies(event_logs)
    tasks = [task for event_log in event_logs for task in build_tasks(obj_dependencies, task_dependencies, event_log)]
    dump_tasks(tasks, 'trace.json')
