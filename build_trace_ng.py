import os
import time
import sys
import simplejson as json
from collections import defaultdict
import redis
import binascii
import subprocess32 as subprocess

from photon import task_from_string, ObjectID

# Hack to suppress Ray errors from printing. These must be kept in sync with
# worker.py.
LOG_POINT = 0
LOG_SPAN_START = 2
LOG_SPAN_END = 2

ROOT_TASK_ID = "0"

def read_event_logs(r):
    event_log_keys = r.keys("event_log:*")
    event_logs = []
    task_dependencies = {}
    for key in event_log_keys:
        log = r.lrange(key, 0, -1)
        log = [json.loads(events) for events in log]
        log = [event for events in log for event in events]
        filtered_log = []
        for event in log:
            time, event_type, kind, contents = event
            task_id = None
            if event_type == "ray:submit_task" and kind == LOG_SPAN_END:
                task_id = contents['task_id']
                event_dict = {
                        'event': 'SCHEDULE',
                        'taskId': task_id,
                        }
            elif event_type == "ray:begin":
                event_dict = {
                        'event': 'DRIVER_BEGIN',
                        }
            elif event_type == "ray:task" and kind == LOG_SPAN_START:
                task_id = contents['task_id']
                event_dict = {
                        'event': 'BEGIN',
                        'taskId': task_id,
                        }
            elif event_type == "ray:get" and kind == LOG_SPAN_START:
                event_dict = {
                        'event': 'PHASE_END',
                        }
            elif event_type == "ray:get" and kind == LOG_SPAN_END:
                object_ids = contents['object_ids'].split()
                event_dict = {
                        'event': 'PHASE_BEGIN',
                        'dependsOn': object_ids,
                        }
            elif event_type == "ray:task" and kind == LOG_SPAN_END:
                result_ids = contents['results'].split()
                results = []
                for result_id in result_ids:
                    results.append({
                        'objectId': result_id,
                        'size': get_object_size(r, result_id),
                        })
                event_dict = {
                        'event': 'END',
                        'results': results,
                        }
            elif event_type == "ray:put" and kind == LOG_SPAN_END:
                event_dict = {
                        'event': 'PUT',
                        'objectId': contents['object_id'],
                        'size': contents['size'],
                        }
            elif event_type == "ray:end":
                event_dict = {
                        'event': 'DRIVER_END',
                        }
            else:
                continue

            event_dict['time'] = time
            filtered_log.append(event_dict)

            if task_id is not None and task_id not in task_dependencies:
                task_dependencies[task_id] = get_task_dependencies(r, task_id)
        event_logs.append(filtered_log)
    return event_logs, task_dependencies

def get_object_size(r, object_id):
    object_id = binascii.unhexlify(object_id)
    return r.hget("OI:{}".format(object_id), 'data_size')

def get_task_dependencies(r, task_id):
    task_id = binascii.unhexlify(task_id)
    spec = r.hget("TT:{}".format(task_id), 'task_spec')
    task = task_from_string(spec)
    dependencies = []
    for arg in task.arguments():
        if isinstance(arg, ObjectID):
            dependencies.append(arg.hex())
    return dependencies

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

    trace_filename = 'trace.json'
    if len(sys.argv) >= 2:
        trace_filename = sys.argv[1]
    print 'Dumping trace built to {0}'.format(trace_filename)

    p = subprocess.Popen(["redis-server"], stdout=subprocess.PIPE)
    time.sleep(1)

    r = redis.StrictRedis()
    event_logs, task_dependencies = read_event_logs(r)

    task_roots, tasks = [], []
    for event_log in event_logs:
        tasks += build_tasks(task_dependencies, event_log, task_roots)
    dump_tasks(task_roots, tasks, trace_filename)

    p.kill()
