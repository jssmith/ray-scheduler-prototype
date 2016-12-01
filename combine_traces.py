import argparse
import simplejson as json
import random

from replaystate import TaskSubmit
from replaystate import computation_decoder


MAX_ID = 2**64

def replace_object_id(computation, old_object_id, new_object_id):
    """
    Replace all instances of old_object_id with new_object_id in the computation.
    """
    old_object_id = str(old_object_id)
    new_object_id = str(new_object_id)
    replaced_task_created = False
    # Replace the old object ID inside any tasks that depend on the object.
    for task in computation._tasks.itervalues():
        for i in range(task.num_phases()):
            phase = task.get_phase(i)
            if old_object_id in phase.depends_on:
                phase.depends_on.remove(old_object_id)
                phase.depends_on.append(new_object_id)
            for put in phase.creates:
                if put.object_id == new_object_id:
                    raise Exception("Found a put with the new object ID {} in "
                                    "task {}".format(new_object_id, task.id()))
                if put.object_id != old_object_id:
                    continue
                # If this phase created the object through a put, replace the
                # old object ID.
                if replaced_task_created:
                    raise Exception("Object can only be created by a single "
                            "task, but found a second task {} that put "
                            "{}".format(task.id(), old_object_id))
                put.object_id = new_object_id
                replaced_task_created = True
        # If this task created the object by returning it, replace the old
        # object ID.
        for result in task.get_results():
            if result.object_id == new_object_id:
                raise Exception("Found a return value with the new object ID {} "
                                "in task {}".format(new_object_id, task.id()))
            if result.object_id != old_object_id:
                continue
            if replaced_task_created:
                raise Exception("Object can only be created by a single "
                        "task, but found a second task {} that returned "
                        "{}".format(task.id(), old_object_id))
            result.object_id = new_object_id
            replaced_task_created = True
    if not replaced_task_created:
        raise Exception("Object {} not created by this "
                        "computation?".format(old_object_id))

def replace_task_id(computation, old_task_id, new_task_id):
    """
    Replace all instances of old_task_id with new_task_id in the computation.
    """
    # Replace ID of the matching Task instance with the new task ID.
    task = computation._tasks.pop(old_task_id)
    task._task_id = new_task_id
    computation._tasks[new_task_id] = task
    # If this is the root task, replace the computation's root task attribute.
    if computation._root_task == old_task_id:
        computation._root_task = new_task_id
        return
    # Else, we have to replace the ID inside the task that submitted this task.
    # There should only be one, so we can return once we've found it.
    for task in computation._tasks.itervalues():
        for i in range(task.num_phases()):
            phase = task.get_phase(i)
            for task_submit in phase.submits:
                if task_submit.task_id == old_task_id:
                    task_submit.task_id = new_task_id
                    return
    # If we're able to reach this line, the task was neither the root task, nor
    # was it submitted by any other task. This is a fatal error in the
    # computation graph.
    raise Exception("Task {} was not root task, nor was it submitted by any "
                    "other task.".format(old_task_id))

def get_task_ids(computation):
    """
    Get a list of all task IDs used by this computation graph.
    """
    return computation._tasks.keys()

def get_object_ids(computation):
    """
    Get a list of all object IDs referenced by this computation graph.
    """
    object_ids = []
    for task_id, task in computation._tasks.iteritems():
        # Add the object IDs that this task returns.
        object_ids += [result.object_id for result in task.get_results()]
        # Add the object IDs that this task puts.
        for i in range(task.num_phases()):
            phase = task.get_phase(i)
            object_ids += [put.object_id for put in phase.creates]
    # Check that all object IDs within the computation are unique.
    assert(len(object_ids) == len(set(object_ids)))
    return object_ids

def merge_computation(computation1, computation2, offset,
                      computation1_task_ids,
                      computation1_object_ids):
    """
    Merge computation2 into computation1. Each computation is an instance of
    ComputationDescription. Returns a tuple of (task_ids, object_ids). These
    are lists of all task IDs and object IDs, respectively, used by the merged
    computation. computation2 will get merged into computation1 as a task that
    gets called by computation1's driver at time offset `offset`.
    """
    computation2_task_ids = get_task_ids(computation2)
    computation2_object_ids = get_object_ids(computation2)

    # Replace all task IDs in computation2 that appear in computation1 with a
    # new task ID.
    for task_id in computation2_task_ids:
        if task_id not in computation1_task_ids:
            continue
        # Task ID collision. Replace all instances of task_id in computation2
        # with a unique task ID.
        new_task_id = str(random.randint(0, MAX_ID))
        while new_task_id in computation1_task_ids:
            new_task_id = str(random.randint(0, MAX_ID))
        replace_task_id(computation2, task_id, new_task_id)
        computation1_task_ids.append(new_task_id)

    # Replace all object IDs in computation2 that appear in computation1 with a
    # new object ID.
    for object_id in computation2_object_ids:
        if object_id not in computation1_object_ids:
            continue
        # Object ID collision. Replace all instances of object_id in computation2
        # with a unique object ID.
        new_object_id = str(random.randint(0, MAX_ID))
        while new_object_id in computation1_object_ids:
            new_object_id = str(random.randint(0, MAX_ID))
        replace_object_id(computation2, object_id, new_object_id)
        computation1_object_ids.append(new_object_id)

    # Merge in all of computation2's tasks into computation1.
    for task_id, task in computation2._tasks.iteritems():
        assert(task_id not in computation1._tasks)
        computation1._tasks[task_id] = task
    # Schedule computation2's root task at a time offset in computation 1's
    # root task's first phase.
    root_task = computation1._tasks[computation1._root_task]
    phase0 = root_task.get_phase(0)
    phase0.submits.append(TaskSubmit(computation2._root_task, offset))
    return computation1_task_ids, computation1_object_ids

def merge_computations(computations, offsets):
    """
    Merge a list of computations together. Returns a single dictionary
    representing the aggregate computation graph.
    """
    computation = computations.pop(0)
    task_ids = get_task_ids(computation)
    object_ids = get_object_ids(computation)
    offset = 0
    while computations:
        computation_to_merge = computations.pop(0)
        offset += offsets.pop(0)
        task_ids, object_ids = merge_computation(computation,
                computation_to_merge, offset, task_ids, object_ids)
    return computation

def serialize_computation(computation):
    """
    Serialize a ComputationDescription instance back to JSON. Return the JSON
    string.
    """
    tasks = []
    for task in computation._tasks.itervalues():
        phases = []
        for i in range(task.num_phases()):
            phase = task.get_phase(i)
            phases.append({
                'phaseId': phase.phase_id,
                'dependsOn': [int(object_id) for object_id in
                              phase.depends_on],
                'submits': [{
                    'taskId': submit.task_id,
                    'timeOffset': submit.time_offset,
                    } for submit in phase.submits],
                'duration': phase.duration,
                'creates': [{
                    'objectId': int(put.object_id),
                    'size': put.size,
                    'timeOffset': put.time_offset,
                    } for put in phase.creates],
                })
        tasks.append({
            'taskId': task.id(),
            'phases': phases,
            'results': [{
                'objectId': int(result.object_id),
                'size': result.size,
                } for result in task.get_results()],
            })
    json_dict = {
            'rootTask': computation._root_task,
            'tasks': tasks,
            }
    return json.dumps(json_dict,
                      sort_keys=True,
                      indent=4,
                      separators=(',', ': '))

parser = argparse.ArgumentParser(description="Take the trace of one job and "
        "repeat it n times, with a constant time offset between each "
        "repetition.")
parser.add_argument("--trace-filename", type=str, required=True,
                    help="Trace filename")
parser.add_argument("--repetitions", type=int, required=True,
                    help="The number of times to repeat the job")
parser.add_argument("--offset", type=float, required=True,
                    help="The time offset between repetitions")
parser.add_argument("--output-filename", default="combined.json", type=str,
                    help="The time offset between repetitions")

if __name__ == '__main__':
    args = parser.parse_args()
    trace_filename = args.trace_filename
    repetitions = args.repetitions
    offset = args.offset
    output_filename = args.output_filename

    offsets = [offset] * (repetitions - 1)
    computations = []
    for i in range(repetitions):
        with open(trace_filename, 'r') as f:
            computations.append(json.loads(f.read(),
                                           object_hook=computation_decoder))
    computation = merge_computations(computations, offsets)
    with open(output_filename, 'w') as f:
        f.write(serialize_computation(computation))
