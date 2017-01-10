import argparse
import simplejson as json
import random
import gzip

from replaystate import TaskSubmit
from replaystate import TaskPhase
from replaystate import TaskResult
from replaystate import Task
from replaystate import ComputationDescription
from replaystate import computation_decoder


# This factor determines the offset between repetitions of a computation, if an
# offset is not already provided by the user. The higher the factor, the closer
# together repetitions will be.
OFFSET_FACTOR = 4

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

def insert_unique_id(ids):
    new_id = str(random.randint(0, MAX_ID))
    while new_id in ids:
        new_id = str(random.randint(0, MAX_ID))
    ids.append(new_id)
    return new_id


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
            computation1_task_ids.append(task_id)
            continue
        # Task ID collision. Replace all instances of task_id in computation2
        # with a unique task ID.
        new_task_id = insert_unique_id(computation1_task_ids)
        replace_task_id(computation2, task_id, new_task_id)

    # Replace all object IDs in computation2 that appear in computation1 with a
    # new object ID.
    for object_id in computation2_object_ids:
        if object_id not in computation1_object_ids:
            computation1_object_ids.append(object_id)
            continue
        # Object ID collision. Replace all instances of object_id in computation2
        # with a unique object ID.
        new_object_id = insert_unique_id(computation1_object_ids)
        replace_object_id(computation2, object_id, new_object_id)

    # Merge in all of computation2's tasks into computation1.
    for task_id, task in computation2._tasks.iteritems():
        assert(task_id not in computation1._tasks)
        computation1._tasks[task_id] = task
    # Schedule computation2's root task at a time offset in computation 1's
    # root task's first phase.
    root_task = computation1._tasks[computation1._root_task]
    phase0 = root_task.get_phase(0)
    phase0.submits.append(TaskSubmit(computation2._root_task, offset))

    # Make sure computation1 waits for computation2 to finish by generating a
    # result object for computation2 that computation1 will depend on in its
    # second phase.
    result_id = insert_unique_id(computation1_object_ids)
    root_task2 = computation2._tasks[computation2._root_task]
    root_task2.add_result(TaskResult(result_id, 0))
    root_last_phase = root_task.get_phase(1)
    root_last_phase.depends_on.append(result_id)

    return computation1_task_ids, computation1_object_ids

def merge_computations(computations, offsets):
    """
    Merge a list of computations together. Returns a single dictionary
    representing the aggregate computation graph.
    """
    # Create a mock computation graph that will submit all of the other
    # computations. Its root task's second phase will depend on all of the
    # actual computations' results, so that the driver does not exit until all
    # computation graphs have finished.
    root_task_id = "0"
    root_phase = TaskPhase(0, [], [], sum(offsets), [])
    root_last_phase = TaskPhase(1, [], [], 0, [])
    computation = ComputationDescription(root_task_id, [
        Task(root_task_id, [root_phase, root_last_phase], []),
        ])
    computation.mark_combined()

    # Merge the computations into the mock computation.
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
                'dependsOn': [str(object_id) for object_id in
                              phase.depends_on],
                'submits': [{
                    'taskId': submit.task_id,
                    'timeOffset': submit.time_offset,
                    } for submit in phase.submits],
                'duration': phase.duration,
                'creates': [{
                    'objectId': str(put.object_id),
                    'size': put.size,
                    'timeOffset': put.time_offset,
                    } for put in phase.creates],
                })
        tasks.append({
            'taskId': task.id(),
            'phases': phases,
            'results': [{
                'objectId': str(result.object_id),
                'size': result.size,
                } for result in task.get_results()],
            })
    json_dict = {
            'rootTask': computation._root_task,
            'tasks': tasks,
            'is_combined': True,
            }
    return json.dumps(json_dict,
                      sort_keys=True,
                      indent=4,
                      separators=(',', ': '))

def compute_offset(computation, num_repetitions):
    return (computation.get_total_task_time() / num_repetitions) / OFFSET_FACTOR

parser = argparse.ArgumentParser(description="Take the trace of one job and "
        "repeat it n times, with a constant time offset between each "
        "repetition.")
parser.add_argument("--trace-filename", type=str, required=True,
                    help="Trace filename")
parser.add_argument("--repetitions", type=int, required=True,
                    help="The number of times to repeat the job")
parser.add_argument("--offset", type=float,
                    help="The time offset between repetitions")
parser.add_argument("--output-filename", default="combined.json", type=str,
                    help="The time offset between repetitions")

if __name__ == '__main__':
    args = parser.parse_args()
    trace_filename = args.trace_filename
    repetitions = args.repetitions
    offset = getattr(args, 'offset', None)
    output_filename = args.output_filename

    # Create the list of computation repetitions.
    computations = []
    for i in range(repetitions):
        if trace_filename.endswith('.gz'):
            f = gzip.open(trace_filename, 'rb')
        else:
            f = open(trace_filename, 'r')
        try:
            computations.append(json.loads(f.read(),
                                           object_hook=computation_decoder))
        finally:
            f.close()
    # Create the list of time offsets. The first repetition will be submitted
    # immediately. If no offset was provided, compute the offset based on a
    # single computation's size.
    if offset is None:
        offset = compute_offset(computations[0], repetitions)
    print("Combining {} repetitions of trace {}, with an offset of"
          " {}".format(repetitions, trace_filename, offset))
    offsets = [offset] * repetitions
    offsets[0] = 0

    computation = merge_computations(computations, offsets)
    if output_filename.endswith('.gz'):
        f = gzip.open(output_filename, 'wb')
    else:
        f = open(output_filename, 'w')
    try:
        f.write(serialize_computation(computation))
    finally:
        f.close()
