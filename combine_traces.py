import simplejson as json


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

def merge_computations(computation1, computation2):
    """
    Merge computation2 into computation1. Each computation is a dictionary
    mapping task ID (str) to task (replaystate.Task). Returns a tuple of
    (task_ids, object_ids), a combined list of all task IDs and object IDs used
    by the merged computation.
    """
    pass

def merge_computations(computations):
    """
    Merge a list of computations together. Returns a single dictionary
    representing the aggregate computation graph.
    """
    pass

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
