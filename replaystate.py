import sys
import heapq
import itertools

from schedulerbase import *


################################################################
#        Scheduler Database that Replays Saved Traces          #
################################################################

class ReplaySchedulerDatabase(AbstractSchedulerDatabase):
    class ScheduledTask():
        def __init__(self, task_id, phase_id, submitting_node_id):
            self.task_id = task_id
            self.phase_id = phase_id
            self.submitting_node_id = submitting_node_id

    class TaskPhaseComplete():
        def __init__(self, task_id, phase_id, worker_id):
            self.task_id = task_id
            self.phase_id = phase_id
            self.worker_id = worker_id

        def __str__(self):
            return ('TaskPhaseComplete({0}, {1}, {2})'.format(self.task_id,
                                                              self.phase_id,
                                                              self.worker_id))

    class ObjectDescription():
        def __init__(self, node_id, size):
            self.node_id = node_id
            self.size = size

    def __init__(self, time_source, computation, num_nodes, num_workers_per_node, data_transfer_time_cost):
        self._ts = time_source
        self._computation = computation
        self._data_transfer_time_cost = data_transfer_time_cost

        self._finished_objects = {}
        self._executing_tasks = {}
        self._pending_needs = {}
        self._pending_info = {}
        self._awaiting_completion = {}

        self._driver_node = 0

        # schedule worker registration
        for i in range(0, num_nodes):
            self._ts.schedule_immediate(RegisterNodeUpdate(i, num_workers_per_node))

        # count total number of phases
        self._phases_pending = computation.get_total_num_phases()

        # schedule root task
        root_task = computation.get_root_task()
        if root_task is not None:
            self._ts.schedule_immediate(self.ScheduledTask(root_task, 0, self._driver_node))

    def submit(self, task):
        print 'Not implemented: submit'
        sys.exit(1)

    def finished(self, task_id):
        print 'Not implemented: finished'
        sys.exit(1)

    def register_node(self, node_id, num_workers):
        print 'Not implemented: register_node'
        sys.exit(1)


    def remove_node(self, node_id):
        print 'Not implemented: remove_node'
        sys.exit(1)

    def _internal_scheduler_schedule(self, task_id, phase_id, worker_id):
        # TODO should probably always call this, even for phase 0
        task_phase = self._computation.get_task(task_id).get_phase(phase_id)
        depends_on = task_phase.depends_on
        needs = []
        for d_object_id in depends_on:
            if not d_object_id in self._finished_objects.keys():
                needs.append(d_object_id)
                if d_object_id in self._awaiting_completion:
                    self._awaiting_completion[d_object_id].append(task_id)
                else:
                    self._awaiting_completion[d_object_id] = [task_id]
        if not needs:
            self._execute_immediate(task_id, phase_id, worker_id)
        else:
            self._pending_needs[task_id] = needs
            self._pending_info[task_id] = (phase_id, worker_id)

    def _internal_scheduler_finish(self, task_id):
        node_id = self._executing_tasks[task_id]
        del self._executing_tasks[task_id]
        for result in self._computation.get_task(task_id).get_results():
            object_id = result.object_id
            self._finished_objects[object_id] = self.ObjectDescription(node_id, result.size)
            if object_id in self._awaiting_completion.keys():
                p_task_ids = self._awaiting_completion[object_id]
                del self._awaiting_completion[object_id]
                for p_task_id in p_task_ids:
                    needs = self._pending_needs[p_task_id]
                    needs.remove(object_id)
                    if not needs:
                        del self._pending_needs[p_task_id]
                        (phase_id, worker_id) = self._pending_info[p_task_id]
                        del self._pending_info[p_task_id]
                        self._execute_immediate(p_task_id, phase_id, worker_id)

    def _execute_immediate(self, task_id, phase_id, node_id):
        # TODO: enforce limit on workers per node, valid node_id
        task_phase = self._computation.get_task(task_id).get_phase(phase_id)
        self._executing_tasks[task_id] = node_id
        data_transfer_time = 0
        for d_object_id in self._computation.get_task(task_id).get_phase(phase_id).depends_on:
            dep_obj = self._finished_objects[d_object_id]
            if dep_obj.node_id != node_id:
                data_transfer_time += dep_obj.size * self._data_transfer_time_cost
        for schedule_task in task_phase.submits:
            self._ts.schedule_delayed(data_transfer_time + schedule_task.time_offset, self.ScheduledTask(schedule_task.task_id, 0, node_id))
        self._ts.schedule_delayed(data_transfer_time + task_phase.duration, self.TaskPhaseComplete(task_id, phase_id, node_id))

    def get_updates(self, timeout_s):
        # This is the main loop for simulation event processing
        time_limit = self._ts.get_time() + timeout_s
        no_results = True
        nextUpdate = self._ts.advance(time_limit)
        while nextUpdate is not None:
            no_results = False
            if isinstance(nextUpdate, self.ScheduledTask):
                yield SubmitTaskUpdate(self._computation.get_task(nextUpdate.task_id), nextUpdate.submitting_node_id)
            if isinstance(nextUpdate, self.TaskPhaseComplete):
                self._phases_pending -= 1
                task = self._computation.get_task(nextUpdate.task_id)
                if nextUpdate.phase_id < task.num_phases() - 1:
                    self._internal_scheduler_schedule(nextUpdate.task_id, nextUpdate.phase_id + 1, nextUpdate.worker_id)
                else:
                    print '{:.6f}: finshed task {} on worker {}'.format(self._ts.get_time(), nextUpdate.task_id, nextUpdate.worker_id)
                    self._internal_scheduler_finish(nextUpdate.task_id)
                    yield FinishTaskUpdate(nextUpdate.task_id)
            if isinstance(nextUpdate, RegisterNodeUpdate):
                yield nextUpdate
            nextUpdate = self._ts.advance(time_limit)
        if self._phases_pending == 0 and self._ts.queue_empty():
            yield ShutdownUpdate()

    def schedule(self, worker_id, task_id):
        print '{:.6f}: execute task {} on worker {}'.format(self._ts.get_time(), task_id, worker_id)
        self._execute_immediate(task_id, 0, worker_id)

    def get_work(self, worker_id, timeout_s):
        print 'Not implemented: get_work'
        sys.exit(1)



################################################################
#        Simulation timesource & deferred execution            #
################################################################

class SystemTime():
    def __init__(self):
        self._t = 0
        self._scheduled = []

    def get_time(self):
        return self._t

    def schedule_at(self, t, data):
        if self._t > t:
            print 'invalid schedule request'
            sys.exit(1)
        heapq.heappush(self._scheduled, (t, data))

    def schedule_delayed(self, delta, data):
        self.schedule_at(self._t + delta, data)

    def schedule_immediate(self, data):
        self.schedule_at(self._t, data)

    def advance(self, time_limit):
        if len(self._scheduled) > 0 and self._scheduled[0][0] <= time_limit:
            (self._t, data) = heapq.heappop(self._scheduled)
            return data
        else:
            self._t = time_limit
            return None

    def queue_empty(self):
        return not self._scheduled



################################################################
#              Data model for saved computations               #
################################################################
class ComputationDescription():
    def __init__(self, root_task, tasks):
        if root_task is None:
            if len(tasks) != 0:
                raise ValidationError('Too many tasks are called')
            else:
                self._root_task = None
                self._tasks = {}
                return

        root_task_str = str(root_task)
        # task ids must be unique
        task_ids = map(lambda x: x.id(), tasks)
        task_ids_set = frozenset(task_ids)
        if len(task_ids_set) != len(task_ids):
            raise ValidationError('Task ids must be unique')

        # all tasks should be called exactly once
        called_tasks = set(root_task_str)
        for task in tasks:
            for phase_id in range(0, task.num_phases()):
                for task_id in map(lambda x: x.task_id, task.get_phase(phase_id).submits):
                    if task_id in called_tasks:
                        raise ValidationError('Duplicate call to task {}'.format(task_id))
                    if task_id not in task_ids_set:
                        raise ValidationError('Call to undefined task {}'.format(task_id))
                    called_tasks.add(task_id)
        if len(called_tasks) < len(task_ids):
            tasks_not_called = task_ids_set.difference(called_tasks)
            raise ValidationError('Some tasks are not called: {}'.format(str(tasks_not_called)))
        if len(called_tasks) > len(task_ids):
            raise ValidationError('Too many tasks are called')

        # no dependencies that don't get created
        result_objects = set()
        for task in tasks:
            for task_result in task.get_results():
                object_id = task_result.object_id
                if object_id in result_objects:
                    raise ValidationError('Duplicate result object id {}'.format(object_id))
                result_objects.add(object_id)
        for task in tasks:
            for phase_id in range(0, task.num_phases()):
                for object_id in task.get_phase(phase_id).depends_on:
                    if object_id not in result_objects:
                        raise ValidationError('Dependency on missing object id {}'.format(object_id))

        # no cycles, everything reachable from roots
        dg = DirectedGraph()
        tasks_map = {}
        for task in tasks:
            tasks_map[task.id()] = task
        for task in tasks:
            prev_phase = None
            for phase_id in range(0, task.num_phases()):
                phase = task.get_phase(phase_id)
                if prev_phase:
                    #print "EDGE: previous phase edge"
                    dg.add_edge(prev_phase, phase)
                for object_id in phase.depends_on:
                    #print "EDGE: phase dependency edge"
                    dg.add_edge(object_id, phase)
                for submits in phase.submits:
                    #print "EDGE: phase schedules edge"
                    dg.add_edge(phase, tasks_map[submits.task_id].get_phase(0))
                    # TODO object id produced in scheduling
                prev_phase = phase
            for task_result in task.get_results():
                #print "EDGE: task result edge"
                dg.add_edge(prev_phase, task_result.object_id)
        dg.verify_dag_root(tasks_map[root_task_str].get_phase(0))

        # verification passed so initialize
        self._root_task = root_task_str
        self._tasks = tasks_map

    def get_root_task(self):
        return self._root_task

    def get_task(self, task_id):
        return self._tasks[task_id]

    def get_total_num_phases(self):
        n = 0
        for task_id, task in self._tasks.items():
            n += task.num_phases()
        return n


class Task():
    def __init__(self, task_id, phases, results):
        task_id_str = str(task_id)
        if not task_id_str:
            raise ValidationError('Task: no id provided')
        if not len(phases):
            raise ValidationError('Task: no phases')
        for idx, phase in enumerate(phases):
            if phase.phase_id != idx:
                raise ValidationError('Task: mismatched phase id')
        # TODO(swang): These lines are not a valid check for the driver
        # task.
        #if not len(results):
        #    raise ValidationError('Task: no results')

        # verification passed so initialize
        self._task_id = task_id_str
        self._phases = phases
        self._results = results

    def id(self):
        return self._task_id

    def get_depends_on(self):
        return self._phases[0].depends_on

    def get_phase(self, phase_id):
        return self._phases[phase_id]

    def num_phases(self):
        return len(self._phases)

    def get_results(self):
        return self._results


class TaskPhase():
    def __init__(self, phase_id, depends_on, submits, duration):
        for s in submits:
            if s.time_offset > duration:
                raise ValidationError('TaskPhase: submits beyond phase duration')

        # verification passed so initialize
        self.phase_id = phase_id
        self.depends_on = map(lambda x: str(x), depends_on)
        self.submits = submits
        self.duration = duration


class TaskResult():
    def __init__(self, object_id, size):
        object_id_str = str(object_id)
        if not object_id_str:
            raise ValidationError('TaskResult: no object id')
        if size < 0:
            raise ValidationError('TaskResult: invalid size - {}'.format(size))

        # verification passed so initialize
        self.object_id = object_id_str
        self.size = size


class TaskSubmit():
    def __init__(self, task_id, time_offset):
        task_id_str = str(task_id)
        if not task_id_str:
            raise ValidationError('TaskSubmit: no task id')

        # verification passed so initialize
        self.task_id = task_id_str
        self.time_offset = time_offset


class DirectedGraph():
    def __init__(self):
        self._id_ct = 0
        self._id_map = {}
        self._edges = []

    def _get_id(self, x):
        if x in self._id_map:
            #print 'found id for {}'.format(x)
            new_id = self._id_map[x]
        else:
            #print 'missing id for {}'.format(x)
            new_id = self._id_ct
            self._id_map[x] = new_id
            self._id_ct += 1
        #print 'id for {} is {}'.format(x, new_id)
        return new_id

    def add_edge(self, a, b):
        id_a = self._get_id(a)
        id_b = self._get_id(b)
        #print 'EDGE: {} => {}'.format(a, b)
        #print 'EDGE: {} -> {}'.format(id_a, id_b)
        self._edges.append((id_a, id_b))

    def verify_dag_root(self, root):
        # TODO(swang): What is the correct check here?
        return
        root_id = self._get_id(root)
        # check that
        #  1/ we have a DAG
        #  2/ all nodes reachable from the root
        # we do this by depth-first search
        visited = [False] * self._id_ct
        in_chain = [False] * self._id_ct
        edge_lists = dict(map(lambda (src_id, edges): (src_id, map(lambda x: x[1], edges)), itertools.groupby(self._edges, lambda x: x[0])))

        #print 'root: {}'.format(root_id)
        #print edge_lists

        def visit(x):
            #print 'visit {}'.format(x)
            if in_chain[x]:
                raise ValidationError('Cyclic dependencies')
            in_chain[x] = True
            if not visited[x]:
                visited[x] = True
                if x in edge_lists.keys():
                    for y in edge_lists[x]:
                        visit(y)
            in_chain[x] = False

        visit(root_id)
        if False in visited:
            raise ValidationError('Reachability from root')


class ValidationError(Exception):
    def __init__(self, message):
        super(ValidationError, self).__init__(message)


def computation_decoder(dict):
    keys = frozenset(dict.keys())
    if keys == frozenset([u'timeOffset', 'taskId']):
        return TaskSubmit(dict[u'taskId'], dict[u'timeOffset'])
    if keys == frozenset([u'duration', u'phaseId', u'submits', u'dependsOn']):
        return TaskPhase(dict[u'phaseId'], dict[u'dependsOn'], dict[u'submits'], dict[u'duration'])
    if keys == frozenset([u'phases', u'results', u'taskId']):
        return Task(dict[u'taskId'], dict[u'phases'], dict[u'results'])
    if keys == frozenset([u'tasks', u'rootTask']):
        return ComputationDescription(dict[u'rootTask'], dict[u'tasks'])
    if keys == frozenset([u'objectId', u'size']):
        return TaskResult(dict[u'objectId'], int(dict[u'size']))
    else:
        print "unexpected map: {}".format(keys)
        sys.exit(1)
