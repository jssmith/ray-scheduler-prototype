import sys
import heapq

from schedulerbase import *


################################################################
#         Scheduler Database that Previous Traces              #
################################################################

class ReplaySchedulerDatabase(AbstractSchedulerDatabase):
    class ScheduledTask():
        def __init__(self, task_id, phase_id):
            self.task_id = task_id
            self.phase_id = phase_id

    class TaskPhaseComplete():
        def __init__(self, task_id, phase_id, worker_id):
            self.task_id = task_id
            self.phase_id = phase_id
            self.worker_id = worker_id

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

        # schedule worker registration
        for i in range(0, num_nodes):
            self._ts.schedule_immediate(RegisterNodeUpdate(i, num_workers_per_node))

        # schedule roots
        for task_id in computation.get_roots():
            self._ts.schedule_immediate(self.ScheduledTask(task_id, 0))

    def schedule(self, task):
        print 'Not implemented: schedule'
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
        depends_on = task_phase.get_depends_on()
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
        for d_object_id in self._computation.get_task(task_id).get_phase(phase_id).get_depends_on():
            dep_obj = self._finished_objects[d_object_id]
            if dep_obj.node_id != node_id:
                data_transfer_time += dep_obj.size * self._data_transfer_time_cost
        for schedule_task in task_phase.get_schedules():
            self._ts.schedule_delayed(data_transfer_time + schedule_task.time_offset, self.ScheduledTask(schedule_task.task_id, 0))
        self._ts.schedule_delayed(data_transfer_time + task_phase.duration, self.TaskPhaseComplete(task_id, phase_id, node_id))

    def get_updates(self, timeout_s):
        # This is the main loop for simulation event processing
        time_limit = self._ts.get_time() + timeout_s
        no_results = True
        nextUpdate = self._ts.advance(time_limit)
        while nextUpdate is not None:
            no_results = False
            if isinstance(nextUpdate, self.ScheduledTask):
                yield ScheduleTaskUpdate(self._computation.get_task(nextUpdate.task_id))
            if isinstance(nextUpdate, self.TaskPhaseComplete):
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
        if no_results and self._ts.queue_empty():
            yield ShutdownUpdate()

    def execute(self, worker_id, task_id):
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
            return None

    def queue_empty(self):
        return not self._scheduled



################################################################
#              Data model for saved computations               #
################################################################
class ComputationDescription():
    def __init__(self, roots, tasks):
        self._roots = roots
        self._tasks = {}
        for task in tasks:
            self._tasks[task.id()] = task

    def get_roots(self):
        return self._roots

    def get_task(self, task_id):
        return self._tasks[task_id]

class Task():
    def __init__(self, task_id, phases, results):
        self._task_id = task_id
        self._phases = phases
        self._results = results

    def id(self):
        return self._task_id

    def get_depends_on(self):
        return self._phases[0].get_depends_on()

    def get_phase(self, phase_id):
        return self._phases[phase_id]

    def num_phases(self):
        return len(self._phases)

    def get_results(self):
        return self._results

class TaskPhase():
    def __init__(self, phase_id, depends_on, schedules, duration):
        self._phase_id = phase_id
        self._depends_on = depends_on
        self._schedules = schedules
        self.duration = duration

    def get_depends_on(self):
        return self._depends_on

    def get_schedules(self):
        return self._schedules

class TaskResult():
    def __init__(self, object_id, size):
        self.object_id = object_id
        self.size = size

class TaskSchedule():
    def __init__(self, task_id, time_offset):
        self.task_id = task_id
        self.time_offset = time_offset

def computation_decoder(dict):
    keys = frozenset(dict.keys())
    if keys == frozenset([u'timeOffset', 'taskId']):
        return TaskSchedule(dict[u'taskId'], dict[u'timeOffset'])
    if keys == frozenset([u'duration', u'phaseId', u'schedules', u'dependsOn']):
        return TaskPhase(dict[u'phaseId'],dict[u'dependsOn'],dict[u'schedules'],dict[u'duration'])
    if keys == frozenset([u'phases', u'results', u'taskId']):
        return Task(dict[u'taskId'], dict[u'phases'], dict[u'results'])
    if keys == frozenset([u'tasks', u'taskRoots']):
        return ComputationDescription(dict[u'taskRoots'], dict[u'tasks'])
    if keys == frozenset([u'objectId', u'size']):
        return TaskResult(dict[u'objectId'], int(dict[u'size']))
    else:
        print "unexpected map: {}".format(keys)
        sys.exit(1)
