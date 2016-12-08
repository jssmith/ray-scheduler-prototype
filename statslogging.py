import os
import json
import gzip
import heapq

from collections import defaultdict
from helpers import TimestampedLogger

class NoopLogger(object):
    def __init__(self, system_time):
        pass

    def task_submitted(self, task_id, node_id, dependencies):
        pass

    def task_scheduled(self, task_id, node_id, is_scheduled_locally):
        pass

    def task_started(self, task_id, node_id):
        pass

    def task_finished(self, task_id, node_id):
        pass

    def task_phase_started(self, task_id, phase_id, node_id):
        pass

    def task_phase_finished(self, task_id, phase_id, node_id):
        pass

    def object_created(self, object_id, node_id, object_size):
        pass

    def object_transfer_started(self, object_id, object_size, src_node_id, dst_node_id):
        pass

    def object_transfer_finished(self, object_id, object_size, src_node_id, dst_node_id):
        pass

    def job_ended(self):
        pass


class PrintingLogger(object):
    def __init__(self, system_time):
        self._pylogger = TimestampedLogger(__name__+'.PrintingLogger', system_time)

    def task_submitted(self, task_id, node_id, dependencies):
        self._pylogger.debug('submitted task {} on node {} - dependencies {}'.format(task_id, node_id, str(dependencies)))

    def task_scheduled(self, task_id, node_id, is_scheduled_locally):
        if is_scheduled_locally:
            how_scheduled = 'locally'
        else:
            how_scheduled = 'globally'
        self._pylogger.debug('scheduled task {} on node {} - scheduled {}'.format(task_id, node_id, how_scheduled))

    def task_started(self, task_id, node_id):
        self._pylogger.debug('started task {} on node {}'.format(task_id, node_id))

    def task_finished(self, task_id, node_id):
        self._pylogger.debug('finished task {} on node {}'.format(task_id, node_id))

    def task_phase_started(self, task_id, phase_id, node_id):
        self._pylogger.debug('started task {} phase {} on node {}'.format(task_id, phase_id, node_id))

    def task_phase_finished(self, task_id, phase_id, node_id):
        self._pylogger.debug('finished task {} phase {} on node {}'.format(task_id, phase_id, node_id))

    def object_created(self, object_id, node_id, object_size):
        self._pylogger.debug('created object {} of size {} on node {}'.format(object_id, object_size, node_id))

    def object_transfer_started(self, object_id, object_size, src_node_id, dst_node_id):
        self._pylogger.debug('started transfer of object {} of size {} from node {} to node {}'.format(object_id, object_size, src_node_id, dst_node_id))

    def object_transfer_finished(self, object_id, object_size, src_node_id, dst_node_id):
        self._pylogger.debug('finished transfer of object {} of size {} from node {} to node {}'.format(object_id, object_size, src_node_id, dst_node_id))

    def job_ended(self):
        self._pylogger.debug('end of job')


class ActivationTracker(object):
    def __init__(self):
        self._objects = set()
        self._object_waiting_tasks = defaultdict(list)
        self._task_waiting_objects = {}

    def task_submitted(self, task_id, dependencies):
        objects_needed = []
        for object_id in dependencies:
            if object_id in self._objects:
                self._object_waiting_tasks[object_id].append(task_id)
                objects_needed.append(object_id)
        if objects_needed:
            self._task_waiting_objects[task_id] = objects_needed
        return not objects_needed

    def object_created(self, object_id):
        self._objects.add(object_id)
        activated = []
        for task_id in self._object_waiting_tasks[object_id]:
            objects_needed = self._task_waiting_objects[task_id]
            objects_needed.remove(object_id)
            if not objects_needed:
                activated.append(task_id)
                del self._task_waiting_objects[task_id]
        del self._object_waiting_tasks[object_id]
        return activated


class SummaryStats(object):
    def __init__(self, system_time):
        self._system_time = system_time
        self.completed_successfully = None
        self.err = None
        self.stats = None

        self._num_tasks_started = 0
        self._num_tasks_finished = 0

        self._task_execution_time = 0
        self._task_phase_execution_time = 0
        self._last_task_finished = 0

        self._submit_to_schedule_time = 0
        self._submit_to_phase0_time = 0
        self._submit_to_activation_time = 0

        self._activation_to_schedule_time = 0
        self._activation_to_phase0_time = 0

        self._num_object_transfers_started = 0
        self._num_object_transfers_finished = 0
        self._object_transfer_time = 0
        self._object_transfer_size = 0

        self._num_objects_created = 0
        self._object_created_size = 0

        self._activation_tracker = ActivationTracker()

        self._task_timer = self.Timer('task execution', self._system_time)
        self._task_phase_timer = self.Timer('task phase execution', self._system_time)
        self._submit_to_schedule_timer = self.Timer('submit to schedule', self._system_time)
        self._submit_to_phase0_timer = self.Timer('submit to phase0', self._system_time)
        self._submit_to_activation_timer = self.Timer('submit to activation', self._system_time)
        self._activation_to_schedule_timer = self.Timer('activation to schedule', self._system_time)
        self._activation_to_phase0_timer = self.Timer('activation to phase0', self._system_time)
        self._object_transfer_timer = self.Timer('object transfer', self._system_time)
        self._node_worker_tracker = self.NodeWorkerTracker()

        self._num_tasks_submitted = 0

        self._num_tasks_scheduled = 0
        self._num_staks_scheduled_locally = 0

    class Timer():
        def __init__(self, name, system_time):
            self._name = name
            self._system_time = system_time

            self._start_times = {}

        def start(self, key):
            if key in self._start_times.keys():
                raise RuntimeError('duplicate start event on timer \'{}\' for key {}'.format(self._name, key))
            self._start_times[key] = self._system_time.get_time()

        def finish(self, key):
            elapsed_time = self._system_time.get_time() - self._start_times[key]
            del self._start_times[key]
            return elapsed_time

    class NodeWorkerTracker():
        def __init__(self):
            self._nodes_active = 0
            self._tasks_active = 0
            self._node_tasks_active = defaultdict(lambda: 0)

            self.max_workers_active = 0
            self.max_nodes_active = 0

        def task_started(self, node_id):
            self._tasks_active += 1
            if self._tasks_active > self.max_workers_active:
                self.max_workers_active = self._tasks_active
            node_tasks = self._node_tasks_active[node_id]
            if node_tasks == 0:
                self._nodes_active += 1
                if self._nodes_active > self.max_nodes_active:
                    self.max_nodes_active = self._nodes_active
            self._node_tasks_active[node_id] =+ 1


        def task_finished(self, node_id):
            self._tasks_active -= 1
            self._node_tasks_active[node_id] -= 1
            if self._node_tasks_active[node_id] == 0:
                self._nodes_active -= 1

    def _activated(self, task_id):
        self._submit_to_activation_time += self._submit_to_activation_timer.finish(task_id)
        self._activation_to_schedule_timer.start(task_id)
        self._activation_to_phase0_timer.start(task_id)

    def task_submitted(self, task_id, node_id, dependencies):
        self._num_tasks_submitted += 1
        self._submit_to_schedule_timer.start(task_id)
        self._submit_to_phase0_timer.start(task_id)
        self._submit_to_activation_timer.start(task_id)
        if self._activation_tracker.task_submitted(task_id, dependencies):
            self._activated(task_id)

    def task_scheduled(self, task_id, node_id, is_scheduled_locally):
        self._num_tasks_scheduled += 1
        if is_scheduled_locally:
            self._num_staks_scheduled_locally += 1
        self._submit_to_schedule_time += self._submit_to_schedule_timer.finish(task_id)
        self._activation_to_schedule_time += self._activation_to_schedule_timer.finish(task_id)

    def task_started(self, task_id, node_id):
        self._num_tasks_started += 1
        self._task_timer.start((task_id, node_id))
        self._node_worker_tracker.task_started(node_id)

    def task_finished(self, task_id, node_id):
        self._num_tasks_finished += 1
        self._task_execution_time += self._task_timer.finish((task_id, node_id))
        self._last_task_finished = self._system_time.get_time()
        self._node_worker_tracker.task_finished(node_id)

    def task_phase_started(self, task_id, phase_id, node_id):
        self._task_phase_timer.start((task_id, phase_id, node_id))
        if phase_id == 0:
            self._submit_to_phase0_time += self._submit_to_phase0_timer.finish(task_id)
            self._activation_to_phase0_time += self._activation_to_phase0_timer.finish(task_id)

    def task_phase_finished(self, task_id, phase_id, node_id):
        self._task_phase_execution_time += self._task_phase_timer.finish((task_id, phase_id, node_id))

    def object_created(self, object_id, node_id, object_size):
        self._num_objects_created += 1
        self._object_created_size += object_size
        for task_id in self._activation_tracker.object_created(object_id):
            self._activated(task_id)

    def object_transfer_started(self, object_id, object_size, src_node_id, dst_node_id):
        self._num_object_transfers_started += 1
        self._object_transfer_timer.start((object_id, src_node_id, dst_node_id))

    def object_transfer_finished(self, object_id, object_size, src_node_id, dst_node_id):
        self._num_object_transfers_finished += 1
        self._object_transfer_time += self._object_transfer_timer.finish((object_id, src_node_id, dst_node_id))
        self._object_transfer_size += object_size

    def job_ended(self):
        stats = {}
        if self._num_tasks_started != self._num_tasks_finished:
            self.err = 'num tasks started {} does not match num tasks finished {}'.format(
                self._num_tasks_started, self._num_tasks_finished)
            self.completed_successfully = False
            return
        if self._num_tasks_started != self._num_tasks_scheduled:
            self.err = 'num tasks started {} does not match num tasks scheduled {}'.format(
                self._num_tasks_started, self._num_tasks_scheduled)
            self.completed_successfully = False
            return
        if self._num_tasks_started != self._num_tasks_submitted:
            self.err = 'num tasks started {} does not match num tasks submitted {} + 1'.format(
                self._num_tasks_started, self._num_tasks_submitted)
            self.completed_successfully = False
            return
        if self._num_object_transfers_started != self._num_object_transfers_finished:
            self.completed_successfully = False
            return

        stats['job_completion_time'] = self._last_task_finished
        stats['num_tasks'] = self._num_tasks_started
        stats['task_time'] = self._task_execution_time
        stats['task_phase_time'] = self._task_phase_execution_time
        stats['num_tasks_scheduled_locally']  = self._num_staks_scheduled_locally
        stats['max_workers_active'] = self._node_worker_tracker.max_workers_active
        stats['max_nodes_active'] = self._node_worker_tracker.max_nodes_active
        stats['num_object_transfers'] = self._num_object_transfers_finished
        stats['object_transfer_size'] = self._object_transfer_size
        stats['object_transfer_time'] = self._object_transfer_time
        stats['object_created_size'] = self._object_created_size
        stats['num_objects_created'] = self._num_objects_created
        stats['submit_to_schedule_time'] = self._submit_to_schedule_time
        stats['submit_to_phase0_time'] = self._submit_to_phase0_time
        stats['submit_to_activation_time'] = self._submit_to_activation_time
        stats['activation_to_schedule_time'] = self._activation_to_schedule_time
        stats['activation_to_phase0_time'] = self._activation_to_phase0_time

        self.completed_successfully = True
        self.stats = stats

    def __str__(self):
        if not self.completed_successfully:
            return str(self.err)
        return str(self.stats)


class DetailedStats(NoopLogger):
    def __init__(self, system_time):
        self._system_time = system_time

        self._activation_tracker = ActivationTracker()

        self._submit_to_phase0_distribution = self.DistributionTimer('submit to phase0', system_time)
        self._task_time_distribution = self.DistributionTimer('task duration', system_time)

        self._ts_workers_active = self.TimeSeries('workers active', system_time, 0, lambda x, y: max(x, y))
        self._ts_workers_blocked = self.TimeSeries('workers blocked', system_time, 0, lambda x, y: min(x, y))
        self._ts_runnable_tasks = self.TimeSeries('runnable tasks', system_time)
        self._ts_object_transfers_active = self.TimeSeries('object transfers_active', system_time, 0, lambda x, y: max(x,y))

        self._worker_tracker = self.WorkerTracker()
        self._worker_activity = self.ResourceStateTimeseries('worker state', system_time)

        self._last_task_finished = 0

        self.completed_successfully = None
        self.stats = None

    class DistributionTimer():
        def __init__(self, name, system_time):
            self._name = name
            self._system_time = system_time

            self._start_times = {}

            self._times = []

        def start(self, key):
            if key in self._start_times.keys():
                raise RuntimeError('duplicate start event on timer \'{}\' for key {}'.format(self._name, key))
            self._start_times[key] = self._system_time.get_time()

        def finish(self, key):
            elapsed_time = self._system_time.get_time() - self._start_times[key]
            del self._start_times[key]
            self._times.append(elapsed_time)

        def get_times(self):
            if self._start_times:
                raise RuntimeError("Have unfinished timers")
            return self._times

    class TimeSeries(object):
        def __init__(self, name, system_time, initial_value = 0, duplicate_merge=lambda x, y: y):
            self._name = name
            self._system_time = system_time
            self._prev_value = initial_value
            self._prev_timestamp = None
            self.values = []
            self._duplicate_merge = duplicate_merge
            self.update(lambda x: initial_value)

        def update(self, fn):
            new_value = fn(self._prev_value)
            timestamp = self._system_time.get_time()
            new_value_plot = new_value
            if timestamp == self._prev_timestamp:
                self.values.pop()
                new_value_plot = self._duplicate_merge(self._prev_value, new_value)
            self.values.append((timestamp, new_value_plot))
            self._prev_value = new_value
            self._prev_timestamp = timestamp

        def increment(self):
            self.update(lambda x: x + 1)

        def decrement(self):
            self.update(lambda x: x - 1)


    class ResourceStateTimeseries(object):

        def __init__(self, name, system_time):
            self._name = name
            self._system_time = system_time
            self.state_log = defaultdict(list)

        def update(self, resource, state):
            self.state_log[resource].append((self._system_time.get_time(), state))


    class WorkerTracker(object):
        def __init__(self):
            self._worker_index = defaultdict(lambda: 0)
            self._free_workers = defaultdict(list)
            self.task_worker_assignments = {}

        def get_worker(self, node_id, task_id):
            if node_id in self._free_workers and self._free_workers[node_id]:
                worker_id = heapq.heappop(self._free_workers[node_id])
            else:
                worker_id = self._worker_index[node_id]
                self._worker_index[node_id] += 1
            self.task_worker_assignments[task_id] = (node_id, worker_id)
            return worker_id

        def release(self, task_id):
            (node_id, worker_id) = self.task_worker_assignments[task_id]
            del self.task_worker_assignments[task_id]
            heapq.heappush(self._free_workers[node_id], worker_id)
            return (node_id, worker_id)


    def _activated(self, task_id):
        self._ts_runnable_tasks.increment()

    def task_submitted(self, task_id, node_id, dependencies):
        self._submit_to_phase0_distribution.start(task_id)
        if self._activation_tracker.task_submitted(task_id, dependencies):
            self._activated(task_id)

    def task_phase_started(self, task_id, phase_id, node_id):
        self._ts_workers_blocked.decrement()
        if phase_id == 0:
            self._submit_to_phase0_distribution.finish(task_id)
        self._worker_activity.update(
            self._worker_tracker.task_worker_assignments[task_id],
            (task_id, 'running'))

    def task_phase_finished(self, task_id, phase_id, node_id):
        self._ts_workers_blocked.increment()
        self._worker_activity.update(
            self._worker_tracker.task_worker_assignments[task_id],
            (task_id, 'blocked'))

    def object_created(self, object_id, node_id, object_size):
        for task_id in self._activation_tracker.object_created(object_id):
            self._activated(task_id)

    def task_started(self, task_id, node_id):
        self._ts_workers_active.increment()
        self._ts_runnable_tasks.decrement()
        self._ts_workers_blocked.increment()
        self._task_time_distribution.start((task_id, node_id))
        worker_id = self._worker_tracker.get_worker(node_id, task_id)
        self._worker_activity.update(
            (node_id, worker_id),
            (task_id, 'initialized'))

    def task_finished(self, task_id, node_id):
        self._ts_workers_active.decrement()
        self._ts_workers_blocked.decrement()
        self._task_time_distribution.finish((task_id, node_id))
        self._last_task_finished = self._system_time.get_time()
        (node_id, worker_id) = self._worker_tracker.release(task_id)
        self._worker_activity.update(
            (node_id, worker_id),
            (task_id, 'freed'))

    def object_transfer_started(self, object_id, object_size, src_node_id, dst_node_id):
        self._ts_object_transfers_active.increment()

    def object_transfer_finished(self, object_id, object_size, src_node_id, dst_node_id):
        self._ts_object_transfers_active.decrement()


    def job_ended(self):
        stats = {}
        stats['submit_to_phase0_time'] = self._submit_to_phase0_distribution.get_times()
        stats['task_time'] = self._task_time_distribution.get_times()

        stats['workers_active_timeseries'] = self._ts_workers_active.values
        stats['workers_blocked_timeseries'] = self._ts_workers_blocked.values
        stats['runnable_tasks_timeseries'] = self._ts_runnable_tasks.values
        stats['object_transfers_active_timeseries'] = self._ts_object_transfers_active.values

        stats['worker_activity'] = self._worker_activity.state_log

        stats['job_completion_time'] = self._system_time.get_time()

        self.completed_successfully = True
        self.stats = stats

    def __str__(self):
        if not self.completed_successfully:
            return str(self.err)
        return str(self.stats)


class StatsLogger(SummaryStats):
    def __init__(self, system_time):
        SummaryStats.__init__(self, system_time)
        self._system_time = system_time
        self._pylogger = TimestampedLogger(__name__+'.StatsLogger', system_time)
        self._stats = SummaryStats(self._system_time)

    def job_ended(self):
        super(StatsLogger, self).job_ended()

        if self._stats.completed_successfully:
            stats = self._stats.stats
            self._pylogger.info('number of tasks executed {}'.format(stats['num_tasks']))
            self._pylogger.info('total task execution time {}'.format(stats['task_time']))

            self._pylogger.info('number of objects transferred {}'.format(stats['num_object_transfers']))
            self._pylogger.info('size of objects transferred {}'.format(stats['object_transfer_size']))
            self._pylogger.info('amount of time in object transfer {}'.format(stats['object_transfer_time']))
        else:
            self._pylogger.info('Error computing stats - {}'.format(self._stats.err))


class EventLogLogger():
    def __init__(self, system_time):
        self._system_time = system_time
        self._event_log = []

    def _add_event(self, event_name, event_data):
        self._event_log.append({'timestamp': self._system_time.get_time(), 'event_name': event_name, 'event_data': event_data})

    def task_submitted(self, task_id, node_id, dependencies):
        self._add_event('task_submitted', { 'task_id': task_id, 'node_id': node_id, 'dependencies': dependencies })

    def task_scheduled(self, task_id, node_id, is_scheduled_locally):
        self._add_event('task_scheduled', { 'task_id': task_id, 'node_id': node_id, 'is_scheduled_locally': is_scheduled_locally })

    def task_started(self, task_id, node_id):
        self._add_event('task_started', { 'task_id': task_id, 'node_id': node_id })

    def task_finished(self, task_id, node_id):
        self._add_event('task_finished', { 'task_id': task_id, 'node_id': node_id })

    def task_phase_started(self, task_id, phase_id, node_id):
        self._add_event('task_phase_started', { 'task_id': task_id, 'phase_id': phase_id, 'node_id': node_id })

    def task_phase_finished(self, task_id, phase_id, node_id):
        self._add_event('task_phase_finished', { 'task_id': task_id, 'phase_id': phase_id, 'node_id': node_id })

    def object_created(self, object_id, node_id, object_size):
        self._add_event('object_created', { 'object_id': object_id, 'node_id': node_id, 'object_size': object_size })

    def object_transfer_started(self, object_id, object_size, src_node_id, dst_node_id):
        self._add_event('object_transfer_started', { 'object_id': object_id, 'object_size': object_size,
            'src_node_id': src_node_id, 'dst_node_id': dst_node_id })

    def object_transfer_finished(self, object_id, object_size, src_node_id, dst_node_id):
        self._add_event('object_transfer_finished', { 'object_id': object_id, 'object_size': object_size,
            'src_node_id': src_node_id, 'dst_node_id': dst_node_id })

    def job_ended(self):
        if not os.path.exists('sweep'):
            os.makedirs('sweep')
        with gzip.open('sweep/sim_events.gz', 'wb') as f:
            f.write(json.dumps(self._event_log, sort_keys=True, indent=4))

class CompoundLogger():
    def __init__(self, loggers):
        self._loggers = loggers

    def _for_loggers(self, fn, args):
        for logger in self._loggers:
            getattr(logger, fn)(*args)

    def task_submitted(self, task_id, node_id, dependencies):
        self._for_loggers('task_submitted', [task_id, node_id, dependencies])

    def task_scheduled(self, task_id, node_id, is_scheduled_locally):
        self._for_loggers('task_scheduled', [task_id, node_id, is_scheduled_locally])

    def task_started(self, task_id, node_id):
        self._for_loggers('task_started', [task_id, node_id])

    def task_finished(self, task_id, node_id):
        self._for_loggers('task_finished', [task_id, node_id])

    def task_phase_started(self, task_id, phase_id, node_id):
        self._for_loggers('task_phase_started', [task_id, phase_id, node_id])

    def task_phase_finished(self, task_id, phase_id, node_id):
        self._for_loggers('task_phase_finished', [task_id, phase_id, node_id])

    def object_created(self, object_id, node_id, object_size):
        self._for_loggers('object_created', [object_id, node_id, object_size])

    def object_transfer_started(self, object_id, object_size, src_node_id, dst_node_id):
        self._for_loggers('object_transfer_started', [object_id, object_size, src_node_id, dst_node_id])

    def object_transfer_finished(self, object_id, object_size, src_node_id, dst_node_id):
        self._for_loggers('object_transfer_finished', [object_id, object_size, src_node_id, dst_node_id])

    def job_ended(self):
        self._for_loggers('job_ended', [])
