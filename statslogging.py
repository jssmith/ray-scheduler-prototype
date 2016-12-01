import os
import json
import gzip

from helpers import TimestampedLogger

class PrintingLogger(object):
    def __init__(self, system_time):
        self._pylogger = TimestampedLogger(__name__+'.PrintingLogger', system_time)

    def task_started(self, task_id, node_id):
        self._pylogger.debug('started task {} on node {}'.format(task_id, node_id))

    def task_finished(self, task_id, node_id):
        self._pylogger.debug('finished task {} on node {}'.format(task_id, node_id))

    def object_transfer_started(self, object_id, object_size, src_node_id, dst_node_id):
        self._pylogger.debug('started transfer of object {} of size {} from node {} to node {}'.format(object_id, object_size, src_node_id, dst_node_id))

    def object_transfer_finished(self, object_id, object_size, src_node_id, dst_node_id):
        self._pylogger.debug('finished transfer of object {} of size {} from node {} to node {}'.format(object_id, object_size, src_node_id, dst_node_id))

    def job_ended(self):
        pass


class StatsLogger(PrintingLogger):
    def __init__(self, system_time):
        PrintingLogger.__init__(self, system_time)
        self._system_time = system_time
        self._pylogger = TimestampedLogger(__name__+'.StatsLogger', system_time)

        self._num_tasks_started = 0
        self._num_tasks_finished = 0
        self._task_execution_time = 0

        self._num_object_transfers_started = 0
        self._num_object_transfers_finished = 0
        self._object_transfer_time = 0
        self._object_transfer_size = 0

        self._task_timer = self.Timer('task execution', self._system_time)
        self._object_transfer_timer = self.Timer('object transfer', self._system_time)

        self._event_log = []

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

    def _add_event(self, event_name, event_data):
        self._event_log.append({'timestamp': self._system_time.get_time(), 'event_name': event_name, 'event_data': event_data})

    def task_started(self, task_id, node_id):
        super(StatsLogger, self).task_started(task_id, node_id)
        self._num_tasks_started += 1
        self._task_timer.start((task_id, node_id))
        self._add_event('task_started', { 'task_id': task_id, 'node_id': node_id })

    def task_finished(self, task_id, node_id):
        super(StatsLogger, self).task_finished(task_id, node_id)
        self._num_tasks_finished += 1
        self._task_execution_time += self._task_timer.finish((task_id, node_id))
        self._add_event('task_finished', { 'task_id': task_id, 'node_id': node_id })

    def object_transfer_started(self, object_id, object_size, src_node_id, dst_node_id):
        super(StatsLogger, self).object_transfer_started(object_id, object_size, src_node_id, dst_node_id)
        self._num_object_transfers_started += 1
        self._object_transfer_timer.start((object_id, src_node_id, dst_node_id))
        self._add_event('object_transfer_started', { 'object_id': object_id, 'object_size': object_size,
            'src_node_id': src_node_id, 'dst_node_id': dst_node_id })

    def object_transfer_finished(self, object_id, object_size, src_node_id, dst_node_id):
        super(StatsLogger, self).object_transfer_started(object_id, object_size, src_node_id, dst_node_id)
        self._num_object_transfers_finished += 1
        self._object_transfer_time += self._object_transfer_timer.finish((object_id, src_node_id, dst_node_id))
        self._object_transfer_size += object_size
        self._add_event('object_transfer_finished', { 'object_id': object_id, 'object_size': object_size,
            'src_node_id': src_node_id, 'dst_node_id': dst_node_id })

    def job_ended(self):
        if self._num_tasks_started != self._num_tasks_finished:
            self._pylogger('unable to summarize - incomplete tasks')
            return
        if self._num_object_transfers_started != self._num_object_transfers_finished:
            self._pylogger('unable to summarize - incomplete object transfers')
            return

        self._pylogger.info('number of tasks executed {}'.format(self._num_tasks_finished))
        self._pylogger.info('total task execution time {}'.format(self._task_execution_time))

        self._pylogger.info('number of objects transferred {}'.format(self._num_object_transfers_finished))
        self._pylogger.info('size of objects transferred {}'.format(self._object_transfer_size))
        self._pylogger.info('amount of time in object transfer {}'.format(self._object_transfer_time))

        if not os.path.exists('sweep'):
            os.makedirs('sweep')
        with gzip.open('sweep/sim_events.gz', 'wb') as f:
            f.write(json.dumps(self._event_log))
