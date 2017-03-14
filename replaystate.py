import sys
import heapq
import itertools
import types

from collections import defaultdict, namedtuple
from collections import deque
from schedulerbase import *
from helpers import TimestampedLogger

################################################################
#        Scheduler Database that Replays Saved Traces          #
################################################################

class ReplaySchedulerDatabase(AbstractSchedulerDatabase):

    class HandlerContext():
        def __init__(self, replay_scheduler_database, update):
            self.replay_scheduler_database = replay_scheduler_database
            self.update = update

    def __init__(self, event_simulation, logger, computation, num_nodes, num_workers_per_node, data_transfer_time_cost, db_message_delay):
        self._pylogger = TimestampedLogger(__name__+'.ReplaySchedulerDatabase', event_simulation)

        self._event_simulation = event_simulation
        self._logger = logger
        self._computation = computation
        self._data_transfer_time_cost = data_transfer_time_cost
        self._db_message_delay = db_message_delay

        self._global_scheduler_update_handlers = []
        self._local_scheduler_update_handlers = defaultdict(list)

        self._finished_objects = {}
        self._executing_tasks = {}
        self._pending_needs = {}
        self._pending_info = {}
        self._awaiting_completion = {}

        # schedule worker registration
        for i in range(0, num_nodes):
            self._event_simulation.schedule_immediate(lambda i=i: self._handle_update(RegisterNodeUpdate(i, num_workers_per_node)))

    def _context(self, update):
        return ReplaySchedulerDatabase.HandlerContext(self, update)

    def submit(self, task, submitting_node_id, is_scheduled_locally):
        if is_scheduled_locally:
            self._logger.task_scheduled(task.id(), submitting_node_id, True)
        self._yield_global_scheduler_update(ForwardTaskUpdate(task, submitting_node_id, is_scheduled_locally))

    def increment_workers(self, submitting_node_id, increment):
        self._yield_global_scheduler_update(AddWorkerUpdate(submitting_node_id, increment=increment))

    def finished(self, task_id):
        #if task_id != self.root_task_id:
        self._yield_global_scheduler_update(FinishTaskUpdate(task_id))

    def object_ready(self, object_description, submitting_node_id):
        self._yield_global_scheduler_update(ObjectReadyUpdate(object_description, submitting_node_id))

    def register_node(self, node_id, num_workers, resource_capacity):
        print 'Not implemented: register_node'
        sys.exit(1)

    def remove_node(self, node_id):
        print 'Not implemented: remove_node'
        sys.exit(1)

    def get_global_scheduler_updates(self, update_handler):
        self._global_scheduler_update_handlers.append(update_handler)

    def get_local_scheduler_updates(self, node_id, update_handler):
        self._local_scheduler_update_handlers[str(node_id)].append(update_handler)

    def _handle_update(self, nextUpdate):
        if isinstance(nextUpdate, RegisterNodeUpdate):
            self._yield_global_scheduler_update(nextUpdate)
        elif isinstance(nextUpdate, ObjectReadyUpdate):
            self._yield_global_scheduler_update(nextUpdate)
        else:
            raise NotImplementedError('Unable to handle update of type {}'.format(type(nextUpdate)))

    def _yield_global_scheduler_update(self, update):
        self._pylogger.debug('sending update to global scheduler: {}'.format(str(update)))
        for handler in self._global_scheduler_update_handlers:
            self._event_simulation.schedule_delayed(self._db_message_delay, lambda handler=handler:handler(update))

    def _yield_local_scheduler_update(self, update):
        self._pylogger.debug('sending update to node {} local scheduler: {}'.format(str(update.node_id), str(update)))
#        print "yield locally targeting {}".format(update.node_id)
#        print "lsh" + str(self._local_scheduler_update_handlers)
        for handler in self._local_scheduler_update_handlers[str(update.node_id)]:
#            print "SDB sending update {} to node {}".format(update, update.node_id)
            self._event_simulation.schedule_delayed(self._db_message_delay, lambda handler=handler: handler(update))

    def schedule(self, node_id, task_id):
        self._logger.task_scheduled(task_id, node_id, False)
#        print("State DB received request to schedule task {} on node {}".format(task_id, node_id))
        # TODO: add delay in propagating to local scheduler
        self._yield_local_scheduler_update(ScheduleTaskUpdate(self._computation.get_task(task_id), node_id))

    def schedule_root(self, node_id):
        # we schedule the root task separately, initiating it out of global state
        # by sending a scheduling update to the local scheduler and a forwarding
        # update to the global scheduler.
        root_task = self._computation.get_root_task()
        if root_task is not None:
            self.root_task_id = root_task.id()
            self._logger.task_submitted(self.root_task_id, node_id, [])
            self._event_simulation.schedule_immediate(lambda: self.schedule(node_id, self.root_task_id))
            self._event_simulation.schedule_immediate(lambda: self._yield_global_scheduler_update(ForwardTaskUpdate(root_task, node_id, True)))


class NoopObjectCache():

    def __init__(self, node_id, event_simulation, logger):
        pass

    def add_object(self, object_id, object_size):
        pass

    def use_object(self, object_id):
        pass


class FIFOObjectCache():

    ObjectInfo = namedtuple('ObjectInfo', ['object_size', 'timestamp', 'cum_ct', 'cum_size'])

    def __init__(self, node_id, event_simulation, logger):
        self.node_id = node_id
        self._event_simulation = event_simulation
        self._logger = logger
        self._object_info = {}
        self._cum_ct = 0
        self._cum_size = 0

    def add_object(self, object_id, object_size):
        self._cum_ct += 1
        self._cum_size += object_size
        self._object_info[object_id] = FIFOObjectCache.ObjectInfo(object_size, self._event_simulation.get_time(), self._cum_ct, self._cum_size)
        self._logger.object_instance_added(object_id, self.node_id, object_size)

    def use_object(self, object_id):
        if not object_id in self._object_info:
            raise RuntimeError('Object {} was not added'.format(object_id))
        object_info = self._object_info[object_id]
        self._logger.object_used(object_id, self.node_id, object_info.object_size, object_info.cum_ct, object_info.cum_size)


class LRUObjectCache():
    class ListNode():
        def __init__(self, object_id, object_size, prev_node, next_node):
            self.object_id = object_id
            self.object_size = object_size
            self.prev_node = prev_node
            self.next_node = next_node

    def __init__(self, node_id, event_simulation, logger):
        self.node_id = node_id
        self._event_simulation = event_simulation
        self._logger = logger
        self._node_lookup = {}
        self._cum_ct = 0
        self._cum_size = 0
        self._root = None

    def add_object(self, object_id, object_size):
        old_root = self._root
        new_root = LRUObjectCache.ListNode(object_id, object_size, None, old_root)
        if old_root is not None:
            old_root.prev_node = new_root
        self._node_lookup[object_id] = new_root
        self._root = new_root
        self._logger.object_instance_added(object_id, self.node_id, object_size)

    def use_object(self, object_id):
        cum_ct = 1
        cum_size = self._root.object_size
        node = self._root
        while node.object_id != object_id:
            node = node.next_node
            if node is None:
                raise RuntimeError('object id {} not found'.format(object_id))
            cum_ct += 1
            cum_size += node.object_size
        object_size = node.object_size
        if node != self._root:
            node.prev_node.next_node = node.next_node
            if node.next_node is not None:
                node.next_node.prev_node = node.prev_node
            self._root.prev_node = node
            node.next_node = self._root
            node.prev_node = None
            self._root = node
        self._logger.object_used(object_id, self.node_id, object_size, cum_ct, cum_size)


class ObjectStoreRuntime():
    def __init__(self, event_simulation, logger, data_transfer_time_cost, db_message_delay, cache_class):
        self._event_simulation = event_simulation
        self._logger = logger
        self._object_locations = defaultdict(lambda: {})
        self._object_sizes = {}
        self._update_handlers = defaultdict(list)
        self._data_transfer_time_cost = data_transfer_time_cost
        self._db_message_delay = db_message_delay
        self._awaiting_completion = defaultdict(list)
        self._awaiting_copy = defaultdict(list)
        self._object_cache = {}
        self._cache_class = cache_class

    def _get_object_cache(self, node_id):
        if node_id not in self._object_cache:
            self._object_cache[node_id] = self._cache_class(node_id, self._event_simulation, self._logger)
        return self._object_cache[node_id]

    def expect_object(self, object_id, node_id):
        if node_id not in self._object_locations[object_id] or self._object_locations[object_id][node_id] != ObjectStatus.READY:
            self._object_locations[object_id][node_id] = ObjectStatus.EXPECTED

    def add_object(self, object_id, node_id, object_size):
        self._logger.object_created(object_id, node_id, object_size)
        self._object_locations[object_id][node_id] = ObjectStatus.READY
        self._object_sizes[object_id] = object_size
        self._get_object_cache(node_id).add_object(object_id, object_size)
        self._yield_object_ready_update(object_id, node_id, object_size)
        if object_id in self._awaiting_completion.keys():
            for (d_node_id, on_done) in self._awaiting_completion[object_id]:
                if node_id == d_node_id:
                    on_done()
                else:
                    self._copy_object(object_id, d_node_id, node_id, on_done) 
            del self._awaiting_completion[object_id]

    def get_updates(self, node_id, update_handler):
        self._update_handlers[str(node_id)].append(update_handler)

    def is_local(self, object_id, node_id):
        if node_id in self._object_locations[object_id]:
            return self._object_locations[object_id][node_id]
        else:
            return ObjectStatus.UNKNOWN

    def is_locally_ready(self, object_id, node_id):
        return self.is_local(object_id, node_id) == ObjectStatus.READY


    def get_object_size(self, node_id, object_id, result_handler):
        handler = lambda: result_handler(self._object_sizes(object_id))
        # Add a delay if the object is not local to the node that requested it.
        if node_id in self._object_locations:
            self._event_simulation.schedule_immediate(handler)
        else:
            self._event_simulation.schedule_delayed(self._db_message_delay, handler)

    def get_object_size_locations(self, object_id, result_handler):
        if object_id in self._object_sizes.keys():
            size = self._object_sizes[object_id]
        else:
            size = None
        if object_id in self._object_locations.keys():
            location_status = self._object_locations[object_id]
        else:
            location_status = None
        handler = lambda: result_handler(object_id, size, location_status)
        self._event_simulation.schedule_delayed(self._db_message_delay, handler)

    def _yield_object_ready_update(self, object_id, node_id, object_size):
        self._yield_update(node_id, ObjectReadyUpdate(ObjectDescription(object_id, node_id, object_size), node_id))

    def _yield_update(self, node_id, update):
        for update_handler in self._update_handlers[node_id]:
            update_handler(update)

    def use_object(self, object_id, node_id):
        if not node_id in self._object_locations[object_id] or self._object_locations[object_id][node_id] != ObjectStatus.READY:
            raise RuntimeError('Use of object not available locally - {} on node {} has status {}'.format(object_id, node_id, self._object_locations[object_id][node_id]))
        self._get_object_cache(node_id).use_object(object_id)

    def require_object(self, object_id, node_id, on_done):
        object_ready_locations = dict(filter(lambda (node_id, status): status == ObjectStatus.READY, self._object_locations[object_id].items()))
        if node_id in object_ready_locations.keys():
            # TODO - we aren't firing the ObjectReadyUpdate in this scenario. Should we be doing so?
#            print "require has locally"
            on_done()
        else:
            if not object_ready_locations:
                self._awaiting_completion[object_id].append((node_id, on_done))
            else:
                # TODO better way to choose an element from a set than list(set)[0]
                source_location = list(object_ready_locations.keys())[0]
                self._copy_object(object_id, node_id, source_location, on_done)

    def _copy_object(self, object_id, dst_node_id, src_node_id, on_done):
        if dst_node_id == src_node_id:
            on_done()
        else:
            if (object_id, dst_node_id) in self._awaiting_copy.keys():
                self._awaiting_copy[(object_id, dst_node_id)].append(on_done)
            elif src_node_id in self._object_locations[object_id].keys() and self._object_locations[object_id][src_node_id] == ObjectStatus.READY:
                self.use_object(object_id, src_node_id)
                object_size = self._object_sizes[object_id]
                data_transfer_time = self._db_message_delay + object_size * self._data_transfer_time_cost
                self._logger.object_transfer_started(object_id, object_size, src_node_id, dst_node_id)
                self._awaiting_copy[(object_id, dst_node_id)].append(on_done)
                self._event_simulation.schedule_delayed(data_transfer_time, lambda: self._object_copied(object_id, object_size, src_node_id, dst_node_id))
            else:
                raise RuntimeError('Unexpected failure to copy object {} to {} from {}'.format(object_id, dst_node_id, src_node_id))

    def _object_copied(self, object_id, object_size, src_node_id, dst_node_id):
        self._logger.object_transfer_finished(object_id, object_size, src_node_id, dst_node_id)
        self._object_locations[object_id][dst_node_id] = ObjectStatus.READY
        self._get_object_cache(dst_node_id).add_object(object_id, object_size)
        self._yield_object_ready_update(object_id, dst_node_id, self._object_sizes[object_id])
        on_done_fns = self._awaiting_copy[(object_id, dst_node_id)]
        del self._awaiting_copy[(object_id, dst_node_id)]
        for on_done in on_done_fns:
            on_done()


class ObjectDescription():
    def __init__(self, object_id, node_id, size):
        self.object_id = str(object_id)
        self.node_id = node_id
        self.size = size

    def __str__(self):
        return "ObjectDescription({}, {}, {})".format(self.object_id, self.node_id, self.size)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.object_id == other.object_id and self.node_id == other.node_id and self.size == other.size
        else:
            return False


class NodeRuntime():
    def __init__(self, event_simulation, object_store, logger, computation, node_id, num_workers, resource_capacity, num_nodes):
        self._pylogger = TimestampedLogger(__name__+'.NodeRuntime', event_simulation)
        self._event_simulation = event_simulation
        self._object_store = object_store
        self._logger = logger
        self._computation = computation
        self._update_handlers = []
        self.num_workers = num_workers
        self.resource_capacity = resource_capacity
        self.node_id = node_id
        self.num_nodes = num_nodes
        self.num_workers_executing = 0
        self.resources_executing = {}
        for resource in self.resource_capacity.keys():
          self.resources_executing[resource] = 0


        self.blocked_tasks = []


        self._time_buffer_size = 30
        self._task_start_times = deque([0], self._time_buffer_size)   
        self._task_times = deque([], self._time_buffer_size)
        self._task_start_times_map = {}

        self._queue_seq = 0
        self._queue = []

        # Just pass through the object store updates
        self._object_store.get_updates(self.node_id, lambda update: self._yield_update(update))

    def is_local(self, object_id):
        return self._object_store.is_local(object_id, self.node_id)

    def get_object_size_locations(self, object_id, result_handler):
        return self._object_store.get_object_size_locations(object_id, result_handler)

    def get_object_size(self, object_id, handler):
        return self._object_store.get_object_size(self.node_id, object_id,
                                                  handler)

    def get_dispatch_queue_size(self):
        #print "threshold scheduler debug: dispatcher queue is: {}".format(self._queue)
        return len(self._queue)

    def get_node_eff_rate(self):
        #print "threshold scheduler debug: task_start_times buffer is {}".format(self._task_start_times)
        if (self._task_start_times[-1] - self._task_start_times[0]) == 0 :
            return 0
        return len(self._task_start_times) / (self._task_start_times[-1] - self._task_start_times[0])

    def get_avg_task_time(self):
        return 1 if len(self._task_times) == 0 else sum(self._task_times) / len(self._task_times)

    def send_to_dispatcher(self, task, priority):
        if priority != 0: #if this is not a continuing task that was already started
           for result in task.get_results():
               self._object_store.expect_object(result.object_id, self.node_id)
        self._pylogger.debug('Dispatcher at node {} received task {} with priority {}'.format(self.node_id, task.id(), priority))
        task_id = task.id()
        heapq.heappush(self._queue, (priority, self._queue_seq, task_id))
        self._queue_seq += 1
        self._event_simulation.schedule_immediate(lambda: self._process_tasks())

    def get_updates(self, update_handler):
        self._update_handlers.append(update_handler)

    def free_workers(self):
        return self.num_workers - self.num_workers_executing

    def free_resource(self, resource):
        return self.resource_capacity[resource] - self.resources_executing[resource]

    def free_resources(self):
        free_resources_flag = False
        for resource in self.resource_capacity.keys():
           if self.resource_capacity[resource] != self.resources_executing[resource]:
              free_resources_flag = True
        return free_resources_flag             


    def enough_resources(self, resources):
        free_resources_flag = True
        for resource in resources:
           free_resources_flag = free_resources_flag and (self.free_resource(resource) > 0)
        return free_resources_flag

    def acquire_resources(self, resources):
       for resource in resources.keys():
         self.resources_executing[resource] += resources[resource]

    def release_resources(self, resources):
       for resource in resources.keys():
         self.resources_executing[resource] -= resources[resource]


    class TaskSubmitted():
        def __init__(self, submitted_task_id, phase_id):
            self.submitted_task_id = submitted_task_id
            self.phase_id = phase_id

    class TaskPhaseComplete():
        def __init__(self, task_id, phase_id):
            self.task_id = task_id
            self.phase_id = phase_id

    class Dependencies():
        def __init__(self, node_runtime, task_id, phase_id):
            self._node_runtime = node_runtime
            self._task_id = task_id
            self._phase_id = phase_id

            self.object_dependencies = set()

        def has_dependencies(self):
            return bool(self.object_dependencies)

        def add_object_dependency(self, object_id):
            self.object_dependencies.add(object_id)

        def object_available(self, object_id):
            self.object_dependencies.remove(object_id)
            if not self.object_dependencies:
                #old implementation without releasing-resources
                #self._node_runtime._execute_phase_immediate(self._task_id, self._phase_id)
                self.send_to_dispatcher(task_id,0)

    def _start_task(self, task_id):
        self._task_start_times.append(self._event_simulation.get_time())
        self._task_start_times_map[task_id] = self._event_simulation.get_time()
        self.num_workers_executing += 1
        self._logger.task_started(task_id, self.node_id)
        self._internal_scheduler_schedule(task_id, 0)


    def _continue_task(self, task_id, task_phase):
        self._logger.task_started(task_id, self.node_id)


    def _process_tasks(self):
        #print "threshold scheduler debug: dispatcher queue in node {} is {}".format(self.node_id, self._queue)
        #old implementation
        #while self.num_workers_executing < self.num_workers and self._queue:
        #    (_, _, task_id) = heapq.heappop(self._queue)
        #    self._start_task(task_id)
        
        #since we now have a vector of resources, we might have elements deeper in the queue that do have enough resources, while the
        #front of the queue does not. Therefore we need to scan through the entire queue (popping out all the elements and pushing them
        #into a new queue) to do that
        if self.free_resources():
            temp_queue = []
            while self._queue:
                (priorirty,seq,task_id) = heapq.heappop(self._queue)
                task_resources = self._computation.get_task(task_id).get_resources()
                if self.enough_resources(task_resources):
                   if self._computation.get_task(task_id).get_next_phase_to_run(): #if this is a continuing task (phase is not 0. it was preemted earlier)
                      self._continue_task(task_id, self._computation.get_task(task_id).get_next_phase_to_run())
                   else: #if this is a new task (phase 0)
                      self._start_task(task_id)
                else:
                   heapq.heappush(self.temp_queue, (priority,seq,task_id))
            self._queue = temp_queue
        

    def _yield_update(self, update):
        for update_handler in self._update_handlers:
            update_handler(update)

    def _execute_phase_immediate(self, task_id, phase_id):
        self._pylogger.debug('executing task {} phase {}'.format(task_id, phase_id))
        self._logger.task_phase_started(task_id, phase_id, self.node_id)
        task_phase = self._computation.get_task(task_id).get_phase(phase_id)
        task_resources = self._computation.get_task(task_id).get_resources()
        self.acquire_resources(task_resources)
        for d_object_id in task_phase.depends_on:
            self._object_store.use_object(d_object_id, self.node_id)
        for put_event in task_phase.creates:
            self._event_simulation.schedule_delayed(
                    put_event.time_offset,
                    lambda put_event=put_event:
                    self._object_store.add_object(put_event.object_id,
                        self.node_id, put_event.size)
                    )
            self._event_simulation.schedule_delayed(
                    put_event.time_offset,
                    lambda put_event=put_event:
                    self._yield_update(ObjectReadyUpdate(ObjectDescription(put_event.object_id,
                        self.node_id, put_event.size), self.node_id))
                    )
        for schedule_task in task_phase.submits:
            self._event_simulation.schedule_delayed(schedule_task.time_offset, lambda s_task_id=schedule_task.task_id: self._handle_update(self.TaskSubmitted(s_task_id, 0)))
        self._computation.get_task(task_id).increment_next_phase_to_run()
        self._event_simulation.schedule_delayed(task_phase.duration, lambda: self._handle_update(self.TaskPhaseComplete(task_id, phase_id)))

    def _internal_scheduler_schedule(self, task_id, phase_id):
        task = self._computation.get_task(task_id)
        task_phase = task.get_phase(phase_id)
        depends_on = task_phase.depends_on
        needs = self.Dependencies(self, task_id, phase_id)
        for d_object_id in depends_on:
            if not self._object_store.is_locally_ready(d_object_id, self.node_id):
                needs.add_object_dependency(d_object_id)
                self._object_store.require_object(d_object_id, self.node_id, lambda d_object_id=d_object_id: needs.object_available(d_object_id))
        if not needs.has_dependencies():
            if task.is_root and phase_id == 0:
                self._yield_update(AddWorkerUpdate())
                self.num_workers += 1
            self._execute_phase_immediate(task_id, phase_id)
        else:
            self.release_resources(task.get_resources())
            self.blocked_tasks.append((task_id,phase_id))
            self._pylogger.debug('task {} phase {} waiting for dependencies: {}'.format(task_id, phase_id, str(needs.object_dependencies)))

    def _handle_update(self, update):
        if isinstance(update, self.TaskSubmitted):
            task = self._computation.get_task(update.submitted_task_id)
            self._logger.task_submitted(update.submitted_task_id, self.node_id, task.get_depends_on())
            self._event_simulation.schedule_immediate(lambda: self._yield_update(SubmitTaskUpdate(task)))
        elif isinstance(update, self.TaskPhaseComplete):
            self._pylogger.debug('completed task {} phase {}'.format(update.task_id, update.phase_id))
            self._logger.task_phase_finished(update.task_id, update.phase_id, self.node_id)
            task = self._computation.get_task(update.task_id)
            if update.phase_id < task.num_phases() - 1:
                self._pylogger.debug('task {} has further phases'.format(update.task_id))
                self._event_simulation.schedule_immediate(lambda: self._internal_scheduler_schedule(update.task_id, update.phase_id + 1))
            else:
                self._pylogger.debug('completed task {}'.format(update.task_id))
                self._logger.task_finished(update.task_id, self.node_id)
                for res in task.get_results():
                    self._object_store.add_object(res. object_id, self.node_id, res.size)
                self._yield_update(FinishTaskUpdate(update.task_id))
                if task.is_root:
                    self._yield_update(AddWorkerUpdate(increment=-1))
                self.num_workers_executing -= 1
                self.release_resources(task.get_resources())
                self._task_times.append(self._event_simulation.get_time() - self._task_start_times_map.get(update.task_id, 0)) 
                #print "task_times: {}".format(self._task_times)
                self._event_simulation.schedule_immediate(lambda: self._process_tasks())
        else:
            raise NotImplementedError('Unknown update: {}'.format(type(update)))



################################################################
#        Simulation timesource & deferred execution            #
################################################################

class SystemTime():
    def __init__(self, event_simulation):
        self._event_simulation = event_simulation

    def get_time(self):
        return self._event_simulation.get_time()

class EventSimulation():
    def __init__(self):
        self._t = 0
        self._scheduled = []
        self._scheduled_seq = 0
        self._pylogger = TimestampedLogger(__name__+'.EventSimulation', self)

    def get_time(self):
        return self._t

    def schedule_at(self, t, fn):
        if self._t > t:
            print 'invalid schedule request'
            sys.exit(1)
        heapq.heappush(self._scheduled, (t, self._scheduled_seq, fn))
        self._scheduled_seq += 1

    def schedule_delayed(self, delta, fn):
        self._pylogger.debug('Scheduling {} {} at time {}'.format(fn,
            fn.__name__, self._t + delta))
        self.schedule_at(self._t + delta, fn)

    def schedule_immediate(self, fn):
        self.schedule_at(self._t, fn)

    def advance(self):
        if len(self._scheduled) > 0:
            (self._t, _, scheduled) = heapq.heappop(self._scheduled)
            self._pylogger.debug('Advancing to {} {} at time '
                '{}'.format(scheduled, scheduled.__name__, self._t))
            scheduled()
        return len(self._scheduled) > 0

    def advance_fully(self):
        while self.advance():
            pass

    def queue_empty(self):
        return not self._scheduled

class EventLoop():
    class EventLoopData():
        def __init__(self, event_loop, timer_id, handler, context):
            self.timer_id = timer_id
            self.handler = handler
            self.context = context
            self.is_cancelled = False

    def __init__(self, timesource):
        self._event_simulation = timesource
        self._timer_id_seq = 1
        self._timers = {}

    def timer_handler(self, context):
        if context.timer_id not in self._timers.keys():
            raise RuntimeError('Invalid timer')
        del self._timers[context.timer_id]
        if not context.is_cancelled:
            context.handler(context.context)

    def add_timer(self, delta, handler, context):
        timer_id = self._timer_id_seq
        self._timer_id_seq += 1
        context = EventLoop.EventLoopData(self, timer_id, handler, context)
        self._timers[timer_id] = context
        self._event_simulation.schedule_delayed(delta, lambda: self.timer_handler(context))
        return timer_id

    def remove_timer(self, timer_id):
        if timer_id not in self._timers.keys():
            raise RuntimeError('Timer is not active')
        self._timers[timer_id].is_cancelled = True


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

        tasks_map = {}
        for task in tasks:
            # task ids must be unique
            if task.id() in tasks_map:
                raise ValidationError('Task ids must be unique')
            tasks_map[task.id()] = task

        self._root_task = str(root_task)
        self._tasks = tasks_map
        self._tasks[self._root_task].is_root = True
        #self.assign_task_depths()

    def assign_task_depths(self):
        root_task = self._tasks[self._root_task]
        root_task.depth = 0

        to_visit = [root_task]
        visited = set()
        object_depths = {}

        while to_visit:
            new_to_visit = []
            for task in to_visit:
                if task in visited:
                    raise ValidationError('task graph must be tree')
                visited.add(task)
                max_depth = 0
                for depends_on_object_id in task.get_depends_on():
                    if depends_on_object_id not in object_depths:
                        raise ValidationError('missing object depth')
                    if object_depths[depends_on_object_id] > max_depth:
                        max_depth = object_depths[depends_on_object_id]
                task.depth = max_depth + 1
                # print "depth of {} is {}".format(task.id(), task.depth)
                for res_object_id in map(lambda r: r.object_id, task.get_results()):
                    object_depths[res_object_id] = task.depth
                for phase_id in range(task.num_phases()):
                    for res_object_id in map(lambda r: r.object_id, task.get_phase(phase_id).creates):
                        object_depths[res_object_id] = task.depth
                for phase_id in range(task.num_phases()):
                    for submits in task.get_phase(phase_id).submits:
                        submits_task = self._tasks[submits.task_id]
                        new_to_visit.append(submits_task)
            to_visit = new_to_visit


    def verify(self):
        tasks = self._tasks.values()
        # all tasks should be called exactly once
        called_tasks = set([self._root_task])
        for task in tasks:
            for phase_id in range(0, task.num_phases()):
                for task_id in map(lambda x: x.task_id, task.get_phase(phase_id).submits):
                    if task_id in called_tasks:
                        raise ValidationError('Duplicate call to task {}'.format(task_id))
                    if task_id not in self._tasks:
                        raise ValidationError('Call to undefined task {}'.format(task_id))
                    called_tasks.add(task_id)
        if len(called_tasks) < len(self._tasks.keys()):
            task_ids_set = set(self._tasks.keys())
            tasks_not_called = task_ids_set.difference(called_tasks)
            raise ValidationError('Some tasks are not called: {}'.format(str(tasks_not_called)))
        if len(called_tasks) > len(self._tasks.keys()):
            raise ValidationError('Too many tasks are called - called {} tasks but have {} tasks'.format(len(called_tasks), len(self._tasks.keys())))

        # no dependencies that don't get created
        result_objects = set()
        for task in tasks:
            for phase_id in range(0, task.num_phases()):
                for object_put in task.get_phase(phase_id).creates:
                    if object_put.object_id in result_objects:
                        raise ValidationError('Duplicate put object id {}'.format(object_put.object_id))
                    result_objects.add(object_put.object_id)
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
        validation_dg = DirectedGraph()
        for task in tasks:
            prev_phase = None
            for phase_id in range(0, task.num_phases()):
                phase = task.get_phase(phase_id)
                if prev_phase:
                    #print "EDGE: previous phase edge"
                    validation_dg.add_edge(validation_dg.get_id(prev_phase), validation_dg.get_id(phase))
                for object_id in phase.depends_on:
                    #print "EDGE: phase dependency edge"
                    validation_dg.add_edge(validation_dg.get_id(object_id), validation_dg.get_id(phase))
                for submits in phase.submits:
                    #print "EDGE: phase schedules edge"
                    validation_dg.add_edge(validation_dg.get_id(phase), validation_dg.get_id(self._tasks[submits.task_id].get_phase(0)))
                for creates in phase.creates:
                    #print "EDGE: phase creates object"
                    validation_dg.add_edge(validation_dg.get_id(phase), validation_dg.get_id(creates.object_id))

                prev_phase = phase
            for task_result in task.get_results():
                #print "EDGE: task result edge"
                validation_dg.add_edge(validation_dg.get_id(prev_phase), validation_dg.get_id(task_result.object_id))
        validation_dg.verify_dag_root(validation_dg.get_id(self._tasks[self._root_task].get_phase(0)))

    def analyze(self):
        # Build the task graph.
        load_dg = DirectedGraph()
        total_num_tasks = 0
        total_num_objects = 0
        total_objects_size = 0
        tasks = self._tasks.values()
        for task in tasks:
            prev_phase = None
            for phase_id in range(0, task.num_phases()):
                phase = task.get_phase(phase_id)
                if prev_phase:
                    #print "EDGE: previous phase edge"
                    load_dg.add_edge(prev_phase, phase)
                for object_id in phase.depends_on:
                    #print "EDGE: phase dependency edge"
                    load_dg.add_edge(object_id, phase)
                for submits in phase.submits:
                    #print "EDGE: phase schedules edge"
                    load_dg.add_edge(phase, self._tasks[submits.task_id].get_phase(0))
                for creates in phase.creates:
                    #print "EDGE: phase creates object"
                    load_dg.add_edge(phase, creates.object_id)
                    total_num_objects += 1
                    total_objects_size += creates.size

                prev_phase = phase
            for task_result in task.get_results():
                #print "EDGE: task result edge"
                load_dg.add_edge(prev_phase, task_result.object_id)
                total_num_objects += 1
                total_objects_size += task_result.size
            total_num_tasks += 1

        #load measure:
        topo_sort_nodes = load_dg.topo_sort(self._tasks[self._root_task].get_phase(0))
        total_tasks_durations = 0
        total_num_tasks_verify = 0
        critical_path = {}
        critical_path[self._tasks[self._root_task].get_phase(0)] = self._tasks[self._root_task].get_phase(0).duration
        #print "topo sort nodes is {}".format(topo_sort_nodes)
        for n in topo_sort_nodes:
            if isinstance(n, TaskPhase):
                total_tasks_durations += n.duration
                if n.phase_id == 0:
                    total_num_tasks_verify += 1
            else:
                #TODO: this means this is an object_id. We need to have a map of (object_id, size) to calculate the object size average, or total object sizes
            #find critical path:
                pass
            for u in load_dg.adj_in(n):
                if isinstance(n, TaskPhase):
                    #print "instance n is {}, instance u is {}".format(n, u)
                    if critical_path.get(n,0) < critical_path[u] + n.duration:
                        critical_path[n] = critical_path[u] + n.duration
                else:
                    if critical_path.get(n,0) < critical_path[u]:
                        critical_path[n] = critical_path[u]
        
        if total_num_tasks_verify != total_num_tasks:
            print "Error: Number of tasks in DAG does not match number of tasks in the trace"
            return             
        
        critical_path_time = 0
        for c in critical_path.keys():
            if critical_path[c] > critical_path_time:
                critical_path_time = critical_path[c]

        normalized_critical_path = critical_path_time / total_tasks_durations
        if total_num_objects > 0:
            average_object_size = total_objects_size / total_num_objects
        else:
            average_object_size = 0
        print "Total number of tasks is {}, total number of objects is {}, and total object sizes is {}".format(total_num_tasks, total_num_objects, total_objects_size)
        print "Critical path time is {} and total tasks duration is {}, so normalized critical path is {}".format(critical_path_time, total_tasks_durations, normalized_critical_path)
        return total_num_tasks, normalized_critical_path, total_tasks_durations, total_num_objects, total_objects_size


    def get_root_task(self):
        if self._root_task is None:
            return None
        else:
            return self._tasks[self._root_task]

    def get_task(self, task_id):
        return self._tasks[task_id]

    def get_task_ids(self):
        return self._tasks.keys()


class Task():
    def __init__(self, task_id, phases, results):
        task_id_str = str(task_id)
        if not task_id_str:
            raise ValidationError('Task: no id provided')
        if not len(phases):
            raise ValidationError('Task: no phases')
        for idx, phase in enumerate(phases):
            if phase.phase_id != idx:
                raise ValidationError('Task: mismatched phase id for task {}, got {}, expected {}'.format(task_id_str, phase.phase_id, idx))
        # TODO(swang): These lines are not a valid check for the driver
        # task.
        #if not len(results):
        #    raise ValidationError('Task: no results')

        # verification passed so initialize
        self._task_id = task_id_str
        self._phases = phases
        self._results = results
        self._resources = {}
        self._resources['cpu'] = 1

        # Depth is assigned by ComputationDescription
        self.depth = None

        self.is_root = False

        self._next_phase_to_run = 0


    def id(self):
        return self._task_id

    def get_depends_on(self):
        return self._phases[0].depends_on

    def get_phase(self, phase_id):
        return self._phases[phase_id]

    def num_phases(self):
        return len(self._phases)

    def phases(self):
        return self._phases[:]

    def get_results(self):
        return self._results

    def get_resources(self):
        return self._resources

    def get_next_phase_to_run(self):
        return self._next_phase_to_run

    def increment_next_phase_to_run(self):
        self._next_phase_to_run += 1


class TaskPhase():
    def __init__(self, phase_id, depends_on, submits, duration, creates=None):
        for s in submits:
            if s.time_offset > duration:
                raise ValidationError('TaskPhase: submits beyond phase duration')

        # verification passed so initialize
        self.phase_id = phase_id
        self.depends_on = map(lambda x: str(x), depends_on)
        self.submits = submits
        self.duration = duration
        if creates is None:
            self.creates = []
        else:
            self.creates = creates


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


class ObjectPut():
    def __init__(self, object_id, size, time_offset):
        object_id_str = str(object_id)
        if not object_id_str:
            raise ValidationError('TaskResult: no object id')
        if size < 0:
            raise ValidationError('TaskResult: invalid size - {}'.format(size))

        # verification passed so initialize
        self.object_id = object_id_str
        self.size = size
        self.time_offset = time_offset


class DirectedGraph():
    def __init__(self):
        self._id_ct = 0
        self._id_map = {}
        self._edges = []

    def get_id(self, x):
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
        #id_a = self._get_id(a)
        #id_b = self._get_id(b)
        #print 'EDGE: {} => {}'.format(a, b)
        #print 'EDGE: {} -> {}'.format(id_a, id_b)
        #self._edges.append((id_a, id_b))
        self._edges.append((a, b))

    def verify_dag_root(self, root):
        root_id = root
        # check that
        #  1/ we have a DAG
        #  2/ all nodes reachable from the root
        # we do this by depth-first search
        # Map from source node ID to list of destination node IDs.
        edge_lists = dict(map(lambda (src_id, edges): (src_id, map(lambda x: x[1], edges)), itertools.groupby(self._edges, lambda x: x[0])))

        to_visit = [(root_id, ())]
        visited = set()
        while to_visit:
            node, chain = to_visit.pop()
            if node in set(chain):
                raise ValidationError('Cyclic dependencies')
            if node in visited:
                continue
            visited.add(node)
            # Add ourselves to this DFS branch.
            chain = chain + (node, )
            if node not in edge_lists:
                continue
            for destination_node in edge_lists[node]:
                to_visit.append((destination_node, chain))

        if len(visited) != self._id_ct:
            raise ValidationError('Reachability from root')


    def topo_sort(self, root):
        #print "root task is {}".format(root)
        #print "edges is {}".format(self._edges)
        topo_sorted_list = []
        q = deque()
        in_count = {}
        for e in self._edges:
            in_count[e[1]] = in_count.get(e[1], 0) + 1
        #assuming we have only 1 root task! if we have several root tasks (a forest of jobs) this will have to change to "for a in nodes: if in_count[a] == 0: q.push(a)
        q.append(root)
        #print "current q in topo_sort is {}".format(q)
        while q:
           cur = q[0]
           topo_sorted_list.append(cur)
           q.popleft()
           #sprint "cur is {}".format(cur)
           for nxt in self.adj_out(cur):
               in_count[nxt] -= 1
               if in_count[nxt] == 0:
                   q.append(nxt)
        return topo_sorted_list


    def adj_out(self, x):
        result = []
        for e in self._edges:
            if e[0] == x:
                result.append(e[1])
        return result 


    def adj_in(self, x):
        result = []
        for e in self._edges:
            if e[1] == x:
                result.append(e[0])
        return result 
 

class ValidationError(Exception):
    def __init__(self, message):
        super(ValidationError, self).__init__(message)


def computation_decoder(dict):
    keys = frozenset(dict.keys())
    if keys == frozenset([u'timeOffset', 'taskId']):
        return TaskSubmit(dict[u'taskId'], dict[u'timeOffset'])
    if keys == frozenset([u'duration', u'phaseId', u'submits', u'dependsOn', u'creates']):
        return TaskPhase(dict[u'phaseId'], dict[u'dependsOn'], dict[u'submits'], dict[u'duration'], dict[u'creates'])
    if keys == frozenset([u'phases', u'results', u'taskId']):
        return Task(dict[u'taskId'], dict[u'phases'], dict[u'results'])
    if keys == frozenset([u'tasks', u'rootTask']):
        return ComputationDescription(dict[u'rootTask'], dict[u'tasks'])
    if keys == frozenset([u'objectId', u'size']):
        return TaskResult(dict[u'objectId'], int(dict[u'size']))
    if keys == frozenset([u'objectId', u'size', u'timeOffset']):
        return ObjectPut(dict[u'objectId'], int(dict[u'size']), dict[u'timeOffset'])
    else:
        print "unexpected map: {}".format(keys)
        sys.exit(1)
