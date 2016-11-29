import sys
import heapq
import itertools
import logging
import types

from collections import defaultdict
from collections import deque
from schedulerbase import *


################################################################
#        Scheduler Database that Replays Saved Traces          #
################################################################

class ReplaySchedulerDatabase(AbstractSchedulerDatabase):

    class HandlerContext():
        def __init__(self, replay_scheduler_database, update):
            self.replay_scheduler_database = replay_scheduler_database
            self.update = update

    def __init__(self, time_source, logger, computation, num_nodes, num_workers_per_node, data_transfer_time_cost, db_message_delay):
        self._pylogger = logging.getLogger(__name__+'.ReplaySchedulerDatabase')

        self._event_simulation = time_source
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
        print "submit called"
        self._yield_global_scheduler_update(ForwardTaskUpdate(task, submitting_node_id, is_scheduled_locally))

    def finished(self, task_id):
        #if task_id != self.root_task_id:
        self._yield_global_scheduler_update(FinishTaskUpdate(task_id))

    def object_ready(self, object_description, submitting_node_id):
        self._yield_global_scheduler_update(ObjectReadyUpdate(object_description, submitting_node_id))

    def register_node(self, node_id, num_workers):
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
        self._pylogger.debug('sending update to global scheduler: {}'.format(str(update)), extra={'timestamp':self._event_simulation.get_time()})
        for handler in self._global_scheduler_update_handlers:
            self._event_simulation.schedule_delayed(self._db_message_delay, lambda handler=handler:handler(update))

    def _yield_local_scheduler_update(self, update):
        self._pylogger.debug('sending update to node {} local scheduler: {}'.format(str(update.node_id), str(update)), extra={'timestamp':self._event_simulation.get_time()})
#        print "yield locally targeting {}".format(update.node_id)
#        print "lsh" + str(self._local_scheduler_update_handlers)
        for handler in self._local_scheduler_update_handlers[str(update.node_id)]:
#            print "SDB sending update {} to node {}".format(update, update.node_id)
            self._event_simulation.schedule_delayed(self._db_message_delay, lambda handler=handler: handler(update))

    def schedule(self, node_id, task_id):
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
            self._event_simulation.schedule_immediate(lambda: self.schedule(node_id, self.root_task_id))
            self._event_simulation.schedule_immediate(lambda: self._yield_global_scheduler_update(ForwardTaskUpdate(root_task, node_id, True)))


class ObjectStoreRuntime():
    def __init__(self, event_simulation, data_transfer_time_cost, db_message_delay):
        self._event_simulation = event_simulation
        self._object_locations = defaultdict(lambda: {})
        self._object_sizes = {}
        self._update_handlers = defaultdict(list)
        self._data_transfer_time_cost = data_transfer_time_cost
        self._db_message_delay = db_message_delay
        self._awaiting_completion = defaultdict(list)

    def expect_object(self, object_id, node_id):
        if node_id not in self._object_locations[object_id] or self._object_locations[object_id][node_id] != ObjectStatus.READY:
            self._object_locations[object_id][node_id] = ObjectStatus.EXPECTED

    def add_object(self, object_id, node_id, object_size):
        self._object_locations[object_id][node_id] = ObjectStatus.READY
        self._object_sizes[object_id] = object_size
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

    def require_object(self, object_id, node_id, on_done):
        object_ready_locations = dict(filter(lambda (node_id, status): status == ObjectStatus.READY, self._object_locations[object_id].items()))
        if node_id in object_ready_locations.keys():
            # TODO - we aren't firing the ObjectReadyUpdate in this scenario. Should we be doing so?
#            print "require has locally"
            on_done()
        else:
            # TODO - I don't think we are properly handling the case where the object completes on a remote
            # node and then we have to copy it over locally.
#            print "require doesn't have locally"
            if not object_ready_locations:
                self._awaiting_completion[object_id].append((node_id, on_done))
            else:
                # TODO better way to choose an element from a set than list(set)[0]
                source_location = list(object_ready_locations.keys())[0]
                self._copy_object(object_id, node_id, source_location, on_done)

    def _copy_object(self, object_id, dst_node_id, src_node_id, on_done):
        if dst_node_id == src_node_id:
            on_done()
        elif src_node_id in self._object_locations[object_id].keys() and self._object_locations[object_id][src_node_id] == ObjectStatus.READY:
            print "moving object to {} from {}".format(dst_node_id, src_node_id)
            data_transfer_time = self._object_sizes[object_id] * self._data_transfer_time_cost
            self._event_simulation.schedule_delayed(data_transfer_time, lambda: self._object_moved(object_id, dst_node_id, on_done))
        else:
            raise RuntimeError('Unexpected failure to copy object {} to {} from {}'.format(object_id, dst_node_id, src_node_id))

    def _object_moved(self, object_id, dst_node_id, on_done):
        self._object_locations[object_id][dst_node_id] = ObjectStatus.READY
        self._yield_object_ready_update(object_id, dst_node_id, self._object_sizes[object_id])
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
    def __init__(self, event_simulation, object_store, logger, computation, node_id, num_workers):
        self._pylogger = logging.getLogger(__name__+'.NodeRuntime')
        self._event_simulation = event_simulation
        self._object_store = object_store
        self._logger = logger
        self._computation = computation
        self._update_handlers = []
        self.num_workers = num_workers
        self.node_id = node_id
        self.num_workers_executing = 0

        self._time_buffer_size = 10
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
        for result in task.get_results():
        	self._object_store.expect_object(result.object_id, self.node_id)
        self._pylogger.debug('Dispatcher at node {} received task {} with priority {}'.format(self.node_id, task.id(), priority), extra={'timestamp':self._event_simulation.get_time()})
        task_id = task.id()
        heapq.heappush(self._queue, (priority, self._queue_seq, task_id))
        self._queue_seq += 1
        self._event_simulation.schedule_immediate(lambda: self._process_tasks())

    def get_updates(self, update_handler):
        self._update_handlers.append(update_handler)

    def free_workers(self):
        return self.num_workers - self.num_workers_executing

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
                self._node_runtime._execute_phase_immediate(self._task_id, self._phase_id)

    def _start_task(self, task_id):
        self._task_start_times.append(self._event_simulation.get_time())
        self._task_start_times_map[task_id] = self._event_simulation.get_time()
        self.num_workers_executing += 1
        self._logger.task_started(task_id, self.node_id)
        self._internal_scheduler_schedule(task_id, 0)

    def _process_tasks(self):
        #print "threshold scheduler debug: dispatcher queue in node {} is {}".format(self.node_id, self._queue)
        while self.num_workers_executing < self.num_workers and self._queue:
            (_, _, task_id) = heapq.heappop(self._queue)
            self._start_task(task_id)

    def _yield_update(self, update):
        for update_handler in self._update_handlers:
            update_handler(update)

    def _execute_phase_immediate(self, task_id, phase_id):
        self._pylogger.debug('executing task {} phase {}'.format(task_id, phase_id), extra={'timestamp':self._event_simulation.get_time()})
        task_phase = self._computation.get_task(task_id).get_phase(phase_id)
        for put_event in task_phase.creates:
            self._event_simulation.schedule_delayed(
                    put_event.time_offset,
                    lambda: self._object_store.add_object(put_event.object_id, self.node_id, put_event.size)
                    )
            self._event_simulation.schedule_delayed(
                    put_event.time_offset,
                    lambda: self._yield_update(ObjectReadyUpdate(ObjectDescription(put_event.object_id, self.node_id, put_event.size), self.node_id))
                    )
        for schedule_task in task_phase.submits:
            self._event_simulation.schedule_delayed(schedule_task.time_offset, lambda s_task_id=schedule_task.task_id: self._handle_update(self.TaskSubmitted(s_task_id, 0)))
        self._event_simulation.schedule_delayed(task_phase.duration, lambda: self._handle_update(self.TaskPhaseComplete(task_id, phase_id)))

    def _internal_scheduler_schedule(self, task_id, phase_id):
        task_phase = self._computation.get_task(task_id).get_phase(phase_id)
        depends_on = task_phase.depends_on
        needs = self.Dependencies(self, task_id, phase_id)
        for d_object_id in depends_on:
            if not self._object_store.is_local(d_object_id, self.node_id):
                needs.add_object_dependency(d_object_id)
                self._object_store.require_object(d_object_id, self.node_id, lambda d_object_id=d_object_id: needs.object_available(d_object_id))
        if not needs.has_dependencies():
            self._execute_phase_immediate(task_id, phase_id)
        else:
            self._pylogger.debug('task {} phase {} waiting for dependencies: {}'.format(task_id, phase_id, str(needs.object_dependencies)), extra={'timestamp':self._event_simulation.get_time()})

    def _handle_update(self, update):
        if isinstance(update, self.TaskSubmitted):
            self._event_simulation.schedule_immediate(lambda: self._yield_update(SubmitTaskUpdate(self._computation.get_task(update.submitted_task_id))))
        elif isinstance(update, self.TaskPhaseComplete):
            self._pylogger.debug('completed task {} phase {}'.format(update.task_id, update.phase_id), extra={'timestamp':self._event_simulation.get_time()})
            task = self._computation.get_task(update.task_id)
            if update.phase_id < task.num_phases() - 1:
                self._pylogger.debug('task {} has further phases'.format(update.task_id), extra={'timestamp':self._event_simulation.get_time()})
                self._event_simulation.schedule_immediate(lambda: self._internal_scheduler_schedule(update.task_id, update.phase_id + 1))
            else:
                self._pylogger.debug('completed task {}'.format(update.task_id), extra={'timestamp':self._event_simulation.get_time()})
                self._logger.task_finished(update.task_id, self.node_id)
                for res in task.get_results():
                    self._object_store.add_object(res. object_id, self.node_id, res.size)
#                print "XXX finished task {} number of phases is {}".format(update.task_id, num_phases)
                self._yield_update(FinishTaskUpdate(update.task_id))
                self.num_workers_executing -= 1
                self._task_times.append(self._event_simulation.get_time() - self._task_start_times_map.get(update.task_id, 0)) 
                print "task_times: {}".format(self._task_times)
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

    def get_time(self):
        return self._t

    def schedule_at(self, t, fn):
        if self._t > t:
            print 'invalid schedule request'
            sys.exit(1)
        heapq.heappush(self._scheduled, (t, self._scheduled_seq, fn))
        self._scheduled_seq += 1

    def schedule_delayed(self, delta, fn):
        self.schedule_at(self._t + delta, fn)

    def schedule_immediate(self, fn):
        self.schedule_at(self._t, fn)

    def advance(self):
        if len(self._scheduled) > 0:
            (self._t, _, scheduled) = heapq.heappop(self._scheduled)
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
        load_dg = DirectedGraph()
        tasks_map = {}
        total_num_tasks = 0
        total_num_objects = 0
        total_objects_size = 0
        for task in tasks:
            tasks_map[task.id()] = task
        for task in tasks:
            prev_phase = None
            for phase_id in range(0, task.num_phases()):
                phase = task.get_phase(phase_id)
                if prev_phase:
                    #print "EDGE: previous phase edge"
                    validation_dg.add_edge(validation_dg.get_id(prev_phase), validation_dg.get_id(phase))
                    load_dg.add_edge(prev_phase, phase)
                for object_id in phase.depends_on:
                    #print "EDGE: phase dependency edge"
                    validation_dg.add_edge(validation_dg.get_id(object_id), validation_dg.get_id(phase))
                    load_dg.add_edge(object_id, phase)
                for submits in phase.submits:
                    #print "EDGE: phase schedules edge"
                    validation_dg.add_edge(validation_dg.get_id(phase), validation_dg.get_id(tasks_map[submits.task_id].get_phase(0)))
                    load_dg.add_edge(phase, tasks_map[submits.task_id].get_phase(0))
                for creates in phase.creates:
                    #print "EDGE: phase creates object"
                    validation_dg.add_edge(validation_dg.get_id(phase), validation_dg.get_id(creates.object_id))
                    load_dg.add_edge(phase, creates.object_id)
                    total_num_objects += 1
                    total_objects_size += creates.size

                prev_phase = phase
            for task_result in task.get_results():
                #print "EDGE: task result edge"
                validation_dg.add_edge(validation_dg.get_id(prev_phase), validation_dg.get_id(task_result.object_id))
                load_dg.add_edge(prev_phase, task_result.object_id)
                total_num_objects += 1
                total_objects_size += task_result.size
            total_num_tasks += 1
        validation_dg.verify_dag_root(validation_dg.get_id(tasks_map[root_task_str].get_phase(0)))


        # verification passed so initialize
        self._root_task = root_task_str
        self._tasks = tasks_map


        #load measure:
        topo_sort_nodes = load_dg.topo_sort(tasks_map[self._root_task].get_phase(0))
        total_tasks_durations = 0
        total_num_tasks_verify = 0
        critical_path = {}
        critical_path[tasks_map[self._root_task].get_phase(0)] = tasks_map[self._root_task].get_phase(0).duration
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
        self.total_num_tasks = total_num_tasks
        self.normalized_critical_path = normalized_critical_path
        self.total_tasks_durations = total_tasks_durations
        self.total_objects_size = total_objects_size
        self.total_num_objects = total_num_objects
        self.average_object_size = average_object_size


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


class PrintingLogger():
    def __init__(self, event_simulation):
        self._event_simulation = event_simulation

    def task_started(self, task_id, node_id):
        print '{:.6f}: execute task {} on node {}'.format(self._event_simulation.get_time(), task_id, node_id)

    def task_finished(self, task_id, node_id):
        print '{:.6f}: finished task {} on node {}'.format(self._event_simulation.get_time(), task_id, node_id)


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
