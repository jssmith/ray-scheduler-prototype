from collections import defaultdict
from collections import namedtuple
from collections import OrderedDict
import sys
import logging

from schedulerbase import *
from itertools import ifilter

class GlobalSchedulerState():
    def __init__(self):
        # Map from node id to node status
        self.nodes = {}

        # List of pending tasks - those whose dependencies are not ready
        self.pending_tasks = []

        # List of runnable tasks
        self.runnable_tasks = []

        # Map from task_id to node id
        self.executing_tasks = {}

        # Map of object id to node id
        # TODOs
        #  - this should be a list of node ids
        #  - add the sizes of object ids
        self.finished_objects = defaultdict(list)

        # Map from task id to Task object
        self.tasks = {}

        self._pending_needs = {}
        self._awaiting_completion = {}

        self.is_shutdown = False

    class _NodeStatus:
        def __init__(self, node_id, num_workers):
            self.node_id = node_id
            self.num_workers = num_workers
            self.num_workers_executing = 0

        def inc_executing(self):
            self.num_workers_executing += 1

        def dec_executing(self):
            self.num_workers_executing -= 1

        def __str__(self):
            return 'NodeStatus({},{},{})'.format(self.node_id, self.num_workers, self.num_workers_executing)

    def set_executing(self, task_id, node_id):
        node_status = self.nodes[node_id]
        node_status.inc_executing()
        if task_id in self.runnable_tasks:
            self.runnable_tasks.remove(task_id)
        self.executing_tasks[task_id] = node_id

    def update(self, update, timestamp):
        print '{:.6f}: GlobalSchedulerState update {}'.format(timestamp, str(update))
        if isinstance(update, ForwardTaskUpdate):
#            print '{} task {} submitted'.format(timestamp, update.task.id())
            self._add_task(update.task, update.submitting_node_id, update.is_scheduled_locally)
        elif isinstance(update, FinishTaskUpdate):
            self._finish_task(update.task_id)
        elif isinstance(update, RegisterNodeUpdate):
            self._register_node(update.node_id, update.num_workers)
        elif isinstance(update, ObjectReadyUpdate):
            self._object_ready(update.object_description.object_id,
                               update.submitting_node_id)
        else:
            raise NotImplementedError('Unknown update {}'.format(update.__class__.__name__))

    def _register_node(self, node_id, num_workers):
        if node_id in self.nodes.keys():
            print 'already registered node {}'.format(node_id)
            sys.exit(1)
        self.nodes[node_id] = self._NodeStatus(node_id, num_workers)

    def _add_task(self, task, submitting_node_id, is_scheduled_locally):
        task_id = task.id()
        self.tasks[task_id] = task
        if is_scheduled_locally:
            self.set_executing(task_id, submitting_node_id)
        else:
            pending_needs = []
            for d_object_id in task.get_depends_on():
                if d_object_id not in self.finished_objects.keys():
                    pending_needs.append(d_object_id)
                    if d_object_id in self._awaiting_completion.keys():
                        self._awaiting_completion[d_object_id].append(task_id)
                    else:
                        self._awaiting_completion[d_object_id] = [task_id]
            if len(pending_needs) > 0:
                self._pending_needs[task_id] = pending_needs
                self.pending_tasks.append(task_id)
            else:
                self.runnable_tasks.append(task_id)

    def _finish_task(self, task_id):
        node_id = self.executing_tasks[task_id]
        self.nodes[node_id].dec_executing()
        del self.executing_tasks[task_id]
        for result in self.tasks[task_id].get_results():
            object_id = result.object_id
            self._object_ready(object_id, node_id)

    def _object_ready(self, object_id, node_id):
        self.finished_objects[object_id].append(node_id)
        if object_id in self._awaiting_completion.keys():
            pending_task_ids = self._awaiting_completion[object_id]
            del self._awaiting_completion[object_id]
            for pending_task_id in pending_task_ids:
                needs = self._pending_needs[pending_task_id]
                needs.remove(object_id)
                if not needs:
                    del self._pending_needs[pending_task_id]
                    self.pending_tasks.remove(pending_task_id)
                    self.runnable_tasks.append(pending_task_id)
        #print "object", object_id, "is on", self.finished_objects[object_id]

    def object_ready(self, object_id, node_id):
        """
        Whether the given object ID is ready on the given node.
        """
        return node_id in self.finished_objects[object_id]

class BaseGlobalScheduler():

    def __init__(self, system_time, scheduler_db):
        self._system_time = system_time
        self._db = scheduler_db
        self._state = GlobalSchedulerState()
        scheduler_db.get_global_scheduler_updates(lambda update: self._handle_update(update))

    def _execute_task(self, node_id, task_id):
#        print "GS executing task {} on node {}".format(task_id, node_id)
        self._state.set_executing(task_id, node_id)
        self._db.schedule(node_id, task_id)

    def _process_tasks(self):
#        print "global scheduler processing tasks, runnable number {} | {}".format(len(self._state.runnable_tasks), self._state.runnable_tasks)
        runnable_tasks = self._state.runnable_tasks[:]
        for task_id in runnable_tasks:
            node_id = self._select_node(task_id)
#            print "process tasks got node id {} for task id {}".format(node_id, task_id)

            if node_id is not None:
                self._execute_task(node_id, task_id)
            else:
                # Not able to schedule so return
                print 'unable to schedule'
                return

    def _select_node(self, task_id):
        raise NotImplementedError()

    def _handle_update(self, update):
        self._state.update(update, self._system_time.get_time())
        # TODO ability to process tasks periodically
        self._process_tasks()


class TrivialGlobalScheduler(BaseGlobalScheduler):

    def __init__(self, system_time, scheduler_db):
        self._pylogger = logging.getLogger(__name__+'.TrivialGlobalScheduler')
        BaseGlobalScheduler.__init__(self, system_time, scheduler_db)

    def _select_node(self, task_id):
        self._pylogger.debug("Runnable tasks are {}, checking task {}".format(
            ', '.join(self._state.runnable_tasks),
            task_id),
            extra={'timestamp':self._system_time.get_time()})
        for node_id, node_status in sorted(self._state.nodes.items()):
            self._pylogger.debug("can we schedule task {} on node {}? {} < {} so {}".format(
                task_id, node_id, node_status.num_workers_executing,
                node_status.num_workers,
                bool(node_status.num_workers_executing < node_status.num_workers)),
                extra={'timestamp':self._system_time.get_time()})
            #print "global scheduler: node {} num of workers executing {} total num of workers {}".format(node_id, node_status.num_workers_executing, node_status.num_workers)
            if node_status.num_workers_executing < node_status.num_workers:
                return node_id
        return None


class LocationAwareGlobalScheduler(BaseGlobalScheduler):

    def __init__(self, system_time, scheduler_db):
        BaseGlobalScheduler.__init__(self, system_time, scheduler_db)

    def _select_node(self, task_id):
        task_deps = self._state.tasks[task_id].get_depends_on()
        best_node_id = None
        best_cost = sys.maxint
        # TODO short-circuit cost computation if there are no dependencies.
        #      also may optimize lookup strategy for one or two dependencies.
        for (node_id, node_status) in sorted(self._state.nodes.items()):
            if node_status.num_workers_executing < node_status.num_workers:
                cost = 0
                for depends_on in task_deps:
                    if not self._state.object_ready(depends_on, node_id):
                        cost += 1
                if cost < best_cost:
                    best_cost = cost
                    best_node_id = node_id
        return best_node_id

class DelayGlobalScheduler(BaseGlobalScheduler):

    def __init__(self, system_time, scheduler_db, event_loop):
        BaseGlobalScheduler.__init__(self, system_time, scheduler_db)
        self._pylogger = logging.getLogger(__name__+'.DelayGlobalScheduler')
        self._event_loop = event_loop
        self._WaitingInfo = namedtuple('WaitingInfo', ['node_id', 'start_waiting_time', 'expiration_time'])
        self._waiting_tasks = OrderedDict()
        self._max_delay = 1

    def _best_node(self, task_id):
        task_deps = self._state.tasks[task_id].get_depends_on()
        best_node_id = None
        best_cost = sys.maxint
        best_node_ready = False
        for (node_id, node_status) in sorted(self._state.nodes.items()):
            cost = 0
            for depends_on in task_deps:
                if not self._state.object_ready(depends_on, node_id):
                    cost += 1
            if cost < best_cost or (cost == best_cost and not best_node_ready):
                best_cost = cost
                best_node_id = node_id
                best_node_ready = node_status.num_workers_executing < node_status.num_workers
        if best_node_id is None:
            raise RuntimeError('unexpected state')
        return (best_node_id, best_node_ready)

    def _process_tasks(self):
        for task_id, waiting_info in self._waiting_tasks.items():
            (best_node_id, best_node_ready) = self._best_node(task_id)
            if best_node_ready:
                self._pylogger.debug("now scheduling delayed task {} on node {} - delay is {}".format(
                    task_id, best_node_id,
                    self._system_time.get_time() - waiting_info.start_waiting_time),
                    extra={'timestamp':self._system_time.get_time()})
                del self._waiting_tasks[task_id]
                self._execute_task(best_node_id, task_id)

        for task_id in self._state.runnable_tasks[:]:
            if task_id not in self._waiting_tasks.keys():
                task_deps = self._state.tasks[task_id].get_depends_on()
                (best_node_id, best_node_ready) = self._best_node(task_id)
                if best_node_ready:
                    self._pylogger.debug("immediately scheduling schedule task {} on node {}".format(
                        task_id, best_node_id),
                        extra={'timestamp':self._system_time.get_time()})
                    self._execute_task(best_node_id, task_id)
                else:
                    self._pylogger.debug("waiting to schedule task {} on node {}".format(
                        task_id, best_node_id),
                        extra={'timestamp':self._system_time.get_time()})
                    self._waiting_tasks[task_id] = self._WaitingInfo(
                        best_node_id,
                        self._system_time.get_time(),
                        self._system_time.get_time() + self._max_delay)
                    self._event_loop.add_timer(self._max_delay,
                        DelayGlobalScheduler._wait_expired, (self, task_id))

    @staticmethod
    def _wait_expired(context):
        (self, task_id) = context
        if task_id in self._waiting_tasks.keys():
            # trivial scheduler algorithm, place anywhere available
            for node_id, node_status in sorted(self._state.nodes.items()):
                if node_status.num_workers_executing < node_status.num_workers:
                    self._pylogger.debug("delay exceeded, scheduling task {} on node {}".format(
                        task_id, node_id),
                        extra={'timestamp':self._system_time.get_time()})
                    del self._waiting_tasks[task_id]
                    self._execute_task(node_id, task_id)
                    return
            self._pylogger.debug("delay exceeded but no nodes available, unable to schedule task {}".format(
                task_id),
                extra={'timestamp':self._system_time.get_time()})

class TransferCostAwareGlobalScheduler(BaseGlobalScheduler):

    def __init__(self, system_time, scheduler_db, event_loop):
        BaseGlobalScheduler.__init__(self, system_time, scheduler_db)
        self._pylogger = logging.getLogger(__name__ + '.TransferCostAwareGlobalScheduler')
        self._event_loop = event_loop;
        self._schedcycle = 1; #seconds
        self._event_loop.add_timer(self._schedcycle,
                                   TransferCostAwareGlobalScheduler._handle_timer, (self, ))

    def _get_worker_capacities(self):
        nodecaps = {}
        for (node_id, node_status) in sorted(self._state.nodes.items()):
            nodecap = 0
            if node_status.num_workers_executing < node_status.num_workers:
                nodecap = node_status.num_workers - node_status.num_workers_executing
            nodecaps[node_id] = nodecap
        return nodecaps

    def _get_object_usage(self):
        self._state.finished_objects.items()

    @staticmethod
    def _handle_timer(context):
        (self, ) = context
        #timer fired, process pending tasks
        #construct the cost matrix C, the usage matrix U
        tnow = self._system_time.get_time()
        numrunnable = len(self._state.runnable_tasks)
        numtasks = len(self._state.tasks)
        print "timer handler fired at time t=%s , runnable left=%s tasks_left=%s" %(tnow, )
        #self._pylogger.debug("timer handler fired at time t={}".format(tnow))


        #set the next timer
        self._event_loop.add_timer(self._schedcycle,
                                   TransferCostAwareGlobalScheduler._handle_timer, (self,))

    def _handle_update(self, update):
        self._state.update(update, self._system_time.get_time())
        runnable_tasks = self._state.runnable_tasks[:]
        for task_id in runnable_tasks:
            # get its object dependencies
            task_deps = self._state.tasks[task_id].get_depends_on()
        #####CONTINUE HERE###################
        #### for now we may want to call process tasks to match the previous flow, while constructing matrices
            

    def _select_node(self, task_id):
        #leave this same as location aware for now
        task_deps = self._state.tasks[task_id].get_depends_on()
        best_node_id = None
        best_cost = sys.maxint
        # TODO short-circuit cost computation if there are no dependencies.
        #      also may optimize lookup strategy for one or two dependencies.
        for (node_id, node_status) in self._state.nodes.items():
            if node_status.num_workers_executing < node_status.num_workers:
                cost = 0
                for depends_on in task_deps:
                    if not self._state.object_ready(depends_on, node_id):
                        cost += 1
                if cost < best_cost:
                    best_cost = cost
                    best_node_id = node_id
        return best_node_id


class PassthroughLocalScheduler():
    def __init__(self, system_time, node_runtime, scheduler_db):
        self._system_time = system_time
        self._node_runtime = node_runtime
        self._node_id = node_runtime.node_id
        self._scheduler_db = scheduler_db

        self._node_runtime.get_updates(lambda update: self._handle_runtime_update(update))
        self._scheduler_db.get_local_scheduler_updates(self._node_id, lambda update: self._handle_scheduler_db_update(update))

    def _handle_runtime_update(self, update):
        print '{:.6f}: LocalScheduler update {}'.format(self._system_time.get_time(), str(update))
        if isinstance(update, ObjectReadyUpdate):
            self._scheduler_db.object_ready(update.object_description, update.submitting_node_id)
        elif isinstance(update, FinishTaskUpdate):
            self._scheduler_db.finished(update.task_id)
        elif isinstance(update, SubmitTaskUpdate):
#            print "Forwarding task " + str(update.task)
            self._scheduler_db.submit(update.task, self._node_runtime.node_id, self._schedule_locally(update.task))
        else:
            raise NotImplementedError('Unknown update: {}'.format(type(update)))


    def _schedule_locally(self, task):
        return False

    def _handle_scheduler_db_update(self, update):
        if isinstance(update, ScheduleTaskUpdate):
#            print "Dispatching task " + str(update.task)
            self._node_runtime.send_to_dispatcher(update.task, 0)
        else:
            raise NotImplementedError('Unknown update: {}'.format(type(update)))

class SimpleLocalScheduler(PassthroughLocalScheduler):
    def __init__(self, system_time, node_runtime, scheduler_db):
        PassthroughLocalScheduler.__init__(self, system_time, node_runtime, scheduler_db)

    def _schedule_locally(self, task):
        if self._node_runtime.free_workers() == 0:
            return False
        for d_object_id in task.get_phase(0).depends_on:
            if not self._node_runtime.is_local(d_object_id):
                return False
        self._node_runtime.send_to_dispatcher(task, 1)
        return True



class ThresholdLocalScheduler(PassthroughLocalScheduler):
    def __init__(self, time_source, node_runtime, scheduler_db):
        PassthroughLocalScheduler.__init__(self, time_source, node_runtime, scheduler_db)

    def _schedule_locally(self, task):
        threshold2 = 3
        if self._node_runtime.free_workers() > 0:
            return False
        print task.get_phase(0)
        objects_transfer_size = 0
        objects_status = {'local_ready' : 0, 'remote_ready' : 0, 'local_notready' : 0, 'remote_notready' : 0, 'no_info' : 0}
        for d_object_id in task.get_phase(0).depends_on:
            if not self._node_runtime.is_local(d_object_id):
                objects_transfer_size = transfer_size + self._node_runtime.get_object_size(d_object_id)
                #TODO: add to node_runtime the function get_object_size() which just return a value from the _object_store sizes map/dict.
                object_status = self._node_runtime.get_object_status()
                if object_status == ready :
                    objects_status[remote_ready] += 1 
                elif (object_status == scheduled and get_location(d_object_id) != self._node_runtime._node_id) :
                    objects_status[local_notready] += 1
        dispatcher_load = self._node_runtime.get_dispatch_queue_size()
        #TODO: add to node_runtime the function get_dispatch_queue_size().
        node_efficiency_rate = self._node_runtime.get_node_eff_rate()
        #TODO: add to node_runtime the function get_node_eff_rate(). It will record a buffer in the form of list of task_start_time for the last 10 or 20 tasks (this will be a constant parameter) sent for execution on the node. The node efficiency rate will be the buffer size (whatever the constant is) divided by the (last_element-first_element) of the buffer.
        local_load = dispatcher_load / node_efficiency_rate
        task_load = objects_transfer_size 

        if task_load > threshold2 :
            return False
        self._node_runtime.send_to_dispatcher(task, 1)
        return True


class BaseScheduler():
    def __init__(self, system_time, scheduler_db):
        self._system_time = system_time
        self._scheduler_db = scheduler_db
        self._global_scheduler = None
        self._local_schedulers = {}

    def get_global_scheduler(self, event_loop):
        if not self._global_scheduler:
            self._global_scheduler = self._make_global_scheduler(event_loop)
        return self._global_scheduler

    def get_local_scheduler(self, node_runtime, event_loop):
        node_id = node_runtime.node_id
        if node_id not in self._local_schedulers.keys():
            self._local_schedulers[node_id] = self._make_local_scheduler(node_runtime, event_loop)
        return self._local_schedulers[node_id]

    def _make_global_scheduler(self, event_loop):
        raise NotImplementedError()

    def _make_local_scheduler(self, node_runtime, event_loop):
        return PassthroughLocalScheduler(self._system_time, node_runtime, self._scheduler_db)


class TrivialScheduler(BaseScheduler):
    def __init__(self, system_time, scheduler_db):
        BaseScheduler.__init__(self, system_time, scheduler_db)

    def _make_global_scheduler(self, event_loop):
        return TrivialGlobalScheduler(self._system_time, self._scheduler_db)


class LocationAwareScheduler(BaseScheduler):

    def __init__(self, system_time, scheduler_db):
        BaseScheduler.__init__(self, system_time, scheduler_db)

    def _make_global_scheduler(self, event_loop):
        return LocationAwareGlobalScheduler(self._system_time, self._scheduler_db)

class TransferCostAwareScheduler(BaseScheduler):

    def __init__(self, system_time, scheduler_db):
        BaseScheduler.__init__(self, system_time, scheduler_db)

    def _make_global_scheduler(self, event_loop):
        return TransferCostAwareGlobalScheduler(self._system_time, self._scheduler_db, event_loop)


class DelayScheduler(BaseScheduler):

    def __init__(self, system_time, scheduler_db):
        BaseScheduler.__init__(self, system_time, scheduler_db)

    def _make_global_scheduler(self, event_loop):
        return DelayGlobalScheduler(self._system_time, self._scheduler_db, event_loop)


class TrivialLocalScheduler(TrivialScheduler):

    def __init__(self, system_time, scheduler_db):
        TrivialScheduler.__init__(self, system_time, scheduler_db)

    def _make_local_scheduler(self, node_runtime, event_loop):
        return SimpleLocalScheduler(self._system_time, node_runtime, self._scheduler_db)
