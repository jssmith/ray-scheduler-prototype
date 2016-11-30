from collections import defaultdict
from collections import namedtuple
from collections import OrderedDict
import sys
import numpy as np
import os

from schedulerbase import *
from itertools import ifilter
from helpers import TimestampedLogger, setup_logging

class GlobalSchedulerState():
    def __init__(self, system_time):
        self._pylogger = TimestampedLogger(__name__+'.GlobalSchedulerState', system_time)

        # Map from node id to node status
        self.nodes = {}

        # List of pending tasks - those whose dependencies are not ready
        self.pending_tasks = []

        # List of runnable tasks
        self.runnable_tasks = []

        # Map from task_id to node id
        self.executing_tasks = {}

        # Map of object id to list of node ids.
        self.finished_objects = defaultdict(list)
        # Map of object id to object size in bytes.
        self.finished_object_sizes = {}

        # Map from task id to Task object
        self.tasks = {}
        self.task_times = {}
        self.finished_tasks = {}

        self._pending_needs = {}
        self._awaiting_completion = defaultdict(list)

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

    def _update_task_timestats(self, task_id, statestr, timestamp):
        assert(statestr in ["started", "added", "finished"])

        if timestamp is not None:
            if task_id not in self.task_times.keys():
                self.task_times[task_id] = {}
            self.task_times[task_id][statestr] = timestamp

    def set_executing(self, task_id, node_id, timestamp):
        node_status = self.nodes[node_id]
        node_status.inc_executing()
        if task_id in self.runnable_tasks:
            self.runnable_tasks.remove(task_id)
        self.executing_tasks[task_id] = node_id
        self._update_task_timestats(task_id, "started", timestamp)

    def update(self, update, timestamp):
        self._pylogger.debug('GlobalSchedulerState update {}'.format(str(update)))
        if isinstance(update, ForwardTaskUpdate):
#            print '{} task {} submitted'.format(timestamp, update.task.id())
            self._add_task(update.task, update.submitting_node_id, update.is_scheduled_locally, timestamp)
            self._update_task_timestats(update.task.id(), "added", timestamp)
            # if update.task.id() not in self.task_times.keys():
            #     self.task_times[update.task.id()] = {}
            # self.task_times[update.task.id()]["added"] = timestamp
        elif isinstance(update, FinishTaskUpdate):
            self._finish_task(update.task_id)
            self._update_task_timestats(update.task_id, "finished", timestamp)
        elif isinstance(update, RegisterNodeUpdate):
            self._register_node(update.node_id, update.num_workers)
        elif isinstance(update, ObjectReadyUpdate):
            self._object_ready(update.object_description.object_id,
                               update.submitting_node_id,
                               update.object_description.size)
        else:
            raise NotImplementedError('Unknown update {}'.format(update.__class__.__name__))

    def _register_node(self, node_id, num_workers):
        if node_id in self.nodes.keys():
            print 'already registered node {}'.format(node_id)
            sys.exit(1)
        self.nodes[node_id] = self._NodeStatus(node_id, num_workers)

    def _add_task(self, task, submitting_node_id, is_scheduled_locally, timestamp):
        task_id = task.id()
        if task_id in self.tasks.keys():
            raise RuntimeError('Duplicate addition of task {}'.format(task_id))
        self.tasks[task_id] = task
        if is_scheduled_locally:
            self.set_executing(task_id, submitting_node_id, timestamp)
        else:
            pending_needs = []
            for d_object_id in task.get_depends_on():
                if d_object_id not in self.finished_objects.keys():
                    pending_needs.append(d_object_id)
                    self._awaiting_completion[d_object_id].append(task_id)
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
            self._object_ready(object_id, node_id, result.size)
        self.finished_tasks[task_id] = self.tasks[task_id]

    def _object_ready(self, object_id, node_id, object_size):
        self.finished_objects[object_id].append(node_id)
        self.finished_object_sizes[object_id] = object_size
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

    def __init__(self, system_time, scheduler_db, event_loop):
        self._system_time = system_time
        self._db = scheduler_db
        self._event_loop = event_loop
        self._state = GlobalSchedulerState(system_time)
        scheduler_db.get_global_scheduler_updates(lambda update: self._handle_update(update))

    def _execute_task(self, node_id, task_id):
#        print "GS executing task {} on node {}".format(task_id, node_id)
        self._state.set_executing(task_id, node_id, self._system_time.get_time())
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

    def __init__(self, system_time, scheduler_db, event_loop):
        self._pylogger = TimestampedLogger(__name__+'.TrivialGlobalScheduler', system_time)
        BaseGlobalScheduler.__init__(self, system_time, scheduler_db,
                                     event_loop)

    def _select_node(self, task_id):
        self._pylogger.debug("Runnable tasks are {}, checking task {}".format(
            ', '.join(self._state.runnable_tasks),
            task_id))
        for node_id, node_status in sorted(self._state.nodes.items()):
            self._pylogger.debug("can we schedule task {} on node {}? {} < {} so {}".format(
                task_id, node_id, node_status.num_workers_executing,
                node_status.num_workers,
                bool(node_status.num_workers_executing < node_status.num_workers)))
            #print "global scheduler: node {} num of workers executing {} total num of workers {}".format(node_id, node_status.num_workers_executing, node_status.num_workers)
            if node_status.num_workers_executing < node_status.num_workers:
                return node_id
        return None


class LocationAwareGlobalScheduler(BaseGlobalScheduler):

    def __init__(self, system_time, scheduler_db, event_loop):
        BaseGlobalScheduler.__init__(self, system_time, scheduler_db,
                                     event_loop)

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

    def __init__(self, system_time, scheduler_db, event_loop, delay=1):
        BaseGlobalScheduler.__init__(self, system_time, scheduler_db,
                                     event_loop)
        self._pylogger = TimestampedLogger(__name__+'.DelayGlobalScheduler', system_time)
        self._WaitingInfo = namedtuple('WaitingInfo', ['start_waiting_time', 'expiration_time'])
        self._waiting_tasks = OrderedDict()
        self._max_delay = delay

    def _best_node(self, task_id):
        task_deps = self._state.tasks[task_id].get_depends_on()
        best_node_id = None
        best_cost = sys.maxint
        best_node_ready = False
        for (node_id, node_status) in sorted(self._state.nodes.items()):
            cost = 0
            for depends_on in task_deps:
                if not self._state.object_ready(depends_on, node_id):
                    # TODO: Assign cost according to size?
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
                    self._system_time.get_time() - waiting_info.start_waiting_time))
                del self._waiting_tasks[task_id]
                self._execute_task(best_node_id, task_id)

        for task_id in self._state.runnable_tasks[:]:
            if task_id not in self._waiting_tasks.keys():
                task_deps = self._state.tasks[task_id].get_depends_on()
                (best_node_id, best_node_ready) = self._best_node(task_id)
                if best_node_ready:
                    self._pylogger.debug("immediately scheduling schedule task {} on node {}".format(
                        task_id, best_node_id))
                    self._execute_task(best_node_id, task_id)
                else:
                    self._pylogger.debug("waiting to schedule task {}".format(task_id))
                    self._waiting_tasks[task_id] = self._WaitingInfo(
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
                        task_id, node_id))
                    del self._waiting_tasks[task_id]
                    self._execute_task(node_id, task_id)
                    return
            # NOTE: Don't we want to place this task somewhere, even if the
            # node is at capacity?
            self._pylogger.debug("delay exceeded but no nodes available, unable to schedule task {}".format(
                task_id))

class TransferCostAwareGlobalScheduler(BaseGlobalScheduler):

    def __init__(self, system_time, scheduler_db, event_loop):
        BaseGlobalScheduler.__init__(self, system_time, scheduler_db,
                                     event_loop)
        self._pylogger = TimestampedLogger(__name__ + '.TransferCostAwareGlobalScheduler', system_time)
        self._event_loop = event_loop;
        self._schedcycle = 1; #seconds
        self._initializing = True
        self._event_loop.add_timer(self._schedcycle,
                                  TransferCostAwareGlobalScheduler._handle_timer, (self, ))

    def _get_worker_capacities(self, node_id_list):
        ''' given an ordered list of node ids, return a corresponding list of node capacities.
        Pre-condition: each node in the node_id_list exists in the nodes table.
        Post-condition: capacities correspond to the order of node ids in node_id_list.
        '''
        node_caps = []
        for node_id in node_id_list:
            node_status = self._state.nodes[node_id]
            node_cap = 0
            if node_status.num_workers_executing < node_status.num_workers:
                node_cap = node_status.num_workers - node_status.num_workers_executing
            node_caps.append(node_cap)

        return node_caps

    def _get_object_usage(self, tasklist):
        '''construct a usage matrix U_to, for each task t, generate a row vector of object usage for task t.
        Pre-condition: list of task_ids is runnable, we assert that all of the dependencies are ready
        Post-condition: object usage array is T tasks by O objects where T==len(tasklist), O=len(object_id_list);
            object usage array is a list of row vectors, one per each task t \in tasklist.
        '''
        object_usage_array = []
        object_id_list = sorted(self._state.finished_objects)
        #used_object_id_list = []

        used_object_id_list = reduce(lambda x, y: x+y,
               [self._state.tasks[tid].get_depends_on() for tid in tasklist])
        used_object_id_set = set(used_object_id_list)
        used_object_id_list = sorted(used_object_id_set) #get rid of duplicates and sort

        # check if all used objects also in finished (intersection same size as the set of used)
        assert(len(used_object_id_set.intersection(set(object_id_list))) == len(used_object_id_list))

        #now construct the usage matrix U
        for i in range(len(tasklist)):
            tid = tasklist[i]
            object_usage_array.append([]) #new task row vector
            for objid in used_object_id_list:
                if objid in self._state.tasks[tid].get_depends_on():
                    object_usage_array[i].append(1)
                else:
                    object_usage_array[i].append(0)

        print object_usage_array
        return (object_usage_array, used_object_id_list)

    def _get_object_cost(self, object_id_list, node_ids):
        ''' For a given set of object ids and node ids, construct a cost matrix C_no that represents cost
            of access from node n to object o
        '''
        #need to construct an inverse map of nodes to objects
        node2object = {}
        for objid in object_id_list:
            node_id_list = self._state.finished_objects[objid]
            for node_id in node_id_list:
                if node_id not in node2object.keys():
                    node2object[node_id] = []
                node2object[node_id].append(objid)

        # now add all nodes not there yet
        for node_id in node_ids:
            if node_id not in node2object.keys():
                node2object[node_id] = []
        assert(len(node2object.keys())== len(node_ids)) #did we cover all nodes given?
        print "node2object"; print node2object
        #expand this dictionary into a 2D array
        node2object_array = []
        for i in range(len(node_ids)):
            node_id = node_ids[i]
            node2object_array.append([]) #new node row vector for ith node
            for objid in object_id_list:
                if objid in node2object[node_id]:
                    node2object_array[i].append(0) #local
                else:
                    object_size = self._state.finished_object_sizes[objid]
                    node2object_array[i].append(object_size) #remote

        return node2object_array

    @staticmethod
    def _handle_timer(context):
        (self, ) = context
        #timer fired, process pending tasks
        tnow = self._system_time.get_time()
        num_runnable = len(self._state.runnable_tasks)
        num_pending = len(self._state.pending_tasks)
        num_executing = len(self._state.executing_tasks)
        num_tasks_total = num_runnable + num_pending + num_executing
        num_tasks_finished = len(self._state.finished_tasks)
        print "timer handler fired at time t=%s , runnable=%s pending=%s executing=%s finished=%s" \
              % (tnow, num_runnable, num_pending, num_executing, num_tasks_finished)
        task_id_list = self._state.runnable_tasks[:]

        #apply task selection policy
        task_id_list = self._apply_task_policy(task_id_list)

        if len(task_id_list) > 0:
            (C,U,workercaps, node_id_list) = self._setup_bop(task_id_list)

            P_sol = self.schedule(C, U, workercaps)
            print "P_sol="; print P_sol
            (numt, numw) = P_sol.shape
            for i in range(numt):
                for j in range(numw):
                    if P_sol[i, j]:
                        #task i is to run on node j
                        task_id = task_id_list[i]
                        node_id = node_id_list[j]
                        #dispatch task_id on node_id
                        self._execute_task(node_id, task_id)

        #always want to set, except one case: when total_tasks==0 AND not initializing
        if num_tasks_total > 0 and self._initializing:
            #done initializing
            self._initializing = False

        if num_tasks_total == 0 and not self._initializing:
            #termination condition, reached zero task count, return
            return

        #adjust scheduling period based on finished tasks (if any)
        # print "[handle_timer] finished_tasks: ", self._state.finished_tasks
        # print "[handle_timer] task_times: ", self._state.task_times
        if len(self._state.finished_tasks.keys()) > 0:
            completion_times = [self._state.task_times[x]["finished"] - self._state.task_times[x]["started"] \
                                for x in self._state.finished_tasks.keys()]
            assert(len(completion_times) > 0)
            # print "completion_times: ", completion_times

            self._schedcycle = np.mean(completion_times)/10.0
            # print "[ALEXEY] schedcycle=%s pending=%s executing=%s" \
            #   % (self._schedcycle, len(self._state.pending_tasks), len(self._state.executing_tasks))

        #in all other cases, fire timer
        self._event_loop.add_timer(self._schedcycle,
                                   TransferCostAwareGlobalScheduler._handle_timer, (self,))

    def _apply_task_policy(self, task_id_list, workercaps=None):
        ''' given a set of tasks and worker capacities, mutate task id list (sort, subsample, reorder)'''

        if not workercaps:
            workercaps = self._get_worker_capacities(self._state.nodes)
        #trivial task policy implementation
        #note: it is possible to return an empty list of tasks (e.g., if no avail. capacity)
        return task_id_list[:sum(workercaps)]

    def _setup_bop(self, task_id_list):
        ''' given a set of tasks, set up everything needed to call schedule
            Pre-condition: task and worker selection policies have been applied.
        '''
        node_id_list = sorted(self._state.nodes) # sorted list of node ids
        workercaps = self._get_worker_capacities(node_id_list)
        print "workercaps = "; print workercaps

        (object_usage_array, object_id_list) = self._get_object_usage(task_id_list)
        print "node_id_list"; print node_id_list
        print "object_id_list"; print object_id_list
        print "object_usage_array"; print object_usage_array
        node2object_array = self._get_object_cost(object_id_list, node_id_list)
        C = np.matrix(node2object_array, dtype=int)
        U = np.matrix(object_usage_array, dtype=int)

        return (C,U, workercaps, node_id_list)

    #comment this out if want one task at a time
    def _handle_update(self, update):
       #just update all the state datastructures
       #wait for the timer to fire
       #intercept TaskFinish events

       self._state.update(update, self._system_time.get_time())
       # if isinstance(update, FinishTaskUpdate):
       #     pass #set scheduling period


    def _select_node(self, task_id):
        #construct an optimization problem just for one task
        #order matters for node, task and object lists!
        #Post-condition : return the node on which this task is supposed to run
        task_id_list = [task_id]
        task_id_list = self._apply_task_policy(task_id_list)
        if len(task_id_list) > 0:
            (C,U, workercaps, node_id_list) = self._setup_bop(task_id_list)

            P_sol = self.schedule(C, U, workercaps)
            print "P_sol="; print P_sol
            (numt, numw) = P_sol.shape
            for i in range(numt):
                for j in range(numw):
                    if P_sol[i, j]:
                        #task i is to run on node j
                        task_id = task_id_list[i]
                        node_id = node_id_list[j]
                        #dispatch task_id on node_id
                        return node_id


        #no node was selected or no tasks to schedule
        return None

    # input: cost matrix C_wo, usage matrix U_to
    # output:  matrix P_tw , s.t. P[t,w] == 1 iff task t is placed on worker w
    def schedule(self, C, U, node_capacities):
        from ortools.linear_solver import pywraplp
        import time

        # sanity check
        numw, numo1 = C.shape
        numt, numo2 = U.shape
        assert (numo1 == numo2)
        # Instantiate a Glop solver, naming it SolveSimpleSystem.
        # solver = pywraplp.Solver('SolveSimpleSystem',
        #                         pywraplp.Solver.GLOP_LINEAR_PROGRAMMING)
        solver = pywraplp.Solver('ray-solver',
                                 pywraplp.Solver.BOP_INTEGER_PROGRAMMING)
        # compute coefficient matrix K
        # K = UC^T
        # coefficient ij = K_ij
        K = U * C.T
        print("C=");
        print(C)
        print("U=");
        print(U)
        print("K=");
        print(K)

        P = []
        # create boolean decision variable matrix P
        for i in range(numt):
            P.append([])  # add new row
            for j in range(numw):
                varname = "x%02d%02d" % (i, j)
                P[i].append(solver.BoolVar(varname))

        # print("P="); print(P)

        objective = solver.Objective()
        for i in range(numt):
            for j in range(numw):
                # set coefficient
                objective.SetCoefficient(P[i][j], K[i, j])
        # set direction of optimization
        objective.SetMinimization()

        # CONSTRAINTS
        # 1. each task is assigned to exactly one worker; one constraint for each task i
        task_constraints = []
        for i in range(numt):
            tconstr = solver.Constraint(1, 1)  # 1<= expr <=1
            for j in range(numw):
                tconstr.SetCoefficient(P[i][j], 1)
            # save constraint
            task_constraints.append(tconstr)

        # 2. each worker is not assigned more than its maxcap worth of work
        worker_constraints = []
        for j in range(numw):  # for each worker
            wconstr = solver.Constraint(0, node_capacities[j])  # 0 <= expr <= MAXCAP
            for i in range(numt):
                wconstr.SetCoefficient(P[i][j], 1)
            # remember constraint
            worker_constraints.append(wconstr)

        # Solve the BOP.
        t1s = time.time()
        status = solver.Solve()
        t2s = time.time()

        # extract solution
        P_sol = np.zeros((numt, numw), dtype=int)
        for i in range(numt):
            for j in range(numw):
                P_sol[i, j] = P[i][j].solution_value()

        # print("Psol="); print(P_sol)
        PC = P_sol * C
        PCU = np.multiply(PC, U)
        # print("PCU=");
        # print(PCU)
        objval = np.sum(PCU)

        # print('Number of variables =', solver.NumVariables())
        # print('Number of constraints =', solver.NumConstraints())
        # # The objective value of the solution.
        # print('Optimal objective value =', objval)
        # print("solver latency = %5.2f ms" % ((t2s - t1s) * 1000))

        # The value of each variable in the solution.
        # print('y = ', y.solution_value())
        return P_sol



class PassthroughLocalScheduler():
    def __init__(self, system_time, node_runtime, scheduler_db, event_loop):
        self._pylogger = TimestampedLogger(__name__+'.PassthroughLocalScheduler', system_time)

        self._system_time = system_time
        self._node_runtime = node_runtime
        self._node_id = node_runtime.node_id
        self._scheduler_db = scheduler_db
        self._event_loop = event_loop

        self._node_runtime.get_updates(lambda update: self._handle_runtime_update(update))
        self._scheduler_db.get_local_scheduler_updates(self._node_id, lambda update: self._handle_scheduler_db_update(update))

    def _handle_runtime_update(self, update):
        self._pylogger.debug('LocalScheduler update {}'.format(str(update)))
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


class FlexiblePassthroughLocalScheduler():
    def __init__(self, system_time, node_runtime, scheduler_db, event_loop):
        self._pylogger = TimestampedLogger(__name__+'.FlexiblePassthroughLocalScheduler', system_time)

        self._system_time = system_time
        self._node_runtime = node_runtime
        self._node_id = node_runtime.node_id
        self._scheduler_db = scheduler_db
        self._event_loop = event_loop

        self._node_runtime.get_updates(lambda update: self._handle_runtime_update(update))
        self._scheduler_db.get_local_scheduler_updates(self._node_id, lambda update: self._handle_scheduler_db_update(update))

    def _handle_runtime_update(self, update):
        self._pylogger.debug('LocalScheduler update {}'.format(str(update)))
        if isinstance(update, ObjectReadyUpdate):
            self._scheduler_db.object_ready(update.object_description, update.submitting_node_id)
        elif isinstance(update, FinishTaskUpdate):
            self._scheduler_db.finished(update.task_id)
        elif isinstance(update, SubmitTaskUpdate):
#            print "Forwarding task " + str(update.task)
            self._filter_forward(update.task)        
        else:
            raise NotImplementedError('Unknown update: {}'.format(type(update)))


    def _forward_to_global(self, task, scheduled_locally):
        self._pylogger.debug('submit task to global scheduler {} - scheduled locally {}'.format(task.id(), scheduled_locally))
        self._scheduler_db.submit(task, self._node_runtime.node_id, scheduled_locally)
    
    def _filter_forward(self, task):
        self._forward_to_global(task, False)

    def _handle_scheduler_db_update(self, update):
        if isinstance(update, ScheduleTaskUpdate):
#            print "Dispatching task " + str(update.task)
            self._node_runtime.send_to_dispatcher(update.task, 0)
        else:
            raise NotImplementedError('Unknown update: {}'.format(type(update)))

class SimpleLocalScheduler(PassthroughLocalScheduler):
    def __init__(self, system_time, node_runtime, scheduler_db, event_loop):
        PassthroughLocalScheduler.__init__(self, system_time, node_runtime,
                                           scheduler_db, event_loop)

    def _schedule_locally(self, task):
        if self._node_runtime.free_workers() == 0:
            return False
        for d_object_id in task.get_phase(0).depends_on:
            if self._node_runtime.is_local(d_object_id) != ObjectStatus.READY:
                return False
        self._node_runtime.send_to_dispatcher(task, 1)
        return True



class ThresholdLocalScheduler(FlexiblePassthroughLocalScheduler):
    def __init__(self, system_time, node_runtime, scheduler_db, event_loop):
        FlexiblePassthroughLocalScheduler.__init__(self, system_time, node_runtime,
                                           scheduler_db, event_loop)
        self._pylogger = TimestampedLogger(__name__+'.ThresholdLocalScheduler', system_time)
        self._size_location_results = defaultdict(list)
        self._size_location_awaiting_results = defaultdict(set)
        self._scheduled_tasks = []
        #get threshold from unix environment variables, so I can sweep over them later in the bash sweep to find good values.
        #os.getenv('KEY_THAT_MIGHT_EXIST', default_value)
        self.threshold1l = os.getenv('RAY_SCHED_THRESHOLD1L', 3)
        self.threshold1h = os.getenv('RAY_SCHED_THRESHOLD1H', 6)
        self.threshold2 = os.getenv('RAY_SCHED_THRESHOLD2', 200)
        #print "threshold scheduler: threshold1l is {}".format(self.threshold1l)
        #print "threshold scheduler: threshold1h is {}".format(self.threshold1h)
        #print "threshold scheduler: threshold2 is {}".format(self.threshold2)
        self.avg_data_transfer_cost = os.getenv('AVG_DTC', 0.00000001)
        self.avg_message_db_delay = os.getenv('AVG_MDBD', 0.001)

    def _size_location_result_handler(self, task, object_id, object_size, object_locations):
        task_id = task.id()
        ready_remote_transfer_size = 0
        expected_remote_transfer_size = 0
        self._size_location_results[task_id].append((object_id, object_size, object_locations))
        self._size_location_awaiting_results[task_id].remove(object_id)
        task_load = float('inf')
        self._pylogger.debug('task {} scheduling recieved object information about object {} with size {} and information {}'.format(task_id, object_id, object_size, object_locations))

        #"short circuit" check
        for node in object_locations.keys():
            if object_locations[node] != ObjectStatus.READY and node != self._node_id and task_id not in self._scheduled_tasks:
                self._scheduled_tasks.append(task_id)
                self._pylogger.debug('local load is medium, but remote objects are not ready yet, so sending task {} to global scheduler'.format(task_id))
                self._forward_to_global(task, scheduled_locally = False) 
                ##need to make sure I ignore the next calls of this function!


        if not self._size_location_awaiting_results[task_id]:
            # have received all handler callbacks for this task
            
            del self._size_location_awaiting_results[task_id]
            if task_id not in self._scheduled_tasks:
                # go on processing with complete results
                # either schedule locally or send to global scheduler
                for obj_id,remote_object_size,locations_status in self._size_location_results[task_id]:
                    self._pylogger.debug('remote object {} status is {} and size is {}'.format(
                        obj_id, locations_status, remote_object_size))
                    for node in locations_status.keys():
                        if locations_status[node] == ObjectStatus.READY and remote_object_size:
                            ready_remote_transfer_size += remote_object_size
                        elif remote_object_size:
                            expected_remote_transfer_size += remote_object_size
            
                if expected_remote_transfer_size>0:
                    pass #was handles in the "short circuit" case at the beginning of the function  

                if ready_remote_transfer_size != 0:
                    #task_load = ready_remote_transfer_size
                    task_load = ready_remote_transfer_size * self.avg_data_transfer_cost if self._node_runtime.get_avg_task_time()==0 else ready_remote_transfer_size * self.avg_data_transfer_cost / self._node_runtime.get_avg_task_time()
                self._pylogger.debug('task load is {}'.format(task_load))
             
                if float(task_load) > float(self.threshold2) :
                    self._pylogger.debug('local load is medium, and task load is high. task load is {} and threshold is {}, so sending task {} to global scheduler'.format(task_load, self.threshold2, task.id()))
                    self._forward_to_global(task, scheduled_locally = False)
                    self._scheduled_tasks.append(task_id)
                else:
                    self._pylogger.debug('local load is medium, and task load is low. task load is {} and threshold is {}, so schedulling task {} locally'.format(task_load, self.threshold2, task.id()))
                    self._node_runtime.send_to_dispatcher(task, 1)
                    self._forward_to_global(task, scheduled_locally = True)
                    self._scheduled_tasks.append(task_id)
            del self._size_location_results[task_id]
            self._scheduled_tasks.remove(task_id)


    def _filter_forward(self, task):


        objects_transfer_size = 0
        objects_status = {'local_ready' : 0,'local_expected' : 0, 'no_info' : 0}
        remote_objects = []

        #avg_task_time = 1
        avg_task_time = self._node_runtime.get_avg_task_time()
        self._pylogger.debug('get_avg_task_time : {}'.format(self._node_runtime.get_avg_task_time()))
        #self._node_runtime.get_avg_task_time() buffers that last local 20 task completion times
        dispatcher_load = self._node_runtime.get_dispatch_queue_size()
        node_efficiency_rate = self._node_runtime.get_node_eff_rate()
        #add to node_runtime the function get_node_eff_rate(). It will record a buffer in the form of list of task_start_time for the last 10 or 20 tasks (this will be a constant parameter) sent for execution on the node. The node efficiency rate will be the buffer size (whatever the constant is) divided by the (last_element-first_element) of the buffer.
        
        self._pylogger.debug('node_efficiency_rate is {}'.format(node_efficiency_rate))
        self._pylogger.debug('avg_task_time is {}'.format(avg_task_time))
        self._pylogger.debug('dispatcher_load is {}'.format(dispatcher_load))
        local_load = 0 if (node_efficiency_rate == 0) else (((dispatcher_load+self._node_runtime.num_workers_executing) / node_efficiency_rate) / avg_task_time)
        self._pylogger.debug('local load is {}'.format(local_load))

        for d_object_id in task.get_phase(0).depends_on:
            if self._node_runtime.is_local(d_object_id) == ObjectStatus.READY:
                objects_status['local_ready'] += 1
                self._pylogger.debug('object {} is ready'.format(d_object_id))
            elif self._node_runtime.is_local(d_object_id) == ObjectStatus.EXPECTED:
                objects_status['local_expected'] += 1
                self._pylogger.debug('object {} is expected'.format(d_object_id))
                #objects_transfer_size = transfer_size + self._node_runtime.get_object_size(d_object_id)
            else:
                remote_objects.append(d_object_id) 
        

        #the trivial case
        if len(task.get_phase(0).depends_on) == objects_status['local_ready'] and self._node_runtime.free_workers() > 0:
            self._pylogger.debug('all objects ready locally and there are free workers, so scheduling task {} locally'.format(task.id()))
            self._node_runtime.send_to_dispatcher(task, 1)
            self._forward_to_global(task, scheduled_locally = True)

        #if the local scheduler has very low load, even with expected objects this still reduces to the trivial case (depending on the "low load" threshold)
        elif ((len(task.get_phase(0).depends_on) == (objects_status['local_ready']+objects_status['local_expected'])) and (float(self.threshold1l) > float(local_load))):
            self._pylogger.debug('all objects are either ready or expected locally, local load is {} and threshold is {}, so scheduling task {} locally'.format(local_load, self.threshold1l, task.id()))
            self._node_runtime.send_to_dispatcher(task, 1)
            self._forward_to_global(task, scheduled_locally = True)


        #if the local scheduler has a very high load, it's better to send the task to the global scheduler, even without querrying about all the remote objects
        elif float(local_load) > float(self.threshold1h):
            self._pylogger.debug('threshold scheduler: local load is very high. local load is {} and threshold is {}, so sending task {} to global scheduler immidietly'.format(local_load, self.threshold1h, task.id()))
            self._forward_to_global(task, scheduled_locally = False)

        #the interesting case, where we need information about remote objects
        #elif local_load < self.threshold1h and local_load > self.threshold1l:
        else:
            if not remote_objects:
                 self._pylogger.debug('local load {} is medium, but all objects will be local, so scheduling task {} locally'.format(local_load, task.id()))
                 self._forward_to_global(task, scheduled_locally = False)
            #querry for remote object sizes and calculate task load
            for remote_object_id in remote_objects:
                self._size_location_awaiting_results[task.id()].add(remote_object_id)
            for remote_object_id in remote_objects:
                self._pylogger.debug('querring for remote object size')
                self._node_runtime.get_object_size_locations(remote_object_id,
                    lambda object_id, size, object_locations:
                    self._size_location_result_handler(task, object_id, size, object_locations))



class BaseScheduler():
    """
    Base class for the scheduler system, including global and local schedulers.

    Args:
        system_time: A source for the global system time.
        scheduler_db: The global state database.
        event_loop: The event loop for the global scheduler.
        global_scheduler_kwargs: A dict storing keyword arguments for the
                                 global scheduler.
        local_scheduler_kwargs: A dict storing keyword arguments for the local
                                scheduler.
        global_scheduler_cls: The global scheduler class to instantiate.
                              Default is BaseGlobalScheduler. The __init__
                              signature must be of the form, __init__(self,
                              system_time, scheduler_db, event_loop).
        local_scheduler_cls: The local scheduler class to instantiate. Default
                             is PassthroughLocalScheduler. The __init__
                             signature must be of the form,
                             __init__(system_time, node_runtime, scheduler_db,
                             node_event_loop).
        local_nodes: A dict keyed by node_id, value is a tuple of
                     (node_runtime, node_event_loop). A local scheduler will be
                     instantiated per node in local_nodes.
    """
    def __init__(self, system_time, scheduler_db, event_loop,
                 global_scheduler_kwargs=None, local_scheduler_kwargs=None,
                 global_scheduler_cls=BaseGlobalScheduler,
                 local_scheduler_cls=PassthroughLocalScheduler,
                 local_nodes=None):
        if global_scheduler_kwargs is None:
            global_scheduler_kwargs = {}
        if local_scheduler_kwargs is None:
            local_scheduler_kwargs = {}
        self._system_time = system_time
        self._scheduler_db = scheduler_db
        self._local_scheduler_kwargs = local_scheduler_kwargs
        self._global_scheduler = global_scheduler_cls(self._system_time,
                                                      self._scheduler_db,
                                                      event_loop,
                                                      **global_scheduler_kwargs)
        self._local_schedulers = {}
        if local_nodes is None:
            local_nodes = {}
        for node_id, node in local_nodes.items():
            node_runtime, node_event_loop = node
            if node_id in self._local_schedulers:
                raise RuntimeError('Found multiple node runtimes with the same node ID.')
            self._local_schedulers[node_id] = local_scheduler_cls(
                    self._system_time, node_runtime, self._scheduler_db,
                    node_event_loop, **local_scheduler_kwargs)

    def get_global_scheduler(self):
        return self._global_scheduler

    def get_local_scheduler(self, node_id):
        return self._local_schedulers[node_id]


class TrivialScheduler(BaseScheduler):

    def __init__(self, system_time, scheduler_db, event_loop,
                 global_scheduler_kwargs=None, local_scheduler_kwargs=None,
                 local_scheduler_cls=PassthroughLocalScheduler,
                 local_nodes=None):
        BaseScheduler.__init__(self, system_time, scheduler_db, event_loop,
                               global_scheduler_kwargs=global_scheduler_kwargs,
                               local_scheduler_kwargs=local_scheduler_kwargs,
                               global_scheduler_cls=TrivialGlobalScheduler,
                               local_scheduler_cls=local_scheduler_cls,
                               local_nodes=local_nodes)


class LocationAwareScheduler(BaseScheduler):

    def __init__(self, system_time, scheduler_db, event_loop,
                 global_scheduler_kwargs=None, local_scheduler_kwargs=None,
                 local_scheduler_cls=PassthroughLocalScheduler,
                 local_nodes=None):
        BaseScheduler.__init__(self, system_time, scheduler_db, event_loop,
                               global_scheduler_kwargs=global_scheduler_kwargs,
                               local_scheduler_kwargs=local_scheduler_kwargs,
                               global_scheduler_cls=LocationAwareGlobalScheduler,
                               local_scheduler_cls=local_scheduler_cls,
                               local_nodes=local_nodes)


class TransferCostAwareScheduler(BaseScheduler):

    def __init__(self, system_time, scheduler_db, event_loop,
                 global_scheduler_kwargs=None, local_scheduler_kwargs=None,
                 local_scheduler_cls=PassthroughLocalScheduler,
                 local_nodes=None):
        BaseScheduler.__init__(self, system_time, scheduler_db, event_loop,
                               global_scheduler_kwargs=global_scheduler_kwargs,
                               local_scheduler_kwargs=local_scheduler_kwargs,
                               global_scheduler_cls=TransferCostAwareGlobalScheduler,
                               local_scheduler_cls=local_scheduler_cls,
                               local_nodes=local_nodes)


class DelayScheduler(BaseScheduler):

    def __init__(self, system_time, scheduler_db, event_loop,
                 global_scheduler_kwargs=None, local_scheduler_kwargs=None,
                 local_scheduler_cls=PassthroughLocalScheduler,
                 local_nodes=None):
        BaseScheduler.__init__(self, system_time, scheduler_db, event_loop,
                               global_scheduler_kwargs=global_scheduler_kwargs,
                               local_scheduler_kwargs=local_scheduler_kwargs,
                               global_scheduler_cls=DelayGlobalScheduler,
                               local_scheduler_cls=local_scheduler_cls,
                               local_nodes=local_nodes)


class TrivialLocalScheduler(TrivialScheduler):

    def __init__(self, system_time, scheduler_db, event_loop,
                 global_scheduler_kwargs=None, local_scheduler_kwargs=None,
                 local_nodes=None):
        TrivialScheduler.__init__(self, system_time, scheduler_db, event_loop,
                                  global_scheduler_kwargs=global_scheduler_kwargs,
                                  local_scheduler_kwargs=local_scheduler_kwargs,
                                  local_scheduler_cls=SimpleLocalScheduler,
                                  local_nodes=local_nodes)


class TrivialThresholdLocalScheduler(TrivialScheduler):

    def __init__(self, system_time, scheduler_db, event_loop,
                 global_scheduler_kwargs=None, local_scheduler_kwargs=None,
                 local_nodes=None):
        TrivialScheduler.__init__(self, system_time, scheduler_db, event_loop,
                                  global_scheduler_kwargs=global_scheduler_kwargs,
                                  local_scheduler_kwargs=local_scheduler_kwargs,
                                  local_scheduler_cls=ThresholdLocalScheduler,
                                  local_nodes=local_nodes)


class TransferCostAwareLocalScheduler(TransferCostAwareScheduler):

    def __init__(self, system_time, scheduler_db, event_loop,
                 global_scheduler_kwargs=None, local_scheduler_kwargs=None,
                 local_nodes=None):
        TransferCostAwareScheduler.__init__(self, system_time, scheduler_db,
                                            event_loop,
                                            global_scheduler_kwargs=global_scheduler_kwargs,
                                            local_scheduler_kwargs=local_scheduler_kwargs,
                                            local_scheduler_cls=SimpleLocalScheduler,
                                            local_nodes=local_nodes)


class TransferCostAwareThresholdLocalScheduler(TransferCostAwareScheduler):

    def __init__(self, system_time, scheduler_db, event_loop,
                 global_scheduler_kwargs=None, local_scheduler_kwargs=None,
                 local_nodes=None):
        TransferCostAwareScheduler.__init__(self, system_time, scheduler_db,
                                            event_loop,
                                            global_scheduler_kwargs=global_scheduler_kwargs,
                                            local_scheduler_kwargs=local_scheduler_kwargs,
                                            local_scheduler_cls=ThresholdLocalScheduler,
                                            local_nodes=local_nodes)


class LocationAwareLocalScheduler(LocationAwareScheduler):

    def __init__(self, system_time, scheduler_db, event_loop,
                 global_scheduler_kwargs=None, local_scheduler_kwargs=None,
                 local_nodes=None):
        LocationAwareScheduler.__init__(self, system_time, scheduler_db,
                                            event_loop,
                                            global_scheduler_kwargs=global_scheduler_kwargs,
                                            local_scheduler_kwargs=local_scheduler_kwargs,
                                            local_scheduler_cls=SimpleLocalScheduler,
                                            local_nodes=local_nodes)


class LocationAwareThresholdLocalScheduler(LocationAwareScheduler):

    def __init__(self, system_time, scheduler_db, event_loop,
                 global_scheduler_kwargs=None, local_scheduler_kwargs=None,
                 local_nodes=None):
        LocationAwareScheduler.__init__(self, system_time, scheduler_db,
                                            event_loop,
                                            global_scheduler_kwargs=global_scheduler_kwargs,
                                            local_scheduler_kwargs=local_scheduler_kwargs,
                                            local_scheduler_cls=ThresholdLocalScheduler,
                                            local_nodes=local_nodes)
