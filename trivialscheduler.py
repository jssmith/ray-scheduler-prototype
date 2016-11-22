from collections import defaultdict
from collections import namedtuple
from collections import OrderedDict
import sys
import logging
import numpy as np
import os

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
        # TODO:  - add the sizes of object ids [atumanov]
        self.finished_objects = defaultdict(list)
        #TODO: calculate the node_id -> object_id inverse map [atumanov]

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
                    node2object_array[i].append(1) #remote: TODO: change this to object size, when available

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
        print "timer handler fired at time t=%s , runnable=%s pending=%s executing=%s" \
              % (tnow, num_runnable, num_pending, num_executing)
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
       self._state.update(update, self._system_time.get_time())

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
        #get threshold from unix environment variables, so I can sweep over them later in the bash sweep to find good values.
        #os.getenv('KEY_THAT_MIGHT_EXIST', default_value)
        threshold1l = os.getenv('RAY_SCHED_THRESHOLD1L', 20)
        threshold1h = os.getenv('RAY_SCHED_THRESHOLD1H', 50)
        threshold2 = os.getenv('RAY_SCHED_THRESHOLD2', 200)

        objects_transfer_size = 0
        objects_status = {'local_ready' : 0, 'remote_ready' : 0, 'local_notready' : 0, 'remote_notready' : 0, 'no_info' : 0}
        remote_objects = []

        #avg_task_time = 1
        avg_task_time = self._node_runtime.get_avg_task_time()
        print "get_avg_task_time : {}".format(self._node_runtime.get_avg_task_time())
        #TODO: Need to implement self._node_runtime.get_avg_task_time() which buffers that last local 20 task completion times
        dispatcher_load = self._node_runtime.get_dispatch_queue_size()
        node_efficiency_rate = self._node_runtime.get_node_eff_rate()
        #add to node_runtime the function get_node_eff_rate(). It will record a buffer in the form of list of task_start_time for the last 10 or 20 tasks (this will be a constant parameter) sent for execution on the node. The node efficiency rate will be the buffer size (whatever the constant is) divided by the (last_element-first_element) of the buffer.
        
        local_load = 0 if (node_efficiency_rate == 0) else ((dispatcher_load / node_efficiency_rate) / avg_task_time)
        print "threshold: local load is {}".format(local_load)

        for d_object_id in task.get_phase(0).depends_on:
            if not self._node_runtime.is_local(d_object_id):
                ###objects_transfer_size = transfer_size + self._node_runtime.get_object_size(d_object_id)
                #TODO: add to node_runtime the function get_object_size() which just return a value from the _object_store sizes map/dict.
#                object_status = self._node_runtime.get_object_status(d_object_id)
#                if object_status == ready :
#                    objects_status['remote_ready'] += 1 
#                elif (object_status == scheduled and get_location(d_object_id) != self._node_runtime._node_id) :
#                    objects_status['local_notready'] += 1
                remote_objects.append(d_object_id)
            else:
                objects_status['local_ready'] += 1  

        if len(task.get_phase(0).depends_on) == objects_status['local_ready'] and self._node_runtime.free_workers() > 0:
            print "threshold: all objects ready"
            self._node_runtime.send_to_dispatcher(task, 1)
            return True
        elif len(task.get_phase(0).depends_on) == objects_status['local_ready'] and local_load < threshold1l:
            print "threshold: all objects ready"
            self._node_runtime.send_to_dispatcher(task, 1)
            return True


        if local_load > threshold1h:
            return False
        elif local_load < threshold1h and local_load > threshold1l:
            #querry for remote object sizes and calculate task load
            for remote_object_id in remote_objects:
                #objects_transfer_size += self._node_runtime.get_object_size(remote_object_id) 
                pass  
            task_load = objects_transfer_size 
            print "task load is {}".format(task_load)
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


class TrivialThresholdLocalScheduler(TrivialScheduler):

    def __init__(self, system_time, scheduler_db):
        TrivialScheduler.__init__(self, system_time, scheduler_db)

    def _make_local_scheduler(self, node_runtime, event_loop):
        return ThresholdLocalScheduler(self._system_time, node_runtime, self._scheduler_db)



class TransferCostAwareLocalScheduler(TransferCostAwareScheduler):

    def __init__(self, system_time, scheduler_db):
        TransferCostAwareScheduler.__init__(self, system_time, scheduler_db)

    def _make_local_scheduler(self, node_runtime, event_loop):
        return SimpleLocalScheduler(self._system_time, node_runtime, self._scheduler_db)

