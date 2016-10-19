import sys

from schedulerbase import *
from itertools import ifilter

class SchedulerState():
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
        self.finished_objects = {}

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
        self.runnable_tasks.remove(task_id)
        self.executing_tasks[task_id] = node_id

    def update(self, update, timestamp):
        #print 'scheduler update ' + str(update)
        if isinstance(update, SubmitTaskUpdate):
            print '{} task {} submitted'.format(timestamp, update.get_task_id())
            self._add_task(update.get_task(), update.get_submitting_node_id())
        elif isinstance(update, FinishTaskUpdate):
            self._finish_task(update.get_task_id())
        elif isinstance(update, RegisterNodeUpdate):
            self._register_node(update.node_id, update.num_workers)
        elif isinstance(update, ShutdownUpdate):
            # TODO - not sure whether extra logic in shutdown
            #        is generally needed, but useful in debugging
            #        nonetheless.
            if not self.runnable_tasks and not self.pending_tasks and not self.executing_tasks:
                self.is_shutdown = True
            else:
                # This isn't really important but useful for debugging shutdown
                print 'pending: {}'.format(str(self.pending_tasks))
                print 'runnable: {}'.format(str(self.runnable_tasks))
                print 'executing: {}'.format(str(self.executing_tasks))
                print 'nodes: {}'.format(','.join(map(lambda x: str(x), self._state.nodes.values())))
        else:
            print 'Unknown update ' + update.__class__.__name__
            sys.exit(1)

    def _register_node(self, node_id, num_workers):
        if node_id in self.nodes.keys():
            print 'already registered node {}'.format(node_id)
            sys.exit(1)
        self.nodes[node_id] = self._NodeStatus(node_id, num_workers)

    def _add_task(self, task, submitting_node_id):
        task_id = task.id()
        self.tasks[task_id] = task
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
            # TODO - should be appending to list of object locations
            self.finished_objects[object_id] = node_id
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

class BaseScheduler():

    def __init__(self, time_source, scheduler_db):
        self._ts = time_source
        self._db = scheduler_db
        self._state = SchedulerState()

#    def _schedule_locally(self, task, submitting_node_id):
#        return False

    def _execute_task(self, node_id, task_id):
        self._state.set_executing(task_id, node_id)
        self._db.schedule(node_id, task_id)

    def _process_tasks(self):
        for task_id in self._state.runnable_tasks:
            node_id = self._select_node(task_id)
            print "process tasks got node id {} for task id {}".format(node_id, task_id)

            if node_id is not None:
                self._execute_task(node_id, task_id)
            else:
                # Not able to schedule so return
                print 'unable to schedule'
                return

    def _select_node(self, task_id):
        raise NotImplementedError()

    def run(self):
        while not self._state.is_shutdown:
            for update in self._db.get_updates(10):
                self._state.update(update, self._ts.get_time())
                # TODO ability to process tasks periodically
                self._process_tasks()


class TrivialScheduler(BaseScheduler):

    def __init__(self, time_source, scheduler_db):
        BaseScheduler.__init__(self, time_source, scheduler_db)
        #super(TrivialScheduler).__init__(time_source, scheduler_db)

    def _select_node(self, task_id):
        for node_id, node_status in self._state.nodes.items():
            if node_status.num_workers_executing < node_status.num_workers:
                return node_id
        return None

class LocationAwareScheduler(BaseScheduler):

    def __init__(self, time_source, scheduler_db):
        BaseScheduler.__init__(self, time_source, scheduler_db)

    def _select_node(self, task_id):
        task_deps = self._state.tasks[task_id].get_depends_on()
        best_node_id = None
        best_cost = sys.maxint
        # TODO short-circuit cost computation if there are no dependencies.
        #      also may optimize lookup strategy for one or two dependencies.
        for (node_id, node_status) in self._state.nodes.items():
            if node_status.num_workers_executing < node_status.num_workers:
                cost = 0
                for depends_on in task_deps:
                    if self._state.finished_objects[depends_on] != node_id:
                        cost += 1
                if cost < best_cost:
                    best_cost = cost
                    best_node_id = node_id
        return best_node_id

class TrivialLocalScheduler(TrivialScheduler):

    def __init__(self, time_source, scheduler_db):
        TrivialScheduler.__init__(self, time_source, scheduler_db)

    def _schedule_locally(self, task, submitting_node_id):
        for d_object_id in task.get_depends_on():
            if d_object_id not in self._finished_objects.keys() or self._finished_objects[d_object_id] != submitting_node_id:
                return False
        node_status = self._state.nodes[submitting_node_id]
        if node_status.num_workers_executing >= node_status.num_workers:
            return False
        self._execute_task(submitting_node_id, task.id())
        return True
