class ScheduleTaskUpdate():
    def __init__(self, task):
        self._task = task

    def get_task_id(self):
        return self._task.id()

    def get_task(self):
        return self._task

    def __str__(self):
        return 'ScheduleTask({})'.format(self._task.id())

class FinishTaskUpdate():
    def __init__(self, task_id):
        self._task_id = task_id

    def get_task_id(self):
        return self._task_id

    def __str__(self):
        return 'FinishTask({})'.format(self._task_id)

class RegisterNodeUpdate():
    def __init__(self, node_id, num_workers):
        self.node_id = node_id
        self.num_workers = num_workers

class RemoveNodeUpdate():
    def __init__(self, node_id):
        self.node_id = node_id

class ShutdownUpdate():
    def __init__(self):
        return

    def __str__(self):
        return 'Shutdown()'

class AbstractSchedulerDatabase():

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

    def get_updates(self, timeout_s):
        print 'Not implemented: get_updates'
        sys.exit(1)

    def execute(self, worker_id, task_id):
        print 'Not implemented: execute'
        sys.exit(1)

    def get_work(self, worker_id, timeout_s):
        print 'Not implemented: get_work'
        sys.exit(1)
