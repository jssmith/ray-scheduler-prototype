import abc

class SubmitTaskUpdate():
    def __init__(self, task, submitting_node_id):
        self._task = task
        self._submitting_node_id = str(submitting_node_id)

    def get_task_id(self):
        return self._task.id()

    def get_task(self):
        return self._task

    def get_submitting_node_id(self):
        return self._submitting_node_id

    def __str__(self):
        return 'ScheduleTask({},{})'.format(self._task.id(), self._submitting_node_id)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._task == other._task and self._submitting_node_id == other._submitting_node_id
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)


class FinishTaskUpdate():
    def __init__(self, task_id):
        self._task_id = str(task_id)

    def get_task_id(self):
        return self._task_id

    def __str__(self):
        return 'FinishTask({})'.format(self._task_id)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._task_id == other._task_id
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)


class RegisterNodeUpdate():
    def __init__(self, node_id, num_workers):
        self.node_id = str(node_id)
        self.num_workers = num_workers

    def __str__(self):
      return 'RegisterNode({},{})'.format(self.node_id, self.num_workers)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.node_id == other.node_id and self.num_workers == other.num_workers
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

class RemoveNodeUpdate():
    def __init__(self, node_id):
        self.node_id = str(node_id)

    def __str__(self):
      return 'RemoveNode({})'.format(self.node_id)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._node_id == other._node_id
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)


class AbstractSchedulerDatabase():
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def submit(self, task):
        """Submit a task to the scheduler

           May be called by a driver or a worker program, either
           directly or by proxy through the local scheduler.

           Args:
               task: Task object describing task to schedule
        """
        return

    @abc.abstractmethod
    def finished(self, task_id):
        """Report task completion to the scheduler

           May be called by a worker program, either
           directly or by proxy through the local scheduler.

           Args:
               task_id: id of the completed task
        """
        return

    @abc.abstractmethod
    def register_node(self, node_id, num_workers):
        """Report addition of a new node

           Args:
               node_id: id of the newly added node
               num_workers: number of workers the node supports
        """
        return

    @abc.abstractmethod
    def remove_node(self, node_id):
        """Report removal of a node

           Args:
               node_id: id of the node being removed
        """
        return

    @abc.abstractmethod
    def get_updates(self, update_handler):
        """May be called by a driver or a worker program, either
           directly or by proxy through the local scheduler.

           Starts returning results as soon as they become available
           but never blocks beyond timeout_s from initiation time.

           Args:
               timeout_s: number of seconds to wait for tasks to
                   become available

           Returns:
               generator of task objects
        """
        return

    @abc.abstractmethod
    def schedule(self, node_id, task_id):
        """Instruct node to execute a task

           Called by the scheduler.

           Args:
               task_id: id of the completed task
        """
        return

    @abc.abstractmethod
    def get_work(self, node_id, timeout_s):
        """Get tasks to execute on this node

           Called by the worker or the local scheduler.

           Args:
               task_id: id of the completed task
        """
        return
