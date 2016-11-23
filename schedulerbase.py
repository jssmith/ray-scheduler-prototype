import abc
import types

class SubmitTaskUpdate():
    def __init__(self, task):
        self.task = task

    def __str__(self):
        return 'SubmitTask({})'.format(self.task.id())

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.task == other.task
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

class ForwardTaskUpdate():
    def __init__(self, task, submitting_node_id, is_scheduled_locally):
        self.task = task
        self.submitting_node_id = str(submitting_node_id)
        if type(is_scheduled_locally) != types.BooleanType:
            raise ValueError("is scheduled locally must be boolean")
        self.is_scheduled_locally = is_scheduled_locally

    def __str__(self):
        return 'ForwardTask({},{},{})'.format(self.task.id(), self.submitting_node_id, self.is_scheduled_locally)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.task == other.task and self.submitting_node_id == other.submitting_node_id and self.is_scheduled_locally == other.is_scheduled_locally
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)


class ScheduleTaskUpdate():
    def __init__(self, task, node_id):
        self.task = task
        self.node_id = node_id

    def __str__(self):
        return 'ScheduleTask({},{})'.format(self.task.id(), self.node_id)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.task == other.task and self.node_id == other.node_id
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)


class FinishTaskUpdate():
    def __init__(self, task_id):
        self.task_id = str(task_id)

    def __str__(self):
        return 'FinishTask({})'.format(self.task_id)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.task_id == other.task_id
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)


class ObjectReadyUpdate():
    def __init__(self, object_description, submitting_node_id):
        self.object_description = object_description
        self.submitting_node_id = str(submitting_node_id)

    def __str__(self):
        return 'ObjectReadyUpdate({},{})'.format(str(self.object_description), self.submitting_node_id)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.object_description == other.object_description and self.submitting_node_id == other.submitting_node_id
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


class ObjectStatus():
    UNKNOWN = 0
    EXPECTED = 1
    READY = 2


class AbstractNodeRuntime():
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def send_to_dispatcher(self, task, priority):
        """Submit work to the worker execution engine. The implementation
           must preserve FIFO sequence for tasks of equal priority.

           Args:
               task: id of the task to schedule
               priority: lower numbers mean higher priority.
                         priorities must be integers
        """
        pass

    @abc.abstractmethod
    def get_updates(self, update_handler):
        """Called by the local scheduler to register a handler for local
           runtime events.
        """
        pass


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
        pass

    @abc.abstractmethod
    def finished(self, task_id):
        """Report task completion to the scheduler

           May be called by a worker program, either
           directly or by proxy through the local scheduler.

           Args:
               task_id: id of the completed task
        """
        pass

    @abc.abstractmethod
    def register_node(self, node_id, num_workers):
        """Report addition of a new node

           Args:
               node_id: id of the newly added node
               num_workers: number of workers the node supports
        """
        pass

    @abc.abstractmethod
    def remove_node(self, node_id):
        """Report removal of a node

           Args:
               node_id: id of the node being removed
        """
        pass

    @abc.abstractmethod
    def get_global_scheduler_updates(self, update_handler):
        """Called by the global scheduler to register a handler for updates

           Args:
               update_hander: function for processing updates
        """
        pass

    @abc.abstractmethod
    def get_local_scheduler_updates(self, node_id, update_handler):
        """Called by the local scheduler to register a handler for updates

           Args:
               update_hander: function for processing updates
        """
        pass

    @abc.abstractmethod
    def schedule(self, node_id, task_id):
        """Instruct node to execute a task

           Called by the scheduler.

           Args:
               task_id: id of the completed task
        """
        pass

