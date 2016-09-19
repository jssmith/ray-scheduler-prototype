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

class ShutdownUpdate():
    def __init__(self):
        return

    def __str__(self):
        return 'Shutdown()'
