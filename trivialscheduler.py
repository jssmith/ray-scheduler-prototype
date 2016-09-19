from schedulerbase import *

class TrivialScheduler():
    def __init__(self, time_source, scheduler_db):
        self._ts = time_source
        self._db = scheduler_db

        self._pending_tasks = []
        self._runnable_tasks = []
        self._executing_tasks = []
        self._finished_tasks = []
        
        self._pending_needs = {}
        self._awaiting_completion = {}

        self._tasks = {}

    def _add_task(self, task):
        task_id = task.id()
        self._tasks[task_id] = task        
        pending_needs = []
        for d_task_id in task.get_depends_on():
            if d_task_id not in self._finished_tasks:
                pending_needs.append(d_task_id)
                if d_task_id in self._awaiting_completion.keys():
                    self._awaiting_completion[d_task_id].append(task_id)
                else:
                    self._awaiting_completion[d_task_id] = [task_id]
        if len(pending_needs) > 0:
            self._pending_needs[task_id] = pending_needs
            self._pending_tasks.append(task_id)
        else:
            self._runnable_tasks.append(task_id)

    def _finish_task(self, task_id):
        self._executing_tasks.remove(task_id)
        self._finished_tasks.append(task_id)
        if task_id in self._awaiting_completion.keys():
            pending_task_ids = self._awaiting_completion[task_id]
            del self._awaiting_completion[task_id]
            for pending_task_id in pending_task_ids:
                needs = self._pending_needs[pending_task_id]
                needs.remove(task_id)
                if not needs:
                    del self._pending_needs[pending_task_id]
                    self._pending_tasks.remove(pending_task_id)
                    self._runnable_tasks.append(pending_task_id)

    def _process_tasks(self):
        for task_id in list(self._runnable_tasks):
            # unlimited resources on one node
            self._runnable_tasks.remove(task_id)
            self._executing_tasks.append(task_id)
            self._db.execute(0, task_id)

    def run(self):
        is_shutdown = False
        while not is_shutdown:
            for update in self._db.get_updates(10):
                #print 'scheduler update ' + str(update)
                if isinstance(update, ScheduleTaskUpdate):
                    self._add_task(update.get_task())
                elif isinstance(update, FinishTaskUpdate):
                    self._finish_task(update.get_task_id())
                elif isinstance(update, ShutdownUpdate):
                    # TODO - not sure whether extra logic in shutdown
                    #        is generally needed, but useful in debugging
                    #        nonetheless.
                    if not self._runnable_tasks and not self._pending_tasks and not self._executing_tasks:
                        is_shutdown = True
                    else:
                        print 'pending: {}'.format(str(self._pending_tasks))
                        print 'runnable: {}'.format(str(self._runnable_tasks))
                        print 'executing: {}'.format(str(self._executing_tasks))

                else:
                    print 'Unknown update ' + update.__class__.__name__
                    sys.exit(1)
                self._process_tasks()
