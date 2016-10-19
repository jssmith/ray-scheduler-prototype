import unittest
import os

# TODO both of these needed?
import replaystate
from replaystate import *

class TestInvalidTrace(unittest.TestCase):
    def __init__(self, name):
        self._method_name = 'test_inv_' + name
        super(TestInvalidTrace, self).__init__(self._method_name)
        self._name = name

    def __getattr__(self, name):
        if name == self._method_name:
            return self.runTest

    def runTest(self):
        import json
        input_fn = os.path.join(script_path(), 'traces', 'invalid', self._name + '.json')
        with self.assertRaises(ValidationError):
            try:
                f = open(input_fn, 'r')
                return json.load(f, object_hook=computation_decoder)
            finally:
                f.close()


def invalid_trace_suite():
    import glob
    files = glob.glob(os.path.join(script_path(), 'traces', 'invalid', '*.json'))
    test_names = list(map(lambda x: os.path.splitext(os.path.split(x)[1])[0], files))
    test_names.sort()
    return unittest.TestSuite(map(TestInvalidTrace, test_names))


def script_path():
    return os.path.dirname(os.path.realpath(__file__))


class TestComputationObjects(unittest.TestCase):

    def test_equality_finish_task_update(self):
        self.assertEquals(FinishTaskUpdate(task_id = 1), FinishTaskUpdate(task_id = 1))
        self.assertEquals(FinishTaskUpdate(task_id = 1), FinishTaskUpdate(task_id = '1'))


class TestSchedulerObjects(unittest.TestCase):
    def test_equality(self):
        phase_0_0 = TaskPhase(phase_id = 0, depends_on = [], schedules = [], duration = 1.0)
        task_0 = Task(task_id = 1, phases = [phase_0_0], results = [TaskResult(object_id = 0, size = 100)])

        phase_1_0 = TaskPhase(phase_id = 0, depends_on = [], schedules = [], duration = 1.0)
        task_1 = Task(task_id = 2, phases = [phase_1_0], results = [TaskResult(object_id = 2, size = 100)])

        self.assertEquals(SubmitTaskUpdate(task_0, 2), SubmitTaskUpdate(task_0, 2))
        self.assertNotEquals(SubmitTaskUpdate(task_0, 2), SubmitTaskUpdate(task_1, 2))
        self.assertNotEquals(SubmitTaskUpdate(task_0, 2), SubmitTaskUpdate(task_0, 3))

        self.assertEquals(RegisterNodeUpdate(node_id = 1, num_workers = 1), RegisterNodeUpdate(node_id = 1, num_workers = 1))
        self.assertNotEquals(RegisterNodeUpdate(node_id = 1, num_workers = 1), RegisterNodeUpdate(node_id = 2, num_workers = 1))
        self.assertNotEquals(RegisterNodeUpdate(node_id = 1, num_workers = 1), RegisterNodeUpdate(node_id = 1, num_workers = 2))

        self.assertItemsEqual([RegisterNodeUpdate(node_id = 1, num_workers = 1)], [RegisterNodeUpdate(node_id = 1, num_workers = 1)])


class TestReplayState(unittest.TestCase):

    def test_no_tasks(self):
        computation = ComputationDescription(root_task = None, tasks = [])
        system_time = SystemTime()
        num_nodes = 1
        num_workers_per_node = 1
        transfer_time_cost = 0
        scheduler_db = ReplaySchedulerDatabase(system_time, computation, num_nodes, num_workers_per_node, transfer_time_cost)

        self.assertEquals([RegisterNodeUpdate(node_id = 0, num_workers = num_workers_per_node)], list(scheduler_db.get_updates(100)))
        self.assertEquals([ShutdownUpdate()], list(scheduler_db.get_updates(100)))
        self.assertEquals(0, system_time.get_time())

        # check for another shutdown
        self.assertEquals([ShutdownUpdate()], list(scheduler_db.get_updates(100)))


    def test_one_task(self):
        phase_0_0 = TaskPhase(phase_id = 0, depends_on = [], schedules = [], duration = 1.0)
        task_0 = Task(task_id = 1, phases = [phase_0_0], results = [TaskResult(object_id = 0, size = 100)])
        computation = ComputationDescription(root_task = 1, tasks = [task_0])
        system_time = SystemTime()
        num_nodes = 1
        num_workers_per_node = 1
        transfer_time_cost = 0
        scheduler_db = ReplaySchedulerDatabase(system_time, computation, num_nodes, num_workers_per_node, transfer_time_cost)

        updates_expected = [RegisterNodeUpdate(node_id = 0, num_workers = num_workers_per_node), SubmitTaskUpdate(task = task_0, submitting_node_id = 0)]
        self.assertItemsEqual(updates_expected, list(scheduler_db.get_updates(100)))

        self.assertEquals([], list(scheduler_db.get_updates(100)))

        scheduler_db.schedule(0, task_0.id())

        updates_expected = [FinishTaskUpdate(task_id = task_0.id())]
        self.assertItemsEqual(updates_expected, list(scheduler_db.get_updates(100)))

        self.assertEquals([ShutdownUpdate()], list(scheduler_db.get_updates(100)))

        # check we get shutdown repeatedly
        self.assertEquals([ShutdownUpdate()], list(scheduler_db.get_updates(100)))

    def test_chained_tasks(self):
        phase_0_0 = TaskPhase(phase_id = 0, depends_on = [], schedules = [TaskSubmit(task_id = 2,time_offset = 0.5)], duration = 1.0)
        task_0 = Task(task_id = 1, phases = [phase_0_0], results = [TaskResult(object_id = 234, size = 100)])

        phase_1_0 = TaskPhase(phase_id = 0, depends_on = [234], schedules = [], duration = 1.0)
        task_1 = Task(task_id = 2, phases = [phase_1_0], results = [TaskResult(object_id = 2, size = 100)])

        computation = ComputationDescription(root_task = 1, tasks = [task_0, task_1])
        system_time = SystemTime()
        num_nodes = 1
        num_workers_per_node = 1
        transfer_time_cost = 0
        scheduler_db = ReplaySchedulerDatabase(system_time, computation, num_nodes, num_workers_per_node, transfer_time_cost)

        updates_expected = [RegisterNodeUpdate(node_id = 0, num_workers = num_workers_per_node), SubmitTaskUpdate(task = task_0, submitting_node_id = 0)]
        self.assertItemsEqual(updates_expected, list(scheduler_db.get_updates(100)))

        self.assertEquals([], list(scheduler_db.get_updates(100)))

        scheduler_db.schedule(0, task_0.id())

        updates_expected = [FinishTaskUpdate(task_id = task_0.id()), SubmitTaskUpdate(task_1, 0)]
        self.assertItemsEqual(updates_expected, list(scheduler_db.get_updates(100)))

        self.assertEquals([], list(scheduler_db.get_updates(100)))
        scheduler_db.schedule(0, task_1.id())

        updates_expected = [FinishTaskUpdate(task_id = task_1.id())]
        self.assertItemsEqual(updates_expected, list(scheduler_db.get_updates(100)))

        self.assertEquals([ShutdownUpdate()], list(scheduler_db.get_updates(100)))


if __name__ == '__main__':
    tests = unittest.TestSuite([invalid_trace_suite()] + list(map(lambda x: unittest.TestLoader().loadTestsFromTestCase(x), [TestComputationObjects, TestSchedulerObjects, TestReplayState])))
    unittest.TextTestRunner(verbosity=2).run(tests)
    
