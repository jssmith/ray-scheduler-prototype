import unittest
import os
import logging
from collections import namedtuple

# TODO both of these needed?
import replaystate
from replaystate import *

from replaytrace import schedulers
from replaytrace import simulate

class TestEventLoopTimers(unittest.TestCase):
    def setUp(self):
        self.ts = SystemTime()
        self.event_loop = EventLoop(self.ts)
        self.callback_contexts = []

    class CallbackContext():
        def __init__(self, test, ts_expected):
            self.test = test
            self.did_execute = False
            self.ts_expected = ts_expected
            self.ts_executed = None
            test.callback_contexts.append(self)
        def __str__(self):
            return "CallbackContext({},{},{})".format(self.test, self.did_execute, self.ts_executed, self.ts_expected)

    @staticmethod
    def basic_handler(context):
        context.did_execute = True
        context.ts_executed = context.test.ts.get_time()

    @staticmethod
    def active_handler(context):
        TestEventLoopTimers.basic_handler(context)
        context.fn()

    class ActiveCallback(CallbackContext):
        def __init__(self, test, ts_expected, fn):
            TestEventLoopTimers.CallbackContext.__init__(self, test, ts_expected)
            self.fn = fn

    def check_all_executed(self):
        for callback_context in self.callback_contexts:
            self.assertTrue(callback_context.did_execute)
            self.assertEquals(callback_context.ts_expected, callback_context.ts_expected)

    def test_no_events(self):
        start_time = self.ts.get_time()
        self.ts.advance_fully()
        self.assertEquals(start_time, self.ts.get_time())

    def test_one_event(self):
        start_time = self.ts.get_time()
        self.event_loop.add_timer(2, TestEventLoopTimers.basic_handler, TestEventLoopTimers.CallbackContext(self, start_time + 2))
        self.ts.advance_fully()
        self.check_all_executed()
        self.assertEquals(start_time + 2, self.ts.get_time())

    def test_two_events(self):
        start_time = self.ts.get_time()
        self.event_loop.add_timer(5, TestEventLoopTimers.basic_handler, TestEventLoopTimers.CallbackContext(self, start_time + 5))
        self.event_loop.add_timer(2, TestEventLoopTimers.basic_handler, TestEventLoopTimers.CallbackContext(self, start_time + 2))
        self.ts.advance_fully()
        self.check_all_executed()
        self.assertEquals(start_time + 5, self.ts.get_time())

    def test_chained_events(self):
        start_time = self.ts.get_time()
        def callback_action():
            self.event_loop.add_timer(5, TestEventLoopTimers.basic_handler, TestEventLoopTimers.CallbackContext(self, start_time + 7))
        self.event_loop.add_timer(2, TestEventLoopTimers.active_handler, TestEventLoopTimers.ActiveCallback(self, start_time + 2, callback_action))
        self.ts.advance_fully()
        self.check_all_executed()
        self.assertEquals(start_time + 7, self.ts.get_time())

    def test_one_event_cancelled(self):
        start_time = self.ts.get_time()

        def failure_action():
            self.fail('should not have called this')

        # remove the timer before advancing
        callback = TestEventLoopTimers.ActiveCallback(self, start_time + 2, failure_action)
        timer_id = self.event_loop.add_timer(2, TestEventLoopTimers.active_handler, callback)
        self.event_loop.remove_timer(timer_id)
        self.ts.advance_fully()
        self.assertEquals(start_time + 2, self.ts.get_time())
        self.assertFalse(callback.did_execute)

        # cancel during the advance by scheduling another callback
        start_time = self.ts.get_time()
        callback_ts_5 = TestEventLoopTimers.ActiveCallback(self, start_time + 5, failure_action)
        timer_id_ts_5 = self.event_loop.add_timer(5, TestEventLoopTimers.active_handler, callback_ts_5)

        def cancel_action():
            self.event_loop.remove_timer(timer_id_ts_5)
        callback_ts_2 = TestEventLoopTimers.ActiveCallback(self, start_time + 2, cancel_action)
        self.event_loop.add_timer(2, TestEventLoopTimers.active_handler, callback_ts_2)
        self.ts.advance_fully()
        self.assertFalse(callback_ts_5.did_execute)
        self.assertTrue(callback_ts_2.did_execute)
        self.assertEquals(start_time + 5, self.ts.get_time())


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


class TestValidTrace(unittest.TestCase):
    def __init__(self, name):
        self._method_name = 'test_valid_' + name
        super(TestValidTrace, self).__init__(self._method_name)
        self._name = name

    def __getattr__(self, name):
        if name == self._method_name:
            return self.runTest

    class TaskTiming():
        def __init__(self, task_id, start_timestamp, end_timestamp):
            self.task_id = str(task_id)
            self.start_timestamp = float(start_timestamp)
            self.end_timestamp = float(end_timestamp)

    class ValidatingLogger():
        def __init__(self, test, system_time, task_timing):
            self._test = test
            self._system_time = system_time
            self._task_timing = {}
            for t in task_timing:
                self._task_timing[t.task_id] = t
            self._timed_tasks = set()

        def task_started(self, task_id, node_id):
            self._test.assertAlmostEqual(self._task_timing[task_id].start_timestamp, self._system_time.get_time())

        def task_finished(self, task_id, node_id):
            self._test.assertAlmostEqual(self._task_timing[task_id].end_timestamp, self._system_time.get_time())
            self._timed_tasks.add(task_id)

        def verify_all_finished(self):
            self._test.assertItemsEqual(self._task_timing.keys(), self._timed_tasks)

    @staticmethod
    def validation_decoder(dict):
        keys = frozenset(dict.keys())
        if keys == frozenset([u'taskId', 'startTimestamp', 'endTimestamp']):
            return TestValidTrace.TaskTiming(dict[u'taskId'], dict[u'startTimestamp'], dict[u'endTimestamp'])
        if keys == frozenset([u'scheduler', u'numNodes', u'workersPerNode', u'transferTimeCost', u'dbMessageDelay', u'taskTiming']):
            return dict
        else:
            print "unexpected map in validation: {}".format(keys)
            sys.exit(1)

    def runTest(self):
        import json
        trace_fn = os.path.join(script_path(), 'traces', 'test', self._name + '.json')
        expected_fn = os.path.join(script_path(), 'traces', 'validation', self._name + '.json')
        trace_f = open(trace_fn, 'r')
        computation = json.load(trace_f, object_hook=computation_decoder)
        trace_f.close()
        expected_f = open(expected_fn, 'r')
        validations = json.load(expected_f, object_hook=TestValidTrace.validation_decoder)
        expected_f.close()

        for validation in validations:
            scheduler_str = str(validation['scheduler'])
            num_nodes = int(validation['numNodes'])
            num_workers_per_node = int(validation['workersPerNode'])
            transfer_time_cost = float(validation['transferTimeCost'])
            db_message_delay = float(validation['dbMessageDelay'])
            system_time = replaystate.SystemTime()
            logger = TestValidTrace.ValidatingLogger(self, system_time, validation['taskTiming'])
            scheduler_type = schedulers[scheduler_str]
            simulate(computation, scheduler_type, system_time, logger, num_nodes, num_workers_per_node, transfer_time_cost, db_message_delay)
            logger.verify_all_finished()


def valid_trace_suite():
    test_names = [
        'forkjoin',
        'singletask',
        'two_chained_tasks',
        'two_parallel_tasks',
        'two_phase',
        'two_results',
        'no_result']
    return unittest.TestSuite(map(TestValidTrace, test_names))

def script_path():
    return os.path.dirname(os.path.realpath(__file__))


class TestComputationObjects(unittest.TestCase):

    def test_equality_finish_task_update(self):
        self.assertEquals(FinishTaskUpdate(task_id = 1), FinishTaskUpdate(task_id = 1))
        self.assertEquals(FinishTaskUpdate(task_id = 1), FinishTaskUpdate(task_id = '1'))


class TestSchedulerObjects(unittest.TestCase):
    def test_equality(self):
        phase_0_0 = TaskPhase(phase_id = 0, depends_on = [], submits = [], duration = 1.0)
        task_0 = Task(task_id = 1, phases = [phase_0_0], results = [TaskResult(object_id = 0, size = 100)])

        phase_1_0 = TaskPhase(phase_id = 0, depends_on = [], submits = [], duration = 1.0)
        task_1 = Task(task_id = 2, phases = [phase_1_0], results = [TaskResult(object_id = 2, size = 100)])

        self.assertEquals(ForwardTaskUpdate(task_0, 2, False), ForwardTaskUpdate(task_0, 2, False))
        self.assertNotEquals(ForwardTaskUpdate(task_0, 2, False), ForwardTaskUpdate(task_1, 2, False))
        self.assertNotEquals(ForwardTaskUpdate(task_0, 2, False), ForwardTaskUpdate(task_0, 3, False))
        self.assertNotEquals(ForwardTaskUpdate(task_0, 2, False), ForwardTaskUpdate(task_0, 2, True))

        self.assertEquals(RegisterNodeUpdate(node_id = 1, num_workers = 1), RegisterNodeUpdate(node_id = 1, num_workers = 1))
        self.assertNotEquals(RegisterNodeUpdate(node_id = 1, num_workers = 1), RegisterNodeUpdate(node_id = 2, num_workers = 1))
        self.assertNotEquals(RegisterNodeUpdate(node_id = 1, num_workers = 1), RegisterNodeUpdate(node_id = 1, num_workers = 2))

        self.assertItemsEqual([RegisterNodeUpdate(node_id = 1, num_workers = 1)], [RegisterNodeUpdate(node_id = 1, num_workers = 1)])


class TestReplayStateBase(unittest.TestCase):

    def setUp(self):
        logging_format = '%(timestamp).6f %(name)s %(message)s'
        logging.basicConfig(format=logging_format)
        logging.getLogger().setLevel(logging.DEBUG)

        self.system_time = SystemTime()
        self.event_loop = EventLoop(self.system_time)
        self.logger = PrintingLogger(self.system_time)
        self.scheduler_db = None
        self.local_scheduler_updates_received = []
        self.global_scheduler_updates_received = []

    def _setup_scheduler_db(self, computation, num_nodes, num_workers_per_node, transfer_time_cost, db_message_delay):
        self.scheduler_db = ReplaySchedulerDatabase(self.system_time, self.logger, computation, num_nodes, num_workers_per_node, transfer_time_cost, db_message_delay)
        self.scheduler_db.get_global_scheduler_updates(lambda update: self.global_scheduler_updates_received.append(update))
        self.scheduler_db.get_local_scheduler_updates(0, lambda update: self.local_scheduler_updates_received.append(update))
        self.scheduler_db.schedule_root(0)

    def _advance_check(self, delta, end_ts_expected, local_scheduler_updates_expected, global_scheduler_updates_expected):
        self.local_scheduler_updates_received = []
        self.global_scheduler_updates_received = []
        self._advance_until(delta)
        self.assertEquals(end_ts_expected, self.system_time.get_time())
        self.assertItemsEqual(global_scheduler_updates_expected, self.global_scheduler_updates_received)
        self.assertItemsEqual(local_scheduler_updates_expected, self.local_scheduler_updates_received)

    def _stop_advance(self):
        self.continue_advance = False

    def _advance_until(self, delta):
        self.continue_advance = True
        self.system_time.schedule_delayed(delta, lambda: self._stop_advance())
        while self.continue_advance and self.system_time.advance():
            pass

    def _end_check(self):
        start_time = self.system_time.get_time()
        self.local_scheduler_updates_received = []
        self.global_scheduler_updates_received = []
        self.system_time.advance_fully()
        self.assertEquals(start_time, self.system_time.get_time())
        self.assertEquals([], self.global_scheduler_updates_received)
        self.assertEquals([], self.local_scheduler_updates_received)


class TestReplayState(TestReplayStateBase):

    def test_no_tasks(self):
        computation = ComputationDescription(root_task = None, tasks = [])
        num_nodes = 1
        num_workers_per_node = 1
        transfer_time_cost = 0
        db_message_delay = 0
        self._setup_scheduler_db(computation, num_nodes, num_workers_per_node, transfer_time_cost, db_message_delay)

        global_scheduler_updates_expected = [RegisterNodeUpdate(node_id = 0, num_workers = num_workers_per_node)]
        local_scheduler_updates_expected = []
        self._advance_check(10, 10, local_scheduler_updates_expected, global_scheduler_updates_expected)

        self._end_check()

    def test_one_task(self):
        phase_0_0 = TaskPhase(phase_id = 0, depends_on = [], submits = [], duration = 1.0)
        task_0 = Task(task_id = 1, phases = [phase_0_0], results = [TaskResult(object_id = 0, size = 100)])
        computation = ComputationDescription(root_task = 1, tasks = [task_0])
        num_nodes = 1
        num_workers_per_node = 2
        transfer_time_cost = 0
        db_message_delay = 0
        self._setup_scheduler_db(computation, num_nodes, num_workers_per_node, transfer_time_cost, db_message_delay)

        local_scheduler_updates_expected = [ScheduleTaskUpdate(task_0, 0)]
        global_scheduler_updates_expected = [RegisterNodeUpdate(node_id = 0, num_workers = num_workers_per_node), ForwardTaskUpdate(task_0, 0, True)]
        self._advance_check(10, 10, local_scheduler_updates_expected, global_scheduler_updates_expected)
        self._end_check()

        self.system_time.schedule_delayed(phase_0_0.duration, lambda: self.scheduler_db.finished(task_0.id()))

        local_scheduler_updates_expected = []
        global_scheduler_updates_expected = [FinishTaskUpdate(task_id = task_0.id())]
        self._advance_check(10, 20, local_scheduler_updates_expected, global_scheduler_updates_expected)
        self._end_check()

    def test_long_task(self):
        phase_0_0 = TaskPhase(phase_id = 0, depends_on = [], submits = [], duration = 1000.0)
        task_0 = Task(task_id = 1, phases = [phase_0_0], results = [TaskResult(object_id = 0, size = 100)])
        computation = ComputationDescription(root_task = 1, tasks = [task_0])
        num_nodes = 1
        num_workers_per_node = 2
        transfer_time_cost = 0
        db_message_delay = 0
        self._setup_scheduler_db(computation, num_nodes, num_workers_per_node, transfer_time_cost, db_message_delay)

        local_scheduler_updates_expected = [ScheduleTaskUpdate(task_0, 0)]
        global_scheduler_updates_expected = [RegisterNodeUpdate(node_id = 0, num_workers = num_workers_per_node),
                                             ForwardTaskUpdate(task_0, 0, True)]
        self._advance_check(100, 100, local_scheduler_updates_expected, global_scheduler_updates_expected)
        self._advance_check(100, 200, [], [])

        self.system_time.schedule_delayed(phase_0_0.duration, lambda: self.scheduler_db.finished(task_0.id()))

        for n in range(2, 11):
            self._advance_check(100, (n + 1) * 100, [], [])

        self.assertEquals(1100, self.system_time.get_time())
        self._advance_check(101, 1201, [], [FinishTaskUpdate(task_id = task_0.id())])

        self._end_check()


    def test_chained_tasks(self):
        phase_0_0 = TaskPhase(phase_id = 0, depends_on = [], submits = [TaskSubmit(task_id = 2,time_offset = 0.5)], duration = 1.0)
        task_0 = Task(task_id = 1, phases = [phase_0_0], results = [TaskResult(object_id = 234, size = 100)])

        phase_1_0 = TaskPhase(phase_id = 0, depends_on = [234], submits = [], duration = 1.0)
        task_1 = Task(task_id = 2, phases = [phase_1_0], results = [TaskResult(object_id = 2, size = 100)])

        computation = ComputationDescription(root_task = 1, tasks = [task_0, task_1])
        system_time = SystemTime()
        event_loop = EventLoop(system_time)
        num_nodes = 1
        num_workers_per_node = 2
        transfer_time_cost = 0
        db_message_delay = 0
        self._setup_scheduler_db(computation, num_nodes, num_workers_per_node, transfer_time_cost, db_message_delay)

        local_scheduler_updates_expected = [ScheduleTaskUpdate(task_0, 0)]
        global_scheduler_updates_expected = [RegisterNodeUpdate(node_id = 0, num_workers = num_workers_per_node), ForwardTaskUpdate(task = task_0, submitting_node_id = 0, is_scheduled_locally = True)]
        self._advance_check(100, 100, local_scheduler_updates_expected, global_scheduler_updates_expected)
        self._advance_check(100, 200, [], [])

        self.system_time.schedule_delayed(phase_0_0.submits[0].time_offset, lambda: self.scheduler_db.submit(task_1, 0, False))
        self.system_time.schedule_delayed(phase_0_0.duration, lambda: self.scheduler_db.finished(task_0.id()))

        local_scheduler_updates_expected = []
        global_scheduler_updates_expected = [ForwardTaskUpdate(task = task_1, submitting_node_id = 0, is_scheduled_locally = False),
                                             FinishTaskUpdate(task_id = task_0.id())]

        self._advance_check(100, 300, local_scheduler_updates_expected, global_scheduler_updates_expected)
        self._advance_check(100, 400, [], [])

        self.scheduler_db.schedule(0, task_1.id())

        local_scheduler_updates_expected = [ScheduleTaskUpdate(task_1, 0)]
        global_scheduler_updates_expected = []
        self._advance_check(100, 500, local_scheduler_updates_expected, global_scheduler_updates_expected)

        self.system_time.schedule_delayed(phase_1_0.duration, lambda: self.scheduler_db.finished(task_1.id()))

        local_scheduler_updates_expected = []
        global_scheduler_updates_expected = [FinishTaskUpdate(task_id = task_1.id())]
        self._advance_check(100, 600, local_scheduler_updates_expected, global_scheduler_updates_expected)

        self._end_check()


class TestReplayStateTimingDetail(TestReplayStateBase):

    def _setup_scheduler_db(self, computation, num_nodes, num_workers_per_node, transfer_time_cost, db_message_delay):
        self.scheduler_db = ReplaySchedulerDatabase(self.system_time, self.logger, computation, num_nodes, num_workers_per_node, transfer_time_cost, db_message_delay)
        self.scheduler_db.get_global_scheduler_updates(lambda update: self.global_scheduler_updates_received.append((self.system_time.get_time(), update)))
        self.scheduler_db.get_local_scheduler_updates(0, lambda update: self.local_scheduler_updates_received.append((self.system_time.get_time(), update)))
        self.scheduler_db.schedule_root(0)

    def testMessageDelay(self):
        phase_0_0 = TaskPhase(phase_id = 0, depends_on = [], submits = [], duration = 1.0)
        task_0 = Task(task_id = 1, phases = [phase_0_0], results = [TaskResult(object_id = 0, size = 100)])
        computation = ComputationDescription(root_task = 1, tasks = [task_0])
        num_nodes = 1
        num_workers_per_node = 2
        transfer_time_cost = 0
        db_message_delay = 0.001
        self._setup_scheduler_db(computation, num_nodes, num_workers_per_node, transfer_time_cost, db_message_delay)

        local_scheduler_updates_expected = [(0.001, ScheduleTaskUpdate(task_0, 0))]
        global_scheduler_updates_expected = [(0.001, RegisterNodeUpdate(node_id = 0, num_workers = num_workers_per_node)), (0.001, ForwardTaskUpdate(task_0, 0, True))]
        self._advance_check(10, 10, local_scheduler_updates_expected, global_scheduler_updates_expected)
        self._end_check()

        self.system_time.schedule_delayed(phase_0_0.duration, lambda: self.scheduler_db.finished(task_0.id()))

        local_scheduler_updates_expected = []
        global_scheduler_updates_expected = [(11.001, FinishTaskUpdate(task_id = task_0.id()))]
        self._advance_check(10, 20, local_scheduler_updates_expected, global_scheduler_updates_expected)
        self._end_check()

class TestObjectStoreRuntime(unittest.TestCase):

    def setUp(self):
        self.system_time = SystemTime()
        self.os = ObjectStoreRuntime(self.system_time, .001)
        self.last_ready = defaultdict(list)

    def _fn_ready(self, object_id, node_id):
        self.last_ready[(object_id, node_id)].append(self.system_time.get_time())

    def _require_object(self, object_id, node_id):
        self.os.require_object(object_id, node_id, lambda: self._fn_ready(object_id, node_id))

    def _last_ready(self, object_id, node_id):
        return self.last_ready[(object_id, node_id)]

    def _add_object(self, object_id, node_id, size):
        self.os.add_object(object_id, node_id, size)

    def test_no_objects(self):
        self.assertItemsEqual([], self.os.get_locations('1'))

    def test_one_object(self):
        self._add_object('1', 0, 100)
        self.assertItemsEqual([0], self.os.get_locations('1'))

    def test_moved_object(self):
        self.assertEquals(0, self.system_time.get_time())
        self._add_object('1', 0, 200)
        self.assertItemsEqual([0], self.os.get_locations('1'))

        self.assertFalse(self.os.is_local('1', 1))

        self._require_object('1', 0)
        self.system_time.advance()
        self.assertEquals([0], self._last_ready('1', 0))

        self._require_object('1', 1)
        self.system_time.advance()
        self.assertEquals([0.2], self._last_ready('1', 1))

    def test_delayed_object(self):
        self.assertEquals(0, self.system_time.get_time())

        self._require_object('1', 0)
        self.assertEquals([], self._last_ready('1', 0))

        self._add_object('1', 0, 100)
        self.system_time.advance()
        self.assertEquals([0], self._last_ready('1', 0))

        self._require_object('2', 1)
        self._require_object('2', 0)
        self._require_object('1', 1)

        self.system_time.advance()

        self.assertEquals([], self._last_ready('2',1))
        self.assertEquals([], self._last_ready('2',0))
        self.assertEquals([0.1], self._last_ready('1',1))

        self._add_object('2', 1, 300)

        self.assertEquals([0.1], self._last_ready('2', 1))

        self.system_time.advance()

        self.assertEquals([0.4], self._last_ready('2', 0))


# TODO(swang): Some helper functions to build object additions.
ObjectAddition = namedtuple('ObjectAddition', ['time_offset', 'object_id', 'node_id', 'object_size'])

class TestNodeRuntime(unittest.TestCase):

    class ObjectStore():
        def __init__(self, system_time, test):
            self._system_time = system_time
            self._test = test
            self._object_locations = defaultdict(set)

            self._awaiting_objects = defaultdict(list)

        def _install_object(self, object_id, node_id):
            self._object_locations[str(object_id)].add(str(node_id))
            for handler in self._awaiting_objects[(str(object_id), str(node_id))]:
                self._system_time.schedule_immediate(handler)

        def add_object(self, object_id, node_id, object_size):
            self._test.objects_added.append(
                    ObjectAddition(
                        self._system_time.get_time(),
                        object_id, node_id, object_size
                        )
                    )

        def get_locations(self, object_id):
            raise NotImplementedError()

        def get_updates(self, node_id, update_handler):
            pass
            #raise NotImplementedError()

        def is_local(self, object_id, node_id):
            return str(node_id) in self._object_locations[str(object_id)]

        def require_object(self, object_id, node_id, on_done):
            if self.is_local(object_id, node_id):
                self._system_time.schedule_immediate(on_done)
            else:
                self._awaiting_objects[(str(object_id), str(node_id))].append(on_done)

    class RecordingLogger():
        def __init__(self, system_time):
#            self._test = test
            self._system_time = system_time
            #self._task_timing = {}
            #for t in task_timing:
            #    self._task_timing[t.task_id] = t
            #self._timed_tasks = set()

        def task_started(self, task_id, node_id):
            pass
            #self._test.assertAlmostEqual(self._task_timing[task_id].start_timestamp, self._system_time.get_time())

        def task_finished(self, task_id, node_id):
            pass
            #self._test.assertAlmostEqual(self._task_timing[task_id].end_timestamp, self._system_time.get_time())
            #self._timed_tasks.add(task_id)


    def setUp(self):
        logging_format = '%(timestamp).6f %(name)s %(message)s'
        logging.basicConfig(format=logging_format)
        logging.getLogger().setLevel(logging.DEBUG)
#        FORMAT = '%(asctime)-15s %(clientip)s %(user)-8s %(message)s'


        self.system_time = SystemTime()
        self.object_store = self.ObjectStore(self.system_time, self)
        self.logger = self.RecordingLogger(self.system_time)

        self.updates = []
        self.objects_added = []

        self.free_workers = []

    def _setup_node_runtime(self, computation, node_id, num_workers):
        self.node_id = node_id
        self.num_workers = num_workers
        self.node_runtime = replaystate.NodeRuntime(self.system_time, self.object_store, self.logger, computation, node_id, num_workers)
        self.node_runtime.get_updates(lambda update: self._update_handler(update))

    def _update_handler(self, update):
        self.updates.append((self.system_time.get_time(), update))

    def _advance(self):
        while self.system_time.advance():
            pass

    def _get_workers(self):
        self.free_workers.append((self.system_time.get_time(), self.node_runtime.free_workers()))

    def test_one_task(self):
        phase_0_0 = TaskPhase(phase_id = 0, depends_on = [], submits = [], duration = 1.0)
        task_0_result = TaskResult(object_id = 0, size = 100)
        task_0 = Task(task_id = 1, phases = [phase_0_0], results = [task_0_result])
        computation = ComputationDescription(root_task = 1, tasks = [task_0])
        node_id = 1
        num_workers = 2

        self._setup_node_runtime(computation, node_id, num_workers)

        self.assertEquals(num_workers, self.node_runtime.free_workers())

        self.node_runtime.send_to_dispatcher(task_0, 0)

        self.system_time.schedule_delayed(0.5, lambda: self._get_workers())

        self._advance()

        self.assertItemsEqual([(0.5, 1)], self.free_workers)
        self.assertItemsEqual([(1.0, FinishTaskUpdate(task_0.id()))], self.updates)
        task_0_result_addition = ObjectAddition(
                phase_0_0.duration,
                task_0_result.object_id,
                node_id,
                task_0_result.size)
        self.assertItemsEqual([task_0_result_addition], self.objects_added)

        self.updates = []
        self._advance()

        self.assertItemsEqual([], self.updates)

    def test_put(self):
        put_event = ObjectPut(0, 100, 0.5)
        phase_0_0 = TaskPhase(phase_id = 0, depends_on = [], submits = [],
                              duration = 1.0, creates=[put_event])
        task_0_result = TaskResult(object_id = 1, size = 200)
        task_0 = Task(task_id = 1, phases = [phase_0_0], results = [task_0_result])
        computation = ComputationDescription(root_task = 1, tasks = [task_0])
        node_id = 1
        num_workers = 2

        self._setup_node_runtime(computation, node_id, num_workers)

        self.assertEquals(num_workers, self.node_runtime.free_workers())

        self.node_runtime.send_to_dispatcher(task_0, 0)

        self.system_time.schedule_delayed(0.5, lambda: self._get_workers())

        self._advance()

        self.assertItemsEqual([(0.5, 1)], self.free_workers)
        self.assertItemsEqual([(1.0, FinishTaskUpdate(task_0.id()))], self.updates)
        put_addition = ObjectAddition(
                put_event.time_offset,
                put_event.object_id,
                node_id,
                put_event.size
                )
        task_0_result_addition = ObjectAddition(
                phase_0_0.duration,
                task_0_result.object_id,
                node_id,
                task_0_result.size)
        self.assertItemsEqual([put_addition, task_0_result_addition],
                              self.objects_added)

        self.updates = []
        self._advance()

        self.assertItemsEqual([], self.updates)

    def test_put_with_phases(self):
        put_event1 = ObjectPut(0, 100, 0.5)
        put_event2 = ObjectPut(1, 200, 0.5)
        phase_0_0 = TaskPhase(phase_id = 0, depends_on = [], submits = [],
                              duration = 1.0, creates=[put_event1])
        phase_0_1 = TaskPhase(phase_id = 1, depends_on = [], submits = [],
                              duration = 1.5, creates=[put_event2])
        task_0 = Task(task_id = 1, phases = [phase_0_0, phase_0_1], results = [TaskResult(object_id = 2, size = 300)])
        computation = ComputationDescription(root_task = 1, tasks = [task_0])
        node_id = 1
        num_workers = 2

        self._setup_node_runtime(computation, node_id, num_workers)

        self.assertEquals(num_workers, self.node_runtime.free_workers())

        self.node_runtime.send_to_dispatcher(task_0, 0)

        self.system_time.schedule_delayed(2.0, lambda: self._get_workers())

        self._advance()

        self.assertItemsEqual([(2.0, 1)], self.free_workers)
        self.assertItemsEqual([(2.5, FinishTaskUpdate(task_0.id()))], self.updates)
        self.assertItemsEqual([ObjectAddition(0.5, '0', 1, 100),
                               ObjectAddition(1.5, '1', 1, 200),
                               ObjectAddition(2.5, '2', 1, 300)],
                              self.objects_added)

        self.updates = []
        self._advance()

        self.assertItemsEqual([], self.updates)

    def test_one_task_with_phases(self):
        phase_0_0 = TaskPhase(phase_id = 0, depends_on = [], submits = [], duration = 1.0)
        phase_0_1 = TaskPhase(phase_id = 1, depends_on = [], submits = [], duration = 1.5)
        task_0 = Task(task_id = 1, phases = [phase_0_0, phase_0_1], results = [TaskResult(object_id = 0, size = 100)])
        computation = ComputationDescription(root_task = 1, tasks = [task_0])
        node_id = 1
        num_workers = 2

        self._setup_node_runtime(computation, node_id, num_workers)

        self.assertEquals(num_workers, self.node_runtime.free_workers())

        self.node_runtime.send_to_dispatcher(task_0, 0)

        self.system_time.schedule_delayed(2.0, lambda: self._get_workers())

        self._advance()

        self.assertItemsEqual([(2.0, 1)], self.free_workers)
        self.assertItemsEqual([(2.5, FinishTaskUpdate(task_0.id()))], self.updates)
        self.assertItemsEqual([ObjectAddition(2.5, '0', 1, 100)],
                              self.objects_added)

        self.updates = []
        self._advance()

        self.assertItemsEqual([], self.updates)

    def test_two_results(self):
        phase_0_0 = TaskPhase(phase_id = 0, depends_on = [], submits = [], duration = 1.0)
        task_0 = Task(task_id = 1, phases = [phase_0_0], results = [TaskResult(object_id = 0, size = 100), TaskResult(object_id = 1, size = 200)])
        computation = ComputationDescription(root_task = 1, tasks = [task_0])
        node_id = 1
        num_workers = 1

        self._setup_node_runtime(computation, node_id, num_workers)

        self.node_runtime.send_to_dispatcher(task_0, 0)

        self._advance()

        self.assertItemsEqual([(1.0, FinishTaskUpdate(task_0.id()))], self.updates)
        self.assertItemsEqual([ObjectAddition(1.0, '0', 1, 100),
                               ObjectAddition(1.0, '1', 1, 200)],
                              self.objects_added)

        self.updates = []
        self.objects_added = []
        self._advance()

        self.assertItemsEqual([], self.updates)
        self.assertItemsEqual([], self.objects_added)

    def test_task_submit(self):
        phase_0_0 = TaskPhase(phase_id = 0, depends_on = [], submits = [], duration = 1.0)
        phase_0_1 = TaskPhase(phase_id = 1, depends_on = [], submits = [TaskSubmit(task_id = 2,time_offset = 0.4)], duration = 1.5)
        task_0 = Task(task_id = 1, phases = [phase_0_0, phase_0_1], results = [TaskResult(object_id = 0, size = 100)])

        phase_1_0 = TaskPhase(phase_id = 0, depends_on = [], submits = [], duration = 1.2)
        task_1 = Task(task_id = 2, phases = [phase_1_0], results = [TaskResult(object_id = 1, size = 100)])

        computation = ComputationDescription(root_task = 1, tasks = [task_0, task_1])
        node_id = 1
        num_workers = 2

        self._setup_node_runtime(computation, node_id, num_workers)

        self.assertEquals(num_workers, self.node_runtime.free_workers())

        self.node_runtime.send_to_dispatcher(task_0, 0)

        self._advance()

        self.assertItemsEqual([(1.4, SubmitTaskUpdate(task_1)), (2.5, FinishTaskUpdate(task_0.id()))], self.updates)
        self.assertItemsEqual([ObjectAddition(2.5, '0', 1, 100)],
                              self.objects_added)

        self.updates = []
        self.objects_added = []
        self._advance()

        self.assertItemsEqual([], self.updates)
        self.assertItemsEqual([], self.objects_added)

        self.node_runtime.send_to_dispatcher(task_1, 0)

        self._advance()

        self.assertItemsEqual([(3.7, FinishTaskUpdate(task_1.id()))], self.updates)
        self.assertItemsEqual([ObjectAddition(3.7, '1', 1, 100)],
                              self.objects_added)

    def test_priorities(self):
        phase_0_0 = TaskPhase(phase_id = 0, depends_on = [], submits = [TaskSubmit(task_id = 2,time_offset = 0.4), TaskSubmit(task_id = 3,time_offset = 0.4)], duration = 1.0)
        task_0 = Task(task_id = 1, phases = [phase_0_0], results = [TaskResult(object_id = 0, size = 100)])
        phase_1_0 = TaskPhase(phase_id = 0, depends_on = [], submits = [], duration = 1.5)
        task_1 = Task(task_id = 2, phases = [phase_1_0], results = [TaskResult(object_id = 1, size = 100)])
        phase_2_0 = TaskPhase(phase_id = 0, depends_on = [], submits = [], duration = 1.5)
        task_2 = Task(task_id = 3, phases = [phase_2_0], results = [TaskResult(object_id = 2, size = 100)])

        computation = ComputationDescription(root_task = 1, tasks = [task_0, task_1, task_2])
        node_id = 1
        num_workers = 1

        self._setup_node_runtime(computation, node_id, num_workers)

        self.node_runtime.send_to_dispatcher(task_1, 0)
        self.node_runtime.send_to_dispatcher(task_2, 0)
        self._advance()
        self.assertItemsEqual([(1.5, FinishTaskUpdate(task_1.id())), (3.0, FinishTaskUpdate(task_2.id()))], self.updates)

        self.updates = []
        self.node_runtime.send_to_dispatcher(task_2, 0)
        self.node_runtime.send_to_dispatcher(task_1, 0)
        self._advance()
        self.assertItemsEqual([(4.5, FinishTaskUpdate(task_2.id())), (6.0, FinishTaskUpdate(task_1.id()))], self.updates)

        self.updates = []
        self.node_runtime.send_to_dispatcher(task_2, 1)
        self.node_runtime.send_to_dispatcher(task_1, 0)
        self._advance()
        self.assertItemsEqual([(7.5, FinishTaskUpdate(task_1.id())), (9.0, FinishTaskUpdate(task_2.id()))], self.updates)

    def test_single_dependency(self):
        phase_0_0 = TaskPhase(phase_id = 0, depends_on = [], submits = [TaskSubmit(task_id = 2,time_offset = 0.4)], duration = 0.9)
        phase_0_1 = TaskPhase(phase_id = 1, depends_on = [1], submits = [], duration = 0.8)
        task_0 = Task(task_id = 1, phases = [phase_0_0, phase_0_1], results = [TaskResult(object_id = 0, size = 100)])

        phase_1_0 = TaskPhase(phase_id = 0, depends_on = [], submits = [], duration = 1.5)
        task_1 = Task(task_id = 2, phases = [phase_1_0], results = [TaskResult(object_id = 1, size = 100)])

        computation = ComputationDescription(root_task = 1, tasks = [task_0, task_1])
        node_id = 1
        num_workers = 2

        self._setup_node_runtime(computation, node_id, num_workers)
        self.node_runtime.send_to_dispatcher(task_0, 0)
        self._advance()

        self.assertItemsEqual([(0.4, SubmitTaskUpdate(task_1))], self.updates)
        self.assertEquals(0.9, self.system_time.get_time())

        self.updates = []
        self.node_runtime.send_to_dispatcher(task_1, 0)
        self._advance()
        self.assertItemsEqual([(2.4, FinishTaskUpdate(task_1.id()))], self.updates)
        self.assertEquals(2.4, self.system_time.get_time())

        self.updates = []
        self.object_store._install_object(1, node_id)
        self._advance()
        self.assertItemsEqual([(3.2, FinishTaskUpdate(task_0.id()))], self.updates)

    def test_multiple_dependencies(self):
        phase_0_0 = TaskPhase(phase_id = 0, depends_on = [], submits = [TaskSubmit(task_id = 2,time_offset = 0.4), TaskSubmit(task_id = 3,time_offset = 0.4)], duration = 0.9)
        phase_0_1 = TaskPhase(phase_id = 1, depends_on = [1, 2], submits = [], duration = 0.8)
        task_0 = Task(task_id = 1, phases = [phase_0_0, phase_0_1], results = [TaskResult(object_id = 0, size = 100)])

        phase_1_0 = TaskPhase(phase_id = 0, depends_on = [], submits = [], duration = 2.5)
        task_1 = Task(task_id = 2, phases = [phase_1_0], results = [TaskResult(object_id = 1, size = 100)])

        phase_2_0 = TaskPhase(phase_id = 0, depends_on = [], submits = [], duration = 0.5)
        task_2 = Task(task_id = 3, phases = [phase_2_0], results = [TaskResult(object_id = 2, size = 500)])

        computation = ComputationDescription(root_task = 1, tasks = [task_0, task_1, task_2])
        node_id = 1
        num_workers = 3

        self._setup_node_runtime(computation, node_id, num_workers)
        self.node_runtime.send_to_dispatcher(task_0, 0)
        self._advance()

        self.assertItemsEqual([(0.4, SubmitTaskUpdate(task_1)), (0.4, SubmitTaskUpdate(task_2))], self.updates)
        self.assertEquals(0.9, self.system_time.get_time())

        self.updates = []
        self.node_runtime.send_to_dispatcher(task_1, 0)
        self.node_runtime.send_to_dispatcher(task_2, 0)
        self._advance()
        self.assertItemsEqual([(3.4, FinishTaskUpdate(task_1.id())), (1.4, FinishTaskUpdate(task_2.id()))], self.updates)
        self.assertEquals(3.4, self.system_time.get_time())

        self.updates = []
        self.object_store._install_object(1, node_id)
        self.object_store._install_object(2, node_id)
        self._advance()
        self.assertItemsEqual([(4.2, FinishTaskUpdate(task_0.id()))], self.updates)


if __name__ == '__main__':
    # If the user wants to use the standard unittest runner, just run it and exit.
    import sys
    if len(sys.argv) >= 2:
        unittest.main()
        sys.exit(0)
    # Else, run all of the tests, generated by the JSON files in ./traces.
    tests = unittest.TestSuite([invalid_trace_suite(), valid_trace_suite()] +
                list(map(lambda x: unittest.TestLoader().loadTestsFromTestCase(x),
                [TestEventLoopTimers, TestComputationObjects, TestSchedulerObjects,
                 TestReplayState, TestObjectStoreRuntime, TestNodeRuntime,
                 TestReplayStateTimingDetail])))
    unittest.TextTestRunner(verbosity=2).run(tests)
    
