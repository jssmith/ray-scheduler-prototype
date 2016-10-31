
import sys
import json
import copy

#********** Data Structure (from original ray scheduling prototype)*************************

class Task():
    def __init__(self, task_id, phases, results):
        task_id_str = str(task_id)
        if not task_id_str:
            raise ValidationError('Task: no id provided')
        if not len(phases):
            raise ValidationError('Task: no phases')
        for idx, phase in enumerate(phases):
            if phase.phase_id != idx:
                raise ValidationError('Task: mismatched phase id')
        # TODO(swang): These lines are not a valid check for the driver
        # task.
        #if not len(results):
        #    raise ValidationError('Task: no results')

        # verification passed so initialize
        self._task_id = task_id_str
        self._phases = phases
        self._results = results

    def __str__(self):
        return "task_id={}".format(self._task_id)	
		
    def id(self):
        return self._task_id

    def get_depends_on(self):
        return self._phases[0].get_depends_on()

    def get_phase(self, phase_id):
        return self._phases[phase_id]

    def num_phases(self):
        return len(self._phases)

    def get_results(self):
        return self._results
		
		
class TaskPhase():
    def __init__(self, phase_id, depends_on, schedules, duration):
        for s in schedules:
            if s.time_offset > duration:
                raise ValidationError('TaskPhase: schedules beyond phase duration')

        # verification passed so initialize
        self.phase_id = phase_id
        self._depends_on = map(lambda x: str(x), depends_on)
        self._schedules = schedules
        self.duration = duration

    def get_depends_on(self):
        return self._depends_on

    def get_schedules(self):
        return self._schedules


class TaskResult():
    def __init__(self, object_id, size):
        object_id_str = str(object_id)
        if not object_id_str:
            raise ValidationError('TaskResult: no object id')
        if size < 0:
            raise ValidationError('TaskResult: invalid size - {}'.format(size))

        # verification passed so initialize
        self.object_id = object_id_str
        self.size = size

class TaskSubmit():
    def __init__(self, task_id, time_offset):
        task_id_str = str(task_id)
        if not task_id_str:
            raise ValidationError('TaskSubmit: no task id')

        # verification passed so initialize
        self.task_id = task_id_str
        self.time_offset = time_offset		
		
		
class ComputationDescription():
    def __init__(self, root_task, tasks):
        if root_task is None:
            if len(tasks) != 0:
                raise ValidationError('Too many tasks are called')
            else:
                self._root_task = None
                self._tasks = {}
                return

        root_task_str = str(root_task)
        # task ids must be unique
        task_ids = map(lambda x: x.id(), tasks)
        task_ids_set = frozenset(task_ids)
        if len(task_ids_set) != len(task_ids):
            raise ValidationError('Task ids must be unique')

        # all tasks should be called exactly once
        called_tasks = set(root_task_str)
        for task in tasks:
            for phase_id in range(0, task.num_phases()):
                for task_id in map(lambda x: x.task_id, task.get_phase(phase_id).get_schedules()):
                    if task_id in called_tasks:
                        raise ValidationError('Duplicate call to task {}'.format(task_id))
                    if task_id not in task_ids_set:
                        raise ValidationError('Call to undefined task {}'.format(task_id))
                    called_tasks.add(task_id)
        if len(called_tasks) < len(task_ids):
            tasks_not_called = task_ids_set.difference(called_tasks)
            raise ValidationError('Some tasks are not called: {}'.format(str(tasks_not_called)))
        if len(called_tasks) > len(task_ids):
            raise ValidationError('Too many tasks are called')

        # no dependencies that don't get created
        result_objects = set()
        for task in tasks:
            for task_result in task.get_results():
                object_id = task_result.object_id
                if object_id in result_objects:
                    raise ValidationError('Duplicate result object id {}'.format(object_id))
                result_objects.add(object_id)
        for task in tasks:
            for phase_id in range(0, task.num_phases()):
                for object_id in task.get_phase(phase_id).get_depends_on():
                    if object_id not in result_objects:
                        raise ValidationError('Dependency on missing object id {}'.format(object_id))

        # no cycles, everything reachable from roots
        dg = DirectedGraph()
        tasks_map = {}
        for task in tasks:
            tasks_map[task.id()] = task
        for task in tasks:
            prev_phase = None
            for phase_id in range(0, task.num_phases()):
                phase = task.get_phase(phase_id)
                if prev_phase:
                    #print "EDGE: previous phase edge"
                    dg.add_edge(prev_phase, phase)
                for object_id in phase.get_depends_on():
                    #print "EDGE: phase dependency edge"
                    dg.add_edge(object_id, phase)
                for schedules in phase.get_schedules():
                    #print "EDGE: phase schedules edge"
                    dg.add_edge(phase, tasks_map[schedules.task_id].get_phase(0))
                    # TODO object id produced in scheduling
                prev_phase = phase
            for task_result in task.get_results():
                #print "EDGE: task result edge"
                dg.add_edge(prev_phase, task_result.object_id)
        dg.verify_dag_root(tasks_map[root_task_str].get_phase(0))

        # verification passed so initialize
        self._root_task = root_task_str
        self._tasks = tasks_map

        self.data_transfer_time_cost = 0
		
    def get_root_task(self):
        return self._root_task

    def get_tasks(self):
        return self._tasks 	
		
    def get_task(self, task_id):
        return self._tasks[task_id]

    def get_total_num_phases(self):
        n = 0
        for task_id, task in self._tasks.items():
            n += task.num_phases()
        return n
 
    def update_data_transfer_time_cost(self, data_transfer_time_cost):
        self.data_transfer_time_cost = data_transfer_time_cost
		
class DirectedGraph():
    def __init__(self):
        self._id_ct = 0
        self._id_map = {}
        self._edges = []

    def _get_id(self, x):
        if x in self._id_map:
            #print 'found id for {}'.format(x)
            new_id = self._id_map[x]
        else:
            #print 'missing id for {}'.format(x)
            new_id = self._id_ct
            self._id_map[x] = new_id
            self._id_ct += 1
        #print 'id for {} is {}'.format(x, new_id)
        return new_id

    def add_edge(self, a, b):
        id_a = self._get_id(a)
        id_b = self._get_id(b)
        #print 'EDGE: {} => {}'.format(a, b)
        #print 'EDGE: {} -> {}'.format(id_a, id_b)
        self._edges.append((id_a, id_b))

    def verify_dag_root(self, root):
        # TODO(swang): What is the correct check here?
        return
        root_id = self._get_id(root)
        # check that
        #  1/ we have a DAG
        #  2/ all nodes reachable from the root
        # we do this by depth-first search
        visited = [False] * self._id_ct
        in_chain = [False] * self._id_ct
        edge_lists = dict(map(lambda (src_id, edges): (src_id, map(lambda x: x[1], edges)), itertools.groupby(self._edges, lambda x: x[0])))

        #print 'root: {}'.format(root_id)
        #print edge_lists

        def visit(x):
            #print 'visit {}'.format(x)
            if in_chain[x]:
                raise ValidationError('Cyclic dependencies')
            in_chain[x] = True
            if not visited[x]:
                visited[x] = True
                if x in edge_lists.keys():
                    for y in edge_lists[x]:
                        visit(y)
            in_chain[x] = False

        visit(root_id)
        if False in visited:
            raise ValidationError('Reachability from root')

			
			

def computation_decoder(dict):
    keys = frozenset(dict.keys())
    if keys == frozenset([u'timeOffset', 'taskId']):
        return TaskSubmit(dict[u'taskId'], dict[u'timeOffset'])
    if keys == frozenset([u'duration', u'phaseId', u'submits', u'dependsOn']):
        return TaskPhase(dict[u'phaseId'],dict[u'dependsOn'],dict[u'submits'],dict[u'duration'])
    if keys == frozenset([u'phases', u'results', u'taskId']):
        return Task(dict[u'taskId'], dict[u'phases'], dict[u'results'])
    if keys == frozenset([u'tasks', u'rootTask']):
        return ComputationDescription(dict[u'rootTask'], dict[u'tasks'])
    if keys == frozenset([u'objectId', u'size']):
        return TaskResult(dict[u'objectId'], int(dict[u'size']))
    else:
        print "unexpected map: {}".format(keys)
        sys.exit(1)


class ValidationError(Exception):
    def __init__(self, message):
        super(ValidationError, self).__init__(message)

		
class SystemTime():
    def __init__(self):
        self._t = 0
        self._scheduled = []

    def __str__(self):
        out = "Schedule is: \n"
        for (start,finish,data) in self._scheduled :
            out = out + " task {} from {} to {} \n".format(data, start, finish)
        return out
		
    def get_time(self):
        return self._t
    
    
    def get_lastest_task(self):
        (start, finish, data) = self._scheduled[-1]
        return data
	
    def schedule_start(self, t, data):
        if self._t > t:
            print 'invalid schedule request'
            sys.exit(1)
        #heapq.heappush(self._scheduled, (t, 'inf', data))
        self._scheduled.append((t, float('inf'), data))

    #def schedule_delayed(self, delta, data):
    #    self.schedule_at(self._t + delta, data)

    #def schedule_immediate(self, data):
    #    self.schedule_at(self._t, data)
	
    def schedule_finish(self, t):
        (start, finish, data) = self._scheduled.pop()
        self._scheduled.append((start, t, data))
        self._t = t
	
    def queue_empty(self):
        return not self._scheduled		


		
class Worker:
    def __init__(self, worker_id, node_id):
        self.worker_id = worker_id
        self.node_id = node_id
        #self._time_line = []    #use SystemTime instead of regular numbers list
        self.timeline = SystemTime()
        #self._ready_objects = []  #redundant. create a global map instead of this
        self.executing_phase = -1
	
    def __str__(self):
	    return "Worker id {} on Node id {}: \n {}".format(self.worker_id, self.node_id, self.timeline)
		
	
    def get_timeline(self):
        return self.timeline
		
	
    def is_executing(self):
        return not (self.executing_phase == -1)
		   
	
    def execute_task(self, computation, task_id, possible_schedule):
        #check if all phase of a task 
		
        task_start_time = max([possible_schedule.system_available_tasks[task_id], self.timeline.get_time()])   
        self.timeline.schedule_start(task_start_time, task_id)

        for k in range (0, computation.get_task(task_id).num_phases()):
            task_phase = computation.get_task(task_id).get_phase(k)
            
            data_transfer_time = 0
            remote_data_ready_time = 0
            data_ready_time = 0
		    
            for d_object_id in task_phase.get_depends_on():
                if not (d_object_id in possible_schedule.system_available_objects.keys()):
                    self.executing_phase = k
                    #print "DEBUG: task {} blocked in phase {} on worker {}".format(task_id,k,self.worker_id)
                    return False
                else:
                    dep_obj = possible_schedule.system_available_objects[d_object_id]
                    if not (self.node_id in [x[0] for x in dep_obj.node_id]):
                        remote_data_ready_time = min([x[1] for x in possible_schedule.system_available_objects[d_object_id].node_id]) 
                        data_transfer_time += dep_obj.size * computation.data_transfer_time_cost
                        possible_schedule.add_available_object(self.node_id, d_object_id, dep_obj.size, data_ready_time+data_transfer_time)
                        if (data_ready_time < remote_data_ready_time+data_transfer_time):
                            data_ready_time = remote_data_ready_time+data_transfer_time
            phase_start_time = max([self.timeline.get_time(),data_ready_time])
            for task_submit in task_phase.get_schedules():
                possible_schedule.add_available_task(task_submit.task_id, phase_start_time+task_submit.time_offset)
        
            self.timeline.schedule_finish(phase_start_time+task_phase.duration)
			
        for result in computation.get_task(task_id).get_results():
            possible_schedule.add_available_object(self.node_id, result.object_id, result.size, self.timeline.get_time())	
        self.executing_phase = -1	
        #print "DEBUG: task {} finished executing on worker {}".format(task_id, self.worker_id)		
        return True 			

			
    def check_finish_executing(self, computation, possible_schedule):
        #check if all phase of a task 
        task_id = self.timeline.get_lastest_task()
        #check if all phase of a task 
        for k in range(self.executing_phase, computation.get_task(task_id).num_phases()):
            task_phase = computation.get_task(task_id).get_phase(k)

            data_transfer_time = 0
            remote_data_ready_time = 0
            data_ready_time = 0
            for d_object_id in task_phase.get_depends_on():
                if not (d_object_id in possible_schedule.system_available_objects.keys()):
                    self.executing_phase = k
                    #print "DEBUG: task {} blocked in phase {} on worker {}".format(task_id,k,self.worker_id)
                    return False
                else:
                    dep_obj = possible_schedule.system_available_objects[d_object_id]
                    if not (self.node_id in [x[0] for x in dep_obj.node_id]):
                        remote_data_ready_time = min([x[1] for x in possible_schedule.system_available_objects[d_object_id].node_id]) 
                        data_transfer_time += dep_obj.size * computation.data_transfer_time_cost
                        possible_schedule.add_available_object(self.node_id, d_object_id, dep_obj.size, data_ready_time+data_transfer_time)
                        if (data_ready_time < remote_data_ready_time+data_transfer_time):
                            data_ready_time = remote_data_ready_time+data_transfer_time
            phase_start_time = max([self.timeline.get_time(),data_ready_time])
            for task_submit in task_phase.get_schedules():
                possible_schedule.add_available_task(task_submit.task_id, phase_start_time+task_submit.time_offset)
        
            self.timeline.schedule_finish(phase_start_time+task_phase.duration)

            			
        for result in computation.get_task(task_id).get_results():
            possible_schedule.add_available_object(self.node_id, result.object_id, result.size,self.timeline.get_time())	
        self.executing_phase = -1
        #print "DEBUG: task {} finished executing on worker {}".format(task_id, self.worker_id)
        return True		


class ObjectDescription():
    def __init__(self, node_id, size, time):
        self.node_id = [(node_id,time)]
        self.size = size

    def __str__(self):
        print "size {} on node {}".format(self.size, self.node_id)

		
class PossibleSchedule:
    def __init__(self):
        self.workers = []
        self.system_available_objects = {}
        self.system_available_tasks = {}
		
    def __str__(self):
        out = "Total Schedule is: \n"
        for worker in self.workers:
           out = out + "\n {} \n".format(worker)
        return out
	
    def add_worker(self, worker):
        self.workers.append(worker)
		
    def add_available_object(self, node_id, object_id, size,time):
        #print "DEBUG: entered add_available_object"
        if (object_id in self.system_available_objects):
            if not (node_id in [x[0] for x in self.system_available_objects[object_id].node_id]):
                self.system_available_objects[object_id].node_id.append((node_id,time))
        else:		
            self.system_available_objects[object_id] = ObjectDescription(node_id,size,time) #add the object to the system-wide ready list
        # since several nodes can have a copy of the object, the key of the dictionary is the object_id, and the value of the dictionary is
		# a list of workers in which the object is present
	
    def print_available_objects(self):
        print "available objects:"
        for obj_id, obj_des in self.system_available_objects.iteritems():
            print "object {} in node {}".format(obj_id, obj_des.node_id)
			
    def add_available_task(self, available_task, time):
        if not (available_task in self.system_available_tasks.keys()):
            self.system_available_tasks[available_task] = time
		
		
class OracleResult:
    def __init__(self):
		self.longest_time = float('inf')
		self.schedule = PossibleSchedule()
		
    def __str__(self):
        return "Total job completion time: {} \n Verbose Schedule: \n {}".format(self.longest_time, self.schedule)

	
#----------------------------------------Main Running part of the Simulation---------------------------------------------
		
def oracle(computation, num_nodes, num_workers_per_node, transfer_time_cost):
    computation.update_data_transfer_time_cost(transfer_time_cost)
    unscheduled_task_pool = computation.get_tasks().keys()
    root_schedule = PossibleSchedule()
    root_schedule.add_available_task(computation.get_root_task(),  0)
    result = OracleResult()
    #create workers
    worker_id = 0
    for node_id in range(num_nodes):
        for worker in range(num_workers_per_node):
            root_schedule.add_worker(Worker(worker_id,node_id))
            worker_id=worker_id+1
    #print "DEBUG: Initial unscheduled task pool is {}".format(unscheduled_task_pool)
    oracle_internal(root_schedule, unscheduled_task_pool, computation, result)
    print "The Optimal Schedule is: \n {}".format(result)

		
def oracle_internal(incoming_schedule, unscheduled_task_pool, computation, result):
    #print "DEBUG: oracle_internal called. incoming_unscheduled_task_pool is: {}".format(unscheduled_task_pool)
    #-------------------recursion limit\boundry condition-----------------------------
    if not unscheduled_task_pool:

        #if one of the tasks hasn't finished excuting, it means it took infinite time
        for worker in incoming_schedule.workers:
            if worker.is_executing():
                if not worker.check_finish_executing(computation, incoming_schedule):
                    worker.timeline.schedule_finish('inf')
	
        max_worker_time = 0
        for worker in incoming_schedule.workers:
            #print "DEBUG: worker {} time after finished schedule: {}".format(worker.worker_id,worker.timeline.get_time())  
            if worker.timeline.get_time() > max_worker_time:
        	    max_worker_time = worker.timeline.get_time()
        if max_worker_time < result.longest_time:
            result.schedule = incoming_schedule
            result.longest_time = max_worker_time
        #print "DEBUG: finished checking potential schedule"
        return

		
    #-------------------if we are not at the base of the recursion-------------		
    #try to schedule a task---------------------------
    previously_attempted = []
    original_unscheduled_task_pool = copy.deepcopy(unscheduled_task_pool)
    original_incoming_schedule = copy.deepcopy(incoming_schedule)
    while unscheduled_task_pool: #while the unscheduled task pool is not empty   
        #print "DEBUG: Entered WHILE loop. unscheduled_task_pool is: {}".format(unscheduled_task_pool)
        task_id = unscheduled_task_pool.pop()
        task = computation.get_task(task_id)
        #print "DEBUG: Selected task to be scheduled is task: {}".format(task)
        #verify there is no deadlock---------------------------------
        live_worker = False
        for worker in incoming_schedule.workers:
            if not worker.is_executing():
                live_worker = True
        if not live_worker:	
            #print "DEBUG: Deadlock Found"
            break # deadlock
        	

        #print "DEBUG"
        #incoming_schedule.print_available_objects()


        #try to schedule the task in different workers
        for worker_it in incoming_schedule.workers:
            #-----------------------------bound conditions-------------------------------------------

            outgoing_schedule = copy.deepcopy(original_incoming_schedule)
            worker = filter(lambda x: x.worker_id == worker_it.worker_id, outgoing_schedule.workers)[0]
            #print "DEBUG: working with worker {}".format(worker.worker_id)
			
            #check the the task can be scheduled (that it is not scheduled before it was submitted)
            #not supported in the current json model
            if not (task_id in outgoing_schedule.system_available_tasks.keys()):
                #print "DEBUG: skip because task {} hasn't been submitted yet".format(task_id)
                continue    
            
            #check if the worker is in the process of executing a previous task, but still does not have all the required dependancies
            if worker.is_executing():
                if not worker.check_finish_executing(computation, outgoing_schedule):
                    #print "DEBUG: skip because worker {} is still executing a blocked task".format(worker.worker_id)
                    continue
            
            #check if dependencies were already scheduled (dont break. put the task back in the unscheduled taks pool and wait for them to be scheduled)
            dependancy_flag = False
            for dependancy in task.get_depends_on():
                if not (dependancy in outgoing_schedule.system_available_objects.keys()): #if the object is not ready yet in the system
                    dependancy_flag = True
                    #dependancy_val = dependancy				
                    #print "dependancy_val is {}".format(dependancy_val)				
            
            if (dependancy_flag):
                if (task_id in previously_attempted):
                    break
                else:
                    #unscheduled_task_pool.insert(0, task_id)
                    previously_attempted.append(task_id)	
                    continue
            
			
            #soft bound condition: check if we the sum of durations is already greater than the latest latency timer
            if worker.timeline.get_time() > result.longest_time:
                #print "DEBUG: break because the worker time of {} is longer that the current best time of {}".format(worker.timeline.get_time(),result.longest_time)
                break
            
                
            #-----------------------------branch action------------------------------------------
            #recursion to create the branch part of the branch&bound
            #print "DEBUG: Executing task {} on worker {}".format(task_id, worker.worker_id)
            worker.execute_task(computation, task_id, outgoing_schedule)
            
            outgoing_unscheduled_task_pool = copy.deepcopy(original_unscheduled_task_pool)
            outgoing_unscheduled_task_pool.remove(task_id)
            #print "DEBUG: calling recursive call"
            oracle_internal(outgoing_schedule, outgoing_unscheduled_task_pool, computation, result)
				
				
	


def usage():
    print 'Usage: oracle_scheduler num_nodes num_workers_per_node transfer_time_cost input.json'

def run_replay(args):
    if len(args) != 5:
        usage()
        sys.exit(1)

    num_nodes = int(args[1])
    num_workers_per_node = int(args[2])
    transfer_time_cost = float(args[3])
    input_fn = args[4]
    print input_fn
    f = open(input_fn, 'r')
    computation = json.load(f, object_hook=computation_decoder)
    f.close()

    # TODO: redo this to fit********************
    oracle(computation, num_nodes, num_workers_per_node, transfer_time_cost)
    # ******************************************
	
	
if __name__ == '__main__':
    run_replay(sys.argv)
