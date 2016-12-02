import sys
import gzip

from replaystate import *
from combine_traces import serialize_computation

def block_object_size(block_dim):
    return block_dim * block_dim * 8

def block_gen_time(block_dim):
    return block_dim * block_dim *  (0.07/(2500*2500)) # random matrix

def block_add_time(block_dim):
    return block_dim * block_dim * (0.025/(2500*2500))

def block_mul_time(block_dim):
    return block_dim * block_dim * block_dim * (0.5/(2500*2500*2500))

def gen_multiply_trace(num_blocks, block_dim):

    counters = [0, 0]
    def next_task_id():
        counters[0] += 1
        return 't' + str(counters[0])

    def next_object_id():
        counters[1] += 1
        return 'o' + str(counters[1])

    root_task_id = next_task_id()

    def res_object(task_id):
        return task_id.get_results()[0].object_id

    def get_gen_task():
        phase0 = TaskPhase(phase_id=0,
            depends_on=[],
            submits=[],
            duration=block_gen_time(block_dim))
        result = TaskResult(next_object_id(), block_object_size(block_dim))
        return Task(next_task_id(), [phase0], [result])

    gen_tasks_a = {}
    for i in range(num_blocks):
        for j in range(num_blocks):
            gen_tasks_a[(i,j)] = get_gen_task()

    gen_tasks_b = {}
    for i in range(num_blocks):
        for j in range(num_blocks):
            gen_tasks_b[(i,j)] = get_gen_task()

    def get_mult_task(i, j, k):
        phase0 = TaskPhase(phase_id=0,
            depends_on=[res_object(gen_tasks_a[(i,k)]), res_object(gen_tasks_b[(j,k)])],
            submits=[],
            duration=block_mul_time(block_dim))
        result = TaskResult(next_object_id(), block_object_size(block_dim))
        return Task(next_task_id(), [phase0], [result])

    mult_tasks = {}
    for i in range(num_blocks):
        for j in range(num_blocks):
            for k in range(num_blocks):
                mult_tasks[(i,j,k)] = get_mult_task(i, j, k)

    add_all_tasks = []
    def reduce_add(input_objects):
        if len(input_objects) == 1:
            return input_objects
        tasks = []
        for i in range(len(input_objects)/2):
            phase0 = TaskPhase(phase_id=0,
                depends_on=[res_object(input_objects[i]), res_object(input_objects[i*2+1])],
                submits=[],
                duration=block_add_time(block_dim))
            result = TaskResult(next_object_id(), block_object_size(block_dim))
            task = Task(next_task_id(), [phase0], [result])
            add_all_tasks.append(task)
            tasks.append(task)
        if len(input_objects) % 2:
            tasks.append(input_objects[-1])
        return reduce_add(tasks)

    add_final_tasks = {}
    for i in range(num_blocks):
        for j in range(num_blocks):
            add_tasks = []
            for k in range(num_blocks):
                add_tasks.append(mult_tasks[(i, j, k)])
            add_final_tasks[(i,j)] = reduce_add(add_tasks)[0]

    root_phase0_tasks_submitted = []
    root_phase0_tasks_submitted += gen_tasks_a.values()
    root_phase0_tasks_submitted += gen_tasks_b.values()
    root_phase0_tasks_submitted += mult_tasks.values()
    root_phase0_tasks_submitted += add_all_tasks

    root_phase0_submitted = []
    time_offset = 0
    time_offset_delta = .000001
    for task in root_phase0_tasks_submitted:
        root_phase0_submitted.append(TaskSubmit(task.id(), time_offset))
        time_offset += time_offset_delta
    root_phase0 = TaskPhase(phase_id=0,
        depends_on=[],
        submits=root_phase0_submitted,
        duration=time_offset,)
    root_phase_1 = TaskPhase(phase_id=1,
        depends_on=map(lambda x: res_object(x), add_final_tasks.values()),
        submits=[],
        duration=0.001)
    root_task = Task(root_task_id, [root_phase0, root_phase_1], [])
    all_tasks = [root_task]
    all_tasks += root_phase0_tasks_submitted
    return ComputationDescription(root_task_id, all_tasks)

def usage():
    print "Usage: synmultiply.py num_blocks block_dim"

if __name__ == '__main__':
    if len(sys.argv) != 3:
        usage()
        sys.exit(1)
    num_blocks = int(sys.argv[1])
    block_dim = int(sys.argv[2])
    computation = gen_multiply_trace(num_blocks, block_dim)
    output_filename = 'traces/sweep/trace_syn_mat_mul_{}_{}.json.gz'.format(num_blocks, block_dim)
    print 'output to {}'.format(output_filename)
    with gzip.open(output_filename, 'w') as f:
        f.write(serialize_computation(computation))
