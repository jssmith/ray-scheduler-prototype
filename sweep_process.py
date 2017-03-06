import os
import sys
import gzip
import time
import copy
import boto.sqs
import socket
import json
import traceback
import ec2config
import gitrev

from boto.sqs.message import Message
from subprocess import call, Popen, PIPE
from ec2config import s3_sync_file, s3_cp

def replay_trace(config):
    start_time = time.time()
    replay_id = config['replay_id']
    sweep_dir = 'sweep'
    print "starting replay", replay_id

    # copy the needed trace to the local instance
    tracefile = str(config['tracefile'])
    while tracefile.startswith('/'):
        tracefile = tracefile[1:]
    local_tracefile = sweep_dir + '/' + tracefile
    s3_sync_file(ec2config.s3_bucket + '/' + tracefile, local_tracefile)

    env = os.environ.copy()
    if 'env' in config and config['env']:
        for name, val in config['env'].items():
            env[name] = str(val)

    # execute the simulator
    sim_log_fn = 'sweep/sim_events.gz'
    if os.path.isfile(sim_log_fn):
        os.remove(sim_log_fn)

    print "executing replaytrace.py"
    proc = Popen(['python', 'replaytrace.py',
            str(config['num_nodes']),
            str(config['num_workers_per_node']),
            '{:f}'.format(config['object_transfer_time_cost']),
            '{:f}'.format(config['db_message_delay']),
            str(config['scheduler']),
            str(config['cache_policy']),
            str(config['validate']),
            local_tracefile
        ], stdout=PIPE, stderr=PIPE, env=env)
    (stdoutdata, stderrdata) = proc.communicate()
    returncode = proc.returncode
    print "replaytrace.py finished with return code {}".format(returncode)

    experiment_dir = sweep_dir + '/' + config['experiment_name']
    if not os.path.exists(experiment_dir):
        os.makedirs(experiment_dir)

    stdout_name = replay_id + '_stdout.gz'
    stderr_name = replay_id + '_stderr.gz'
    stdout_fn = experiment_dir + '/' + stdout_name
    stderr_fn = experiment_dir + '/' + stderr_name
    write_output(stdout_fn, stdoutdata)
    write_output(stderr_fn, stderrdata)
    s3_cp(stdout_fn, ec2config.s3_bucket + '/' + experiment_dir + '/' + stdout_name)
    s3_cp(stderr_fn, ec2config.s3_bucket + '/' + experiment_dir + '/' + stderr_name)

    log_name = replay_id + '_event_log.gz'
    s3_cp(sim_log_fn, ec2config.s3_bucket + '/' + experiment_dir + '/' + log_name)

    stats_name = replay_id + '_stats.json'
    call(['python', 'analyze_basic_json.py', sim_log_fn, 'sweep/' + stats_name])
    s3_cp('sweep/' + stats_name, ec2config.s3_bucket + '/' + experiment_dir + '/' + stats_name)

    end_time = time.time()

    print "finished replay {} in {:.3f}".format(replay_id, end_time - start_time)

    config_etc = copy.copy(config)

    config_etc['end_time'] = end_time
    config_etc['returncode'] = returncode
    config_etc['stdout_fn'] = stdout_name
    config_etc['stdout_fn'] = stderr_name
    config_etc['log_fn'] = log_name
    config_etc['is-start'] = False

    return config_etc

def write_output(fn, text):
    with gzip.open(fn, 'wb') as f:
        f.write(text)

def sweep_process(sleep_time, iteration_limit=None):
    conn = ec2config.sqs_connect()
    queue = conn.get_queue(ec2config.sqs_sweep_queue)

    def do_iter():
        try:
            m = queue.read()
            if m is None:
                print "waiting for more input"
                time.sleep(sleep_time)
            else:
                queue.delete_message(m)
                config = json.loads(m.get_body())
                print config

                replay_id = config['replay_id']
                config_start = copy.copy(config)
                config_start['hostname'] = socket.gethostname()
                config_start['gitrev'] = gitrev.get_rev()
                config_start['start_time'] = time.time()
                config_start['is-start'] = True
                sdb_conn = ec2config.sdb_connect()
                dom = sdb_conn.get_domain(ec2config.sdb_sweep_domain)
                dom.put_attributes(replay_id + '-start', config)
                sdb_conn.close()

                config_etc = replay_trace(config_start)

                if 'env' in config_etc:
                    config_etc['env'] = json.dumps(config_etc['env'])

                sdb_conn = ec2config.sdb_connect()
                dom = sdb_conn.get_domain(ec2config.sdb_sweep_domain)
                dom.put_attributes(replay_id, config_etc)
                sdb_conn.close()


        except ValueError as err:
            print err
            traceback.print_exc()
        except RuntimeError as err:
            print err
            traceback.print_exc()
        # except:
        #     print "Unexpected error", sys.exc_info()[0]
    if iteration_limit is None:
        while True:
            do_iter()
    else:
        for _ in range(iteration_limit):
            do_iter()


if __name__ == '__main__':
    if len(sys.argv) == 2:
        sleep_time = int(sys.argv[1])
        print 'setting sleep time to {}'.format(sleep_time)
    else:
        sleep_time = 30
    sweep_process(sleep_time)
