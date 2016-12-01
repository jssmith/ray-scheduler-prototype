import os
import sys
import gzip
import time
import copy
import boto.sqs
import socket
import json
import uuid
import hashlib
import ec2config

from boto.sqs.message import Message
from subprocess import call, Popen, PIPE

def replay_trace(config):
    start_time = time.time()
    replay_id = new_id()
    sweep_dir = 'sweep'
    print "starting replay", replay_id

    # copy the neede trace to the local instance
    tracefile = str(config['tracefile'])
    while tracefile.startswith('/'):
        tracefile = tracefile[1:]
    local_tracefile = sweep_dir + '/' + tracefile
    s3_sync_file(ec2config.s3_bucket + '/' + tracefile, local_tracefile)

    # execute the simulator
    sim_log_fn = 'sweep/sim_events.gz'
    if os.path.isfile(sim_log_fn):
        os.remove(sim_log_fn)

    proc = Popen(['python', 'replaytrace.py',
            str(config['num_nodes']),
            str(config['num_workers_per_node']),
            '{:f}'.format(config['object_transfer_time_cost']),
            '{:f}'.format(config['db_message_delay']),
            str(config['scheduler']),
            str(config['validate']),
            local_tracefile
        ], stdout=PIPE, stderr=PIPE)
    (stdoutdata, stderrdata) = proc.communicate()
    print stdoutdata
    print stderrdata
    stdout_name = replay_id + '_stdout.gz'
    stderr_name = replay_id + '_stderr.gz'

    stdout_fn = sweep_dir + '/' + stdout_name
    stderr_fn = sweep_dir + '/' + stderr_name
    write_output(stdout_fn, stdoutdata)
    write_output(stderr_fn, stderrdata)
    s3_cp(stdout_fn, ec2config.s3_bucket + '/sweep/' + stdout_name)
    s3_cp(stderr_fn, ec2config.s3_bucket + '/sweep/' + stderr_name)

    log_name = replay_id + '_event_log.gz'
    s3_cp(sim_log_fn, ec2config.s3_bucket + '/sweep/' + log_name)

    end_time = time.time()

    config_etc = copy.copy(config)
    config_etc['hostname'] = socket.gethostname()
    config_etc['start_time'] = time.time()
    config_etc['end_time'] = time.time()
    config_etc['stdout_fn'] = stdout_name
    config_etc['stdout_fn'] = stderr_name
    config_etc['log_fn'] = log_name

    return replay_id, config_etc

def new_id():
    return hashlib.sha1(str(uuid.uuid1())).hexdigest()[:8]

def write_output(fn, text):
    with gzip.open(fn, 'wb') as f:
        f.write(text)

def s3_sync_file(src, dst):
    if os.path.isfile(dst):
        print 'already have {}'.format(dst)
    else:
        print 'not found locally {}'.format(dst)
        s3_cp(src, dst)
    print 'sync from {} to {}'.format(src, dst)

def s3_cp(src, dst):
    print 'copy from {} to {}'.format(src, dst)
    call(['aws', 's3', 'cp', src, dst])

def process_sweep(sleep_time):
    conn = ec2config.sqs_connect()
    queue = conn.get_queue(ec2config.sqs_sweep_queue)

    while True:
        try:
            m = queue.read()
            if m is None:
                print "waiting for more input"
                time.sleep(sleep_time)
            else:
                queue.delete_message(m)
                config = json.loads(m.get_body())
                print config

                replay_id, config_etc = replay_trace(config)

                sdb_conn = ec2config.sdb_connect()
                dom = sdb_conn.get_domain(ec2config.sdb_sweep_domain)
                dom.put_attributes(replay_id, config_etc)
                sdb_conn.close()


        except ValueError as err:
            print err
        except RuntimeError as err:
            print err
        # except:
        #     print "Unexpected error", sys.exc_info()[0]

if __name__ == '__main__':
    if len(sys.argv) == 2:
        sleep_time = int(sys.argv[1])
        print 'setting sleep time to {}'.format(sleep_time)
    else:
        sleep_time = 30
    process_sweep(sleep_time)
