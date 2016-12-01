import sys
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
    replay_id = new_id()
    sweep_dir = 'sweep'
    print "starting replay", replay_id

    # copy the neede trace to the local instance
    tracefile = str(config['tracefile'])
    while tracefile.startswith('/'):
        tracefile = tracefile[1:]
    local_tracefile = sweep_dir + '/' + tracefile
    s3_sync(ec2config.s3_bucket + '/' + tracefile, local_tracefile)

    # execute the simulator
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
    stdout_fn = sweep_dir + '/' + replay_id + '_stdout'
    stderr_fn = sweep_dir + '/' + replay_id + '_stderr'
    write_output(stdout_fn, stdoutdata)
    write_output(stderr_fn, stderrdata)

    s3_cp(stdout_fn, ec2config.s3_bucket + '/sweep/' + replay_id + '_stdout')
    s3_cp(stderr_fn, ec2config.s3_bucket + '/sweep/' + replay_id + '_stderr')
    return replay_id

def new_id():
    return hashlib.sha1(str(uuid.uuid1())).hexdigest()[:8]

def write_output(fn, text):
    f = open(fn, 'w')
    f.write(text)
    f.close()

def s3_sync(src, dst):
    print 'sync from {} to {}'.format(src, dst)
    call(['aws', 's3', 'sync', src, dst])

def s3_cp(src, dst):
    print 'copy from {} to {}'.format(src, dst)
    call(['aws', 's3', 'cp', src, dst])

def process_sweep():
    conn = ec2config.sqs_connect()
    queue = conn.get_queue(ec2config.sqs_sweep_queue)

    while True:
        try:
            m = queue.read()
            if m is None:
                print "waiting for more input"
                time.sleep(30)
            else:
                queue.delete_message(m)
                config = json.loads(m.get_body())
                print config

                start_time = time.time()
                replay_id = replay_trace(config)
                end_time = time.time()

                config_etc = copy.copy(config)
                config_etc['hostname'] = socket.gethostname()
                config_etc['start_time'] = time.time()
                config_etc['end_time'] = time.time()
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
    process_sweep()
