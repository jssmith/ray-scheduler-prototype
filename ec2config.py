import boto
import boto.sdb

import os

from subprocess import call

aws_region = 'us-west-2'

sdb_sweep_domain = 'raysched-sweep'
sqs_sweep_queue = 'raysched-sweep'

s3_bucket = 's3://edu.berkeley.ray-scheduler-prototype'

def sqs_connect():
    return boto.sqs.connect_to_region(aws_region)

def sdb_connect():
    return boto.sdb.connect_to_region(aws_region)

def create():
    conn = sdb_connect()
    all_sdb_domains = map(lambda x: x.name, conn.get_all_domains())
    def create_if_not_exists(domain):
        if domain not in all_sdb_domains:
            conn.create_domain(domain)
    create_if_not_exists(sdb_sweep_domain)

def s3_sync_file(src, dst):
    if os.path.isfile(dst):
        print 'already have {}'.format(dst)
    else:
        print 'not found locally {}'.format(dst)
        s3_cp(src, dst)

def s3_cp(src, dst):
    print 'copy from {} to {}'.format(src, dst)
    call(['aws', 's3', 'cp', src, dst])

if __name__ == '__main__':
    create()
