import boto

aws_region = 'us-west-2'

sdb_sweep_domain = 'raysched-sweep'
sqs_sweep_queue = 'raysched-sweep'

s3_bucket = 's3://edu.berkeley.ray-scheduler-prototype'

def sqs_connect():
    return boto.sqs.connect_to_region(aws_region)

def sdb_connect():
    return boto.connect_sdb()

def create():
    conn = sdb_connect()
    all_sdb_domains = map(lambda x: x.name, conn.get_all_domains())
    def create_if_not_exists(domain):
        if domain not in all_sdb_domains:
            conn.create_domain(domain)
    create_if_not_exists(sdb_sweep_domain)


if __name__ == '__main__':
    create()
