import os
import sys
import json
import ec2config

from analyze_basic import analyze_basic

def usage():
    print "Usage: gen_csv.py experiment_name"

def extract_data(name, data):
    log_fn = data['log_fn']
    log_fn_remote = ec2config.s3_bucket + '/sweep/' + log_fn
    log_fn_local = 'sweep/' + log_fn
    ec2config.s3_sync_file(log_fn_remote, log_fn_local)
    return analyze_basic(log_fn_local)


def gen_csv(experiment_name):
    sdb_conn = ec2config.sdb_connect()
    dom = sdb_conn.get_domain(ec2config.sdb_sweep_domain)
    query = "select * from `{}` where (experiment_name='{}' or comment='{}')".format(
        ec2config.sdb_sweep_domain, experiment_name, experiment_name)

    rs = dom.select(query)

    cols = ['tracefile','num_tasks','task_time','num_objects_created','object_created_size','norm_critical_path','num_nodes','num_workers_per_node', 'object_transfer_time_cost', 'scheduler', 'job_completion_time']
    all_stats = []
    with open('{}.csv'.format(experiment_name), 'w') as f:
        f.write(','.join(cols))
        f.write('\n')
        for r in reversed(list(rs)):
            try:
                print '>>>>', r['tracefile']
                print r.name, r
                print "extracting data"
                stats = extract_data(r.name, r)
                print "updating"
                stats.update(dict((x, y) for x, y in r.items()))
                if 'env' in stats:
                    stats['env'] = json.loads(stats['env'])
                stats['norm_critical_path'] = -1
                line = ','.join(map(lambda x: str(stats[x]), cols))
                print "writing output"
                f.write(line)
                f.write('\n')
                all_stats.append(stats)
            except IOError as err:
                print err
    with open('{}.json'.format(experiment_name), 'w') as f:
        f.write(json.dumps(all_stats))
    sdb_conn.close()

if __name__ == '__main__':
    if len(sys.argv) != 2:
        usage()
        sys.exit(1)
    experiment_name = sys.argv[1]
    gen_csv(experiment_name)
