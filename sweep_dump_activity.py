import sys
import ec2config

def usage():
    print "Usage: sweep_dump_activity.py experiment_name"

def dump_activity(experiment_name):
    sdb_conn = ec2config.sdb_connect()
    dom = sdb_conn.get_domain(ec2config.sdb_sweep_domain)
    query = "select * from `{}` where experiment_name like '{}%' and hostname ='50aaaa988e94'".format(
        ec2config.sdb_sweep_domain, experiment_name, experiment_name)
    rs = dom.select(query)
    end_times = []
    elapsed_times = []
    for r in reversed(list(rs)):
        print r.name, r
    sdb_conn.close()

def agg_activity(experiment_name):

    sdb_conn = ec2config.sdb_connect()
    dom = sdb_conn.get_domain(ec2config.sdb_sweep_domain)
    query = "select * from `{}` where experiment_name like '{}%' and hostname ='50aaaa988e94'".format(
        ec2config.sdb_sweep_domain, experiment_name, experiment_name)
    rs = dom.select(query)
    end_times = []
    elapsed_times = []
    for r in reversed(list(rs)):
        print r.name, r
    sdb_conn.close()


def measure_activity(experiment_name):
    sdb_conn = ec2config.sdb_connect()
    dom = sdb_conn.get_domain(ec2config.sdb_sweep_domain)
    query = "select start_time, end_time from `{}` where end_time > '1480000000' and experiment_name like '{}%' order by end_time desc".format(
        ec2config.sdb_sweep_domain, experiment_name, experiment_name)
    rs = dom.select(query)
    end_times = []
    elapsed_times = []
    for r in reversed(list(rs)):
        end_time = float(r['end_time'])
        start_time = float(r['start_time'])
        elapsed_time = end_time - start_time
        end_times.append(end_time)
        elapsed_times.append(elapsed_time)
    print '{} simulations run for experiment {}'.format(len(end_times), experiment_name)
    elapsed_sec = max(end_times) - min(end_times)
    print 'elapsed time {:.0f} seconds ({:.2f} hours)'.format(elapsed_sec, elapsed_sec/3600)
    print 'avg simulation duartion {:.2f} seconds'.format(sum(elapsed_times) / float(len(elapsed_times)))
    sdb_conn.close()

if __name__ == '__main__':
    if len(sys.argv) != 2:
        usage()
        sys.exit(1)
    experiment_name = sys.argv[1]
    dump_activity(experiment_name)
    measure_activity(experiment_name)
    # agg_activity(experiment_name)