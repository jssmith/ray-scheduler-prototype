import ec2config

def dump_activity():
    sdb_conn = ec2config.sdb_connect()
    dom = sdb_conn.get_domain(ec2config.sdb_sweep_domain)
    query = "select end_time from `{}` where end_time > '1480000000' and comment='jss-run-1' order by end_time desc".format(ec2config.sdb_sweep_domain)
    rs = dom.select(query)
    end_times = []
    for r in reversed(list(rs)):
        print r.name, r
        end_times.append(float(r['end_time']))
    print 'count', len(end_times)
    print 'elapsed', max(end_times) - min(end_times), 's'
    print 'elapsed', (max(end_times) - min(end_times)) / 3600, 'hr'
    sdb_conn.close()


if __name__ == '__main__':
    dump_activity()
