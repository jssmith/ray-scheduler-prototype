import ec2config

def dump_activity():
    sdb_conn = ec2config.sdb_connect()
    dom = sdb_conn.get_domain(ec2config.sdb_sweep_domain)
    query = "select * from `{}` where end_time > '1480000000' order by end_time desc limit 10".format(ec2config.sdb_sweep_domain)
    rs = dom.select(query)
    for r in rs:
        print r.name, r
    sdb_conn.close()


if __name__ == '__main__':
    dump_activity()
