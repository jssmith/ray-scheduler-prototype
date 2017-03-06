import os
import sys
import gitrev
from sweep_process import sweep_process


if __name__ == '__main__':
    if len(sys.argv) == 2:
        sleep_time = int(sys.argv[1])
        print 'setting sleep time to {}'.format(sleep_time)
    else:
        sleep_time = 30
    remote_ref = 'origin/master'
    while True:
        gitrev.fetch()
        (ahead, behind) = gitrev.get_relationship('HEAD', remote_ref)
        if behind > 0:
            gitrev.pull()
            print "respawning - git revision behind"
            executable = sys.executable
            os.execl(executable, executable, 'sweep_process_updating.py', str(sleep_time))
        else:
            print "git revision up to date"
        sweep_process(sleep_time, 1)
