from subprocess import call, Popen, PIPE

def fetch():
    call(['git', 'fetch'])

def pull():
    call(['git', 'pull'])

def get_rev(refname='HEAD'):
    proc = Popen(['git', 'rev-parse', refname], stdout=PIPE)
    (stdout, stderr) = proc.communicate()
    return stdout.strip()

def get_relationship(refname, other_refname):
    proc = Popen(['git', 'rev-list', '--left-right', '--count', refname + '...' + other_refname], stdout=PIPE)
    (stdout, stderr) = proc.communicate()
    (ahead, behind) = map(lambda x: int(x), stdout.strip().split())
    return (ahead, behind)


if __name__ == '__main__':
    print 'HEAD is at', get_rev()
    print 'origin/master is at', get_rev('origin/master')
    (ahead, behind) = get_relationship('HEAD','origin/master')
    print 'HEAD is {} ahead {} behind origin/master'.format(ahead, behind)
