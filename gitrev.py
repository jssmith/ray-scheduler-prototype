from subprocess import Popen, PIPE

def get_git_rev():
    proc = Popen(['git', 'rev-parse', 'HEAD'], stdout=PIPE)
    (stdout, stderr) = proc.communicate()
    return stdout.strip()

if __name__ == '__main__':
    print get_git_rev()