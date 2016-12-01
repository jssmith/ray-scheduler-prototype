import sys
import gzip
import json

from statslogging import SummaryStats
from helpers import setup_logging

def usage():
    print "Usage: analyze_basic.py log.gz"

def analyze_basic(fn):
    setup_logging()

    def load_log(fn):
        with gzip.open(fn, 'rb') as f:
            return json.load(f)
    events = load_log(sys.argv[1])

    system_time = SystemTime()
    stats = SummaryStats(system_time)

    for event in events:
        system_time.set_time(event['timestamp'])
        getattr(stats, event['event_name'])(**(event['event_data']))

    stats.job_ended()

    print stats


class SystemTime(object):
    def __init__(self):
        self._t = 0

    def set_time(self, t):
        if t < self._t:
            raise RuntimeError('Time decreased')
        self._t = t

    def get_time(self):
        return self._t


if __name__ == '__main__':
    if len(sys.argv) != 2:
        usage()
        sys.exit(1)
    analyze_basic(sys.argv)