import sys
import json

from analyze_basic import analyze_basic

from helpers import setup_logging

def analyze_basic_json(in_fn, out_fn):
    stats = analyze_basic(in_fn)
    with open(out_fn, 'w') as f:
        f.write(json.dumps(stats,
                      sort_keys=True,
                      indent=4,
                      separators=(',', ': ')))

if __name__ == '__main__':
    if len(sys.argv) != 3:
        usage()
        sys.exit(1)
    setup_logging()
    analyze_basic_json(sys.argv[1], sys.argv[2])
