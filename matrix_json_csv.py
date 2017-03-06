import sys

from gen_matrix_sweep import experiment_name_matrix_sweep
from sweep_csv import gen_csv

def usage():
    print "Usage: matrix_json_csv.py"

def matrix_json_csv():
    gen_csv(experiment_name_matrix_sweep)


if __name__ == '__main__':
    if len(sys.argv) != 1:
        usage()
        sys.exit(1)
    matrix_json_csv()