import sys
import gen_global_3 as gen_global
# import gen_global

from sweep_csv import gen_csv

def usage():
    print "Usage: global_json_csv.py"

def global_json_csv():
    gen_csv(gen_global.experiment_name_poster_synmatmul)
    gen_csv(gen_global.experiment_name_poster_rnn)
    gen_csv(gen_global.experiment_name_poster_rlpong)


if __name__ == '__main__':
    if len(sys.argv) != 1:
        usage()
        sys.exit(1)
    global_json_csv()