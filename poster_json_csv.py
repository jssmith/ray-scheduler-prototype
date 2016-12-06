import sys
import poster_gen

from sweep_csv import gen_csv

def usage():
    print "Usage: poster_json_csv.py"

def poster_json_csv():
    gen_csv(poster_gen.experiment_name_poster_synmatmul)
    gen_csv(poster_gen.experiment_name_poster_rnn)
    gen_csv(poster_gen.experiment_name_poster_rlpong)
    gen_csv(poster_gen.experiment_name_t1l)


if __name__ == '__main__':
    if len(sys.argv) != 1:
        usage()
        sys.exit(1)
    poster_json_csv()