import poster_gen
import json
import gzip

fn = 'sweep-summaries/' + poster_gen.experiment_name_poster_rnn + '.json.gz'

with gzip.open(fn,'rb') as f:
    data = json.load(f)

    t_transfer_aware = filter(lambda x: x['scheduler'] == 'transfer_aware' and x['num_nodes'] == '3', data)[0]['job_completion_time']
    t_transfer_aware_threshold_local = filter(lambda x: x['scheduler'] == 'transfer_aware_threshold_local' and x['num_nodes'] == '3', data)[0]['job_completion_time']

    print '{} / {} = {}'.format(t_transfer_aware_threshold_local, t_transfer_aware,
        float(t_transfer_aware_threshold_local) / float(t_transfer_aware))