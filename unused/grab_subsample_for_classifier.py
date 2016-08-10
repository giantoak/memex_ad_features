import datetime
import glob
import gzip
import ujson as json

import pandas as pd

true_positives = pd.read_csv('true_positives_price.csv')
true_pos_id_set = set(true_positives['doc_id'].tolist())
true_negatives = pd.read_csv('negative_sample.csv')
true_neg_id_set = set(true_negatives['cdr_id'].tolist())
true_neg_id_set = true_neg_id_set - true_pos_id_set
keep_ids = true_neg_id_set.union(true_pos_id_set)
config = {'filenames': glob.glob('/home/ubuntu/2016_summer_camp/classifier/data/initial/lattice/*gz')}
out_list = []
for filename in config['filenames']:
    print(filename)
    print(datetime.datetime.now())
    data = [json.loads(line) for line in gzip.open(filename)]
    print(len(data))
    data = [i for i in data if i['_id'] in keep_ids]
    out_list.extend(data)
    #print(len(data))
    #dataframe = pd.DataFrame(data)
    #del dataframe['title']
    #for i in dataframe.columns:
        #if '_lat_' in i:
            #del dataframe[i]
    #self.dataframe = self.dataframe.append(dataframe, ignore_index=True)
open('subset.json','w').write(json.dumps(out_list))
