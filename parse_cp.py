import json
out_file_name = 'cp1_subset'
out_list = []
count = 0
with open('CP1_train_ads.json') as f:
    for line in f:
        data = json.loads(line)
        out={}
        out['doc_id']=data['doc_id']
        out['content'] = data['extracted_text']
        if data.has_key('extractions'):
            if data['extractions'].has_key('rate'):
                #out['rate_info'] = data['extractions']['rate']
                out['has_rate'] = True
            else:
                out['has_rate'] = False
        else:
            out['has_rate'] = False
        out_list.append(out)
        print(count)
        count += 1

with open('true_positives_text.json','w') as f:
    for line in out_list:
        f.write(json.dumps(line) + '\n')
#result_df = pandas.DataFrame(out_list)
#result_df.to_csv('true_positives_text.csv', index=False, encoding='utf-8')

