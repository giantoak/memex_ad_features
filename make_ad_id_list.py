import pandas as pd
import numpy as np
#prices =pd.read_csv('ad_price_ad_level.csv')
prices = pd.read_csv('data/escort_cdr_2/rates-text.tsv', sep='\t', names=['ad_id', 'price'])
ad_ids = set(prices['ad_id'])
all_ads = set(pd.read_csv('ad_id_list.csv', names=['ad_id'])['ad_id'])
out_with_price = []
out_no_price = []
fraction = .0001
with open('data/escort_cdr_2/content.tsv') as f:
    for line in f:
        ad_id = int(line.split('\t')[0])
        content = line.split('\t')[4]
        if ad_id in ad_ids:
            out_with_price.append({'content': content, 'ad_id': ad_id})
        else:
            if np.random.uniform() > fraction:
                out_no_price.append({'content': content, 'ad_id': ad_id})

df = pd.DataFrame(out_with_price)
df['content'] = df['content'].apply(lambda x: x.strip('"'))
df.to_csv('data/temp/ad_ids_with_price.csv', index=False)

df_no_price = pd.DataFrame([i for i in np.random.choice(out_no_price, 1000000)])
#df_no_price = pd.DataFrame(out_no_price)
df_no_price['content'] = df_no_price['content'].apply(lambda x: x.strip('"'))
df_no_price.to_csv('data/temp/ad_ids_no_price.csv', index=False)
