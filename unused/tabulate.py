import pandas as pd
import locale
locale.setlocale(locale.LC_ALL, 'en_US.utf8')
data = pd.read_csv('ad_price_ad_level_all.csv')
del data['incall']
del data['no_incall']
del data['outcall']
del data['no_outcall']
print('There are %s advertisements' % locale.format("%d",  data.shape[0], grouping=True))
print('The ads are from %s unique sites' % locale.format("%d",  data['ad_id'].apply(lambda x: x.split(':')[0]).unique().shape[0], grouping=True))
print('There are %s ads with any price extracted' % locale.format("%d",  (~data['1hr'].isnull()).sum(), grouping=True))
print('There are %s ads with an MSA extracted' % locale.format("%d",  (~data['census_msa_code'].isnull()).sum(), grouping=True))
print('There are %s ads with both an MSA and a price extracted' % locale.format("%d",  ((~data['census_msa_code'].isnull()) & (~data['1hr'].isnull())).sum(), grouping=True))
print('There are %s unique providers before censoring' % locale.format("%d",  data['cluster_id'].unique().shape[0], grouping=True))
print('%s unique MSAs' % locale.format("%d",  data['census_msa_code'].unique().shape[0], grouping=True))
print('The average price at the ad level: $%0.2f' % data['price_per_hour'].mean())
print('The std deviation of  price at the ad level: $%0.2f' % data['price_per_hour'].std())
a = data.groupby('census_msa_code')['price_per_hour'].describe()
b = a.xs('mean', level=1)

m = b[0:89].mean()
s = b[0:89].std()
import numpy as np
print('The average msa avg price: $%0.2f' % m)
print('The between msa price std: $%0.2f' % s)
print('The within msa price std: $%0.2f' % np.sqrt(data['price_per_hour'].std()**2 + s**2))
