"""
Merge in data at the MSA level to create one master-level file

JAB
"""

import pandas as pd
import datetime
import numpy as np

## This code brings in american community survey data as well as data
## from uniform crime reporting and law enforcement staffing reporting,
## but is ignored for classification
#acs = pd.read_csv('acs.csv')
#ucr_year = pd.read_csv('ucr.csv')
#ucr_avg = ucr_year.groupby('census_msa_code')[['property', 'rape', 'violent']].sum()
#ucr_avg.reset_index(inplace=True)
#lemas = pd.read_csv('lemas.csv')

# Begin working with prices
prices = pd.read_csv('ad_price_ad_level.csv')
prices = prices[~prices['date_str'].isnull()]
prices = prices[~prices['census_msa_code'].isnull()]
prices['date'] = prices['date_str'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
prices.index = pd.DatetimeIndex(prices['date'])
prices = prices.reindex()
prices['month'] = prices['date'].apply(lambda x: int(x.strftime('%m')))
prices['year'] = prices['date'].apply(lambda x: int(x.strftime('%Y')))
prices = prices[prices['year'] > 2010]
##### Do MSA aggregations of prices
msa_aggregate_prices = prices.groupby('census_msa_code')['price_per_hour'].aggregate(
    {'ad_median_msa': np.median,
     'ad_count_msa': len,
     'ad_mean_msa': np.mean,
     'ad_p50_msa': lambda x: np.percentile(x, q=50),
     'ad_p10_msa': lambda x: np.percentile(x, q=10),
     'ad_p90_msa': lambda x: np.percentile(x, q=90)})
msa_aggregate_prices.reset_index(inplace=True)

#out = ucr_avg.copy()
#out = out.merge(acs, how='outer')
#out = out.merge(lemas, how='outer')
#out = out.merge(msa_aggregate_prices, how='outer')

out = out.copy()
out.to_csv('msa_characteristics.csv', index=False)
