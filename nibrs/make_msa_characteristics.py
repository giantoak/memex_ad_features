"""
Merge in data at the MSA level to create one master-level file

JAB
"""

import pandas as pd
import datetime
import ipdb
import json
import numpy as np

acs = pd.read_csv('acs.csv')
violence_nibrs = pd.read_csv('violence_nibrs.csv')
violence_nibrs.rename(columns={'mean':'share_of_incidents_violent','sum':'number_of_indicents_violent','size':'number_of_incidents_total'}, inplace=True)
female_violence_nibrs = pd.read_csv('female_violence_nibrs.csv')
female_violence_nibrs.rename(columns={'mean':'share_of_incidents_violent_to_women','sum':'number_of_indicents_violent_to_women'}, inplace=True)
del female_violence_nibrs['size']
prostitution_nibrs = pd.read_csv('prostitution_nibrs.csv')
prostitution_nibrs.rename(columns={'mean':'share_of_incidents_prostitution','sum':'number_of_indicents_prostitution'}, inplace=True)
del prostitution_nibrs['size']
nibrs = pd.merge(violence_nibrs,female_violence_nibrs)
nibrs = nibrs.merge(prostitution_nibrs)
ucr_year = pd.read_csv('ucr.csv')
ucr_avg=ucr_year.groupby('census_msa_code')[['property','rape','violent']].sum()
ucr_avg.reset_index(inplace=True)
lemas = pd.read_csv('lemas.csv')

# Begin working with prices
prices = pd.read_csv('ad_price_ad_level.csv')
prices = prices[~prices['date_str'].isnull()]
prices = prices[~prices['census_msa_code'].isnull()]
prices['date'] = prices['date_str'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S' ))
prices.index = pd.DatetimeIndex(prices['date'])
prices.reindex(inplace=True)
prices['month'] = prices['date'].apply(lambda x: int(x.strftime('%m')))
prices['year'] = prices['date'].apply(lambda x: int(x.strftime('%Y')))
prices = prices[prices['year'] > 2010]
##### Do MSA aggregations of prices
msa_aggregate_prices = prices.groupby('census_msa_code')['price_per_hour'].aggregate({'ad_median':np.median, 'ad_count':len,'ad_mean':np.mean, 'ad_p50':lambda x: np.percentile(x,q=50), 'ad_p10':lambda x: np.percentile(x, q=10), 'ad_p90':lambda x: np.percentile(x, q=90),'num_ads_with_price':np.size})
msa_aggregate_prices.reset_index(inplace=True)

out = pd.merge(nibrs, ucr_avg, how='outer')
out = out.merge(acs, how='outer')
out = out.merge(lemas, how='outer')
out = out.merge(msa_aggregate_prices, how='outer')
out.to_csv('msa_characteristics.csv', index=False)
