import pandas as pd
import datetime
import ipdb
import json
import numpy as np

data = pd.read_csv('ad_price_ad_level.csv')
data = data[~data['date_str'].isnull()]
data = data[~data['census_msa_code'].isnull()]
data['date'] = data['date_str'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S' ))
data.index = pd.DatetimeIndex(data['date'])
data.reindex(inplace=True)
data['month'] = data['date'].apply(lambda x: int(x.strftime('%m')))
data['year'] = data['date'].apply(lambda x: int(x.strftime('%Y')))
data = data[data['year'] > 2010]

##### Do MSA-month aggregations
month_msa_aggregate_prices = data.groupby(['month','year','census_msa_code'])['price_per_hour'].aggregate({'ad_median':np.median, 'ad_count':len,'ad_mean':np.mean, 'ad_p50':lambda x: np.percentile(x,q=50), 'ad_p10':lambda x: np.percentile(x, q=10), 'ad_p90':lambda x: np.percentile(x, q=90)})
month_msa_aggregate_prices.reset_index(inplace=True)
month_msa_aggregate_prices['date_str'] = month_msa_aggregate_prices.apply(lambda x: str(x['month']) + '-' + str(x['year']), axis=1)
import datetime
month_msa_aggregate_prices['dp']=month_msa_aggregate_prices['date_str'].apply(lambda x: pd.Period(x, 'M'))
month_msa_chars = month_msa_aggregate_prices.copy()

month_msa_chars.to_csv('msa_month_ad_aggregates.csv')
