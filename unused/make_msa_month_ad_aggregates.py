import pandas as pd
from datetime import datetime
# import ipdb
# import ujson as json
import numpy as np

data = pd.read_csv('ad_price_ad_level.csv')
data = data[~data['date_str'].isnull()]
data = data[~data['census_msa_code'].isnull()]
data['date'] = data['date_str'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
data.index = pd.DatetimeIndex(data['date'])
data = data.reindex()
data['month'] = data['date'].apply(lambda x: int(x.strftime('%m')))
data['year'] = data['date'].apply(lambda x: int(x.strftime('%Y')))
data = data[data['year'] > 2010]

# Do MSA-month aggregations
month_msa_aggregate_prices = data.groupby(['month', 'year', 'census_msa_code'])['price_per_hour'].aggregate(
    {'ad_median_monthly': np.median,
     'ad_count_monthly': len,
     'ad_mean_monthly': np.mean,
     'ad_p50_monthly': lambda x: np.percentile(x, q=50),
     'ad_p10_monthly': lambda x: np.percentile(x, q=10),
     'ad_p90_monthly': lambda x: np.percentile(x, q=90)})
month_msa_aggregate_prices.reset_index(inplace=True)
month_msa_aggregate_prices['date_str'] = month_msa_aggregate_prices.apply(
    lambda x: '{}-{}'.format(x['month'], x['year']), axis=1)

month_msa_aggregate_prices['dp'] = month_msa_aggregate_prices['date_str'].apply(lambda x: pd.Period(x, 'M'))


# Do MSA-month aggregations of incall/outcall rates
month_msa_ad_types = data.groupby(['month', 'year', 'census_msa_code'])['is_massage_parlor_ad',
                                                                        'incall', 'no_incall',
                                                                        'outcall', 'no_outcall',
                                                                        'incalloutcall', 'no_incalloutcall'].mean()
month_msa_ad_types.reset_index(inplace=True)
month_msa_aggregate_prices = month_msa_aggregate_prices.merge(month_msa_ad_types)


month_msa_aggregate_prices.to_csv('msa_month_ad_aggregates.csv', index=False)
