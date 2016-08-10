from datetime import datetime
import pandas as pd
# import datetime
# import ipdb
# import ujson as json
import numpy as np
# from sklearn import linear_model
# from sklearn import datasets

msa_features_panel = pd.read_csv('all_merged.csv')
data = pd.read_csv('normalized_prices.csv')
if False:
    msa = pd.read_csv('data/forGiantOak3/msa_locations.tsv.gz', sep='\t', header=None,
                      compression='gzip', names=['ad_id', 'census_msa_code'])
    ts = pd.read_csv('data/forGiantOak3/doc-provider-timestamp.tsv.gz', sep='\t', header=None,
                     compression='gzip', names=['ad_id', 'cluster', 'date_str'])
else:
    msa = pd.read_csv('data/forGiantOak3/msa_locations.tsv', sep='\t', header=None,
                      names=['ad_id', 'census_msa_code'])
    ts = pd.read_csv('data/forGiantOak3/doc-provider-timestamp.tsv', sep='\t', header=None,
                     names=['ad_id', 'cluster', 'date_str'])
print('merging prices and cluster/time info')
data = pd.merge(data, ts)
print('Parsing info')
data = data[data['date_str'] != '\N']
data['date'] = data['date_str'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
data.index = pd.DatetimeIndex(data['date']) 
data = data.reindex(inplace=True)
counts = pd.DataFrame(data.groupby('ad_id')['ad_id'].count())
counts.rename(columns={'ad_id': 'count'}, inplace=True)
# out=data.resample('M', how='mean')
# Progress; Need to resample both counts and prices to the monthly
# level. But first want to merge in actual data so we don't average the
# wrong thing. Then we merge these counts into the all_merged data at
# the month-msa level
# Begin aggregating ad level data up to the MSA-month level
data['1hr'] = data['time_str'] == '1 HOUR'
a = data.groupby('ad_id')['1hr'].sum()
a = a > 0
del data['1hr']
a = pd.DataFrame(a)
data = pd.merge(data, a, left_on='ad_id', right_index=True)
ad_level_hourly = pd.DataFrame(data[data['1hr']])
ad_level_no_hourly = pd.DataFrame(data[~data['1hr']])
ad_level_no_hourly.index = ad_level_no_hourly['ad_id']
ad_level_no_hourly_prices = pd.DataFrame(data[~data['1hr']].groupby('ad_id')['price_per_hour'].mean())
ad_level_no_hourly['price_per_hour'] = ad_level_no_hourly_prices
ad_level = pd.concat([ad_level_hourly, ad_level_no_hourly], axis=0)
# Having now recombined the hourly and non-hourly quoted price pieces,
# continue merging in characteristics
ad_level = pd.merge(ad_level, msa, on='ad_id', how='left')  # Note: we drop lots of ads with price  but not MSA
msa_features = msa_features_panel.groupby(['census_msa_code', 'msaname']).mean()  # Take mean over time of MSA features
msa_features.reset_index(inplace=True)
ad_level = pd.merge(ad_level, msa_features, how='left')
ad_level = pd.merge(counts, ad_level, left_index=True, right_on='ad_id', how='left')
ad_level = ad_level.drop_duplicates('ad_id')
# ad_level = pd.DataFrame(data.groupby('ad_id')['price_per_hour'].mean())
# ad_level['ad_id'] = ad_level.index
# ad_level.drop_duplicates('ad_id', inplace=True)
# ad_level = pd.merge(ad_level, msa, left_index=True, right_on='ad_id')  # Note: we drop lots of ads with price  but
# not MSA
# NOTE: at this point we grow with a merge instead of shrinking. This is
# because we have more than one msa per ad
ad_level = pd.merge(ad_level, data[['ad_id', 'date']])
ad_level['month'] = ad_level['date'].apply(lambda x: int(x.strftime('%m')))
ad_level['year'] = ad_level['date'].apply(lambda x: int(x.strftime('%Y')))


# Compute month_msa_counts without extraction
print('Merging raw MSA and Date info for raw counts')
raw_ads = pd.merge(msa, ts)
raw_ads = raw_ads[raw_ads['date_str'] != '\N']
raw_ads['date'] = raw_ads['date_str'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
raw_ads['month'] = raw_ads['date'].apply(lambda x: int(x.strftime('%m')))
raw_ads['year'] = raw_ads['date'].apply(lambda x: int(x.strftime('%Y')))
month_msa_counts = pd.DataFrame(raw_ads.groupby(['month', 'year', 'census_msa_code']).size(), columns=['raw_counts'])

# ad_level = pd.merge(ad_level, msa_features_panel, how='left')
month_msa_aggregate_prices = ad_level.groupby(['month', 'year', 'census_msa_code'])['price_per_hour'].aggregate(
    {'ad_median': np.median,
     'ad_count': len,
     'ad_mean': np.mean,
     'ad_p50': lambda x: np.percentile(x, q=50),
     'ad_p10': lambda x: np.percentile(x, q=10),
     'ad_p90': lambda x: np.percentile(x, q=90)})
month_msa_aggregates_with_features = pd.merge(month_msa_aggregate_prices,
                                              msa_features_panel,
                                              left_index=True, right_on=['month', 'year', 'census_msa_code'])
month_msa_aggregates_with_features = pd.merge(month_msa_aggregates_with_features,
                                              month_msa_counts,
                                              left_on=['month', 'year', 'census_msa_code'], right_index=True)
month_msa_aggregate_prices.to_csv('ad_prices_msa_month.csv')

# Code below here creates a pandas "Panel" object
j = month_msa_aggregates_with_features.copy()
j.reset_index(inplace=True)    
j['date_str'] = j.apply(lambda x: '{}-{}'.format(x['month'], x['year']), axis=1)
j['dp'] = j['date_str'].apply(lambda x: pd.Period(x, 'M'))
subset = j[['dp', 'census_msa_code']]
subset.to_records(index=False).tolist()
index = pd.MultiIndex.from_tuples(subset.to_records(index=False).tolist(), names=subset.columns.tolist())
j.index = index
j.reindex()
j.rename(columns={'female_mean.wage': 'female_mean',
                  'male_mean.wage': 'male_mean',
                  'female_sum.wght': 'female_num_jobs',
                  'male_sum.wght': 'male_num_jobs'},
         inplace=True)
panel = j.to_panel()
# Panel is our panel object
diff_cols = ['female_p25', 'female_p50', 'female_p75',
             'male_p25', 'male_p50', 'male_p75',
             'female_num_jobs', 'male_num_jobs', 'female_mean', 'male_mean',
             'ad_p10', 'ad_p50', 'ad_p90', 'ad_mean', 'ad_count']
for col in diff_cols:
    panel['d_' + col] = panel[col] - panel[col].shift(-1)
    panel['d_%s_pos' % col] = panel['d_' + col] > 0  # Generate dummies for positive and negative changes
# Use panel functionality to take first differences


f = panel.to_frame()
f.to_csv('monthly_panel.csv')
j['MonthDate'] = j['dp'].apply(lambda x: str(x)+'-01 00:00:00')
j['counts'] = j['ad_count']
j['region'] = j['msaname']
j[['region', 'MonthDate', 'counts', 'ad_p10', 'ad_p50', 'ad_mean', 'raw_counts']].to_csv('counts.csv', index=False)
