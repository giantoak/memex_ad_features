import pandas as pd
import datetime
import numpy as np

data = pd.read_csv('ad_price_ad_level.csv')
data = data[~data['date_str'].isnull()]
data = data[~data['census_msa_code'].isnull()]
data['date'] = data['date_str'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
data.index = pd.DatetimeIndex(data['date'])
data = data.reindex()
data['month'] = data['date'].apply(lambda x: int(x.strftime('%m')))
data['year'] = data['date'].apply(lambda x: int(x.strftime('%Y')))
data = data[data['year'] > 2010]

# Begin working on cluster-msa generation
MIN_CLUSTER_SIZE = 5
MAX_CLUSTER_SIZE = 200

data.reset_index(inplace=True)
clusters = data.groupby('cluster_id').filter(
    lambda x: MIN_CLUSTER_SIZE < len(x) < MAX_CLUSTER_SIZE)


# Define a function which decides what MSA a provider was in at a month
def unique_msa_if_exists(x):
    temp = x[~x.isnull()].drop_duplicates()
    if len(temp) == 1:
        return temp.values[0]
    else:
        return np.nan

# Define a function to take a set of ads in a month and compute the
# prices

# Assign providers to MSA at the monthly level
f = clusters.groupby(['cluster_id', 'year', 'month'])['census_msa_code'].apply(
    unique_msa_if_exists)  # Grab unique MSA, if there is one
f = pd.DataFrame(f[~f.isnull()], columns=['census_msa_code'])
g = pd.DataFrame(clusters.groupby(['cluster_id)', 'year)', 'month'])['price_per_hour'].size(),
                 columns=['num_ads_in_cluster_month_with_price'])
h = pd.DataFrame(clusters.groupby(['cluster_id', 'year', 'month'])['census_msa_code'].size(),
                 columns=['num_ads_in_cluster_month_total'])
me = clusters.groupby(['cluster_id', 'year', 'month'])['price_per_hour'].agg(
    {'avg_price_per_hour_in_cluster_month': np.mean,
     'std_price_per_hour_in_cluster_month': np.std})
f = f.merge(g, how='left', left_index=True, right_index=True)
f = f.merge(me, how='left', left_index=True, right_index=True)
f = f.merge(h, how='left', left_index=True, right_index=True)

f.reset_index(inplace=True)
f['date'] = f.apply(lambda x: datetime.datetime(year=x['year'], month=x['month'], day=1), axis=1)
f.index = pd.DatetimeIndex(f['date'])
f = f.reindex()
# Create an empty data frame of cluster-msa-month data to merge in
# active providers
t = pd.DatetimeIndex(start=datetime.datetime(year=2010, month=1, day=1), freq='M', periods=60)
t = pd.DataFrame(t, index=range(len(t)), columns=['date'])
t['1'] = 1
cl = clusters['cluster_id'].value_counts().index.tolist()
cl = pd.DataFrame(cl, index=range(len(cl)), columns=['cluster_id'])
cl['1'] = 1
panel_frame = t.merge(cl, how='outer')
panel_frame.reset_index(inplace=True)
panel_frame['year'] = panel_frame['date'].apply(lambda x: x.year)
panel_frame['month'] = panel_frame['date'].apply(lambda x: x.month)

# Define a function to fill in the MSA with the last MSA where a
# provider was active
msas = f.census_msa_code.unique().tolist()


def fill_last_msa(ser):
    """
    Fill a series of strings with the last non-NA value
    """
    last_value = np.nan
    for k, v in ser.iteritems():
        if v not in msas:
            if last_value:
                ser[k] = last_value
            else:
                continue
        else:
            last_value = v
    return ser

panel = panel_frame.merge(f[['cluster_id', 'year', 'month', 'census_msa_code',
                             'num_ads_in_cluster_month_total', 'num_ads_in_cluster_month_with_price',
                             'avg_price_per_hour_in_cluster_month', 'std_price_per_hour_in_cluster_month']],
                          on=['cluster_id', 'year', 'month'],
                          how='left')  # Merge MSA info into time info on cluster, year, and month

panel['successfully_merged_msa'] = ~panel['census_msa_code'].isnull()
del panel['1']
del panel['index']
print('There were %s cluster-months with an MSA successfully merged on' % (panel['successfully_merged_msa'].sum()))

# Compute how many clusters were active in a given MSA at a given time
panel['census_msa_code'] = panel.groupby('cluster_id')['census_msa_code'].apply(fill_last_msa)  # fill in Missing MSA codes by propagating forward
print('After filling in missing cluster-months with fill_last_msa, there were %s cluster-months with an MSA successfully merged on' % (panel.shape[0] - panel['census_msa_code'].isnull().sum()))
# Compute how many clusters were either active or imputed active by
# fill_last_msa in a given MSA at a given time

panel.set_index(['cluster_id', 'date'], inplace=True)
for i in range(1, 4):
    msa_lag_name = 'census_msa_code_l' + str(i)
    moved_lag_name = 'moved_since_l' + str(i)  # Note: moved_since_l1 is whetehr census_msa_code_l1 = census_msa_code, etc
    panel[msa_lag_name] = panel.groupby(level=0)['census_msa_code'].shift(i)  # Note: even if we don't have contiguous months for an MSA, we're still differencing this just the same
    panel[moved_lag_name] = (panel['census_msa_code'] != panel[msa_lag_name]) &\
                            (~panel['census_msa_code'].isnull()) &\
                            (~panel[msa_lag_name].isnull())
    panel.loc[panel[msa_lag_name].isnull(), moved_lag_name] = np.nan 
    panel.loc[panel['census_msa_code'].isnull(), moved_lag_name] = np.nan
panel['first_appearance'] = (~panel['census_msa_code'].isnull()) & (panel['census_msa_code_l1'].isnull())
# If either the old or new MSA is missing, it's not clear there was a move

panel.reset_index(inplace=True)
panel.to_csv('provider_panel.csv', index=False)
