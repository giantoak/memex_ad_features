"""
Use 'method 2', or cross section, in order to make CPI estimates

5/7/2015

JAB
"""
import pandas as pd
from datetime import datetime
import numpy as np

data = pd.read_csv('ad_price_ad_level.csv')
market_segments = ['sex_ad', 'is_massage_parlor_ad']
time = ['month', 'year']
place = ['census_msa_code']
time_place_market_segments = place + time + market_segments
time_place = place + time
place_market_segments = place + market_segments

# Create month/year values
data = data[~data['date_str'].isnull()]
data = data[~data['census_msa_code'].isnull()]
data['date'] = data['date_str'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
data.index = pd.DatetimeIndex(data['date'])
data = data.reindex(inplace=True)
data['month'] = data['date'].apply(lambda x: int(x.strftime('%m')))
data['year'] = data['date'].apply(lambda x: int(x.strftime('%Y')))
data = data[data['year'] > 2010]

product_ratios = pd.DataFrame(data.groupby(market_segments).size()/data.shape[0], columns=['product_weight'])
product_ratios.reset_index(inplace=True)

msa_ratios = pd.DataFrame(data.groupby(place).size()/data.shape[0], columns=['msa_weight'])
msa_ratios.reset_index(inplace=True)

# Create empty price index frame to account for missing data:
t = pd.DatetimeIndex(start=datetime(year=2010, month=1, day=1), freq='M', periods=60)
t = pd.DataFrame(t, index=range(len(t)), columns=['date'])
t['1'] = 1
msa = data['census_msa_code'].unique().tolist()
msa = pd.DataFrame(msa, index=range(len(msa)), columns=['census_msa_code'])
msa['1'] = 1
sex_ad = pd.DataFrame([0, 1], index=range(2), columns=['sex_ad'])
sex_ad['1'] = 1
is_massage_parlor_ad = pd.DataFrame([0, 1], index=range(2), columns=['is_massage_parlor_ad'])
is_massage_parlor_ad['1'] = 1
panel_frame = t.merge(msa, how='outer')
panel_frame = panel_frame.merge(sex_ad, how='outer')
panel_frame = panel_frame.merge(is_massage_parlor_ad, how='outer')
panel_frame.reset_index(inplace=True)
panel_frame['year'] = panel_frame['date'].apply(lambda x: x.year)
panel_frame['month'] = panel_frame['date'].apply(lambda x: x.month)


def gmean(x):
    """
    Geometric mean, computed by logging then summing then exponentiating
    """
    return np.exp(np.log(x).mean())

m = data.groupby(time_place_market_segments)['price_per_hour'].aggregate({'gmean': gmean, 'size': np.size})
# This is the key groupby command. It computes a geometric mean and also
# the number of ads in a given place/time
m.reset_index(inplace=True)
print(panel_frame.shape)
print(m.shape)
m = pd.merge(panel_frame, m, how='left')
print(m.shape)
m.set_index(time_place_market_segments, inplace=True)
m.sort_index(ascending=True, inplace=True)  # Sort by index, so that groupby shift makes sense

m.loc[m['size'].isnull(), 'size'] = 0
counts = pd.DataFrame(m.groupby(level=time_place)['size']
                      .sum()).reset_index().rename(columns={'size': 'ad_counts'})

m['gmean_lag'] = m.groupby(level=place_market_segments)['gmean'].shift()

# Now we need to merge these numbers into a full panel of month/msa so
# we don't skip periods. Then pick back up with M here...
m['price_multiple'] = m['gmean']/m['gmean_lag']

# Once we have this price multiple, we can compute the index by cumprod
# the price_multiple 
m['basic_price_index'] = m.groupby(level=place_market_segments)['price_multiple'].cumprod()  # Turn the price ratios
# into indexes
m.reset_index(inplace=True)
m = m.merge(product_ratios)
m['weighted_basic_price_index'] = m['basic_price_index']*m['product_weight']


def sum_existing_weights(x):
    return x['weighted_basic_price_index'].sum()/x['product_weight'].sum()

# Multiply product weights by basic price index, getting ready to sum these numbers to an index
price_index = pd.DataFrame(m.groupby(time_place).apply(sum_existing_weights), columns=['price_index'])
annual_normalization = pd.DataFrame(price_index.xs(2013, level='year')
                                    .groupby(level=['census_msa_code'])['price_index']
                                    .mean()).reset_index()
annual_normalization.rename(columns={'price_index': 'normalization'}, inplace=True)

price_index.reset_index(inplace=True)
price_index = price_index.merge(annual_normalization)
price_index['price_index'] = 100*price_index['price_index']/price_index['normalization']

price_index = price_index.merge(counts, how='left')
price_index.loc[price_index['ad_counts'].isnull(), 'ad_counts'] = 0
# Merge in counts of ads creating index, and then make sure missing bins
# get 0 as well

del price_index['normalization']
price_index.to_csv('cpi_crosssection.csv', index=False)

for i in [10, 100, 1000, 2000, 5000, 7000, 10000]:
    print('__________')
    print('Within-MSA standard deviation of price index when restricting to MSA-months with more than %s counts:' % i)
    print(price_index.groupby(['census_msa_code', 'year', 'month']).filter(lambda x: x['ad_counts'].sum() > i)
          .groupby('census_msa_code')['price_index'].std().describe())
