#!/usr/bin/python

"""
This script loads the initial price data that Sam loaded from DeepDive on about 12/25/2014

It also adds census data from sam on 1/8/2015.

It then creates a data set of only ads with two prices "doubles" where
the implied fixed cost is listed
"""
import pandas
import datetime
import ipdb
import json
import numpy as np
from sklearn import linear_model
from sklearn import datasets

def census_lookup(geo_id, table_value, verbose=False):
    """
    table_value is like B01001001
    Where B01001 is the table id and 001 is the value id
    """
    try:
        table = table_value[0:6]
        value = table_value[6:]
        data = combined['data'][geo_id][table]['estimate'][table_value]
        if verbose:
            print('Info for: %s ' % combined['geography'][geo_id]['name'])
            print('Found value: %s' % data)
        return data
    except:
        return np.nan

#data = pandas.read_csv('rates_locs.csv')
if False:
    data = pandas.read_csv('forGiantOak3/rates.tsv.gz', sep='\t', compression='gzip', header=None)
else:
    data = pandas.read_csv('forGiantOak3/rates.tsv', sep='\t', header=None)

print('There are %s observations' % data.shape[0]) # about 2.1M
data.rename(columns={0:'ad_id', 1:'rate'}, inplace=True)
data['time_str'] = data['rate'].apply(lambda x: x.split(',')[1])
data['price'] = data['rate'].apply(lambda x: x.split(',')[0])
data['unit'] = data['time_str'].apply(lambda x: x.split(' ')[1])
data = data[data['unit'] != 'DURATION'] # about 1.7M
print('There are %s observations after dropping no duration prices' % data.shape[0])
data['timeValue'] = data['time_str'].apply(lambda x: x.split(' ')[0])
data['unit'][data['unit'] == 'HOURS'] = 'HOUR'
data['minutes'] = np.nan
data.minutes.loc[data['unit']=='MINS'] = data.timeValue.loc[data['unit']=='MINS'].astype(np.integer)
data.minutes.loc[data['unit']=='HOUR'] = 60*data.timeValue.loc[data['unit']=='HOUR'].astype(np.integer)

data['price'] = data['price'].apply(lambda x: x.replace('$',''))
data['price'] = data['price'].apply(lambda x: x.replace('roses',''))
data['price'] = data['price'].apply(lambda x: x.replace('rose',''))
data['price'] = data['price'].apply(lambda x: x.replace('bucks',''))
data['price'] = data['price'].apply(lambda x: x.replace('kisses',''))
data['price'] = data['price'].apply(lambda x: x.replace('kiss',''))
data['price'] = data['price'].apply(lambda x: x.replace('dollars',''))
data['price'] = data['price'].apply(lambda x: x.replace('dollar',''))
data = data[data['price'].apply(lambda x: 'euro' not in x)]
data = data[data['price'].apply(lambda x: 'eur' not in x)]
data = data[data['price'].apply(lambda x: 'aud' not in x)]
print('There are %s prices after dropping foreign prices' % data.shape[0])
data['price'] = data['price'].astype('int')
# This code is useful for dealing with the 'price' string problem in
# sam's rates_locs file from 12/29

data['price_per_hour'] = 60*data['price']/data['minutes']
# Begin merging information from census
if False:
    sexad = pandas.read_csv('forGiantOak3/isssexad.tsv.gz', sep='\t', header=None,compression='gzip')
    sexad.rename(columns={0:'ad_id', 1:'sex_ad'}, inplace=True)
else:
    sexad = pandas.read_csv('forGiantOak3/isssexad.tsv', sep='\t', header=None)
    sexad.rename(columns={0:'ad_id', 1:'sex_ad'}, inplace=True)
data = pandas.merge(data, sexad, on='ad_id', how='left')
data = data[data['sex_ad'] == 1] # remove non- sex ads
print('There are %s prices after dropping Non-sex ad prices' % data.shape[0])

data.to_csv('normalized_prices.csv', index=False)

counts = pandas.DataFrame(data.groupby('ad_id')['ad_id'].count())
print('The %s extracted prices pertain to %s observations' % (data.shape[0], counts.shape[0]))
counts.rename(columns={'ad_id':'counts'}, inplace=True)
counts.to_csv('price_extraction_counts.csv')
pandas.DataFrame(counts['counts'].value_counts()/counts['counts'].value_counts().sum(), columns=['distribution']).to_csv('num_prices_extracted_dist.csv')
out = pandas.merge(data, counts,left_on='ad_id', right_index=True)
doubles = out.copy()
doubles = doubles[doubles['counts']==2]
print('There are %s ads after restricting to ads with 2 prices' % doubles.shape[0])

calcs=doubles.groupby('ad_id').agg({'price':['min','max'], 'minutes':['min','max']})
doubles = pandas.merge(doubles, pandas.DataFrame(calcs['price']['min']), left_on='ad_id', right_index=True)
doubles.rename(columns={'min':'p1'}, inplace=True)
doubles = pandas.merge(doubles, pandas.DataFrame(calcs['price']['max']), left_on='ad_id', right_index=True)
doubles.rename(columns={'max':'p2'}, inplace=True)
doubles = pandas.merge(doubles, pandas.DataFrame(calcs['minutes']['min']), left_on='ad_id', right_index=True)
doubles.rename(columns={'min':'m1'}, inplace=True)
doubles = pandas.merge(doubles, pandas.DataFrame(calcs['minutes']['max']), left_on='ad_id', right_index=True)
doubles.rename(columns={'max':'m2'}, inplace=True)
doubles['zero_price'] = (doubles['p1'] * doubles['m2'] - doubles['m1'] * doubles['p2']) / (doubles['m2'] - doubles['m1'])
doubles=doubles[~doubles['ad_id'].duplicated()] # remove duplicates
print('There are %s ads after dropping duplicates' % doubles.shape[0])
doubles =doubles[doubles['m1'] != doubles['m2']] # remove those with two prices for the same time...
doubles['marginal_price'] = (doubles['p2'] - doubles['p1']) / (doubles['m2'] - doubles['m1']) * 60
doubles.to_csv('zero_price.csv', index=False)
out.index = range(out.shape[0])
out.reindex()
out.to_csv('ad_prices.csv', index=False)
#reg = linear_model.LinearRegression()
#(p1 m2 - m1 p2)/ (m2 - m1)
#out = reg.fit(X=data.minutes.values[:,np.newaxis],y=data.price.values[:,np.newaxis])

# Begin using MSA data
if False:
    msa = pandas.read_csv('forGiantOak3/msa_locations.tsv.gz', sep='\t', header=None, compression='gzip', names=['ad_id','census_msa_code'])
else:
    msa = pandas.read_csv('forGiantOak3/msa_locations.tsv', sep='\t', header=None, names=['ad_id','census_msa_code'])
doubles = pandas.merge(doubles, msa) # Add census MSA code to the fixed price info
#msa_features_panel = pandas.read_csv('all_merged.csv', index_col=['month','year','census_msa_code'])
msa_features_panel = pandas.read_csv('all_merged.csv')
msa_features = msa_features_panel.groupby(['census_msa_code','msaname']).mean() # Take mean over time of MSA features
msa_features.reset_index(inplace=True)
#msa_features = msa_features_panel[(msa_features_panel['month'] == 12) & (msa_features_panel['year']==2013)]
#msa_features = msa_features_panel.xs(12, level='month').xs(2013, level='year') # Grab a single year
zero_price = pandas.merge(doubles, msa_features, left_on='census_msa_code', right_on='census_msa_code')
print('There are %s ads that merge in with MSAs, representing %s of %s MSAs' % (zero_price.shape[0], zero_price.census_msa_code.value_counts().shape[0], doubles.census_msa_code.value_counts().shape[0]))

# Merge and export zero price aggregates
zp_aggregates = zero_price[zero_price['counts'] == 2].groupby('census_msa_code')['zero_price'].aggregate({ 'zero_price_count':len,'zp_mean':np.mean, 'zp_p50':lambda x: np.percentile(x,q=50), 'zp_p10':lambda x: np.percentile(x, q=10), 'zp_p25':lambda x: np.percentile(x, q=25), 'zp_p75':lambda x: np.percentile(x, q=75), 'zp_p90':lambda x: np.percentile(x, q=90)})
msa_aggregates=pandas.merge(msa_features, zp_aggregates, left_on='census_msa_code', right_index=True)
mp_aggregates = zero_price.groupby('census_msa_code')['marginal_price'].aggregate({ 'marginal_price_count':len,'mp_mean':np.mean, 'mp_p50':lambda x: np.percentile(x,q=50), 'mp_p10':lambda x: np.percentile(x, q=10), 'mp_p25':lambda x: np.percentile(x, q=25), 'mp_p75':lambda x: np.percentile(x, q=75), 'mp_p90':lambda x: np.percentile(x, q=90)})
msa_aggregates=pandas.merge(msa_aggregates, mp_aggregates, left_on='census_msa_code', right_index=True)
msa_aggregates.to_csv('zero_price_msa_aggregates.csv', index=False)

zp_individual = zero_price[zero_price['counts'] == 2].copy()

zero_price = zero_price[zero_price.zero_price > 0]
zero_price = zero_price[zero_price.zero_price < 200] # very few are above 200
zero_price.to_csv('zero_price_msa_micro.csv', index=False)
# Export zero price micro data 

# Begin merging msa info into price data
# This next block of code merges to the hourly level
data['1hr'] = data['time_str'] == '1 HOUR'
a = data.groupby('ad_id')['1hr'].sum()
a = a>0
del data['1hr']
a = pandas.DataFrame(a)
data = pandas.merge(data, a, left_on='ad_id', right_index=True)
ad_level_hourly = pandas.DataFrame(data[data['1hr']])
ad_level_no_hourly = pandas.DataFrame(data[~data['1hr']])
ad_level_no_hourly.index = ad_level_no_hourly['ad_id']
ad_level_no_hourly_prices = pandas.DataFrame(data[~data['1hr']].groupby('ad_id')['price_per_hour'].mean())
ad_level_no_hourly['price_per_hour'] = ad_level_no_hourly_prices
ad_level = pandas.concat([ad_level_hourly, ad_level_no_hourly], axis=0)
print('There are %s prices from ads when computing hourly price' % ad_level.shape[0])
# Having now recombined the hourly and non-hourly quoted price pieces,
# continue merging in characteristics
ad_level = pandas.merge(ad_level, msa, on='ad_id', how='left') # Note: we drop lots of ads with price  but not MSA
print('There are %s prices from ads once we merge in MSAs' % ad_level.shape[0])
#ad_level = pandas.merge(ad_level, msa, left_index=True, right_on='ad_id', how='left') # Note: we drop lots of ads with price  but not MSA
ad_level = pandas.merge(ad_level, msa_features, how='left')
print('There are %s prices from ads once we merge in MSA features' % ad_level.shape[0])
ad_level['platform'] = ad_level['ad_id'].apply(lambda x: x.split(':')[0])
ad_level = pandas.merge(counts, ad_level, left_index=True, right_on='ad_id',how='right')
print('There are %s prices from ads once we merge in price counts per ad' % ad_level.shape[0])
ad_level.to_csv('ad_level_prices.csv', index=False)
ad_level = ad_level[ad_level['counts'] == 2]
print('There are %s prices from ads once we restrict to those with exactly 2 prices' % ad_level.shape[0])
ad_level = ad_level.drop_duplicates('ad_id')
print('There are %s prices from ads once we drop duplicates on ad_id' % ad_level.shape[0])

# Begin aggregating ad level data up to the MSA level
ad_aggregate_prices = ad_level[ad_level['counts'] == 2].groupby('census_msa_code')['price_per_hour'].aggregate({'median':np.median, 'ad_count':len,'mean':np.mean, 'ad_p50':lambda x: np.percentile(x,q=50), 'ad_p10':lambda x: np.percentile(x, q=10), 'ad_p25':lambda x: np.percentile(x, q=25), 'ad_p90':lambda x: np.percentile(x, q=90), 'ad_p75':lambda x: np.percentile(x, q=75),})
ad_aggregate_prices = pandas.merge(ad_aggregate_prices, msa_features, left_index=True, right_on='census_msa_code')
msa_counts = ad_level.groupby('census_msa_code')['counts'].aggregate({'prices_per_ad':np.mean, 'fraction_zero_price':lambda x: (x == 2).mean()})
ad_aggregate_prices = pandas.merge(ad_aggregate_prices, msa_counts, left_on='census_msa_code', right_index=True)
ad_aggregate_prices = pandas.merge(ad_aggregate_prices, msa_aggregates[['zero_price_count','zp_mean','zp_p50','zp_p10','zp_p25','zp_p75','zp_p90','mp_mean','mp_p50','mp_p10','mp_p25','mp_p75','mp_p90', 'census_msa_code']])
ad_aggregate_prices.to_csv('ad_prices_msa.csv', index=False)

#ad_level = pandas.merge(ad_level, msa_aggregates[['zero_price_count','zp_mean','zp_p50','zp_p10','zp_p25','zp_p75','zp_p90','mp_mean','mp_p50','mp_p10','mp_p25','mp_p75','mp_p90', 'census_msa_code']], how='left')
ad_level = pandas.merge(ad_level, ad_aggregate_prices, how='left')
ad_level = pandas.merge(ad_level, zp_individual[['zero_price','marginal_price','ad_id']], on='ad_id', how='left')
ad_level.to_csv('ad_prices_msa_micro.csv',index=False)
