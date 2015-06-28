#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
This script takes ad price extraction and creates ad_prices_ad_level.csv (ad level clean price data)

It then creates a data set of only ads with two prices "doubles" with 
the implied fixed cost 
"""
import pandas
import datetime
import ipdb
import json
import numpy as np
nrows=None

if False:
    data = pandas.read_csv('data/forGiantOak3/rates.tsv.gz', sep='\t', compression='gzip', header=None, nrows=nrows)
else:
    data = pandas.read_csv('data/forGiantOak3/rates2.tsv', sep='\t', header=None, nrows=nrows)

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
data['price'] = data['price'].apply(lambda x: x.replace('dlr',''))
data = data[data['price'].apply(lambda x: 'euro' not in x)]
data = data[data['price'].apply(lambda x: 'eur' not in x)]
data = data[data['price'].apply(lambda x: 'eu' not in x)]
data = data[data['price'].apply(lambda x: 's' not in x)]
data = data[data['price'].apply(lambda x: '¥' not in x)]
data = data[data['price'].apply(lambda x: '\xef\xbc\x90' not in x)]
data = data[data['price'].apply(lambda x: 'aud' not in x)]
print('There are %s prices after dropping foreign prices' % data.shape[0])
data['price'] = data['price'].astype('int')
# This code is useful for dealing with the 'price' string problem in
# sam's rates_locs file from 12/29

# Begin merging information from census
if False:
    sexad = pandas.read_csv('data/forGiantOak3/isssexad.tsv.gz', sep='\t', header=None,compression='gzip', nrows=nrows)
    sexad.rename(columns={0:'ad_id', 1:'sex_ad'}, inplace=True)
else:
    sexad = pandas.read_csv('data/forGiantOak3/isssexad.tsv', sep='\t', header=None, nrows=nrows)
    sexad.rename(columns={0:'ad_id', 1:'sex_ad'}, inplace=True)
data = pandas.merge(data, sexad, on='ad_id', how='left')
del sexad
#data = data[data['sex_ad'] == 1] # remove non- sex ads
##print('There are %s prices after dropping Non-sex ad prices' % data.shape[0])


# Merge in massage parlor information
massage = pandas.read_csv('data/forGiantOak3/ismassageparlorad.tsv', sep='\t', header=None, nrows=nrows)
massage.rename(columns={0:'ad_id', 1:'massage_ad'}, inplace=True)
data = pandas.merge(data, massage, on='ad_id', how='left')
del massage

counts = pandas.DataFrame(data.groupby('ad_id')['ad_id'].count())
print('The %s extracted prices pertain to %s observations' % (data.shape[0], counts.shape[0]))
counts.rename(columns={'ad_id':'prices_from_ad'}, inplace=True)
out = pandas.merge(data, counts,left_on='ad_id', right_index=True)
del counts

# Begin using MSA data
if False:
    msa = pandas.read_csv('data/forGiantOak3/msa_locations.tsv.gz', sep='\t', header=None, compression='gzip', names=['ad_id','census_msa_code'], nrows=nrows)
else:
    msa = pandas.read_csv('data/forGiantOak3/msa_locations.tsv', sep='\t', header=None, names=['ad_id','census_msa_code'], nrows=nrows)
if False:
    cluster = pandas.read_csv('data/forGiantOak3/msa_locations.tsv.gz', sep='\t', header=None, compression='gzip', names=['ad_id','census_msa_code'], nrows=nrows)
else:
    msa = pandas.read_csv('data/forGiantOak3/msa_locations.tsv', sep='\t', header=None, names=['ad_id','census_msa_code'], nrows=nrows)
out = pandas.merge(out, msa, how='left') # Add census MSA code to the fixed price info
del msa

# Merge in cluster ID
if False:
    ts = pandas.read_csv('data/forGiantOak3/doc-provider-timestamp.tsv.gz', sep='\t', header=None, compression='gzip', names=['ad_id','cluster','date_str'], nrows=nrows)
else:
    ts = pandas.read_csv('data/forGiantOak3/doc-provider-timestamp.tsv', sep='\t', header=None, names=['ad_id','cluster_id','date_str'], nrows=nrows)
out = out.merge(ts, how='left')
del ts
out[out['cluster_id'] == '\N'] = np.nan
out[out['date_str'] == '\N'] = np.nan


# Merge in massage parlor flag
massage = pandas.read_csv('data/forGiantOak3/ismassageparlorad.tsv', sep='\t', header=None, names=['ad_id','is_massage_parlor_ad'], nrows=nrows)
out = out.merge(massage, how='left')
del massage

# Merge in incall
incall = pandas.read_csv('data/forGiantOak6/incall-new.tsv', sep='\t', header=None, names=['ad_id', 'incall_input'], nrows=nrows)
out = out.merge(incall, how='left')
del incall
out['incall'] = out['incall_input'] == 1
out['no_incall'] = out['incall_input'] == -1
del out['incall_input']

# Merge in outcall
outcall = pandas.read_csv('data/forGiantOak6/outcall-new.tsv', sep='\t', header=None, names=['ad_id', 'outcall_input'], nrows=nrows)
out = out.merge(outcall, how='left')
del outcall
out['outcall'] = out['outcall_input'] == 1
out['no_outcall'] = out['outcall_input'] == -1
del out['outcall_input']

# Merge in incalloutcall
incalloutcall = pandas.read_csv('data/forGiantOak6/incalloutcall-new.tsv', sep='\t', header=None, names=['ad_id', 'incalloutcall_input'], nrows=nrows)
out = out.merge(incalloutcall, how='left')
del incalloutcall
out['incalloutcall'] = out['incalloutcall_input'] == 1
out['no_incalloutcall'] = out['incalloutcall_input'] == -1
del out['incalloutcall_input']



del out['unit']
del out['timeValue']
out.to_csv('ad_prices_price_level.csv', index=False)

# Begin work on fixed prices
out = out[out['prices_from_ad']==2]
print('There are %s ads after restricting to ads with 2 prices' % out.shape[0])

calcs=out.groupby('ad_id').agg({'price':['min','max'], 'minutes':['min','max']})
out = pandas.merge(out, pandas.DataFrame(calcs['price']['min']), left_on='ad_id', right_index=True)
out.rename(columns={'min':'p1'}, inplace=True)
out = pandas.merge(out, pandas.DataFrame(calcs['price']['max']), left_on='ad_id', right_index=True)
out.rename(columns={'max':'p2'}, inplace=True)
out = pandas.merge(out, pandas.DataFrame(calcs['minutes']['min']), left_on='ad_id', right_index=True)
out.rename(columns={'min':'m1'}, inplace=True)
out = pandas.merge(out, pandas.DataFrame(calcs['minutes']['max']), left_on='ad_id', right_index=True)
out.rename(columns={'max':'m2'}, inplace=True)
out['zero_price'] = (out['p1'] * out['m2'] - out['m1'] * out['p2']) / (out['m2'] - out['m1'])
out=out[~out['ad_id'].duplicated()] # remove duplicates
print('There are %s ads after dropping duplicates' % out.shape[0])
out =out[out['m1'] != out['m2']] # remove those with two prices for the same time...
out['marginal_price'] = (out['p2'] - out['p1']) / (out['m2'] - out['m1']) * 60
out.to_csv('ad_zero_prices.csv', index=False)

# Re-read ad_prices_price_level.csv to aggregate from file
del out
data = pandas.read_csv('ad_prices_price_level.csv')
print(data.shape)
# Begin computing price per hour:
# If we have a 1 hour price, that's it. Otherwise, multiply all the
# quoted prices by a 'multiplier' which represents the average ratio of
# hourly price to the given time period price

# The below blocks of code compute 'price_ratios' which is the ratio of
# average prices for 1 hour for other ads that also posted the same
# price
minute_values=pandas.Series((data['minutes'].value_counts()/data.shape[0] > .0001).index.map(int))
minute_string_series = minute_values.map(lambda x: 'price_%s_mins' % x)
minute_string_series.index = minute_values
def get_prices(x):
    out = pandas.Series(np.nan,index=minute_values)
    for mins in minute_values:
        matching_prices = x[x['minutes'] == mins]['price']
        if len(matching_prices):
            out[mins] = matching_prices.mean()
    return out
me = data.groupby('ad_id').apply(get_prices) # This is REALLLLY slow
price_ratios = pandas.Series(np.nan, index=minute_values)
for m in minute_values:
    hour_price = me[(~me[60].isnull()) & (~me[m].isnull())][60].mean()
    m_price = me[(~me[m].isnull()) & (~me[m].isnull())][m].mean()
    price_ratios[m] = hour_price/m_price

print('Computed price ratios for acts of given length to acts of 1 hour')
print(price_ratios)

# Now split the data by whether there's a posted price of 1 hr
data['1hr'] = data['time_str'] == '1 HOUR'
a = data.groupby('ad_id')['1hr'].sum()
a = a>0
del data['1hr']
a = pandas.DataFrame(a)
data = pandas.merge(data, a, left_on='ad_id', right_index=True)
price_level_hourly = pandas.DataFrame(data[data['1hr']])
price_level_hourly['price_per_hour'] = price_level_hourly['price'] # If there's an hourly price, use it
price_level_no_hourly = pandas.DataFrame(data[~data['1hr']])
price_level_no_hourly.index = price_level_no_hourly['ad_id']
# Otherwise use the multiplier
price_level_no_hourly['multiplier'] = price_level_no_hourly['minutes'].apply(lambda x: price_ratios[x])
price_level_no_hourly['price_per_hour'] = price_level_no_hourly['price'] * price_level_no_hourly['multiplier']
price_level_no_hourly_prices = pandas.DataFrame(price_level_no_hourly.groupby('ad_id')['price_per_hour'].mean())
price_level_no_hourly['price_per_hour'] = price_level_no_hourly_prices
price_level = pandas.concat([price_level_hourly, price_level_no_hourly], axis=0)
price_level.sort('1hr', ascending=False, inplace=True)
ad_level_prices = pandas.DataFrame(price_level.groupby('ad_id')['price_per_hour'].mean(), columns=['price_per_hour'])
ad_level = price_level.drop_duplicates('ad_id')[['ad_id','sex_ad','census_msa_code','cluster_id','date_str','is_massage_parlor_ad','1hr','incall','no_incall','outcall','no_outcall','incalloutcall','no_incalloutcall']]
out = pandas.merge(ad_level_prices, ad_level, left_index=True, right_on='ad_id', how='left')
# Clean up some unused data...
print('cleaning up old data...')
del data
del price_level
del ad_level
del ad_level_prices

# Filter out spam guys with > 200 ads in our sample period and save
spam = pandas.DataFrame(out.groupby('cluster_id').apply(lambda x: x.shape[0] > 200), columns=['spam'])
spam.reset_index(inplace=True)
out = out.merge(spam)
out.to_csv('ad_price_ad_level.csv', index=False)

del out['cluster_id']
del out['date_str']
#out = ts.merge(out, how='outer')

del out['census_msa_code']
#out = msa.merge(out, how='outer')
#out.to_csv('ad_price_ad_level_all.csv', index=False)

