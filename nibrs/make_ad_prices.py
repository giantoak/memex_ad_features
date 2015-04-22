#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
This script takes ad price extraction and creates ad_prices.csv (ad level clean price data)

It then creates a data set of only ads with two prices "doubles" with 
the implied fixed cost 
"""
import pandas
import datetime
import ipdb
import json
import numpy as np

if False:
    data = pandas.read_csv('forGiantOak3/rates.tsv.gz', sep='\t', compression='gzip', header=None)
else:
    data = pandas.read_csv('forGiantOak3/rates2.tsv', sep='\t', header=None)

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

data['price_per_hour'] = 60*data['price']/data['minutes']
# Begin merging information from census
if False:
    sexad = pandas.read_csv('forGiantOak3/isssexad.tsv.gz', sep='\t', header=None,compression='gzip')
    sexad.rename(columns={0:'ad_id', 1:'sex_ad'}, inplace=True)
else:
    sexad = pandas.read_csv('forGiantOak3/isssexad.tsv', sep='\t', header=None)
    sexad.rename(columns={0:'ad_id', 1:'sex_ad'}, inplace=True)
data = pandas.merge(data, sexad, on='ad_id', how='left')
data.to_csv('normalized_prices.csv', index=False)
data = data[data['sex_ad'] == 1] # remove non- sex ads
#print('There are %s prices after dropping Non-sex ad prices' % data.shape[0])

counts = pandas.DataFrame(data.groupby('ad_id')['ad_id'].count())
print('The %s extracted prices pertain to %s observations' % (data.shape[0], counts.shape[0]))
counts.rename(columns={'ad_id':'prices_from_ad'}, inplace=True)
out = pandas.merge(data, counts,left_on='ad_id', right_index=True)

# Begin using MSA data
if False:
    msa = pandas.read_csv('forGiantOak3/msa_locations.tsv.gz', sep='\t', header=None, compression='gzip', names=['ad_id','census_msa_code'])
else:
    msa = pandas.read_csv('forGiantOak3/msa_locations.tsv', sep='\t', header=None, names=['ad_id','census_msa_code'])
if False:
    cluster = pandas.read_csv('forGiantOak3/msa_locations.tsv.gz', sep='\t', header=None, compression='gzip', names=['ad_id','census_msa_code'])
else:
    msa = pandas.read_csv('forGiantOak3/msa_locations.tsv', sep='\t', header=None, names=['ad_id','census_msa_code'])
out = pandas.merge(out, msa, how='left') # Add census MSA code to the fixed price info

# Merge in cluster ID
if False:
    ts = pandas.read_csv('forGiantOak3/doc-provider-timestamp.tsv.gz', sep='\t', header=None, compression='gzip', names=['ad_id','cluster','date_str'])
else:
    ts = pandas.read_csv('forGiantOak3/doc-provider-timestamp.tsv', sep='\t', header=None, names=['ad_id','cluster_id','date_str'])
out = out.merge(ts, how='left')
out[out['cluster_id'] == '\N'] = ''

# Merge in massage parlor flag
massage = pandas.read_csv('forGiantOak3/ismassageparlorad.tsv', sep='\t', header=None, names=['ad_id','is_massage_parlor_ad'])
out = out.merge(massage, how='left')

out.to_csv('ad_prices.csv', index=False)

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
