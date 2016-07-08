#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
This script takes ad price extraction and creates ad_prices_ad_level.csv (ad level clean price data)

It then creates a data set of only ads with two prices "doubles" with 
the implied fixed cost 
"""
import pandas as pd
import ipdb
import numpy as np
nrows = None

if False:
    data = pd.read_csv('data/forGiantOak3/rates.tsv.gz', sep='\t', compression='gzip', header=None, nrows=nrows)
else:
    data = pd.read_csv('data/forGiantOak3/rates2.tsv', sep='\t', header=None, nrows=nrows)


def all_call_merge(df, merge_dir):
    """
    Merge in incall, outcall, incalloutcall
    :param df: Dataframe that needs calls merged in
    :param str merge_dir: 'right' or 'left'
    :return: DataFrame with merged calls
    """
    for call_type in ['incall', 'outcall', 'incalloutcall']:
        fpath = 'data/forGiantOak6/{}-new.tsv'.format(call_type)
        call_input = '{}_input'.format(call_type)
        no_call_type = 'no_{}'.format(call_type)

        df = df.merge(pd.read_csv(fpath, sep='\t', header=None, names=['ad_id', call_input], nrows=nrows),
                      how=merge_dir)
        df[call_type] = df[call_input] == 1
        df[no_call_type] = df[call_input] == -1
        del df[call_input]

    return df


print('There are %s observations' % data.shape[0])  # about 2.1M
data.rename(columns={0: 'ad_id', 1: 'rate'}, inplace=True)
data['time_str'] = data['rate'].apply(lambda x: x.split(',')[1])
data['price'] = data['rate'].apply(lambda x: x.split(',')[0])
data['unit'] = data['time_str'].apply(lambda x: x.split(' ')[1])
data = data[data['unit'] != 'DURATION']  # about 1.7M
print('There are %s observations after dropping no duration prices' % data.shape[0])
data['timeValue'] = data['time_str'].apply(lambda x: x.split(' ')[0])
data['unit'][data['unit'] == 'HOURS'] = 'HOUR'
data['minutes'] = np.nan
data.minutes.loc[data['unit'] == 'MINS'] = data.timeValue.loc[data['unit'] == 'MINS'].astype(np.integer)
data.minutes.loc[data['unit'] == 'HOUR'] = 60*data.timeValue.loc[data['unit'] == 'HOUR'].astype(np.integer)

dollar_synonyms = ['$', 'roses', 'rose', 'bucks', 'kisses', 'kiss', 'dollars', 'dollar', 'dlr']
for d_s in dollar_synonyms:
    data.ix[:, 'price'] = data['price'].apply(lambda x: x.replace(d_s, ''))

other_currencies = ['euro', 'eur', 'eu', 's', '¥', '\xef\xbc\x90', 'aud']
for o_c in other_currencies:
    data = data.ix[data['price'].apply(lambda x: o_c not in x)]

print('There are %s prices after dropping foreign prices' % data.shape[0])
data.ix[:, 'price'] = data['price'].astype('int')
# This code is useful for dealing with the 'price' string problem in
# sam's rates_locs file from 12/29

# Begin merging information from census
if False:
    sexad = pd.read_csv('data/forGiantOak3/isssexad.tsv.gz',
                        sep='\t', header=None, compression='gzip', nrows=nrows)
    sexad.rename(columns={0: 'ad_id', 1: 'sex_ad'}, inplace=True)
else:
    sexad = pd.read_csv('data/forGiantOak3/isssexad.tsv',
                        sep='\t', header=None, nrows=nrows)
    sexad.rename(columns={0: 'ad_id', 1: 'sex_ad'}, inplace=True)
data = pd.merge(data, sexad, on='ad_id', how='left')
del sexad
# data = data[data['sex_ad'] == 1] # remove non- sex ads
# print('There are %s prices after dropping Non-sex ad prices' % data.shape[0])


# Merge in massage parlor information
massage = pd.read_csv('data/forGiantOak3/ismassageparlorad.tsv', sep='\t', header=None, nrows=nrows)
massage.rename(columns={0: 'ad_id', 1: 'massage_ad'}, inplace=True)
data = pd.merge(data, massage, on='ad_id', how='left')
del massage

counts = pd.DataFrame(data.groupby('ad_id')['ad_id'].count())
print('The %s extracted prices pertain to %s observations' % (data.shape[0], counts.shape[0]))
counts.rename(columns={'ad_id': 'prices_from_ad'}, inplace=True)
out = pd.merge(data, counts, left_on='ad_id', right_index=True)
del counts

# Begin using MSA data
if False:
    msa = pd.read_csv('data/forGiantOak3/msa_locations.tsv.gz',
                      sep='\t', header=None, compression='gzip', names=['ad_id' 'census_msa_code'], nrows=nrows)
else:
    msa = pd.read_csv('data/forGiantOak3/msa_locations.tsv',
                      sep='\t', header=None, names=['ad_id', 'census_msa_code'], nrows=nrows)
if False:
    cluster = pd.read_csv('data/forGiantOak3/msa_locations.tsv.gz',
                          sep='\t', header=None, compression='gzip', names=['ad_id', 'census_msa_code'],
                          nrows=nrows)
else:
    msa = pd.read_csv('data/forGiantOak3/msa_locations.tsv',
                      sep='\t', header=None, names=['ad_id', 'census_msa_code'], nrows=nrows)
out = pd.merge(out, msa, how='left')  # Add census MSA code to the fixed price info
# del msa

# Merge in cluster ID
if False:
    ts = pd.read_csv('data/forGiantOak3/doc-provider-timestamp.tsv.gz',
                     sep='\t', header=None, compression='gzip', names=['ad_id', 'cluster', 'date_str'], nrows=nrows)
else:
    ts = pd.read_csv('data/forGiantOak3/doc-provider-timestamp.tsv',
                     sep='\t', header=None, names=['ad_id', 'cluster_id', 'date_str'], nrows=nrows)
out = out.merge(ts, how='left')
# del ts
out[out['cluster_id'] == '\N'] = np.nan
out[out['date_str'] == '\N'] = np.nan


# Merge in massage parlor flag
massage = pd.read_csv('data/forGiantOak3/ismassageparlorad.tsv',
                      sep='\t', header=None, names=['ad_id', 'is_massage_parlor_ad'], nrows=nrows)
out = out.merge(massage, how='left')
del massage


out = all_call_merge(out, 'left')


del out['unit']
del out['timeValue']
out.to_csv('ad_prices_price_level.csv', index=False)

# Begin work on fixed prices
out = out[out['prices_from_ad'] == 2]
print('There are %s ads after restricting to ads with 2 prices' % out.shape[0])

calcs = out.groupby('ad_id').agg({'price': ['min', 'max'], 'minutes': ['min', 'max']})
out = pd.merge(out, pd.DataFrame(calcs['price']['min']), left_on='ad_id', right_index=True)
out.rename(columns={'min': 'p1'}, inplace=True)
out = pd.merge(out, pd.DataFrame(calcs['price']['max']), left_on='ad_id', right_index=True)
out.rename(columns={'max': 'p2'}, inplace=True)
out = pd.merge(out, pd.DataFrame(calcs['minutes']['min']), left_on='ad_id', right_index=True)
out.rename(columns={'min': 'm1'}, inplace=True)
out = pd.merge(out, pd.DataFrame(calcs['minutes']['max']), left_on='ad_id', right_index=True)
out.rename(columns={'max': 'm2'}, inplace=True)
out['zero_price'] = (out['p1'] * out['m2'] - out['m1'] * out['p2']) / (out['m2'] - out['m1'])
out = out[~out['ad_id'].duplicated()]  # remove duplicates
print('There are %s ads after dropping duplicates' % out.shape[0])
out = out[out['m1'] != out['m2']]  # remove those with two prices for the same time...
out['marginal_price'] = (out['p2'] - out['p1']) / (out['m2'] - out['m1']) * 60
out.to_csv('ad_zero_prices.csv', index=False)

# Re-read ad_prices_price_level.csv to aggregate from file
del out
data = pd.read_csv('ad_prices_price_level.csv')
print(data.shape)
# Begin computing price per hour:
# If we have a 1 hour price, that's it. Otherwise, multiply all the
# quoted prices by a 'multiplier' which represents the average ratio of
# hourly price to the given time period price

try:
    price_ratios = pd.read_csv('price_ratios.csv')
except:
# The below blocks of code compute 'price_ratios' which is the ratio of
# average prices for 1 hour for other ads that also posted the same
# price
    minute_values = pd.Series((data['minutes'].value_counts()/data.shape[0] > .0001).index.map(int))
    minute_string_series = minute_values.map(lambda x: 'price_%s_mins' % x)
    minute_string_series.index = minute_values


    def get_prices(x):
        out = pd.Series(np.nan, index=minute_values)
        for mins in minute_values:
            matching_prices = x[x['minutes'] == mins]['price']
            if len(matching_prices):
                out[mins] = matching_prices.mean()
        return out

    me = data.groupby('ad_id').apply(get_prices)  # This is REALLLLY slow
    price_ratios = pd.Series(np.nan, index=minute_values)
    price_ratios_counts = pd.Series(np.nan, index=minute_values)
    for m in minute_values:
        hour_price = me[(~me[60].isnull()) & (~me[m].isnull())][60].mean()
        m_price = me[(~me[m].isnull()) & (~me[m].isnull())][m].mean()
        price_ratios[m] = hour_price/m_price
        price_ratios_counts[m] = me[(~me[m].isnull()) & (~me[m].isnull())].shape[0]

    print('Computed price ratios for acts of given length to acts of 1 hour')
    print(price_ratios)
    print(price_ratios_counts)
    price_ratios.to_csv('price_ratios.csv', index=False)

# Now split the data by whether there's a posted price of 1 hr
data['1hr'] = data['time_str'] == '1 HOUR'
a = data.groupby('ad_id')['1hr'].sum()
a = a > 0
del data['1hr']
a = pd.DataFrame(a)
data = pd.merge(data, a, left_on='ad_id', right_index=True)
price_level_hourly = pd.DataFrame(data[data['1hr']])
price_level_hourly['price_per_hour'] = price_level_hourly['price']  # If there's an hourly price, use it
price_level_no_hourly = pd.DataFrame(data[~data['1hr']])
price_level_no_hourly.index = price_level_no_hourly['ad_id']
# Otherwise use the multiplier
price_level_no_hourly['multiplier'] = price_level_no_hourly['minutes'].apply(lambda x: price_ratios[x])
price_level_no_hourly['price_per_hour'] = price_level_no_hourly['price'] * price_level_no_hourly['multiplier']
price_level_no_hourly_prices = pd.DataFrame(price_level_no_hourly.groupby('ad_id')['price_per_hour'].mean())
price_level_no_hourly['price_per_hour'] = price_level_no_hourly_prices
price_level = pd.concat([price_level_hourly, price_level_no_hourly], axis=0)
price_level.sort('1hr', ascending=False, inplace=True)
ad_level_prices = pd.DataFrame(price_level.groupby('ad_id')['price_per_hour'].mean(), columns=['price_per_hour'])
ad_level = price_level.drop_duplicates('ad_id')[['ad_id', 'sex_ad', 'census_msa_code', 'cluster_id', 'date_str',
                                                 'is_massage_parlor_ad', '1hr', 'incall', 'no_incall', 'outcall',
                                                 'no_outcall', 'incalloutcall', 'no_incalloutcall']]
out = pd.merge(ad_level_prices, ad_level, left_index=True, right_on='ad_id', how='left')
# Clean up some unused data...
print('cleaning up old data...')
del data
del price_level
del price_level_hourly
del price_level_no_hourly
del price_level_no_hourly_prices
del ad_level
del ad_level_prices
for col in ['sex_ad','is_massage_parlor_ad','1hr','incall','no_incall','outcall','no_outcall','incalloutcall','no_incalloutcall']:
    out[col] = out[col].astype('bool')

# Filter out spam guys with > 200 ads in our sample period and save
spam = pd.DataFrame(out.groupby('cluster_id').apply(lambda x: x.shape[0] > 200), columns=['spam'])
spam.reset_index(inplace=True)
out = out.merge(spam)
del spam

# Add site filter
out['site'] = out['ad_id'].apply(lambda x: x.split(':')[0])

# Compute the cluster size
out = out.merge(out.groupby('cluster_id').size().to_frame('cluster_count').reset_index())
out.to_csv('ad_price_ad_level.csv', index=False)

# Now begin rebuilding DF by merging in original raw files, so we can
# see how much stuff is missing...
del out['cluster_id']
del out['date_str']
out = ts.merge(out, how='outer')
del ts

del out['census_msa_code']
for col in ['is_massage_parlor_ad','1hr','incall','no_incall','outcall','no_outcall','incalloutcall','no_incalloutcall']:
    out[col] = out[col].fillna(False).to_sparse()
import gc
gc.collect()
out = msa.merge(out, how='outer')

phones=pd.read_csv('data/forGiantOak3/phone_numbers.tsv', sep='\t', header=None, names=['ad_id','phone'])
steve_phones = pd.read_csv('data/bach/phones.csv')
phones = phones.merge(steve_phones, how='left')
out = out.merge(phones, how='left')

# Merge in massage parlor flag
del out['is_massage_parlor_ad']
massage = pd.read_csv('data/forGiantOak3/ismassageparlorad.tsv',
                      sep='\t', header=None, names=['ad_id', 'is_massage_parlor_ad'], nrows=nrows)
out = out.merge(massage, how='right')
del massage

del out['incall']
del out['outcall']
del out['incalloutcall']
ipdb.set_trace()
out = all_call_merge(out, 'right')

out.to_csv('ad_price_ad_level_all.csv', index=False)
