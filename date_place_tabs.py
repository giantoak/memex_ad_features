#!/usr/bin/python
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from datetime import datetime

# dates = pd.read_csv('data/escort_cdr_2/post_date-dom.tsv', sep='\t',names=['ad_id','date'])
# ads =

try:
    ad_level_prices = pd.read_csv('data/temp/ad_level_temp.csv')
except:
    data = pd.read_csv('data/escort_cdr_2/rates-text.tsv', sep='\t', header=None, names=['ad_id', 'rate'])
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
    data.minutes.loc[data['unit'] == 'MINS'] = data.timeValue.loc[data['unit'] == 'MINS'].astype(np.int)
    data.minutes.loc[data['unit'] == 'HOUR'] = 60 * data.timeValue.loc[data['unit'] == 'HOUR'].astype(np.int)

    dollar_synonyms = ['$', 'roses', 'rose', 'bucks', 'kisses', 'kiss', 'dollars', 'dollar', 'dlr']
    for d_s in dollar_synonyms:
        data.ix[:, 'price'] = data['price'].apply(lambda x: x.replace(d_s, ''))

    other_currencies = ['euro', 'eur', 'eu', 's', '¥', '\xef\xbc\x90', 'aud']
    for o_c in other_currencies:
        data = data.ix[data['price'].apply(lambda x: o_c not in x)]

    print('There are %s prices after dropping foreign prices' % data.shape[0])
    data.ix[:, 'price'] = data['price'].astype('int')
    data['1hr'] = data['time_str'] == '1 HOUR'
    a = data.groupby('ad_id')['1hr'].sum()
    a = a > 0
    del data['1hr']
    a = pd.DataFrame(a)
    price_ratios = {60: 1.000000,
                    30: 1.559643,
                    15: 1.694609,
                    120: 0.620485,
                    45: 1.351469,
                    }
    data = pd.merge(data, a, left_on='ad_id', right_index=True)
    price_level_hourly = pd.DataFrame(data[data['1hr']])
    price_level_hourly['price_per_hour'] = price_level_hourly['price']  # If there's an hourly price, use it
    price_level_no_hourly = pd.DataFrame(data[~data['1hr']])
    price_level_no_hourly.index = price_level_no_hourly['ad_id']
    # Otherwise use the multiplier
    try:
        price_level_no_hourly['multiplier'] = price_level_no_hourly['minutes'].apply(lambda x: price_ratios[x])
    except:
        print('error using price ratio multiplier... setting price per hour equal to quoted price')
        price_level_no_hourly['multiplier'] = 1
    price_level_no_hourly['price_per_hour'] = price_level_no_hourly['price'] * price_level_no_hourly['multiplier']
    price_level_no_hourly_prices = pd.DataFrame(price_level_no_hourly.groupby('ad_id')['price_per_hour'].mean())
    price_level_no_hourly['price_per_hour'] = price_level_no_hourly_prices
    price_level = pd.concat([price_level_hourly, price_level_no_hourly], axis=0)
    price_level.sort('1hr', ascending=False, inplace=True)
    ad_level_prices = pd.DataFrame(price_level.groupby('ad_id')['price_per_hour'].mean(), columns=['price_per_hour'])
    # ad_level = price_level.drop_duplicates('ad_id')[['ad_id', 'sex_ad', 'census_msa_code', 'cluster_id', 'date_str',
    # 'is_massage_parlor_ad', '1hr', 'incall', 'no_incall', 'outcall',
    # 'no_outcall', 'incalloutcall', 'no_incalloutcall']]
    # data = pd.merge(ad_level_prices, ad_level, left_index=True, right_on='ad_id', how='left')

    ad_level_prices = ad_level_prices.reset_index()
    ad_level_prices.to_csv('data/temp/ad_level_temp.csv', index=False)

dates = pd.read_table('data/escort_cdr_2/post_date-dom.tsv',
                      names=['ad_id', 'date_str'])
# ads =
data = dates.merge(ad_level_prices, how='left')
data['date'] = data['date_str'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
data.index = pd.DatetimeIndex(data['date'])
data = data.reindex()
###
# df.groupby([pd.Grouper(freq='1M',key='Date'),'Buyer']).sum()
# http://pd.pydata.org/pandas-docs/stable/groupby.html

names = ['ad_id', 'location_text', 'some_code', 'city', 'city_again', 'state', 'country', 'city_type', 'source', 'lat',
         'lon']
locations = pd.read_table('data/escort_cdr_2/locations-combined.tsv',
                          names=names)
data = data.merge(locations[['ad_id', 'city', 'state', 'city_again', 'country']])
data = data[data['country'] == 'United States']
# counts=data['ad_id'].resample('M',how='count')
city_panel = data.groupby([pd.Grouper(freq='M', key='date'), 'city']).size()
city_panel.to_csv('city_counts.csv')
city_panel = data.groupby([pd.Grouper(freq='Q', key='date'), 'city']).size()
city_panel.to_csv('city_counts_quarter.csv')
state_panel = data.groupby([pd.Grouper(freq='M', key='date'), 'state']).size()
state_panel.to_csv('state_counts.csv')
state_panel = data.groupby([pd.Grouper(freq='Q', key='date'), 'state']).size()
state_panel.to_csv('state_counts_quarter.csv')
city_again_panel = data.groupby([pd.Grouper(freq='M', key='date'), 'city_again']).size()
city_again_panel.to_csv('city_again_counts.csv')
city_again_panel = data.groupby([pd.Grouper(freq='Q', key='date'), 'city_again']).size()
city_again_panel.to_csv('city_again_counts_quarter.csv')
