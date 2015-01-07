#!/usr/bin/python

"""
This script loads the initial price data that Sam loaded from DeepDive on about 12/25/2014
"""
import pandas
import numpy as np
from sklearn import linear_model
from sklearn import datasets

#data = pandas.read_csv('rates_locs.csv')
data = pandas.read_csv('rates_pop.csv', sep='\t')
data['unit'] = data['time_str'].apply(lambda x: x.split(' ')[1])
data = data[data['unit'] != 'DURATION']
data['timeValue'] = data['time_str'].apply(lambda x: x.split(' ')[0])
data['unit'][data['unit'] == 'HOURS'] = 'HOUR'
data['minutes'] = np.nan
data['minutes'][data['unit']=='MINS'] = data['timeValue'][data['unit']=='MINS'].astype(np.integer)
data['minutes'][data['unit']=='HOUR'] = 60*data['timeValue'][data['unit']=='HOUR'].astype(np.integer)

#data['price'] = data['price'].apply(lambda x: x.replace('$',''))
#data['price'] = data['price'].apply(lambda x: x.replace('roses',''))
#data['price'] = data['price'].apply(lambda x: x.replace('rose',''))
#data['price'] = data['price'].apply(lambda x: x.replace('kisses',''))
#data['price'] = data['price'].apply(lambda x: x.replace('dollars',''))
#data = data[data['price'].apply(lambda x: 'euro' not in x)]
#data['price'] = data['price'].astype(np.integer)
# This code is useful for dealing with the 'price' string problem in
# sam's rates_locs file from 12/29


counts = pandas.DataFrame(data.groupby('ad_id')['ad_id'].count())
counts.rename(columns={'ad_id':'counts'}, inplace=True)
out = pandas.merge(data, counts,left_on='ad_id', right_index=True)
doubles = out.copy()
calcs=doubles.groupby('ad_id').agg({'price':['min','max'], 'minutes':['min','max']})
doubles = pandas.merge(doubles, pandas.DataFrame(calcs['price']['min']), left_on='ad_id', right_index=True)
doubles.rename(columns={'min':'p1'}, inplace=True)
doubles = pandas.merge(doubles, pandas.DataFrame(calcs['price']['max']), left_on='ad_id', right_index=True)
doubles.rename(columns={'max':'p2'}, inplace=True)
doubles = pandas.merge(doubles, pandas.DataFrame(calcs['minutes']['min']), left_on='ad_id', right_index=True)
doubles.rename(columns={'min':'m1'}, inplace=True)
doubles = pandas.merge(doubles, pandas.DataFrame(calcs['minutes']['max']), left_on='ad_id', right_index=True)
doubles.rename(columns={'max':'m2'}, inplace=True)
doubles['f'] = (doubles['p1'] * doubles['m2'] - doubles['m1'] * doubles['p2']) / (doubles['m2'] - doubles['m1'])
doubles=doubles[~doubles['ad_id'].duplicated()] # remove duplicates
doubles =doubles[doubles['m1'] != doubles['m2']] # remove those with two prices for the same time...
doubles.to_csv('intercept.csv', index=False)
out.index = range(out.shape[0])
out.reindex()
out.to_csv('prices_12292014.csv', index=False)
#reg = linear_model.LinearRegression()
#(p1 m2 - m1 p2)/ (m2 - m1)
#out = reg.fit(X=data.minutes.values[:,np.newaxis],y=data.price.values[:,np.newaxis])
