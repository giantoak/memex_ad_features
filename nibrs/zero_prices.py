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
    data = pandas.read_csv('forGiantOak/rates.tsv.gz', sep='\t', compression='gzip', header=None)
else:
    data = pandas.read_csv('forGiantOak/rates.tsv', sep='\t', header=None)
data.rename(columns={0:'ad_id', 1:'rate'}, inplace=True)
data['time_str'] = data['rate'].apply(lambda x: x.split(',')[1])
data['price'] = data['rate'].apply(lambda x: x.split(',')[0])
data['unit'] = data['time_str'].apply(lambda x: x.split(' ')[1])
data = data[data['unit'] != 'DURATION']
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
data['price'] = data['price'].astype('int')
# This code is useful for dealing with the 'price' string problem in
# sam's rates_locs file from 12/29

data['price_per_hour'] = 60*data['price']/data['minutes']
# Begin merging information from census

data.to_csv('normalized_prices.csv', index=False)

counts = pandas.DataFrame(data.groupby('ad_id')['ad_id'].count())
counts.rename(columns={'ad_id':'counts'}, inplace=True)
counts.to_csv('price_extraction_counts.csv')
pandas.DataFrame(counts['counts'].value_counts()/counts['counts'].value_counts().sum(), columns=['distribution']).to_csv('num_prices_extracted_dist.csv')
out = pandas.merge(data, counts,left_on='ad_id', right_index=True)
doubles = out.copy()
doubles = doubles[doubles['counts']==2]
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
doubles =doubles[doubles['m1'] != doubles['m2']] # remove those with two prices for the same time...
doubles['marginal_price'] = (doubles['p2'] - doubles['p1']) / (doubles['m2'] - doubles['m1'])
doubles.to_csv('zero_price.csv', index=False)
out.index = range(out.shape[0])
out.reindex()
out.to_csv('ad_prices.csv', index=False)
#reg = linear_model.LinearRegression()
#(p1 m2 - m1 p2)/ (m2 - m1)
#out = reg.fit(X=data.minutes.values[:,np.newaxis],y=data.price.values[:,np.newaxis])

# Begin using MSA data
msa = pandas.read_csv('forGiantOak/msa_locations.tsv.gz', sep='\t', header=None, compression='gzip', names=['ad_id','census_msa_code'])
doubles = pandas.merge(doubles, msa) # Add census MSA code to the fixed price info
#msa_features_panel = pandas.read_csv('all_merged.csv', index_col=['month','year','census_msa_code'])
msa_features_panel = pandas.read_csv('all_merged.csv')
msa_features = msa_features_panel[(msa_features_panel['month'] == 12) & (msa_features_panel['year']==2013)]
#msa_features = msa_features_panel.xs(12, level='month').xs(2013, level='year') # Grab a single year
zero_price = pandas.merge(doubles, msa_features, left_on='census_msa_code', right_on='census_msa_code')
zero_price = zero_price[zero_price.zero_price > 0]
zero_price = zero_price[zero_price.zero_price < 200] # very few are above 200
zero_price.to_csv('zero_price_msa_micro.csv', index=False)

zp_aggregates = zero_price.groupby('census_msa_code')['zero_price'].aggregate({ 'zero_price_count':len,'zp_mean':np.mean, 'zp_p50':lambda x: np.percentile(x,q=50), 'zp_p10':lambda x: np.percentile(x, q=10), 'zp_p90':lambda x: np.percentile(x, q=90)})
msa_aggregates=pandas.merge(msa_features, zp_aggregates, left_on='census_msa_code', right_index=True)
msa_aggregates.to_csv('zero_price_msa_aggregates.csv', index=False)

# Begin merging msa info into price data
ad_level = pandas.DataFrame(data.groupby('ad_id')['price_per_hour'].mean())
ad_level = pandas.merge(ad_level, msa, left_index=True, right_on='ad_id', how='left') # Note: we drop lots of ads with price  but not MSA
ad_level = pandas.merge(ad_level, msa_features, how='left')
ad_level = pandas.merge(counts, ad_level, left_index=True, right_on='ad_id',how='left')
ad_level = ad_level.drop_duplicates('ad_id')
ad_level.to_csv('ad_prices_msa_micro.csv',index=False)

# Begin aggregating ad level data up to the MSA level
ad_aggregate_prices = ad_level.groupby('census_msa_code')['price_per_hour'].aggregate({'median':np.median, 'ad_count':len,'mean':np.mean, 'p50':lambda x: np.percentile(x,q=50), 'p10':lambda x: np.percentile(x, q=10), 'p90':lambda x: np.percentile(x, q=90)})
ad_aggregate_prices = pandas.merge(ad_aggregate_prices, msa_features, left_index=True, right_on='census_msa_code')
msa_counts = ad_level.groupby('census_msa_code')['counts'].aggregate({'prices_per_ad':np.mean, 'fraction_zero_price':lambda x: (x == 2).mean()})
ad_aggregate_prices = pandas.merge(ad_aggregate_prices, msa_counts, left_on='census_msa_code', right_index=True)
ad_aggregate_prices.to_csv('ad_prices_msa.csv', index=False)

j=out.copy()
j.reset_index(inplace=True)    
j['date_str'] = j.apply(lambda x: str(x['month']) + '-' + str(x['year']), axis=1)   
import datetime
j['dp']=j['date_str'].apply(lambda x: pandas.Period(x, 'M'))
subset = j[['dp','census_msa_code']]
subset.to_records(index=False).tolist()
index = pandas.MultiIndex.from_tuples(subset.to_records(index=False).tolist(), names=subset.columns.tolist())
j.index = index
j.reindex()
j.rename(columns={'female_mean.wage':'female_mean','male_mean.wage':'male_mean','female_sum.wght':'female_num_jobs', 'male_sum.wght':'male_num_jobs'}, inplace=True)
panel = j.to_panel()
# Panel is our panel object
diff_cols = ['female_p25','female_p50','female_p75','female_mean.wght','female_sum.wght','male_p25','male_p50','male_p75','male_mean.wght','male_sum.wght']
diff_cols = ['female_p25','female_p50','female_p75','male_p25','male_p50','male_p75', 'female_num_jobs','male_num_jobs','female_mean','male_mean']
for col in diff_cols:
    panel['d_' + col] = panel[col] - panel[col].shift(-1)
    panel['d_%s_pos'% col] = panel['d_' + col] > 0 # Generate dummies for positive and negative changes
# Use panel functionality to take first differences

panel.to_frame().to_csv('monthly_panel.csv')
