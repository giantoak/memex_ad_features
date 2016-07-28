#!/usr/bin/python
# -*- coding: utf-8 -*-
import pandas as pd
import ipdb
import numpy as np
import phonenumbers
import datetime
def parse_phone(x):
    if not x:
        return('')
    try:
        if np.isnan(x):
            return('')
    except:
        pass
    if x.__class__ == np.float64:
        to_parse = str(int(x))
    else:
        to_parse = x
    try:
        parsed=phonenumbers.parse(to_parse, 'US')
        out_str = phonenumbers.format_number(parsed,phonenumbers.PhoneNumberFormat.NATIONAL)  
        return(out_str)
    except ValueError as e:
        ipdb.set_trace()
        return('')
    except Exception as e:
        ipdb.set_trace()
ad_ids = pd.read_csv('ad_id_list.csv', header=None, names=['ad_id'])
phones = pd.read_csv('data/escort_cdr_2/phones-combined.tsv', sep='\t', header=None, names=['ad_id','phone'])
phones['phone_1'] = phones['phone'].apply(lambda x: x.split('|')[0])

# Repeat ad price stuff
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
data['1hr'] = data['time_str'] == '1 HOUR'
a = data.groupby('ad_id')['1hr'].sum()
a = a > 0
del data['1hr']
a = pd.DataFrame(a)
price_ratios = {60:    1.000000,
        30 :   1.559643,
        15 :   1.694609,
        120:   0.620485,
        45 :   1.351469,
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
#ad_level = price_level.drop_duplicates('ad_id')[['ad_id', 'sex_ad', 'census_msa_code', 'cluster_id', 'date_str',
                                                 #'is_massage_parlor_ad', '1hr', 'incall', 'no_incall', 'outcall',
                                                 #'no_outcall', 'incalloutcall', 'no_incalloutcall']]
#data = pd.merge(ad_level_prices, ad_level, left_index=True, right_on='ad_id', how='left')


ad_level_prices= ad_level_prices.reset_index()
ad_level_prices[['ad_id','price_per_hour']].to_csv('ad_price_ad_level.csv', index=False)
ads = pd.merge(ad_ids, phones[['ad_id','phone_1']], how='left')
ads = pd.merge(ads, ad_level_prices[['ad_id','price_per_hour']], how='left')

start = datetime.datetime.now()
prices = pd.read_csv('data/TGG/provider_prices.csv')
stats = pd.read_csv('data/TGG/provider_stats.csv')
del stats['source']
del stats['service']
out = prices.merge(stats, how='left')
review_prices = out.groupby('phone_1')['price'].mean().reset_index().rename(columns={'price':'review_price'})
review_prices['phone_1'] = review_prices['phone_1'].apply(parse_phone) 
out['phone_1'] = out['phone_1'].apply(parse_phone) 

me=ads.merge(review_prices, how='left')
print('There are %s ads with missing phone' % me['phone_1'].isnull().sum())
phone_level = me.groupby('phone_1')[['price_per_hour','review_price']].mean().reset_index()
print('There are %s unique phone numbers in the review data' % out['phone_1'].unique().shape[0])
print('There are prices from reviews for %s unique phones' % phone_level['review_price'].notnull().sum())
print('There are %s unique phones from ads' % ads['phone_1'].unique().shape[0])
print('Review data ads prices to %s phone numbers' % ((phone_level['price_per_hour'].isnull()) & ~(phone_level['review_price'].isnull())).sum())


phones_in_reviews_with_no_ad_price=phone_level.loc[((phone_level['price_per_hour'].isnull()) & ~(phone_level['review_price'].isnull()))]['phone_1'].tolist()
phones_in_reviews_with_ad_price=phone_level.loc[((phone_level['price_per_hour'].notnull()) & ~(phone_level['review_price'].isnull()))]['phone_1'].tolist()
phones_in_reviews_with_or_without_ad_price=phones_in_reviews_with_ad_price + phones_in_reviews_with_no_ad_price
matching_reviews = out[out['phone_1'].isin(phones_in_reviews_with_or_without_ad_price)]
matching_ads = ads[ads['phone_1'].isin(phones_in_reviews_with_or_without_ad_price)]
matching_ads_with_price = ads[ads['phone_1'].isin(phones_in_reviews_with_ad_price)]
matching_ads_no_price = ads[ads['phone_1'].isin(phones_in_reviews_with_no_ad_price)]
non_matching_ads = ads[~ads['phone_1'].isin(phones_in_reviews_with_or_without_ad_price)]
print('Price distribution in ads that do not match reviews:')
print(non_matching_ads['price_per_hour'].describe())
print('Price distribution in ads that do match reviews:')
print(matching_ads['price_per_hour'].describe())
print('Price distribution in reviews that do match any ads:')
print(out[out['phone_1'].isin(phones_in_reviews_with_or_without_ad_price)]['price'].describe())
print('Price distribution in reviews that do NOT match any ads:')
print(out[~out['phone_1'].isin(phones_in_reviews_with_or_without_ad_price)]['price'].describe())

all_ads_with_review_phones=ads[ads['phone_1'].isin(phones_in_reviews_with_no_ad_price)]
print('There are %s ads with no price associated with reviews with a price' % all_ads_with_review_phones.shape[0])
print('There are %s ads with a price associated with reviews with a price' % matching_ads_with_price.shape[0])
all_reviews_with_matched_phones=out[out['phone_1'].isin(phones_in_reviews_with_ad_price)]
print('There are %s reviews associated with the %s phone numbers where ads are matched' % (all_reviews_with_matched_phones.shape[0], ((phone_level['price_per_hour'].notnull()) & ~(phone_level['review_price'].isnull())).sum()))
print('There are %s reviews associated with the %s phone numbers which match ads' % (out[out['phone_1'].isin(phones_in_reviews_with_or_without_ad_price)].shape[0], len(phones_in_reviews_with_or_without_ad_price)))
print('There are %s ads associated with the %s phone numbers which match ads and reviews' % (ads[ads['phone_1'].isin(phones_in_reviews_with_or_without_ad_price)].shape[0], len(phones_in_reviews_with_or_without_ad_price)))

print('Matching phone number review count distribution:')
print((matching_reviews.phone_1.value_counts().value_counts()/matching_reviews.phone_1.unique().shape[0]).head(10))
print('Overall review count distribution:')
print((out.phone_1.value_counts().value_counts()/out.phone_1.unique().shape[0]).head(10))
print('Matching phone number ad count distribution:')

print('Exploring missing ad prices:')
print('Fraction of all matching ads with missing prices: %0.4f'% matching_ads.price_per_hour.isnull().mean())
print('Fraction of all ads with missing prices: %0.4f'% ads.price_per_hour.isnull().mean())
print('Fraction of all matching ads with at least one price with missing prices: %0.4f'% matching_ads_with_price.price_per_hour.isnull().mean())
print((matching_ads.phone_1.value_counts().value_counts()/matching_ads.phone_1.unique().shape[0]).head(10))
