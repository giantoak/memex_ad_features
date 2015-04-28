import pandas
import datetime
import ipdb
import json
import numpy as np

# Merge in QCEW data
instruments = pandas.read_csv('month_msa_wage_instruments.csv')
month_msa_chars = instruments.copy()

# Merge in UCR data
ucr = pandas.read_csv('ucr.csv')
month_msa_chars = month_msa_chars.merge(ucr, how='outer')

# Merge in the msa-month price aggregates
prices = pandas.read_csv('msa_month_ad_aggregates.csv')
month_msa_chars = month_msa_chars.merge(prices, how='outer')

# Compute statistics at the provider level

panel = pandas.read_csv('provider_panel.csv')

movers1=panel.groupby(['census_msa_code','month','year'])['moved_since_l1'].agg([np.sum,np.mean])
movers1.rename(columns={'sum':'total_movers_l1', 'mean':'share_movers_l1'}, inplace=True)
movers2=panel.groupby(['census_msa_code','month','year'])['moved_since_l2'].agg([np.sum,np.mean])
movers2.rename(columns={'sum':'total_movers_l2', 'mean':'share_movers_l2'}, inplace=True)
movers3=panel.groupby(['census_msa_code','month','year'])['moved_since_l3'].agg([np.sum,np.mean])
movers3.rename(columns={'sum':'total_movers_l3', 'mean':'share_movers_l3'}, inplace=True)
active_providers = pandas.DataFrame(panel.groupby(['census_msa_code','month','year'])['successfully_merged_msa'].sum())
active_providers.rename(columns={'successfully_merged_msa':'active_providers'}, inplace=True)
active_providers_imputed = pandas.DataFrame(panel[~panel['census_msa_code'].isnull()].groupby(['census_msa_code','month','year'])['census_msa_code'].size())
active_providers_imputed.rename(columns={0:'active_providers_imputed'}, inplace=True)

# Compute price aggregates at the month-msa level
prices = panel.groupby(['census_msa_code','month','year'])['avg_price_per_hour_in_cluster_month'].aggregate({'ad_price_avg':np.mean, 'ad_price_p25':lambda x: np.percentile(x,.25), 'ad_price_p75':lambda x: np.percentile(x,.75), 'ad_price_std':np.std}) # Note: Prices are merged on at only when there were ACTUAL ads, which is NOT every cluster-month that exists in this data.

# Work to compute moments of the price distribution among
# movers/non-movers
moved_since_l1 = panel['moved_since_l1'] == 1
not_moved_since_l1 = panel['moved_since_l1'] == 0
mover_prices = panel[moved_since_l1].groupby(['census_msa_code','month','year'])['avg_price_per_hour_in_cluster_month'].aggregate({'ad_price_avg_movers':np.mean, 'ad_price_p25_movers':lambda x: np.percentile(x,.25), 'ad_price_p75_movers':lambda x: np.percentile(x,.75), 'ad_price_std_movers':np.std}) 
nonmover_prices = panel[not_moved_since_l1].groupby(['census_msa_code','month','year'])['avg_price_per_hour_in_cluster_month'].aggregate({'ad_price_avg_nonmovers':np.mean, 'ad_price_p25_nonmovers':lambda x: np.percentile(x,.25), 'ad_price_p75_nonmovers':lambda x: np.percentile(x,.75), 'ad_price_std_nonmovers':np.std}) 


# Merge the msa-month level data together
month_msa_chars = pandas.merge(month_msa_chars, movers1, left_on=['census_msa_code','month','year'], right_index=True, how='outer')
month_msa_chars = pandas.merge(month_msa_chars, movers2, left_on=['census_msa_code','month','year'], right_index=True, how='outer')
month_msa_chars = pandas.merge(month_msa_chars, movers3, left_on=['census_msa_code','month','year'], right_index=True, how='outer')
month_msa_chars = pandas.merge(month_msa_chars, active_providers, left_on=['census_msa_code','month','year'], right_index=True, how='outer')
month_msa_chars = pandas.merge(month_msa_chars, active_providers_imputed, left_on=['census_msa_code','month','year'], right_index=True, how='outer')
month_msa_chars = pandas.merge(month_msa_chars, prices, left_on=['census_msa_code','month','year'], right_index=True, how='left')
month_msa_chars = pandas.merge(month_msa_chars, mover_prices, left_on=['census_msa_code','month','year'], right_index=True, how='left')
month_msa_chars = pandas.merge(month_msa_chars, nonmover_prices, left_on=['census_msa_code','month','year'], right_index=True, how='left')

panel.groupby(['census_msa_code','month','year'])

month_msa_chars.to_csv('msa_month_characteristics.csv')
