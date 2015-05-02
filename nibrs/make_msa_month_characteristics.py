'''
This file creates the 'final' version of msa-month level data, with everything but provider information merged in.

-JAB 2015-05-01
'''
import pandas as pd
import datetime
import ipdb
import json
import numpy as np

msa_ad_aggregates = pd.read_csv('msa_month_ad_aggregates.csv')
ucr = pd.read_csv('ucr.csv')
#nibrs = pd.read_csv('nibrs_monthly.csv')

# Begin computing MSA-month aggregates of provider-level information
provider_panel = pd.read_csv('provider_panel.csv')
# Need to roll some of these statistics up to the msa-month level, if
# desired... 
# Micro price is avg_price_per_hour_in_cluster_month. 
provider_stats = provider_panel.groupby(['census_msa_code','year','month'])['avg_price_per_hour_in_cluster_month'].aggregate({'avg_price_of_active_clusters_in_month':np.mean,'active_clusters':np.size})
provider_counts_with_price = provider_panel.groupby(['census_msa_code','year','month'])['num_ads_in_cluster_month_with_price'].aggregate({'active_providers_with_price':np.sum})
provider_counts_total = provider_panel.groupby(['census_msa_code','year','month'])['num_ads_in_cluster_month_total'].aggregate({'active_providers_total':np.sum})
# active_providers_with_price - is the number of clusters offering with an MSA in a
# given MSA-month where a price was extracted
# active_providers_with_total - is the number of clusters offering with an MSA in a
# given MSA-month with or without a price
# avg_price_of_active_clusters_in_month - Averaging the price of a
# provider across providers in a msa-month

out = pd.merge(msa_ad_aggregates, ucr, how='outer')
out = out.merge(provider_stats, right_index=True, left_on = ['census_msa_code','year','month'])
out = out.merge(provider_counts_with_price, right_index=True, left_on = ['census_msa_code','year','month'])
out = out.merge(provider_counts_total, right_index=True, left_on = ['census_msa_code','year','month'])
out.to_csv('msa_month_characteristics.csv', index=False)
