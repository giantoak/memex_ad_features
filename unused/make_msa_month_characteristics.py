"""
This file creates the 'final' version of msa-month level data, with everything but provider information merged in.

-JAB 2015-05-01
"""
import pandas as pd
# import datetime
# import ipdb
# import ujson as json
import numpy as np

msa_ad_aggregates = pd.read_csv('msa_month_ad_aggregates.csv')
ucr = pd.read_csv('ucr.csv')
# nibrs = pd.read_csv('nibrs_monthly.csv')

violence_nibrs = pd.read_csv('violence_nibrs.csv')
violence_nibrs.rename(columns={'mean': 'share_of_incidents_violent',
                               'sum': 'number_of_indicents_violent',
                               'size': 'number_of_incidents_total'},
                      inplace=True)
female_violence_nibrs = pd.read_csv('female_violence_nibrs.csv')
female_violence_nibrs.rename(columns={'mean': 'share_of_incidents_violent_to_women',
                                      'sum': 'number_of_indicents_violent_to_women'},
                             inplace=True)
del female_violence_nibrs['size']
prostitution_nibrs = pd.read_csv('prostitution_nibrs.csv')
prostitution_nibrs.rename(columns={'mean': 'share_of_incidents_prostitution',
                                   'sum': 'number_of_indicents_prostitution'},
                          inplace=True)
del prostitution_nibrs['size']
nibrs = pd.merge(violence_nibrs, female_violence_nibrs)
nibrs = nibrs.merge(prostitution_nibrs)

# Begin computing MSA-month aggregates of provider-level information
provider_panel = pd.read_csv('provider_panel.csv')
# Need to roll some of these statistics up to the msa-month level, if
# desired... 
# Micro price is avg_price_per_hour_in_cluster_month. 
provider_stats =\
    provider_panel.groupby(['census_msa_code', 'year', 'month'])['avg_price_per_hour_in_cluster_month']\
    .aggregate({'avg_price_of_active_providers_in_month': np.mean})
provider_counts_with_price =\
    provider_panel.groupby(['census_msa_code', 'year', 'month'])['num_ads_in_cluster_month_with_price']\
    .aggregate({'active_providers_with_price': np.sum})
provider_counts_total =\
    provider_panel.groupby(['census_msa_code', 'year', 'month'])['num_ads_in_cluster_month_total']\
    .aggregate({'active_providers_total': np.sum})
# active_providers_with_price - is the number of clusters offering with an MSA in a
# given MSA-month where a price was extracted
# active_providers_with_total - is the number of clusters offering with an MSA in a
# given MSA-month with or without a price
# avg_price_of_active_clusters_in_month - Averaging the price of a
# provider across providers in a msa-month

# Test code 7/1
census_names = pd.read_csv('qcew_msa.txt', sep='\t')


def census(v1, v2, listall=True):
    l1 = set(v1['census_msa_code'].tolist())
    l2 = set(v2['census_msa_code'].tolist())
    print('First input: %s' % len(l1))
    if listall:
        names = []
        for i in list(l1):
            try:
                names.append(msa_name_lookup[i])
            except KeyError:
                print('Error finding MSA for %s' % i)
        print('\n'.join(names))
    print('Second input: %s' % len(l2))
    if listall:
        names = []
        for i in list(l2):
            try:
                names.append(msa_name_lookup[i])
            except KeyError:
                print('Error finding MSA for %s' % i)
        print('\n'.join(names))
    print('Intersection: %s' % len(l1.intersection(l2)))
    print('Difference 2 minus 1: %s' % len(l2.difference(l1)))
    if listall:
        names = []
        for i in list(l2.difference(l1)):
            try:
                names.append(msa_name_lookup[i])
            except KeyError:
                print('Error finding MSA for %s' % i)
        print('\n'.join(names))


census_names['census_msa_code'] = census_names['qcew_code'].apply(lambda x: '31000US%s0' % x.replace('C', ''))  # 310000 is the MSA code
msa_name_lookup = {row['census_msa_code']: row['msa'] for index, row in census_names.iterrows()}


# End test code 7/1
# ipdb.set_trace()
wage_instruments = pd.read_csv('month_msa_wage_instruments.csv')
wage_instruments['msa'] = wage_instruments['census_msa_code'].apply(lambda x: msa_name_lookup[x])
msa_ad_aggregates = msa_ad_aggregates[~msa_ad_aggregates['census_msa_code'].isin(['Service', 'Pennsylvannia'])]
out = pd.merge(msa_ad_aggregates, ucr, how='outer')
out = pd.merge(out, nibrs, how='outer')
out = pd.merge(out, wage_instruments, how='outer')
out = out.merge(provider_stats, right_index=True, left_on=['census_msa_code', 'year', 'month'], how='outer')
out = out.merge(provider_counts_with_price, right_index=True, left_on=['census_msa_code', 'year', 'month'], how='outer')
out = out.merge(provider_counts_total, right_index=True, left_on=['census_msa_code', 'year', 'month'], how='outer')
out.to_csv('msa_month_characteristics.csv', index=False)
out = out.merge(census_names)
