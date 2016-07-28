import pandas as pd
import numpy as np
import ipdb

d = pd.read_csv('temp.csv') # ad_price_ad_level.csv, but with cluster_count column
d = d[~d['spam']]
d = d[d['price_per_hour'] <= 1000]
msa_characteristics = pd.read_csv('../../msa_characteristics.csv')

census_names = pd.read_csv('../../qcew_msa.txt', sep='\t')
census_names['census_msa_code'] = census_names['qcew_code'].apply(lambda x: '31000US%s0' % x.replace('C', '')) # 310000 is the MSA code
msa_name_lookup = {row['census_msa_code']: row['msa'] for index, row in census_names.iterrows()}


def lookup(x):
    try:
        return(msa_name_lookup[x])
    except:
        return(np.nan)
d['msa'] = d['census_msa_code'].apply(lookup)
d=d.merge(msa_characteristics[['census_msa_code','population']])
d['log_price_per_hour'] = np.log(d['price_per_hour'])


def desc(name):
    a = d.groupby(name)['price_per_hour'].describe()
    print(a)
    print(a[True] - a[False])

d['site'] = d['ad_id'].apply(lambda x: x.split(':')[0])
d['size'] = 'unset'
d['size'][(d['cluster_count'] >= 200)] = 5
d['size'][(d['cluster_count'] >= 100) & (d['cluster_count'] < 200)] = 4
d['size'][(d['cluster_count'] >= 50) & (d['cluster_count'] < 100)] = 3
d['size'][(d['cluster_count'] >= 20) & (d['cluster_count'] < 50)] = 2
d['size'][(d['cluster_count'] >= 5) & (d['cluster_count'] < 20)] = 1
d['size'][d['cluster_count'] < 5] = 0
size_mapping = {
        0:'< 5',
        1:'5 to 19',
        2:'20 to 49',
        3:'50 to 99',
        4:'100 to 199',
        5:'200 +',
        }
print(d.groupby('site')['price_per_hour'].describe())
print(d.groupby('size')['price_per_hour'].describe())
a=d.groupby('site')['price_per_hour'].aggregate([np.mean, np.std, np.size, lambda x: np.std(x)/np.sqrt(len(x))]) 
a = a.sort('mean')
print(a) #  craigslist and massage troll (both of which are massage heavy) have lower prices. Utopia guide appears upscale

msa=d.groupby('msa')['price_per_hour'].aggregate([np.mean, np.std, np.size, lambda x: np.std(x)/np.sqrt(len(x))]) 
msa = msa.sort('mean')
# Note: boston () has a high mean compared to riversid (31000US40140) of
# 125

lmsa=d.groupby('msa')['log_price_per_hour'].aggregate([np.mean, np.std, np.size, lambda x: np.std(x)/np.sqrt(len(x))]) 
lmsa = lmsa.sort('mean')

print('look at correlations between group size and price')
print('price level, vs entity posts level')
print(np.corrcoef(d['price_per_hour'], d['cluster_count']) )
print('price log, vs entity posts log')
print(np.corrcoef(np.log(d['price_per_hour']), np.log(d['cluster_count']) ))
print('price level, vs entity posts log')
print(np.corrcoef(d['price_per_hour'], np.log(d['cluster_count'])))
print('price log, vs entity posts level')
print(np.corrcoef(np.log(d['price_per_hour']), d['cluster_count']))

print('page means')
print(d.groupby('site')[['outcall','sex_ad','is_massage_parlor_ad']].mean())
print('within site-msa stds')
within=d.groupby(['site','msa'])['price_per_hour'].std()
print('between std err: %s' % np.std(within))
print('ANOVA for site and MSA')
within_std=d.groupby(['site','msa'])['price_per_hour'].var().mean()
between_std=d.groupby(['site','msa'])['price_per_hour'].mean().var()
print('Total price variance: %s' % (within_std + between_std))
print('Price variance within markets: %s' % (within_std ))
print('Price variance between markets: %s' % (between_std))
print('Fraction of variance between markets: %s' % (between_std/(within_std + between_std) )) # 37%
print('ANOVA for site ')
within_std=d.groupby(['site'])['price_per_hour'].var().mean()
between_std=d.groupby(['site'])['price_per_hour'].mean().var()
print('Total price variance: %s' % (within_std + between_std))
print('Price variance within markets: %s' % (within_std ))
print('Price variance between markets: %s' % (between_std))
print('Fraction of variance between markets: %s' % (between_std/(within_std + between_std) )) # 2.3%
print('ANOVA for MSA ')
within_std=d.groupby(['msa'])['price_per_hour'].var().mean()
between_std=d.groupby(['msa'])['price_per_hour'].mean().var()
print('Total price variance: %s' % (within_std + between_std))
print('Price variance within markets: %s' % (within_std ))
print('Price variance between markets: %s' % (between_std))
print('Fraction of variance between markets: %s' % (between_std/(within_std + between_std) )) # 4.8%

# Tabulate sex vs massage parlor ads
sex_vs_massage = d.groupby(['is_massage_parlor_ad'])['price_per_hour'].aggregate({'mean':np.mean, 'std. error':lambda x: np.std(x)/np.sqrt(len(x)), 'N':np.size}).unstack('sex_ad').T.swaplevel(0,1).sort_index(ascending=[True, False])
print(sex_vs_massage)
sex_vs_massage.to_csv('sex_ad_vs_massage.csv')

# Tabulate incall vs outcall
incall_vs_outcall = d.groupby(['incall','outcall'])['price_per_hour'].aggregate({'mean':np.mean, 'std. error':lambda x: np.std(x)/np.sqrt(len(x)), 'N':np.size}).unstack('incall').T.swaplevel(0,1).sort_index(ascending=[True, False])
print(incall_vs_outcall)
incall_vs_outcall.to_csv('incall_vs_outcall.csv')

# Tabulate by entity size
entity_size = d.groupby(['size'])['price_per_hour'].aggregate({'mean':np.mean, 'std. error':lambda x: np.std(x)/np.sqrt(len(x)), 'N':np.size})
entity_size.index = entity_size.index.to_series().replace(size_mapping)
entity_size = entity_size[['mean','std. error', 'N']]
print(entity_size)
entity_size.to_csv('entity_size.csv')

ipdb.set_trace()
msa_stats = d.groupby('msa')['price_per_hour'].aggregate({'Obs.': np.size,
                                                          'Mean': np.mean,
                                                          'Std.': np.std,
                                                          '10%': lambda x: np.percentile(x, 10),
                                                          '90%': lambda x: np.percentile(x, 90),
                                                          '50%': lambda x: np.percentile(x, 50)})
# Tabulate price by MSA
#msa_stats = d.groupby('msa')['price_per_hour'].describe().unstack('msa').T.sort('mean') 
msa_stats = msa_stats.merge(msa_characteristics[['msa', 'population']],
                            left_index=True,
                            right_on='msa',
                            how='left')
msa_stats['ads_per_100k_capita'] = msa_stats['Mean']*100000/msa_stats['population']
msa_stats = msa_stats[~msa_stats['ads_per_100k_capita'].isnull()]
msa_stats = msa_stats.rename(columns={'msa': 'MSA', 'ads_per_100k_capita': 'Ads/100k Pop.'})
msa_stats = msa_stats.sort('Mean', ascending=True)
msa_stats[['MSA', 'Obs.', 'Ads/100k Pop.', 'Mean', 'Std.', '10%', '50%', '90%']].to_csv('msa_stats.csv', index=False)

log_price_vs_count = np.corrcoef(np.log(d['price_per_hour']), np.log(d['cluster_count']))
print('The correlation coefficient is: %0.3f' % log_price_vs_count[0][1])

# Markets are different, and occur at the site-MSA level
site_characteristics = d.groupby('site')[['outcall', 'sex_ad', 'is_massage_parlor_ad']].mean()
print(site_characteristics)
site_characteristics.to_csv('site_characteristics.csv')
