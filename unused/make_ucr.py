import pandas as pd

# Add LEMAS and UCR data
lemas = pd.read_csv('ucr_lemas_msa.csv')
lemas = lemas.loc[:, ['msa_parents',
                      'rape_2010', 'rape_2011', 'rape_2012',
                      'violent_2010', 'violent_2011', 'violent_2012',
                      'property_2010', 'property_2011', 'property_2012']]
lemas = pd.melt(lemas, id_vars=['msa_parents'])
lemas['year'] = lemas['variable'].apply(lambda x: int(x.split('_')[1]))
lemas['measure'] = lemas['variable'].apply(lambda x: x.split('_')[0])
lemas.rename(columns={'msa_parents': 'census_msa_code'}, inplace=True)

aggregated = lemas.groupby(['census_msa_code', 'year', 'measure'])['value'].sum().to_frame()
aggregated = aggregated.unstack('measure')
aggregated.columns = ['property', 'rape', 'violent']
aggregated.to_csv('ucr.csv')
