import pandas
lemas = pandas.read_csv('ucr_lemas_msa.csv')
lemas=lemas[['msa_parents','swnauthemp','swnftemp']]
lemas.rename(columns={'msa_parents':'census_msa_code'}, inplace=True)
aggregated = lemas.groupby(['census_msa_code'])['swnauthemp','swnftemp'].sum()
aggregated.to_csv('lemas.csv')
