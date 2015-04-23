"""
Merge in data at the MSA level to create one master-level file

JAB
"""

import pandas
import datetime
import ipdb
import json
import numpy as np

acs = pandas.read_csv('acs.csv')
violence_nibrs = pandas.read_csv('violence_nibrs.csv')
violence_nibrs.rename(columns={'mean':'share_of_incidents_violent','sum':'number_of_indicents_violent','size':'number_of_incidents_total'}, inplace=True)
female_violence_nibrs = pandas.read_csv('female_violence_nibrs.csv')
female_violence_nibrs.rename(columns={'mean':'share_of_incidents_violent_to_women','sum':'number_of_indicents_violent_to_women'}, inplace=True)
del female_violence_nibrs['size']
prostitution_nibrs = pandas.read_csv('prostitution_nibrs.csv')
prostitution_nibrs.rename(columns={'mean':'share_of_incidents_prostitution','sum':'number_of_indicents_prostitution'}, inplace=True)
del prostitution_nibrs['size']
nibrs = pandas.merge(violence_nibrs,female_violence_nibrs)
nibrs = nibrs.merge(prostitution_nibrs)
ucr_year = pandas.read_csv('ucr.csv')
ucr_avg=ucr_year.groupby('census_msa_code')[['property','rape','violent']].sum()
ucr_avg.reset_index(inplace=True)
lemas = pandas.read_csv('lemas.csv')

out = pandas.merge(nibrs, ucr_avg, how='outer')
out = out.merge(acs, how='outer')
out = out.merge(lemas, how='outer')
out.to_csv('msa_characteristics.csv')
