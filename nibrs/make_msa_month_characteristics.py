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
month_msa_chars = month_msa_chars.merge(ucr)

# Merge in the msa-month price aggregates
prices = pandas.read_csv('msa_month_ad_aggregates.csv')
month_msa_chars = month_msa_chars.merge(prices, how='left')

month_msa_chars.to_csv('msa_month_characteristics.csv')
