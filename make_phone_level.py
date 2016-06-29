#!/usr/bin/python
# -*- coding: utf-8 -*-
import pandas as pd
# import datetime
import ipdb
# import json
import numpy as np
nrows = None

prices = pd.read_csv('ad_price_ad_level.csv')
phones=pd.read_csv('data/forGiantOak3/phone_numbers.tsv', sep='\t', header=None, names=['ad_id','phone'])
steve_phones = pd.read_csv('data/bach/phones.csv')
phones = phones.merge(steve_phones, how='left')
phones = phones.merge(prices, how='left')

