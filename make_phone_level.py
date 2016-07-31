#!/usr/bin/python
# -*- coding: utf-8 -*-
import pandas as pd

nrows = None

prices = pd.read_csv('ad_price_ad_level.csv')
phones = pd.read_table('data/forGiantOak3/phone_numbers.tsv',
                       header=None,
                       names=['ad_id', 'phone'])
steve_phones = pd.read_csv('data/bach/phones.csv')
phones = phones.merge(steve_phones, how='left')
phones = phones.merge(prices, how='left')
