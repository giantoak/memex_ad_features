#!/usr/bin/python
# -*- coding: utf-8 -*-

import pandas as pd
import ipdb
import numpy as np
ad_price_ad_level = pd.read_csv('ad_price_ad_level.csv')
bach_phones = pd.read_csv('data/bach/phones.csv')
phones = pd.read_csv('data/forGiantOak3/phone_numbers.tsv', sep='\t', header=None, names=['ad_id','phone'])

bach_phones.to_csv('phone_characeteristics.csv', index=False
