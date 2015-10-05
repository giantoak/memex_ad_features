"""
Create one monthly panel level file including panel level provider characteristics and 
msa-month level data

JAB 5-1-2015
"""

import pandas as pd
# import datetime
# import ipdb
# import json
# import numpy as np

msa_month_characteristics = pd.read_csv('msa_month_characteristics.csv')
