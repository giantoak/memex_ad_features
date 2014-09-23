# coding: utf-8
from fips import FIPSCountyReshaper
fcr = FIPSCountyReshaper()
fcr.pipeline('data/fips_county.csv', 'data/fips_county2.csv')
