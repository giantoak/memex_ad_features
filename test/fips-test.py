# coding: utf-8
import nose
import os
import sys

path = os.path.dirname(
        os.path.dirname(
            os.path.realpath(__file__)))
sys.path.append(os.path.join(path, 'preprocess'))

from fipser import Fipser

def test_state():
    fipser = Fipser()
    def run_test_state(state, result):
        assert fipser.get_state(state) == result
    fixtures = {
            'VA': ('51', 'virginia'),
            'Alabama': ('01', 'alabama'),
            'haberdash': (None, None)
            }

    for t, r in fixtures.iteritems():
        yield run_test_state, t, r

def test_fips():
    def run_fips(f, loc, ans):
        r = f.resolve(loc)
        assert r == ans

    fipser = Fipser()
    fixtures = {
            # city state no punctuation
            'Alexandria VA': {
                'state_fips': '51',
                'place': 'alexandria city', 
                'place_fips': '1000',
                'county': 'alexandria city',
                'county_fips': '510',
                },
            # multi-word state
            'North Dakota': {
                'state_fips': '38',
                'place': None, 
                'place_fips': None,
                'county': None,
                'county_fips': None,
                },
            # multi-word place, multi-word state
            'Charles Mix, South Dakota': {
                'state_fips': '46',
                'place': None,
                'place_fips': None,
                'county': 'charles mix county',
                'county_fips': '023',
                },
            'Clarendon, South Carolina 29042': {
                'state_fips': '45',
                'place': None,
                'place_fips': None,
                'county': 'clarendon county',
                'county_fips': '027',
                },
            # this doesn't exist
            'Gibberish, South Carolina': {
                'state_fips': '45',
                'place': None,
                'place_fips': None,
                'county': None,
                'county_fips': None,
                },
            # accept "county" as suffix
            'Humboldt County, California': {
                'state_fips': '06',
                'place': None,
                'place_fips': None,
                'county': 'humboldt county',
                'county_fips': '023',
                },
            # daytona beach is encoded as daytona in census
            'Daytona, Florida': {
                'state_fips': '12',
                'place': 'daytona beach city',
                'place_fips': '16525',
                'county': None,
                'county_fips': None,
                },
            }

    for loc, ans in fixtures.iteritems():
        yield run_fips, fipser, loc, ans

