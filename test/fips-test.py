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
            'Alabama': ('01', 'alabama')
            }

    for t, r in fixtures.iteritems():
        yield run_test_state, t, r

def test_fips():
    def run_fips(f, loc, ans):
        assert f.resolve(loc) == ans

    fipser = Fipser()
    fixtures = {
            'Alexandria VA': {
                'state_fips': '51',
                'place': 'alexandria city', 
                'place_fips': '1000',
                'county': 'alexandria city',
                'county_fips': '510'
                }
            }

    for loc, ans in fixtures.iteritems():
        yield run_fips, fipser, loc, ans

if __name__ == '__main__':
    merge_fips()
