import pandas as pd
import sys
import os
import re

class Fipser(object):
    """
    A converter from ambiguous place names to FIPS codes.
    """
    def __init__(self):
        path = os.path.dirname(
                os.path.dirname(
                    os.path.realpath(__file__)))

        datadir = os.path.join(path, 'data')

        # TODO: script load these files into a database (preprocessing)
        self.county = pd.read_csv(os.path.join(datadir, 'fips_county2.csv'), 
                index_col='county_name', dtype=object)
        self.states = pd.read_csv(os.path.join(datadir, 'fips_states_all.csv'),
                index_col='Abbreviation', dtype=object)
        self.place = pd.read_csv(os.path.join(datadir, 'fips_place_lower.csv'),
                index_col='name', dtype=object)
        
        return

    def resolve(self, location):
        """Return FIPS code at multiple levels of specificity for location.

        """
        def filt_func(s):
            # filter empty strings and commas
            return len(s.strip()) and s.strip() != ','

        parts_raw = re.split(r'([^, ]+)', location)
        parts = [x.strip() for x in filter(filt_func, parts_raw)]
        
        if len(parts) == 1:
            # assume that this is a state for now 
            try:
                sfips, state = self.get_state(parts[0])
            except IndexError:
                print 'Skipping badly formatted input: {}'.format(location)
                sfips, state = None, None
            cfips, county, pfips, place = '000', None, '00000', None
        else:
            i = -1
            if re.match(r'\d\d\d\d\d', parts[-1]):
                i-=1

            stateac = parts[i]
            loc = ' '.join(parts[:i])
            try:
                sfips, state = self.get_state(stateac)
            except IndexError:
                print 'Skipping badly formatted input: {}'.format(location)
                sfips, state = None, None
            cfips, county = self.get_county(loc, sfips)
            pfips, place = self.get_place(loc, sfips)

        return {
                'state_fips': sfips,
                'place': place,
                'place_fips': pfips,
                'county': county,
                'county_fips': cfips,
                }
        
    def get_state(self, stateac):
        try:
            fips  = self.states.ix[stateac.upper(), 'fips']
            state = self.states.ix[stateac.upper(), 'state']
        except KeyError, ValueError:
            try:
                fips = self.states[self.states.state == stateac.lower()].ix[:,
                        'fips'].iloc[0]
                state = stateac.lower()
            except KeyError:
                raise Exception('State {} not found'.format(stateac))

        return fips, state

    def get_county(self, loc, statefp):

        county_dfs = self.county[self.county.state_fips == statefp]
        try:
            fips  = county_dfs.ix[loc.lower(), 'county_fips']
            county = county_dfs.ix[loc.lower(), 'county']
        except KeyError:
            fips, county = None, None

        return fips, county

    def get_place(self, loc, statefp):
        place_dfs = self.place[self.place.statefp == statefp]
        try:
            fips  = place_dfs.ix[loc.lower(), 'placefp']
            place = place_dfs.ix[loc.lower(), 'placename']
        except KeyError:
            fips, place = None, None
        
        return fips, place

