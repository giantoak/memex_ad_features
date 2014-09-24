from __future__ import print_function
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

    def resolve(self, location, verbose=False):
        """Return FIPS code at multiple levels of specificity for location.

        """
        def filt_func(s):
            # filter empty strings and commas
            return len(s.strip()) and s.strip() != ','

        parts_raw = re.split(r'([^, ]+)', location)
        parts = [x.strip() for x in filter(filt_func, parts_raw)]

        # i is index for the place name (place/county)
        # j is the index for the state name
        # k is the index for the zip code
        # i <= j <= k
        
        # for each pass, the number of filled in parts in the fips code is
        results = {}

        # how this permutation is done:
        # start with the edge case "Alabama". We want this to be considered
        # as a possible place, state, and zip
        # this means allowing all of the variables to reach len(parts) + 1
        #
        # take "Alexandria, VA". we want place_candidate to be [0, 1] and
        # state_candidate to be [1, 2] (i=1, j=2, z=2)
        for i in range(0, len(parts)+1):
            for z in range(len(parts)-1, len(parts)+1):

                # first, try to resolve each of the segments into fips codes
                place_candidate = ' '.join(parts[0:i])
                state_candidate = ' '.join(parts[i:z])
                zip_candidate = parts[z:len(parts)]
                
                
                sfips, state = self.get_state(state_candidate)
                pfips, place = self.get_place(place_candidate, sfips)
                cfips, county = self.get_county(place_candidate, sfips)
                
                # note: place and county are attempts at resolving the same
                # entity across different spatial levels. for this purpose,
                # we are going to resolve it toward the larger area by default

                # for deciding the optimal split, we create a score for each split:
                # the split gets one point for resolving a state, and one point
                # for resolving either place or county (but only one point for both).

                # ties for one point are resolved by picking the split that resolved
                # the higher-level area.
                score = 0
                score += (sfips is not None)
                score += ((pfips is not None) or (cfips is not None))
                
                ans = {
                        'state_fips': sfips,
                        'place': place,
                        'place_fips': pfips,
                        'county': county,
                        'county_fips': cfips,
                        'score': score,
                        }
                if verbose:
                    print(i, z, '*{}*{}*'.format(place_candidate, state_candidate))
                    print(ans)

                results[(i, z)] = ans

                # no ties for two points should arise
        
        # get max from results
        # return the winner.
        df = pd.DataFrame(results).T

        max_score = df['score'].max()
        # if the split resolved both state and place, then select that candidate
        if max_score == 2:
            result = df[df['score'] == max_score].drop('score', 1).T.to_dict()\
                    .values()[0]
        
        # TODO: refactor out some of this repetitive stuff into a method
        elif max_score == 1:
            # if the split only resolved one, first try to select the candidate that 
            # did state
            cands = df[df['score'] == max_score].drop('score', 1)
            
            raw = cands[~cands['state_fips'].isnull()].T.to_dict()
            if not len(raw):
                raw = cands[~cands['county_fips'].isnull()].T.to_dict()
            if not len(raw):
                raw = cands.T.to_dict()

            result = raw.values()[0]

        
        # no candidates were chosen
        else:
            result = {
                        'state_fips': None,
                        'place': None,
                        'place_fips': None,
                        'county': None,
                        'county_fips': None,
                    }
        
        return result

        # get the candidates by 1) finding all the max scores
    
     
    
    #TODO: This is really ugly:
    def get_state(self, stateac):
        if stateac == '':
            return None, None

        try:
            fips  = self.states.ix[stateac.upper(), 'fips']
            state = self.states.ix[stateac.upper(), 'state']
        except KeyError, ValueError:
            fipsdf = self.states[self.states.state == stateac.lower()].ix[:,
                    'fips']
            if len(fipsdf) > 0:
                fips = fipsdf.iloc[0]
                state = stateac.lower()
            else:
                fips, state = None, None


        return fips, state

    def get_county(self, loc, statefp=None):
        if loc == '':
            return None, None

        if statefp:
            county_dfs = self.county[self.county.state_fips == statefp]
        else:
            county_dfs = self.county

        try:
            fips  = county_dfs.ix[loc.lower(), 'county_fips']
            county = county_dfs.ix[loc.lower(), 'county']
            if not isinstance(fips, basestring):
                fips = fips.iloc[0]
                county= county.iloc[0]
        except KeyError:
            fips, county = None, None

        return fips, county

    def get_place(self, loc, statefp=None):
        if loc == '':
            return None, None

        if statefp:
            # inconsistent preprocessing has caused this to be read in as a 
            # str'd int. Augh. 
            place_dfs = self.place[self.place.statefp == str(int(statefp))]
        else:
            place_dfs = self.place

        try:
            fips  = place_dfs.ix[loc.lower(), 'placefp']
            place = place_dfs.ix[loc.lower(), 'placename']
            
            # these can be both strings and series
            # when they are series, they didn't successfully disambiguate

            if not isinstance(fips, basestring):
                fips = fips.iloc[0]
                place= place.iloc[0]

        except KeyError:
            fips, place = None, None
        
        return fips, place

