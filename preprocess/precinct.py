import pandas as pd
from itertools import product
import os
import sys


class PrecinctExtractor(object):
    """Extracts location names from police dept names.
    Takes advantage of standard naming practices, but may not extend beyond
    the standard US naming scheme employed in the UCR.
    """
    def __init__(self):
        prefixes = ['Police', 'Sheriff', 'Marshall', 'Public Safety']
        suffixes = [' Department', ' Dept', ' Office', '\'s Office']

        # "cross products" of combining prefixes and suffixes, so
        # Police Department, Police Dept, ..., Public Safety's Office
        # (but lowercase)
        self.extensions = [''.join((a, b)).lower() 
                for a, b in product(prefixes, suffixes)]
        return

    def extract_ucr(self, agency):
        """
        Takes advantage of semi-standard naming practices employed in the UCR.
        Uses some strategies specific to UCR.

        agency: string

        returns location part of 'agency', lower-cased
        """
        
        # weird exception
        vt = 'vt. state police-'
        lower_agency = agency.lower()
        if lower_agency.startswith(vt):
            return lower_agency[len(vt):]
        
        # standard names
        for ext in self.extensions:
            i = lower_agency.find(ext)
            if i != -1:
                return lower_agency[:i].strip()
        
        # no extension found
        return lower_agency

if __name__ == '__main__':
    path = os.path.dirname(
            os.path.dirname(
                os.path.realpath(__file__)))

    os.chdir(path)

    df = pd.read_csv('crime_scrape2013.csv')
    p = PrecinctExtractor()
    names = []
    for a in df.Agency:
        name = p.extract_ucr(a)
        names.append(name)
    
    namesdf = pd.Series(names)
    df_new = pd.concat([df, namesdf], axis=1)
    
    df_new.to_csv('crime_scrape2013_names.csv', index=False)
