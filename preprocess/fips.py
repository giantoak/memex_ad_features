# coding: utf-8
import pandas as pd

class FIPSCountyReshaper(object):
    """Reads in a csv with a 5-digit fips code and city,
    splits it into a csv with 2-digit state code, 3-digit county code,
    and city"""
    valid_suffixes = ['county', 'parish', 'city', 'borough', 'area']
    
    def __init__(self):
        return
    
    def pipeline(self, f, out):
        """
        Takes data from one file stream
        Outputs into the other
        """
        df = pd.read_csv(f, dtype=object)
        df2 = self.transform(df)
        df2.to_csv(out, index=False)

    def transform(self, df):
        def get_name(s):
            x = s.split()
            if x[-1].lower() in self.valid_suffixes:
                return ' '.join(x[:-1])

        def get_suffix(s):
            x = s.split()
            if x[-1].lower() in self.valid_suffixes:
                return x[-1]


        df['state_fips'] = df['fips'].apply(lambda x: x[:2])
        df['county_fips'] = df['fips'].apply(lambda x: x[2:])
        df['county_name'] = df['county'].apply(get_name).str.lower()
        df['county_suffix'] = df['county'].apply(get_suffix).str.lower()

        df['county'] = df['county'].str.lower()
        return df

# random preprocessing garbage. shouldve organized this better:

def acs_crime_merge():
    # merge ACS with crime data
    dfdemo = pd.read_csv('demographics_v2.csv')
    dfcrime = pd.read_csv('crime_scrape2013_names.csv')
    dfcrime['Full Name'] = dfcrime['Extracted Name'] + ', ' + dfcrime['State'] + ', USA'
    dfcrime['Full Name'] = dfcrime['Full Name'].apply(lambda x: x.lower())
    dfdemo['Full Name'] = dfdemo['location'].apply(lambda x: x.lower() if type(x) is str else x)
    dfdemo.index = dfdemo['Full Name']
    dfcrime.index = dfcrime['Full Name']
    dffull = dfdemo2.merge(dfcrime, on='Full Name').sort('B01001_001', ascending=False)
    dffull.drop(['Unnamed: 0', 'Full Name'], 1, inplace=True)
    dffull.drop(['Extracted Name'], 1, inplace=True)

    dffull2 = dffull.rename({'State_x': 'State'})
    dffull2.drop(['State_y'], 1, inplace=True)
    dffull2.to_csv('./demo_crime1.csv')

def clean_fips():
    # clean fips place
    df_place = pd.read_csv('fips_place.csv', delimiter='|')
    df_place['SUFFIX'] = df_place['PLACENAME'].apply(lambda x: x.split()[-1])
    df_place['NAME'] = df_place['PLACENAME'].apply(lambda x: ' '.join(x.split()[:-1]))
    df_place.to_csv('fips_place.csv', index=False)

def crime_to_fips(crimeloc):
    df = pd.read_csv(crimeloc)
    fipser = Fipser()
    crimeloc['FIPS'] = fipser.resolve(crimeloc['Extracted Name'])

