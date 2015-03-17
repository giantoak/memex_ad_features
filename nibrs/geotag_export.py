"""
This started from code from Sam on 1/8/2015 to grab his calls to the census data explorer and put them in a unified structure

JAB
"""
import json
import requests
#url = "http://ec2-54-166-98-195.compute-1.amazonaws.com:8000/geotag/export/"
#data = requests.get(url).json()
data = json.loads(open('analytical_report_acs.json','r').read())
combined = {}
for k, v in data[0].items():
    combined[k] = {}

for group in data:
    for k, v in group.items():
        combined[k].update(v)
open('census.json','w').write(json.dumps(combined))
"""
Turn data from sam's geotag export into a dictionary that looks like a list of:
    'release' - Release information description, e.g. 5 year, 2013
    'tables' - Top level table id info, like B15003 is education
    'data' - dictionary with geo_id as the key and then table numbers as a key
    'geography' - a dictionary of geo_id to name
"""

"""
This script loads the initial price data that Sam loaded from DeepDive on about 12/25/2014

It also adds census data from sam on 1/8/2015.

It then creates a data set of only ads with two prices "doubles" where
the implied fixed cost is listed
"""
import pandas
import datetime
import ipdb
import json
import numpy as np
from sklearn import linear_model
from sklearn import datasets

def census_lookup(geo_id, table_value, verbose=False):
    """
    table_value is like B01001001
    Where B01001 is the table id and 001 is the value id
    """
    try:
        table = table_value[0:6]
        value = table_value[6:]
        data = combined['data'][geo_id][table]['estimate'][table_value]
        if verbose:
            print('Info for: %s ' % combined['geography'][geo_id]['name'])
            print('Found value: %s' % data)
        return data
    except:
        return np.nan

# Begin merging information from census
combined = json.loads(open('census.json','r').read())
tables_list = [
              "B15003001", # Total population
              "B23025005", # Unemployed
              "B23025002", # In Labor Force
              #"B19013_001", # Median Income
              "B02001001", # Total Population
              "B02001002", # Total White Population
              "B02001003", # Total Black Population
              "B15003002", # No schooling completed 
              "B15003003", #  Nursery school
              "B15003004", #  Kindergarten
              "B15003005", #  1st Grade
              "B15003006", #  2nd Grade
              "B15003007", #  3rd Grade
              "B15003008", #  4th Grade
              "B15003009", #  5th Grade
              "B15003010", #  6th Grade
              "B15003011", #  7th Grade
              "B15003012", #  8th Grade
              "B15003013", #  9th Grade
              "B15003014", #  10th Grade
              "B15003015", #  11th Grade
              "B15003016", #  12th Grade, no diploma
              "B15003017", #  High School Diploma
              "B15003018", #  GED or alternative diploma
              "B15003019", #  Some college < 1 year
              "B15003020", #  Some college, 1+ years
              "B15003021", #  Associates degree
              "B15003022", #  Bachelors degree
              "B15003023", #  Masters degree
              "B15003023", #  Professional degree
              "B15003024", #  Doctoral degree
              "B19013001", # Household Median Income
              "B08303001", # Total Commuters
              "B08303002", # Less than 5 Minutes
              "B08303003", # 5 to 9 minutes
              "B08303004", # 10 to 14 minutes
              "B08303005", # 15 to 19 minutes
              "B08303006", # 20 to 24 minutes
              "B08303007", # 25 to 29 minutes
              "B08303008", # 30 to 34 minutes
              "B08303009", # 35 to 39 minutes
              "B08303010", # 40 to 44 minutes
              "B08303011", # 45 to 59 minutes
              "B08303012", # 60 to 89 minutes
              "B08303013", # 90 or more minutes
          ]
hs_educations = [
              "B15003017", #  High School Diploma
              "B15003018", #  GED or alternative diploma
        ]
some_college_educations = [
              "B15003019", #  Some college < 1 year
              "B15003020", #  Some college, 1+ years
              "B15003021", #  Associates degree
        ]
college_plus_educations = [
              "B15003022", #  Bachelors degree
              "B15003023", #  Masters degree
              "B15003023", #  Professional degree
              "B15003024", #  Doctoral degree
        ]
less_than_hs_educations = [
              "B15003002", # No schooling completed 
              "B15003003", #  Nursery school
              "B15003004", #  Kindergarten
              "B15003005", #  1st Grade
              "B15003006", #  2nd Grade
              "B15003007", #  3rd Grade
              "B15003008", #  4th Grade
              "B15003009", #  5th Grade
              "B15003010", #  6th Grade
              "B15003011", #  7th Grade
              "B15003012", #  8th Grade
              "B15003013", #  9th Grade
              "B15003014", #  10th Grade
              "B15003015", #  11th Grade
              "B15003016", #  12th Grade, no diploma
              ]
commute_times = [
              "B08303002", # Less than 5 Minutes
              "B08303003", # 5 to 9 minutes
              "B08303004", # 10 to 14 minutes
              "B08303005", # 15 to 19 minutes
              "B08303006", # 20 to 24 minutes
              "B08303007", # 25 to 29 minutes
              "B08303008", # 30 to 34 minutes
              "B08303009", # 35 to 39 minutes
              "B08303010", # 40 to 44 minutes
              "B08303011", # 45 to 59 minutes
              "B08303012", # 60 to 89 minutes
              "B08303013", # 90 or more minutes
          ]
commute_centers = [
              2.5, #"B08303002", # Less than 5 Minutes
              7.5, #"B08303003", # 5 to 9 minutes
              12.5, #"B08303004", # 10 to 14 minutes
              17.5, #"B08303005", # 15 to 19 minutes
              22.5, #"B08303006", # 20 to 24 minutes
              27.5, #"B08303007", # 25 to 29 minutes
              32.5, #"B08303008", # 30 to 34 minutes
              37.5, #"B08303009", # 35 to 39 minutes
              42.5, #"B08303010", # 40 to 44 minutes
              52.5, #"B08303011", # 45 to 59 minutes
              72.5, #"B08303012", # 60 to 89 minutes
              100, #"B08303013", # 90 or more minutes
              ]


data = pandas.read_csv('female_opportunity_index.csv')
geo_ids = pandas.DataFrame(data.area_fips.unique(), columns=['area_fips'])
geo_ids['codes']=geo_ids['area_fips'].apply(lambda x: '31000US%s0' % x.replace('C','')) # 310000 is the MSA code
for i in tables_list:
    geo_ids[i] = np.nan # Create a column
    geo_ids[i]=geo_ids.apply(lambda x: census_lookup(x['codes'],i), axis=1)    # Do census lookups
    # Look up the census code for the given geo id and column name

data = pandas.merge(data, geo_ids) # Merge geo_id lookups back to data on 'area_fips' column
data['population'] = data["B15003001"] # Total population
data['unemployment'] = data["B23025005"]/data["B23025002"]
data['lt_highschool'] = data[less_than_hs_educations].sum(axis=1)/data['population']
data['highschool'] = data[hs_educations].sum(axis=1)/data['population']
data['some_college'] = data[some_college_educations].sum(axis=1)/data['population']
data['college_plus'] = data[college_plus_educations].sum(axis=1)/data['population']
data['median_income'] = data['B19013001']
data['frac_white'] = data["B02001002"]/data["B02001001"]
data['avg_commute'] = data[commute_times].dot(commute_centers)/data["B08303001"]

# End merging information from census

crosswalk = pandas.read_csv('crosswalk_tract_msa.csv')
crosswalk = crosswalk[~crosswalk.MSA.isnull()]
msa_cw = crosswalk[['ORI9','MSA']].drop_duplicates()
msa_cw['codes'] = msa_cw.MSA.apply(lambda x: '31000US%s' % str(int(x)))

out = pandas.merge(data, msa_cw, on='codes')
# This dataframe is at the ORI9-month-year level. There are tons of rows
# here: 48 months and 12k ORI9s

out1=out.copy()
fv = pandas.read_csv('female_violence.csv')
fv.rename(columns={'size':'total_reports'}, inplace=True) # Rename 'size' column to total reports, the total number of crime reports in the MSA over 2 years
fv.rename(columns={'mean':'female_violence_fraction'}, inplace=True) # column represents fraction of all crime reports which were violence against women
fv.rename(columns={'sum':'female_violence_counts'}, inplace=True) # column represents total number of reports of violence against women
out = pandas.merge(out, fv[['total_reports','female_violence_fraction','female_violence_counts','codes']], on='codes')

v = pandas.read_csv('violence.csv')
del v['size'] # This is the total number of reports, which is the same as from female violence
v.rename(columns={'mean':'violence_fraction'}, inplace=True) # column represents fraction of all crime reports which were violence against men OR women
v.rename(columns={'sum':'violence_counts'}, inplace=True) # column represents total number of reports of violence against men OR women
out = pandas.merge(out, v[['violence_fraction','violence_counts','codes']], on='codes')

p = pandas.read_csv('prostitution.csv')
del p['size'] # This is the total number of reports, which is the same as from female violence
p.rename(columns={'mean':'prostitution_fraction'}, inplace=True) # column represents fraction of all crime reports which were prostitution arrests
p.rename(columns={'sum':'prostitution_counts'}, inplace=True) # column represents total number of reports of prostitution
out = pandas.merge(out, p[['prostitution_fraction','prostitution_counts','codes']], on='codes')

for col in out.columns:
    if col[0] =='B':
        # This is a raw census column, delete!
        del out[col]
out.rename(columns={'codes':'census_msa_code'}, inplace=True)

out.drop_duplicates(['sex','month','year','census_msa_code'], inplace=True)
subset = out[['sex','month','year','census_msa_code']]
subset.to_records(index=False).tolist()
index = pandas.MultiIndex.from_tuples(subset.to_records(index=False).tolist(), names=subset.columns.tolist())
out.index = index
out.reindex()
out = out.unstack('sex')
# Note: this leaves us with a DF with the right shape, but it has a
# multiindex for columns when we really need most things to NOT be
# there...
out.columns.names=['main','sex']
del out['sex']
female = out.xs(2, level='sex',axis=1)
male = out.xs(1, level='sex',axis=1)
female = female[['index_p50','index_p25','index_p75','index_mean.wage','index_sum.wght']]
female.rename(columns={'index_p75':'female_p75','index_p50':'female_p50','index_p25':'female_p25','index_mean.wage':'female_mean.wage','index_sum.wght':'female_sum.wght'}, inplace=True)
male.rename(columns={'index_p75':'male_p75','index_p50':'male_p50','index_p25':'male_p25','index_mean.wage':'male_mean.wage','index_sum.wght':'male_sum.wght'}, inplace=True)
out = pandas.concat([male, female], axis=1)
delcols = ['month','year','census_msa_code','ORI9']
for c in delcols:
    del out[c]
out.to_csv('all_merged.csv')
