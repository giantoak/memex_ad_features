"""
This started from code from Sam on 1/8/2015 to grab his calls to the census data explorer and put them in a unified structure

JAB
"""

import pandas
# import datetime
# import ipdb
# import json
import numpy as np

acs = pandas.read_csv('acs_2015_03_18.csv')
acs_geos = acs.geoid.tolist()
missing_geographies = []


def census_lookup(geo_id, table_value, verbose=False):
    """
    table_value is like B01001001
    Where B01001 is the table id and 001 is the value id
    """
    if geo_id in acs_geos:
        try:
            output = acs[acs['geoid'] == geo_id][table_value + '-estimate'].values[0]
            if verbose:
                print('Info for: %s ' % geo_data['name'])
                print('Found value: %s' % output)
            return output
        except KeyError:
            if verbose:
                print('No census data found for table: %s; geography %s' % (table_value, geo_id))
            return np.nan
        geo_data = acs[geo_id]
    else:
        if verbose:
            print('Geography %s not found' % geo_id)
        missing_geographies.append(geo_id)
        return np.nan

tables_list = [
              "B15003001",  # Total population
              "B23025005",  # Unemployed
              "B23025002",  # In Labor Force
              # "B19013_001",  # Median Income
              "B02001001",  # Total Population
              "B02001002",  # Total White Population
              "B02001003",  # Total Black Population
              "B15003002",  # No schooling completed 
              "B15003003",  # Nursery school
              "B15003004",  # Kindergarten
              "B15003005",  # 1st Grade
              "B15003006",  # 2nd Grade
              "B15003007",  # 3rd Grade
              "B15003008",  # 4th Grade
              "B15003009",  # 5th Grade
              "B15003010",  # 6th Grade
              "B15003011",  # 7th Grade
              "B15003012",  # 8th Grade
              "B15003013",  # 9th Grade
              "B15003014",  # 10th Grade
              "B15003015",  # 11th Grade
              "B15003016",  # 12th Grade, no diploma
              "B15003017",  # High School Diploma
              "B15003018",  # GED or alternative diploma
              "B15003019",  # Some college < 1 year
              "B15003020",  # Some college, 1+ years
              "B15003021",  # Associates degree
              "B15003022",  # Bachelors degree
              "B15003023",  # Masters degree
              "B15003023",  # Professional degree
              "B15003024",  # Doctoral degree
              "B19013001",  # Household Median Income
              "B08303001",  # Total Commuters
              "B08303002",  # Less than 5 Minutes
              "B08303003",  # 5 to 9 minutes
              "B08303004",  # 10 to 14 minutes
              "B08303005",  # 15 to 19 minutes
              "B08303006",  # 20 to 24 minutes
              "B08303007",  # 25 to 29 minutes
              "B08303008",  # 30 to 34 minutes
              "B08303009",  # 35 to 39 minutes
              "B08303010",  # 40 to 44 minutes
              "B08303011",  # 45 to 59 minutes
              "B08303012",  # 60 to 89 minutes
              "B08303013",  # 90 or more minutes
          ]
hs_educations = [
              "B15003017",  # High School Diploma
              "B15003018",  # GED or alternative diploma
        ]
some_college_educations = [
              "B15003019",  # Some college < 1 year
              "B15003020",  # Some college, 1+ years
              "B15003021",  # Associates degree
        ]
college_plus_educations = [
              "B15003022",  # Bachelors degree
              "B15003023",  # Masters degree
              "B15003023",  # Professional degree
              "B15003024",  # Doctoral degree
        ]
less_than_hs_educations = [
              "B15003002",  # No schooling completed 
              "B15003003",  # Nursery school
              "B15003004",  # Kindergarten
              "B15003005",  # 1st Grade
              "B15003006",  # 2nd Grade
              "B15003007",  # 3rd Grade
              "B15003008",  # 4th Grade
              "B15003009",  # 5th Grade
              "B15003010",  # 6th Grade
              "B15003011",  # 7th Grade
              "B15003012",  # 8th Grade
              "B15003013",  # 9th Grade
              "B15003014",  # 10th Grade
              "B15003015",  # 11th Grade
              "B15003016",  # 12th Grade, no diploma
              ]
commute_times = [
              "B08303002",  # Less than 5 Minutes
              "B08303003",  # 5 to 9 minutes
              "B08303004",  # 10 to 14 minutes
              "B08303005",  # 15 to 19 minutes
              "B08303006",  # 20 to 24 minutes
              "B08303007",  # 25 to 29 minutes
              "B08303008",  # 30 to 34 minutes
              "B08303009",  # 35 to 39 minutes
              "B08303010",  # 40 to 44 minutes
              "B08303011",  # 45 to 59 minutes
              "B08303012",  # 60 to 89 minutes
              "B08303013",  # 90 or more minutes
          ]
commute_centers = [
              2.5,  # "B08303002",  # Less than 5 Minutes
              7.5,  # "B08303003",  # 5 to 9 minutes
              12.5,  # "B08303004",  # 10 to 14 minutes
              17.5,  # "B08303005",  # 15 to 19 minutes
              22.5,  # "B08303006",  # 20 to 24 minutes
              27.5,  # "B08303007",  # 25 to 29 minutes
              32.5,  # "B08303008",  # 30 to 34 minutes
              37.5,  # "B08303009",  # 35 to 39 minutes
              42.5,  # "B08303010",  # 40 to 44 minutes
              52.5,  # "B08303011",  # 45 to 59 minutes
              72.5,  # "B08303012",  # 60 to 89 minutes
              100,  # "B08303013",  # 90 or more minutes
              ]

msas = pandas.read_csv('qcew_msa.txt', sep='\t')  # Read in a list of census MSAs
msas['census_msa_code'] = msas['qcew_code'].apply(lambda x: '31000US%s0' % x.replace('C', ''))  # 310000 is the MSA
# code
for i in tables_list:
    msas[i] = np.nan  # Create a column
    msas[i] = msas.apply(lambda x: census_lookup(x['census_msa_code'],i), axis=1)    # Do census lookups
msas['population'] = msas["B15003001"] # Total population
msas['unemployment'] = msas["B23025005"]/msas["B23025002"]
msas['lt_highschool'] = msas[less_than_hs_educations].sum(axis=1)/msas['population']
msas['highschool'] = msas[hs_educations].sum(axis=1)/msas['population']
msas['some_college'] = msas[some_college_educations].sum(axis=1)/msas['population']
msas['college_plus'] = msas[college_plus_educations].sum(axis=1)/msas['population']
# msas['median_income'] = msas['B19013001']
msas['frac_white'] = msas["B02001002"]/msas["B02001001"]
msas['avg_commute'] = msas[commute_times].dot(commute_centers)/msas["B08303001"]
for col in msas.columns:
    if col[0] == 'B':
        # This is a raw census column, delete!
        del msas[col]
# Finished merging in data from ACS published tables

# Begin merging data from custom ACS tabs
acs_wage_tabs = pandas.read_csv('acs_2013_msa_gender_wage.csv')
acs_wage_tabs['census_msa_code']=acs_wage_tabs['met2013'].apply(lambda x: '31000US%s' % str(int(x)))  # 310000 is the MSA code
del acs_wage_tabs['met2013']
acs_wage_tabs.rename(columns={
    'p05': 'wage_p05',
    'p10': 'wage_p10',
    'p25': 'wage_p25',
    'p50': 'wage_p50',
    'p75': 'wage_p75',
    'p90': 'wage_p90',
    'p95': 'wage_p95',
    'sum.wght': 'wage_sum.wght',
    'mean.wage': 'wage_mean.wage',
    'var.wage': 'wage_var.wage',
    }, inplace=True)

msa_sex = msas.merge(acs_wage_tabs, on='census_msa_code')
# NOTE: we only keep 260 of the 405 MSAs here. need to investigate why
# ALSO NOTE: this dataframe goes from being at the msa level to the
# msa-gender level

# Begin reshaping on sex: this bit gets a little awkward
subset = msa_sex[['sex', 'census_msa_code']]
subset.to_records(index=False).tolist()
index = pandas.MultiIndex.from_tuples(subset.to_records(index=False).tolist(), names=subset.columns.tolist())
msa_sex.index = index
msa_sex.reindex()
msa_sex = msa_sex.unstack('sex')
# Note: this leaves us with a DF with the right shape, but it has a
# multiindex for columns when we really need most things to NOT be
# there...
msa_sex.columns.names=['main', 'sex']
del msa_sex['sex']
female = msa_sex.xs(2, level='sex', axis=1)
male = msa_sex.xs(1, level='sex', axis=1)
# female = female[['index_p50','index_p25','index_p75','index_mean.wage','index_sum.wght']]
female.rename(columns={
    'wage_p05': 'female_wage_p05',
    'wage_p10': 'female_wage_p10',
    'wage_p25': 'female_wage_p25',
    'wage_p50': 'female_wage_p50',
    'wage_p75': 'female_wage_p75',
    'wage_p90': 'female_wage_p90',
    'wage_p95': 'female_wage_p95',
    'wage_mean.wage': 'female_wage_mean',
    'epop': 'female_epop',
    'N': 'female_N',
    },
    inplace=True)
male.rename(columns={
    'wage_p05': 'male_wage_p05',
    'wage_p10': 'male_wage_p10',
    'wage_p25': 'male_wage_p25',
    'wage_p50': 'male_wage_p50',
    'wage_p75': 'male_wage_p75',
    'wage_p90': 'male_wage_p90',
    'wage_p95': 'male_wage_p95',
    'wage_mean.wage': 'male_wage_mean',
    'epop': 'male_epop',
    'N': 'male_N',
    }, inplace=True)
duplicate_columns = [i for i in male.columns if 'male' not in i]
for c in duplicate_columns:
    del male[c]
msa_sex = pandas.concat([male, female], axis=1)
msa_sex.to_csv('acs.csv', index=False)  # Don't write the census_msa_code index, because the column already exists
