import glob
import pandas
outputs = []
try:
    data = pandas.read_csv('temp.csv')
    print('Found temp.csv on disk, loading that...')
except:
    print('No temp.csv found, creating...')
    for i in glob.glob('*singlefile*csv'):
        with open(i,'r') as f:
            print('reading file: %s' % i)
            new = pandas.read_csv(i)
            new = new[new.agglvl_code==44]
            outputs.append(new)
    data = pandas.concat(outputs, axis=0)
d=data[['area_fips','industry_code','year','qtr',u'month1_emplvl', u'month2_emplvl', u'month3_emplvl']]
e=pandas.melt(d,id_vars=[u'area_fips', u'industry_code', u'year', u'qtr'], var_name='sub_month', value_name='employment') # Reshape to make months verticle instead of horizontal... 
e['sub_month'].replace({'month1_emplvl':1,'month2_emplvl':2,'month3_emplvl':3}, inplace=True) # Remap month names
e['month'] = (e['qtr'] - 1) * 3 + e['sub_month'] # Create month of year var
del e['sub_month']
del e['qtr']

cw = pandas.read_csv('msa_crosswalk.csv')
wages=pandas.read_csv('metro_level.csv')
wages= pandas.merge(wages, cw, left_on='metarea', right_on='ipums_code')


# Need to do this merge on BOTH industry and MSA
out = pandas.merge(wages, e, left_on='qcew_code', right_on='area_fips')
