import glob
import pandas
import ipdb
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
d=data[['area_fips','industry_code','year','qtr',u'month1_emplvl', u'month2_emplvl', u'month3_emplvl']] # Keep only select columns
e=pandas.melt(d,id_vars=[u'area_fips', u'industry_code', u'year', u'qtr'], var_name='sub_month', value_name='employment') # Reshape to make months verticle instead of horizontal... 
e['sub_month'].replace({'month1_emplvl':1,'month2_emplvl':2,'month3_emplvl':3}, inplace=True) # Remap month names
e['month'] = (e['qtr'] - 1) * 3 + e['sub_month'] # Create month of year var
del e['sub_month']
del e['qtr']



cw = pandas.read_csv('msa_crosswalk.csv')
wages=pandas.read_csv('metro_level.csv')
industry_remap={
# This dictionary maps naics2 from IPUMS to the QCEW format
'11':'11',
'21':'21',
'22':'22',
'23':'23',
'31':'31-33',
'32':'31-33',
'33':'31-33',
'3M':'31-33',
'42':'42',
'44':'44-45',
'45':'44-45',
'48':'48-89',
'49':'48-89',
'49':'48-89',
'4M':'44-45',
'51':'51',
'52':'52',
'53':'53',
'54':'54',
'55':'55',
'56':'56',
'61':'61',
'62':'62',
'71':'71',
'72':'72',
'81':'81',
}
wages= wages[wages['naics2'] != '92'] # Delete public administration industry - not covered in QCEW
wages['industry_code'] = wages['naics2'].apply(lambda x: industry_remap[x])
wages= pandas.merge(wages, cw, left_on='metarea', right_on='ipums_code')


# Need to do this merge on BOTH industry and MSA
merged = pandas.merge(wages, e, left_on=['qcew_code','industry_code'], right_on=['area_fips','industry_code'])

b=merged.groupby(['sex','month','year','area_fips'])['employment'].aggregate({'total_workers':sum})
# Compute the sum of workers by gender and month in a particular area
merged=pandas.merge(merged, b, left_on=['sex','month','year','area_fips'],right_index=True)
# Merge in counts back to DF
merged['industry_share'] = merged['employment']/merged['total_workers']

# Test that the industry shares sum to 1 (to machine precision)
test = merged.groupby(['sex','month','year','area_fips'])['industry_share'].sum()
(test - 1).sum() # The differences with 1 should be very small
print(test.sum() - test.shape[0]) # Should be 0

def create_index(df,varname='p50'):
    df['ws'] = df[varname] * df['industry_share'] # Compute a weighted share for summing
    df=pandas.merge(df, df.groupby(['sex','month','year','area_fips'])['ws'].aggregate({'index_' + varname :sum}), left_on=['sex','month','year','area_fips'], right_index=True)
# Group by sex, month, and area, then sum the weight share, and merge
# this in as 'p50_index'
    del df['ws'] # Remove temporary weighted share
    return df

indices = ['p50','p25','p75','mean.wage']
for i in indices:
    merged = create_index(merged, i)

output_cols = ['sex','month','year','area_fips']
for i in indices:
    output_cols.append('index_' + i)
out = merged[output_cols]
out = out.drop_duplicates(['sex','month','year','area_fips'])
out.sort(['sex','area_fips','year','month'], inplace=True)
out.to_csv('female_opportunity_index.csv', index=False)
