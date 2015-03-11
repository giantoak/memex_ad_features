import glob
import pandas
outputs = []
for i in glob.glob('*singlefile*csv'):
    with open(i,'r') as f:
        print('reading file: %s' % i)
        new = pandas.read_csv(i)
        new = new[new.agglvl_code==44]
        outputs.append(new)
#a=pandas.read_csv("2014.q1-q2.singlefile.csv")
#a=a[a.agglvl_code == 44]   # Select only major industry
data = pandas.concat(outputs, axis=0)
e=pandas.melt(d,id_vars=[u'area_fips', u'industry_code', u'year', u'qtr'], var_name='sub_month', value_name='employment') # Reshape to make months verticle instead of horizontal... 
e['sub_month'].replace({'month1_emplvl':1,'month2_emplvl':2,'month3_emplvl':3}, inplace=True) # Remap month names
e['month'] = (e['qtr'] - 1) * 3 + e['sub_month'] # Create month of year var
del e['sub_month']
del e['qtr']
