import pandas
import random
import ipdb
import numpy as np
import glob

STATES = (
    ("AL","Alabama"),
    ("AK","Alaska"),
    ("AZ","Arizona"),
    ("AR","Arkansas"),
    ("CA","California"),
    ("CO","Colorado"),
    ("CT","Connecticut"),
    ("DE","Delaware"),
    ("DC","District of Columbia"),
    ("FL","Florida"),
    ("GA","Georgia"),
    ("HI","Hawaii"),
    ("ID","Idaho"),
    ("IL","Illinois"),
    ("IN","Indiana"),
    ("IA","Iowa"),
    ("KS","Kansas"),
    ("KY","Kentucky"),
    ("LA","Louisiana"),
    ("ME","Maine"),
    ("MT","Montana"),
    ("NE","Nebraska"),
    ("NV","Nevada"),
    ("NH","New Hampshire"),
    ("NJ","New Jersey"),
    ("NM","New Mexico"),
    ("NY","New York"),
    ("NC","North Carolina"),
    ("ND","North Dakota"),
    ("OH","Ohio"),
    ("OK","Oklahoma"),
    ("OR","Oregon"),
    ("MD","Maryland"),
    ("MA","Massachusetts"),
    ("MI","Michigan"),
    ("MN","Minnesota"),
    ("MS","Mississippi"),
    ("MO","Missouri"),
    ("PA","Pennsylvania"),
    ("RI","Rhode Island"),
    ("SC","South Carolina"),
    ("SD","South Dakota"),
    ("TN","Tennessee"),
    ("TX","Texas"),
    ("UT","Utah"),
    ("VT","Vermont"),
    ("VA","Virginia"),
    ("WA","Washington"),
    ("WV","West Virginia"),
    ("WI","Wisconsin"),
("WY","Wyoming"),
("PR","Puerto Rico")
)
header = open('norm_US_Canadaa_header.csv','r').read().split(',')
header = [i.strip() for i in header]
h = ['rownum']
h.extend(header)
out = pandas.DataFrame(columns=header)
files=glob.glob('norm_US_Canadaa[a-z].csv') 
for i in files:
    try:
        a = pandas.read_csv(i, names=header)
        a.index = range(len(a))
        sample = random.sample(a.index, int(float(len(a))/5))
        rs = a.ix[sample]
        out = pandas.concat([out, rs], axis=0)
        #ipdb.set_trace()
        out.index = range(len(out))
    except:
        pass
#out = pandas.read_csv('norm_US_Canadaaa.csv.price')
out = out[~(out.country == 'Canada')]
out=out[~out.state.isnull()]
out.state=out.state.apply(lambda x: x.strip())
for i in STATES:
    out.state[out.state == i[0]] = i[1]
out.state=out.state.apply(lambda x: x.title())
out = out[~(out.state == 'Mt 59901')]
out = out[~(out.state == 'British Columbia')]
out = out[~(out.state == 'Guam')]
out.state[out.state == 'Lousiana'] = 'Louisiana'
out = out[~(out.state == 'Uk')]
out.Cost_hour_mean[out.Cost_hour_mean < 0] = np.nan
out=out.reindex(range(len(out)))  
out.city[~out.city.isnull()]=out.city[~out.city.isnull()].apply(lambda x: x.title()) # title-case all city names

acs = pandas.read_csv('bp_acs.csv')
new = pandas.merge(out, acs, left_on='region', right_on='place') 
new.to_csv('coded.csv')

all=new.groupby('region').size()
all.to_csv('sample_counts_region.csv')

new.Cup_mean[new.Cup_mean < 0] = np.nan
nocup=new[~(new.Cup_mean.isnull())].groupby('region').size()
nocup = nocup/all.astype('float')
nocup.to_csv('nocupsize_region.csv')

new.Age_mean[new.Age_mean < 0] = np.nan
age=new[~(new.Age_mean.isnull())].groupby('region').size()
age = age/all.astype('float')
age.to_csv('age_counts_region.csv')
sample = random.sample(new.index, 100000)
rs = new.ix[sample]
new.to_csv('cleaned_region.csv')

# Compute 'completeness' which gets a point for every value we're able to
# parse
new['completeness']=pandas.Series(0,index=new.index)
new['completeness'][new.Cost_hour_mean > 0] += 1
new['completeness'][new.Age_mean > 0] += 1
new['completeness'][new.Cup_mean > 0] += 1
completeness = new[['region','completeness']].groupby('region').mean()
completeness.to_csv('completeness_region.csv')
