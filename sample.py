import pandas
import random
import ipdb
import numpy as np
import glob
header = open('norm_US_Canadaa_header.csv','r').read().split(',')
header = [i.strip() for i in header]
h = ['rownum']
h.extend(header)
out = pandas.DataFrame(columns=header)
files=glob.glob('norm_US_Canadaa[a-z].csv') 
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

for i in files:
    print(i, out.shape)
    try:
        a = pandas.read_csv(i, names=header)
        a.index = range(len(a))
        sample = random.sample(a.index, int(float(len(a))/20))
        rs = a.ix[sample]
        out = pandas.concat([out, rs], axis=0)
        #ipdb.set_trace()
        out.index = range(len(out))
    except:
        pass

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
ipdb.set_trace()

out = out[~out.city.isnull()]
out.state = out.state.apply(lambda x: x.strip())
g = out.groupby(['date','region'])

sample = random.sample(out.index, 100000)
rs = out.ix[sample]
out.to_csv('cleaned.csv')
