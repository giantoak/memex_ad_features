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
out = pandas.read_csv('norm_US_Canadaaa.csv.price')
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

acs = pandas.read_csv('~/bp_acs.csv')
new = pandas.merge(out, acs, left_on='region', right_on='place') 
