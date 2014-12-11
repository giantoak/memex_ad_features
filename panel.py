import pandas
import datetime
import random
import ipdb
import numpy as np
import glob
import bls

def mtime(datestr):
    try:
        d = datetime.datetime.strptime(datestr, '%Y-%m-%d')
        return datetime.datetime.strptime(datetime.datetime.strftime(d, '%Y-%m'), '%Y-%m')
    except TypeError:
        return np.nan
def mday(datestr):
    try:
        d = datetime.datetime.strptime(datestr, '%Y-%m-%d')
        return datetime.date.fromordinal(datetime.datetime.strptime(datetime.datetime.strftime(d, '%Y-%m'), '%Y-%m').toordinal())
    except TypeError:
        return np.nan
def ptime(datestr):
    try:
        return datetime.datetime.strptime(datestr, '%Y-%m-%d')
    except TypeError:
        return np.nan
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

if True:
    header = open('norm_US_Canadaa_header.csv','r').read().split(',')
    header = [i.strip() for i in header]
    h = ['rownum']
    h.extend(header)
    out = pandas.DataFrame(columns=header)
    files=glob.glob('norm_US_Canadaa[a-z].csv') 
    files=glob.glob('norm_US_Canadaaa.csv') 
    for i in files:
        try:
            a = pandas.read_csv(i, names=header)
            a.index = range(len(a))
            sample = random.sample(a.index, int(float(len(a))/5))
            rs = a.ix[sample]
            out = pandas.concat([out, rs], axis=0)
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
    #out.Cost_hour_mean[out.Cost_hour_mean < 0] = np.nan
    out=out.reindex(range(len(out)))  
    #out.city[~out.city.isnull()]=out.city[~out.city.isnull()].apply(lambda x: x.title()) # title-case all city names
    def bls_code(x):
        try:
    	    out = 'LAUCT%02d%05d00000003' % ( x['state_fips'], x['place_fips'])
    	    return out 
        except TypeError:
    	    return np.nan
    
    def get_LAU(x):
        try: 
    	    if np.isnan(x['bls_code']):
    	        return np.nan
        except TypeError:
    	    out = bls.get_series(x['bls_code'], startyear=2013, endyear=2013)
    	return out.mean()[0]
        #except:
    	#return np.nan
    
    out.to_csv('all.csv')
    acs = pandas.read_csv('bp_acs.csv')
    acs.index=acs.place
    new = pandas.merge(out, acs, left_on='region', right_on='place') 
    fips = pandas.read_csv('locations_resolved.csv')
    fips.url=fips.url.str[1:]
    fips.index = fips.url
    fips['bls_code'] = fips.apply(bls_code, axis=1)
    #fips['unemployment'] = fips.apply(get_LAU, axis=1)
    acs = pandas.concat([fips, acs], axis=1)
    acs['region'] = acs.url
    new = pandas.merge(new, fips, left_on='region', right_on='url') 
    #new['completeness']=pandas.Series(0,index=new.index)
    #new['completeness'][new.Cost_hour_mean > 0] += 1
    #new['completeness'][new.Age_mean > 0] += 1
    #new['completeness'][new.Cup_mean > 0] += 1
    new.to_csv('coded.csv')
else:
    new = pandas.read_csv('coded.csv')

new['Date'] = new.date.apply(ptime)
new['MonthDate'] = new.date.apply(mtime)
#new['Week'] = new.Date.dt.week
new['Year'] = new.Date.dt.year
new['Month'] = new.Date.dt.month
#all=new.groupby(['region','Week']).size()
#all=new.groupby(['region','Year','Week']).size()
#all=new.groupby(['region','Year','Month']).size()
all=new.groupby(['region','MonthDate']).size()
df = pandas.DataFrame(all, columns=['counts'])
df.to_csv('counts.csv')
all.to_csv('sample_counts_region.csv')
ipdb.set_trace()
