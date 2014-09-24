import pandas
import numpy as np
import ipdb
import datetime
#files=glob.glob('norm_US_Canadaa[a-z].csv.price') 
def ptime(datestr):
    try:
        return datetime.datetime.strptime(datestr, '%Y-%m-%d')
    except TypeError:
        return np.nan
#for i in files:
    #a = pandas.read_csv(i, names=header)
    #a = a[a.Cost_hour_mean > 0]
    #a = a[a.country == 'USA']
    #a.to_csv(i + '.price')
header = open('norm_US_Canadaa_header.csv','r').read().split(',')
header = [i.strip() for i in header]
a = pandas.read_csv('norm_US_Canadaac.csv.price', names=header)
a = a[a.country == 'USA']
a.date = a.date.apply(ptime)
for i in a.columns:
    try:
        a[i][a[i] < 0] = np.nan
    except ValueError:
        a[i].dtype = np.dtype('float64')
        a[i][a[i] < 0] = np.nan
    #try:
    #except Error as e:
        #print(e)
        
