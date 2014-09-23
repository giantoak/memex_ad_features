from fipser import Fipser
import pandas as pd

fipser = Fipser()
df = pd.read_csv('locations.csv')
d = {}

def resolve(loc):
    if loc not in d:
        r = fipser.resolve(loc)
        d[loc] = r
    else:
        r = d[loc]
    
    return r

results = df['loc'].apply(resolve)
