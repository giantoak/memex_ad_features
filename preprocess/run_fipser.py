from fipser import Fipser
import pandas as pd

fipser = Fipser()
df = pd.read_csv('bp.csv')
d = {}

def resolve(loc):
    if loc not in d:
        r = fipser.resolve(loc)
        d[loc] = r
    else:
        r = d[loc]
    
    return r

resolved = []
for i, row in df.iterrows():
    query = row['place'] + ',' + row['state']
    ans = resolve(query)

#    print query, ans
    resolved.append(ans)

df_results = pd.DataFrame(list(resolved)) 
df_results = pd.concat([df_results, df], axis=1)

df_results.to_csv('data/locations_resolved.csv', index=False)
