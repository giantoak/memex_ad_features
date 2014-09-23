import pandas as pd

df = pd.read_csv('regions.csv')

df2 = df.copy()

df2['loc_parts'] = df.location.apply(lambda x: x.split(','))

df['loc'] = df2['loc_parts'].apply(lambda x: ','.join(x[:-1]))
df = df[df2['loc_parts'].apply(lambda x: x[-1].strip()) == 'USA']\
        .drop('location', 1)

df.to_csv('locations.csv', index=False)
