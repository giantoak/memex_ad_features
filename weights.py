#
"""
This is an example of doing clustering in python
"""
import pandas
from sklearn import cluster
import numpy as np

a=pandas.read_csv('demographics_v2.csv')
b=a[['B01001_001','B19013_001','B15002_003']]
# Take a subset of columns

for i in range(3):
     b=b[~(b[b.columns[i]].isnull())]
    # remove missing rows

k=cluster.KMeans(n_clusters=6)
fitted = k.fit(b)

print(fitted.cluster_centers_)
# These are the centers of the cluster, in an n_clusters X cols of b
# array

print(pandas.Series(fitted.fit_predict(b)).value_counts())
# The tabulated values of the cluster label. This is a n-vector.

print(pandas.Series(fitted.labels_).value_counts())
# Same as fit_predict, except is just the labels for the input data

# Note: the cluster centers could also be computed like:
b['cluster'] = fitted.labels_
print(b.groupby('cluster').apply(np.mean))
