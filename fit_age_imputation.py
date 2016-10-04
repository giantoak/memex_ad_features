#!/usr/bin/python
# -*- coding: utf-8 -*-
import pandas
import numpy as np
import datetime
import ipdb
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_selection import SelectFromModel
from sklearn.pipeline import Pipeline
from sklearn.cross_validation import cross_val_score
from sklearn.linear_model import LinearRegression
import cPickle

from scipy.sparse import hstack
from sklearn.ensemble import RandomForestRegressor 
data = pandas.read_csv('data/temp/ad_ids_with_age.csv', sep='\t')
#data1 = data.loc[np.random.choice(data.index, 100)]
#data2 = data.loc[np.random.choice(data.index, 1000)]
thresh = .00001
count = 2000000
np.random.seed(4)
#data3 = data.loc[np.random.choice(data.index, count)]
data3=data.copy()
data3.loc[data3['content'].isnull(),'content']=''


rf=RandomForestRegressor(n_jobs=-1)
cv = CountVectorizer()
X = cv.fit_transform(data3['content'])
y = data3['age']
rf_fitted=rf.fit(X, data3['age'])
sm=SelectFromModel(rf, prefit=True, threshold=thresh)
good_features=pandas.Series({v:k for k, v in cv.vocabulary_.iteritems()})[sm.get_support()].values.tolist()

new_cv = CountVectorizer(vocabulary=good_features) # create a count vectorizer that only checks for existence of our "good" features
cPickle.dump(new_cv,open('age_imputation_text_extractor.pkl','wb'))
X_new=sm.transform(X)
print('%s features' % X_new.shape[1])
rf_new = RandomForestRegressor(oob_score=True, n_jobs=-1)
rf_new.fit(X_new,y)
cPickle.dump(rf_new,open('age_imputation_model.pkl','wb'))
if True:
    rf_new_eval = RandomForestRegressor(oob_score=True, n_jobs=-1)
    results = cross_val_score(rf_new_eval, X_new, y, n_jobs=-1)
    print('RF R2 scores from CV: %0.3f' % np.mean(results))
