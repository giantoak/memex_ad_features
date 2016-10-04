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
data = pandas.read_csv('data/temp/ad_ids_with_price.csv')
prices = pandas.read_csv('ad_price_ad_level.csv')
#data1 = data.loc[np.random.choice(data.index, 100)]
#data2 = data.loc[np.random.choice(data.index, 1000)]
thresh = .0001
count = 1000000
np.random.seed(4)
data3 = data.loc[np.random.choice(data.index, count)]
data3 = data3.merge(prices[['ad_id','price_per_hour']])
data3['content'] = data3['content'].apply(lambda x: re.sub(r'(\b)[\d]{1,5}[hr]{0,3}(\b)',r'\1\2', x))


rf=RandomForestRegressor(n_jobs=-1)
cv = CountVectorizer()
X = cv.fit_transform(data3['content'])
y = data3['price_per_hour']
rf_fitted=rf.fit(X, data3['price_per_hour'])
sm=SelectFromModel(rf, prefit=True, threshold=thresh)
good_features=pandas.Series({v:k for k, v in cv.vocabulary_.iteritems()})[sm.get_support()].values.tolist()

new_cv = CountVectorizer(vocabulary=good_features) # create a count vectorizer that only checks for existence of our "good" features
cPickle.dump(new_cv,open('price_imputation_text_extractor.pkl','wb'))
X_new=sm.transform(X)
rf_new = RandomForestRegressor(oob_score=True, n_jobs=-1)
rf_new.fit(X_new,y)
cPickle.dump(rf_new,open('price_imputation_model.pkl','wb'))
if False:
    rf_new_eval = RandomForestRegressor(oob_score=True, n_jobs=-1)
    results = cross_val_score(rf_new_eval, X_new, y, n_jobs=-1)
    print('RF R2 scores from CV: %0.3f' % np.mean(results))
