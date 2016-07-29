#!/usr/bin/python
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import ujson as json
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
new_cv = cPickle.load(open('price_imputation_text_extractor.pkl', 'rb'))
rf_new = cPickle.load(open('price_imputation_model.pkl', 'rb'))
# Begin actual imputation on whole data set
true_positives = [json.loads(i) for i in open('true_positives_text.json')]
content = [i['content'] for i in true_positives]
doc_id = [i['doc_id'] for i in true_positives]
X = new_cv.transform(content)
price = rf_new.predict(X)
out = pd.DataFrame({'doc_id': doc_id, 'price_imputed': price})
out.to_csv('true_positives_price.csv', index=False)
