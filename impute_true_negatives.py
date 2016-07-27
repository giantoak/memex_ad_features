#!/usr/bin/python
# -*- coding: utf-8 -*-
import pandas
import numpy as np
import json
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
new_cv = cPickle.load(open('price_imputation_text_extractor.pkl','rb'))
rf_new = cPickle.load(open('price_imputation_model.pkl','rb'))
# Begin actual imputation on whole data set
true_negatives = []
error_count = 0
for i in open('true_negatives_text.json').readlines():
    try:
        true_negatives.append(json.loads(i.strip()))
    except:
        error_count += 1
content = [i['_source']['extracted_text'] for i in true_negatives if i['_source']['extracted_text']]
doc_id = [i['_source']['doc_id'] for i in true_negatives if i['_source']['extracted_text']]
X=new_cv.transform(content)
price = rf_new.predict(X)
out = pandas.DataFrame({'doc_id':doc_id, 'price_imputed':price})
out.to_csv('true_negatives_price.csv', index=False)
pass
