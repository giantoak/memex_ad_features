#!/usr/bin/python
# -*- coding: utf-8 -*-
import pandas as pd
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
new_cv = cPickle.load(open('price_imputation_text_extractor.pkl','rb'))
rf_new = cPickle.load(open('price_imputation_model.pkl','rb'))
# Begin actual imputation on whole data set
step = 10000
max_counts = 80000000
prefix='lsdfka'
for i in range(0, max_counts/step):
    print('Working on group %s to %s' % (step*i, str(step*(i + 1))))
    try:
        data = pd.read_csv('data/escort_cdr_2/content.tsv', sep='\t', skiprows = step*i, nrows=step, names=['ad_id','site','type','url','text','content'])
        data.loc[data['content'].isnull(), 'content']=''
        X=new_cv.transform(data['content'])
        price = rf_new.predict(X)
        out = pd.DataFrame({'imputed_price':price})
        out['ad_id'] = data['ad_id']
        out.to_csv('tmp/partial_prices_%s_%s.csv' % (prefix,str(i)), index=False)
    except:
        print('Error on group %s to %s' %  (step*i, str(step*(i + 1))))

out_list = []
for i in glob.glob('tmp/partial_price_%s*' %  prefix):
    out_list.append(pd.read_csv(i))

out = pd.concat(out_list)
out.to_csv('ad_price_imputed.csv', index=False)
import os
for i in glob.glob('tmp/partial_price_%s*' %  prefix):
    if os.path.exists(i):
        os.remove(i)
