import pandas
import cPickle
import ipdb
import numpy as np
#prices =pandas.read_csv('ad_price_ad_level.csv')
age_text = pandas.read_csv('data/temp/ad_ids_with_age.csv', sep='\t')
locations = pandas.read_csv('data/escort_cdr_2/cbsa-combined.tsv', sep='\t', names=['ad_id','msa','type']) 
age_text = pandas.merge(age_text, locations, how='left')
age_text = age_text[age_text['msa'].notnull()]
age_text = age_text[age_text['content'].notnull()]

cv = cPickle.load(open('/home/ubuntu/memex_ad_features/age_imputation_text_extractor.pkl','rb'))
rf_age = cPickle.load(open('/home/ubuntu/memex_ad_features/age_imputation_model.pkl','rb'))
X=cv.transform(age_text['content'])
age = rf_age.predict(X)
age_text['age_imputed'] = age

age_text[['ad_id','age','msa','type','age_imputed']].to_csv('age_city_imputation_export.csv', index=False)

no_age = pandas.read_csv('data/temp/ad_ids_no_age.csv', sep='\t')
no_age = pandas.merge(no_age, locations, how='left')
no_age = no_age[no_age['msa'].notnull()]
no_age = no_age[no_age['content'].notnull()]
X=cv.transform(no_age['content'])
no_age_imputed = rf_age.predict(X)
no_age['age_imputed'] = no_age_imputed
no_age[['ad_id','msa','type','age_imputed']].to_csv('age_city_imputation_export_imputation_only.csv', index=False)
