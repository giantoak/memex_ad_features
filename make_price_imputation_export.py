import pandas
import cPickle
import ipdb
import numpy as np
#prices =pandas.read_csv('ad_price_ad_level.csv')
price = pandas.read_csv('data/temp/ad_ids_with_price.csv')
price_computations = pandas.read_csv('ad_price_ad_level.csv')
price = pandas.merge(price, price_computations[['ad_id','price_per_hour']], how='left')
locations = pandas.read_csv('data/escort_cdr_2/cbsa-combined.tsv', sep='\t', names=['ad_id','msa','type']) 
price = pandas.merge(price, locations, how='left')
price = price[price['msa'].notnull()]
price = price[price['content'].notnull()]

cv = cPickle.load(open('/home/ubuntu/memex_ad_features/price_imputation_text_extractor.pkl','rb'))
rf_price = cPickle.load(open('/home/ubuntu/memex_ad_features/price_imputation_model.pkl','rb'))
X=cv.transform(price['content'])
price_imputed = rf_price.predict(X)
price['price_imputed'] = price_imputed

price[['ad_id','price_per_hour','msa','type','price_imputed']].to_csv('price_city_imputation_export.csv', index=False)

no_price = pandas.read_csv('data/temp/ad_ids_no_price.csv')
no_price = pandas.merge(no_price, locations, how='left')
no_price = no_price[no_price['msa'].notnull()]
no_price = no_price[no_price['content'].notnull()]
X=cv.transform(no_price['content'])
no_price_imputed = rf_price.predict(X)
no_price['price_imputed'] = no_price_imputed
no_price[['ad_id','msa','type','price_imputed']].to_csv('price_city_imputation_export_imputation_only.csv', index=False)
