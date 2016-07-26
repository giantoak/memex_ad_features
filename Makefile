### System Variables
HTTP_GET = wget
GET_FROM_DEEPDIVE_S3 = aws s3 cp --profile stanford
PUT_TO_GIANTOAK_S3 = aws s3 cp
EXPORT_BUCKET = giantoakmemex
GET_FROM_GIANTOAK_S3 = aws s3 cp

.PHONY: cleanish clean local export

### Download Related targets

### Data from Tempus export
acs_2015_03_18.csv:
	$(HTTP_GET) http://$(EXPORT_BUCKET).s3.amazonaws.com/sex_ad_analysis/input/acs_2015_03_18.csv
### Data from Greg
crosswalk_tract_msa.csv:
	$(HTTP_GET) http://$(EXPORT_BUCKET).s3.amazonaws.com/sex_ad_analysis/input/crosswalk_tract_msa.csv

ucr_lemas_msa.csv:
	# Get UCR data from Greg
	$(HTTP_GET) http://$(EXPORT_BUCKET).s3.amazonaws.com/sex_ad_analysis/input/ucr_lemas_msa.csv

### Data from Steve Bach
data/bach/phones.csv:
	mkdir -p data/bach/
	$(GET_FROM_GIANTOAK_S3) s3://$(EXPORT_BUCKET)/sex_ad_analysis/input/phones.csv .
	mv phones.csv data/bach/
data/bach/phones_by_month.csv:
	mkdir -p data/bach/
	$(GET_FROM_GIANTOAK_S3) s3://$(EXPORT_BUCKET)/sex_ad_analysis/input/phones_by_month.csv .
	mv phones_by_month.csv data/bach/

### Data from TGG
data/TGG/provider_prices.csv:
	mkdir -p data/TGG/
	$(GET_FROM_GIANTOAK_S3) s3://$(EXPORT_BUCKET)/sex_ad_analysis/input/ter_datasets.7z .
	7z e -odata/TGG/ ter_datasets.7z  
	rm ter_datasets.7z
### Data from Stanford
data/forGiantOak3/doc-provider-timestamp.tsv:
	# Get data from the Deep Dive data drop, and extract it
	$(GET_FROM_DEEPDIVE_S3) s3://memex-data/escort_beta/forGiantOak3.tgz .
	tar xvf forGiantOak3.tgz -C data/
	$(GET_FROM_DEEPDIVE_S3) s3://memex-data/escort_beta/rates2.tsv.gz .
	mv rates2.tsv.gz data/forGiantOak3/
	gunzip -f data/forGiantOak3/rates2.tsv.gz
data/forGiantOak3/ismassageparlorad.tsv:
	# Get massage parlor extraction from the Deep Dive data drop, and extract it
	$(GET_FROM_DEEPDIVE_S3) s3://memex-data/escort_beta/ismassageparlorad.tsv.gz .
	gunzip -f ismassageparlorad.tsv.gz
	mv ismassageparlorad.tsv data/forGiantOak3/
data/forGiantOak3/msa_locations.tsv: data/forGiantOak3/doc-provider-timestamp.tsv
	$(GET_FROM_DEEPDIVE_S3) s3://memex-data/escort_beta/msa_locations_round2.tsv.gz .
	gunzip msa_locations_round2.tsv.gz
	mv msa_locations_round2.tsv data/forGiantOak3/msa_locations.tsv
data/forGiantOak6/incall-new.tsv:
	$(GET_FROM_DEEPDIVE_S3) s3://memex-data/escort_beta/forGiantOak6.tgz .
	tar xvf forGiantOak6.tgz -C data/
	rm forGiantOak6.tgz
data/forGiantOak3/isssexad.tsv:
	# Get the sex ad flag from deep dive
	$(HTTP_GET) http://$(EXPORT_BUCKET).s3.amazonaws.com/sex_ad_analysis/input/isssexad.tsv 
	mv isssexad.tsv data/forGiantOak3/
escort_cdr_2:
	mkdir -p data/escort_cdr_2/
	$(GET_FROM_DEEPDIVE_S3) s3://memex-data/escort_cdr_2/cdr/age-combined.tsv.bz2 data/escort_cdr_2/
	$(GET_FROM_DEEPDIVE_S3) s3://memex-data/escort_cdr_2/cdr/cbsa-combined.tsv.bz2 data/escort_cdr_2/
	$(GET_FROM_DEEPDIVE_S3) s3://memex-data/escort_cdr_2/cdr/content.tsv.bz2 data/escort_cdr_2/
	$(GET_FROM_DEEPDIVE_S3) s3://memex-data/escort_cdr_2/cdr/doc_id_mapping.tsv.bz2 data/escort_cdr_2/
	$(GET_FROM_DEEPDIVE_S3) s3://memex-data/escort_cdr_2/cdr/email-text.tsv.bz2 data/escort_cdr_2/
	$(GET_FROM_DEEPDIVE_S3) s3://memex-data/escort_cdr_2/cdr/ethnicities-dom.tsv.bz2 data/escort_cdr_2/
	$(GET_FROM_DEEPDIVE_S3) s3://memex-data/escort_cdr_2/cdr/flags.tsv.bz2 data/escort_cdr_2/
	$(GET_FROM_DEEPDIVE_S3) s3://memex-data/escort_cdr_2/cdr/locations-combined.tsv.bz2 data/escort_cdr_2/
	$(GET_FROM_DEEPDIVE_S3) s3://memex-data/escort_cdr_2/cdr/massage_places-combined.tsv.bz2 data/escort_cdr_2/
	$(GET_FROM_DEEPDIVE_S3) s3://memex-data/escort_cdr_2/cdr/phones-combined.tsv.bz2 data/escort_cdr_2/
	$(GET_FROM_DEEPDIVE_S3) s3://memex-data/escort_cdr_2/cdr/post_date-dom.tsv.bz2 data/escort_cdr_2/
	$(GET_FROM_DEEPDIVE_S3) s3://memex-data/escort_cdr_2/cdr/rates-text.tsv.bz2 data/escort_cdr_2/
	$(GET_FROM_DEEPDIVE_S3) s3://memex-data/escort_cdr_2/cdr/service-text.tsv.bz2 data/escort_cdr_2/

## Publicly available data sources
cols_17.txt:
	# Get a hand-curated list of the columns of the usa_00017.dat file
	$(HTTP_GET) http://$(EXPORT_BUCKET).s3.amazonaws.com/sex_ad_analysis/input/cols_17.txt

usa_00017.dat:
	# IPUMS 2000 census and QCEW are used to create the wage instruments:
	# Get data from 2000 census, from IPUMS extract
	$(HTTP_GET) http://$(EXPORT_BUCKET).s3.amazonaws.com/sex_ad_analysis/input/usa_00017.dat.gz 
	gunzip -f usa_00017.dat.gz

usa_00018.dat: 
	# Get data from IPUMS extract of 2013 ACS
	$(HTTP_GET) http://$(EXPORT_BUCKET).s3.amazonaws.com/sex_ad_analysis/input/usa_00018.dat.gz 
	gunzip -f usa_00018.dat.gz

cols_18.txt:
	# Get a hand-curated list of the columns of the usa_00018.dat file
	$(HTTP_GET) http://$(EXPORT_BUCKET).s3.amazonaws.com/sex_ad_analysis/input/cols_18.txt

35036-0001-Data.txt:
	# Get data from ICPSR for the National Incident Based Reporting
	# system
	$(HTTP_GET) http://$(EXPORT_BUCKET).s3.amazonaws.com/sex_ad_analysis/input/35036-0001-Data.txt.gz
	gunzip -f 35036-0001-Data.txt.gz

36120-0003-Data.txt:
	# Get data from ICPSR for the National Incident Based Reporting
	# system for 2013, the offense level file
	$(HTTP_GET) http://$(EXPORT_BUCKET).s3.amazonaws.com/sex_ad_analysis/input/36120-0003-Data.txt.gz
	gunzip -f 36120-0003-Data.txt.gz

36120-0005-Data.txt:
	# Get data from ICPSR for the National Incident Based Reporting
	# system for 2013, the victim level file
	$(HTTP_GET) http://$(EXPORT_BUCKET).s3.amazonaws.com/sex_ad_analysis/input/36120-0005-Data.txt.gz
	gunzip -f 36120-0005-Data.txt.gz

ICPSR_34603/DS0001/34603-0001-Data.txt:
	# Download one of the raw ICPSR extract files for NIBRS
	# computations
	mkdir -p ICPSR_34603/
	mkdir -p ICPSR_34603/DS0001/
	$(HTTP_GET) http://$(EXPORT_BUCKET).s3.amazonaws.com/sex_ad_analysis/input/34603-0001-Data.txt.gz
	mv 34603-0001-Data.txt.gz ICPSR_34603/DS0001/
	gunzip -f ICPSR_34603/DS0001/34603-0001-Data.txt.gz 

2011.q1-q4.singlefile.csv: 
	## Work on QCEW downloads
	$(HTTP_GET) http://www.bls.gov/cew/data/files/2014/csv/2014_qtrly_singlefile.zip
	$(HTTP_GET) http://www.bls.gov/cew/data/files/2013/csv/2013_qtrly_singlefile.zip
	$(HTTP_GET) http://www.bls.gov/cew/data/files/2012/csv/2012_qtrly_singlefile.zip
	$(HTTP_GET) http://www.bls.gov/cew/data/files/2011/csv/2011_qtrly_singlefile.zip
	unzip 2014_qtrly_singlefile.zip
	unzip 2013_qtrly_singlefile.zip
	unzip 2012_qtrly_singlefile.zip
	unzip 2011_qtrly_singlefile.zip
	rm 2014_qtrly_singlefile.zip
	rm 2013_qtrly_singlefile.zip
	rm 2012_qtrly_singlefile.zip
	rm 2011_qtrly_singlefile.zip

#### End Download related targets

#### Begin intermediate data targets
price_imputation_model.pkl: fit_price_imputation.py ad_price_ad_level.csv 
	python fit_price_imputation.py

ad_price_imputed.csv: impute.py price_imputation_model.pkl price_imputation_text_extractor.pkl
	python impute.py
CP1_train_ads.json:
	$(GET_FROM_GIANTOAK_S3) s3://$(EXPORT_BUCKET)/CP1_train_ads.json


true_negatives_text.json:
	$(GET_FROM_GIANTOAK_S3) s3://$(EXPORT_BUCKET)/CP1_negatives_KH_unofficial-4.zip .
	unzip -F CP1_negatives_KH_unofficial-4.zip
	rm CP1_negatives_KH_unofficial-4.zip
	mv  CP1_negatives_KH_unofficial-4 true_negatives_text.json

true_negatives_price.csv: true_negatives_text impute_true_negatives.py
	python impute_true_negatives.py
true_positives_text.json: parse_cp.py CP1_train_ads.json
	python parse_cp.py
true_positives_price.csv: true_positives_text.json impute_true_positives.py
	python impute_true_positives.py
msa_characteristics.csv: make_msa_characteristics.py ad_price_ad_level.csv
	python make_msa_characteristics.py

ad_prices_price_level.csv: make_ad_prices.py data/forGiantOak3/msa_locations.tsv data/forGiantOak3/doc-provider-timestamp.tsv data/forGiantOak3/isssexad.tsv data/forGiantOak3/ismassageparlorad.tsv data/forGiantOak6/incall-new.tsv data/bach/phones.csv data/bach/phones_by_month.csv

	python make_ad_prices.py
phone_characteristics.csv: make_phone_characteristics.py data/bach/phones.csv 
	cp data/bach/phones.csv phone_characteristics.csv
############ End intermediate data targets

############ Begin final targets

local: ad_prices_price_level.csv ad_zero_prices.csv   ad_zero_prices.csv msa_characteristics.csv ad_price_ad_level_all.csv

export: msa_characteristics.csv ad_price_ad_level.csv ad_price_ad_level_all.csv phone_characteristics.csv 

	# Export final results
	zip ad_price_ad_level.zip ad_price_ad_level.csv
	$(PUT_TO_GIANTOAK_S3) ad_price_ad_level.zip s3://$(EXPORT_BUCKET)/pipeline/output/ --acl public-read
	#zip ad_price_ad_level_all.zip ad_price_ad_level_all.csv
	#$(PUT_TO_GIANTOAK_S3) ad_price_ad_level_all.zip s3://$(EXPORT_BUCKET)/sex_ad_analysis/output/
	#zip ad_price_price_level.zip ad_prices_price_level.csv
	#$(PUT_TO_GIANTOAK_S3) ad_price_ad_level.zip s3://$(EXPORT_BUCKET)/sex_ad_analysis/output/ --acl public-read
	#zip provider_panel.zip provider_panel.csv
	#$(PUT_TO_GIANTOAK_S3) provider_panel.zip s3://$(EXPORT_BUCKET)/sex_ad_analysis/output/ --acl public-read
	#$(PUT_TO_GIANTOAK_S3) msa_month_characteristics.csv s3://$(EXPORT_BUCKET)/sex_ad_analysis/output/ --acl public-read
	$(PUT_TO_GIANTOAK_S3) msa_characteristics.csv s3://$(EXPORT_BUCKET)/pipeline/output/ --acl public-read
	$(PUT_TO_GIANTOAK_S3) phone_characteristics.csv s3://$(EXPORT_BUCKET)/pipeline/output/ --acl public-read

############ End final targets

############ Begin cleaning targets

cleanish:
	rm -fv ad_prices_price_level.csv
	rm -fv ad_zero_prices.csv
	rm -fv census_2000_msa_industry_gender_wage.csv
	rm -fv acs_2013_msa_gender_wage.csv
	rm -fv ad_zero_prices.csv acs.csv
	rm -fv month_msa_wage_instruments.csv
	rm -fv acs.csv prostitution_nibrs.csv
	rm -fv female_violence_nibrs.csv
	rm -fv violence_nibrs.csv
	rm -fv provider_panel.csv
	rm -fv msa_month_characteristics.csv
	rm -fv msa_characteristics.csv
	rm -fv cpi_crosssection.csv
	rm -fv ad_price_ad_level_all.csv

clean:
	rm -fv *.csv
	rm -fv 2014_qtrly_singlefile.zip
	rm -fv 2013_qtrly_singlefile.zip
	rm -fv 2012_qtrly_singlefile.zip
	rm -fv 2011_qtrly_singlefile.zip
	rm -fv usa_00017.dat
	rm -fv usa_00018.dat
	rm -fv 35036-0001-Data.txt
	rm -fv 36120-0003-Data.txt
	rm -fv 36120-0005-Data.txt
	rm -fv 34603-0001-Data.txt.gz
	rm -fv ICPSR_34603/DS0001/34603-0001-Data.txt
	rm -fv forGiantOak3.tgz
	rm -fv forGiantOak6.tgz
	rm -rfv data/forGiantOak3/
	rm -rfv data/forGiantOak6/

############ End cleaning targets

#####
reports/usage/graph_data.csv: reports/usage/usage-report-do.do ad_price_ad_level.csv msa_characteristics.csv msa_month_characteristics.csv
	stata-mp < reports/usage/usage-report-do.do
usage: reports/usage/graph_data.csv reports/usage/graphs.R
	R --vanilla < reports/usage/graphs.R

#####
# These are leftover targets from before the rewrite
panel_analysis: monthly_panel.csv monthly_panel.r
	R --vanilla < reports/usage/monthly_panel.r

zp_analysis: zero_price_msa_aggregates.csv msa_level_analysis.r
	# Creates:
	# zero_price_msa_diagnostics_mean.csv
	# zero_price_msa_diagnostics_mean.txt
	# zero_price_msa_diagnostics_p90.csv
	# zero_price_msa_diagnostics_p10.txt
	# zero_price_msa_diagnostics_p10.csv
	# zero_price_msa_diagnostics_p90.txt
	R --vanilla < msa_level_analysis.r
micro_analysis: ad_prices_msa_micro.csv individual_level_analysis.r
	# Creates:
	# Tons of files in results/micro_zero_price/
	R --vanilla < individual_level_analysis.r

tempus: all_merged.csv msa_panel.py zero_prices.py
	# Produces counts.csv and region_features.csv,
	# ad_prices_msa_month.csv, and monthly_panel.csv. Also need to run
	# zero_prices to clean up and remove 
	python msa_panel.py
	python zero_prices.py
# These are leftover targets from before the rewrite
#####

### Below here are simultaneously created files. The approach to these is
# to make each of these dependent on the file that actually has a rule defined above
2012.q1-q4.singlefile.csv: 2011.q1-q4.singlefile.csv
2013.q1-q4.singlefile.csv: 2011.q1-q4.singlefile.csv
2014.q1-q3.singlefile.csv: 2011.q1-q4.singlefile.csv
data/forGiantOak3/email_addresses.tsv: data/forGiantOak3/doc-provider-timestamp.tsv    
data/forGiantOak3/ids_to_urls.tsv: data/forGiantOak3/doc-provider-timestamp.tsv   
data/forGiantOak3/links.tsv: data/forGiantOak3/doc-provider-timestamp.tsv  
data/forGiantOak3/phone_numbers.tsv: data/forGiantOak3/doc-provider-timestamp.tsv 
data/forGiantOak3/rates2.tsv: data/forGiantOak3/doc-provider-timestamp.tsv 
data/forGiantOak3/rates.tsv: data/forGiantOak3/doc-provider-timestamp.tsv 
ad_price_ad_level.csv: ad_prices_price_level.csv
ad_price_ad_level_all.csv: ad_prices_price_level.csv
data/forGiantOak6/incalloutcall-new.tsv: data/forGiantOak6/incall-new.tsv
data/forGiantOak6/outcall-new.tsv: data/forGiantOak6/incall-new.tsv
price_imputation_text_extractor.pkl: price_imputation_model.pkl
