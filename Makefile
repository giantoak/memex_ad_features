### System Variables
HTTP_GET = wget
GET_FROM_DEEPDIVE_S3 = s3cmd -c ~/mdata.cfg get --skip-existing
PUT_TO_GIANTOAK_S3 = s3cmd -c ~/jeffgo.cfg put -P
EXPORT_BUCKET = giantoakmemex

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
	$(GET_FROM_DEEPDIVE_S3) s3://memex-data/escort_beta/ismassageparlorad.tsv.gz
	gunzip -f ismassageparlorad.tsv.gz
	mv ismassageparlorad.tsv data/forGiantOak3/
data/forGiantOak3/msa_locations.tsv: data/forGiantOak3/doc-provider-timestamp.tsv
	$(GET_FROM_DEEPDIVE_S3) s3://memex-data/escort_beta/msa_locations_round2.tsv.gz
	gunzip msa_locations_round2.tsv.gz
	mv msa_locations_round2.tsv data/forGiantOak3/msa_locations.tsv
data/forGiantOak6/incall-new.tsv:
	$(GET_FROM_DEEPDIVE_S3) s3://memex-data/escort_beta/forGiantOak6.tgz
	tar xvf forGiantOak6.tgz -C data/
	rm forGiantOak6.tgz
data/forGiantOak3/isssexad.tsv:
	# Get the sex ad flag from deep dive
	$(HTTP_GET) http://$(EXPORT_BUCKET).s3.amazonaws.com/sex_ad_analysis/input/isssexad.tsv
	mv isssexad.tsv data/forGiantOak3/

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

female_violence_nibrs.csv: make_nibrs.py crosswalk_tract_msa.csv ICPSR_34603/DS0001/34603-0001-Data.txt 35036-0001-Data.txt
	# The female violence tabulations come from reading NIBRS data from
	# ICPSR as well as a crosswalk file from Greg DeAngelo
	python make_nibrs.py

month_msa_wage_instruments.csv: make_month_msa_wage_instruments.py census_2000_msa_industry_gender_wage.csv msa_crosswalk.csv 2011.q1-q4.singlefile.csv
	# This opportunity index is based on 2000 census data and computed
	# based on QCEW data
	python make_month_msa_wage_instruments.py

msa_crosswalk.csv: msa_crosswalk.py
	python msa_crosswalk.py
	# Hand recode counties bewteen Census 2000 in IPUMS and 2013 FIPS
	# used by QCEW
	# NOTE: this depends on 'ipums_msa.txt' and 'qcew_msa.txt' which
	# are in the git repository and don't need to be downloaded

census_2000_msa_industry_gender_wage.csv: make_census_2000_msa_industry_gender_wage.r cols_17.txt usa_00017.dat
	R --vanilla < make_census_2000_msa_industry_gender_wage.r

acs_2013_msa_gender_wage.csv: make_acs_2013_msa_gender_wage.r cols_18.txt usa_00018.dat
	R --vanilla < make_acs_2013_msa_gender_wage.r

lemas.csv: make_lemas.py ucr_lemas_msa.csv
	# Take ucr_lemas_msa data from Greg, keep only the lemas information
	# that happens once in the sample, and save as lemas.csv
	python make_lemas.py

ucr.csv: make_ucr.py ucr_lemas_msa.csv
	# Take ucr_lemas_msa data from Greg, keep only the ucr information
	# that happens annually, reshape to the msa-yera level, and save as ucr.csv
	python make_ucr.py

acs.csv: make_acs.py acs_2013_msa_gender_wage.csv acs_2015_03_18.csv
	python make_acs.py

msa_month_characteristics.csv: make_msa_month_characteristics.py ucr.csv month_msa_wage_instruments.csv msa_month_ad_aggregates.csv provider_panel.csv
	python make_msa_month_characteristics.py

msa_month_ad_aggregates.csv: make_msa_month_ad_aggregates.py ad_price_ad_level.csv 
	python make_msa_month_ad_aggregates.py

provider_panel.csv: ad_price_ad_level.csv make_provider_panel.py
	python make_provider_panel.py

msa_characteristics.csv: make_msa_characteristics.py acs.csv violence_nibrs.csv female_violence_nibrs.csv prostitution_nibrs.csv ucr.csv lemas.csv ad_price_ad_level.csv
	python make_msa_characteristics.py

ad_prices_price_level.csv: make_ad_prices.py data/forGiantOak3/msa_locations.tsv data/forGiantOak3/doc-provider-timestamp.tsv data/forGiantOak3/isssexad.tsv data/forGiantOak3/ismassageparlorad.tsv data/forGiantOak6/incall-new.tsv

	python make_ad_prices.py
############ End intermediate data targets

############ Begin final targets

cpi_crosssection.csv: make_cpicrosssection.py ad_price_ad_level.csv
	python make_cpicrosssection.py

local: ad_prices_price_level.csv ad_zero_prices.csv census_2000_msa_industry_gender_wage.csv acs_2013_msa_gender_wage.csv ad_zero_prices.csv acs.csv month_msa_wage_instruments.csv acs.csv prostitution_nibrs.csv female_violence_nibrs.csv violence_nibrs.csv provider_panel.csv msa_month_characteristics.csv msa_characteristics.csv cpi_crosssection.csv ad_price_ad_level_all.csv

export: ad_prices_price_level.csv ad_zero_prices.csv census_2000_msa_industry_gender_wage.csv acs_2013_msa_gender_wage.csv ad_zero_prices.csv acs.csv month_msa_wage_instruments.csv acs.csv prostitution_nibrs.csv female_violence_nibrs.csv violence_nibrs.csv provider_panel.csv msa_month_characteristics.csv msa_characteristics.csv cpi_crosssection.csv ad_price_ad_level_all.csv
	$(PUT_TO_GIANTOAK_S3) prostitution_nibrs.csv s3://$(EXPORT_BUCKET)/sex_ad_analysis/intermediate/
	$(PUT_TO_GIANTOAK_S3) female_violence_nibrs.csv s3://$(EXPORT_BUCKET)/sex_ad_analysis/intermediate/
	$(PUT_TO_GIANTOAK_S3) violence_nibrs.csv s3://$(EXPORT_BUCKET)/sex_ad_analysis/intermediate/
	$(PUT_TO_GIANTOAK_S3) month_msa_wage_instruments.csv s3://$(EXPORT_BUCKET)/sex_ad_analysis/intermediate/
	$(PUT_TO_GIANTOAK_S3) acs_2013_msa_gender_wage.csv s3://$(EXPORT_BUCKET)/sex_ad_analysis/intermediate/
	$(PUT_TO_GIANTOAK_S3) census_2000_msa_industry_gender_wage.csv s3://$(EXPORT_BUCKET)/sex_ad_analysis/intermediate/
	$(PUT_TO_GIANTOAK_S3) acs.csv s3://$(EXPORT_BUCKET)/sex_ad_analysis/intermediate/
	$(PUT_TO_GIANTOAK_S3) ucr.csv s3://$(EXPORT_BUCKET)/sex_ad_analysis/intermediate/
	$(PUT_TO_GIANTOAK_S3) lemas.csv s3://$(EXPORT_BUCKET)/sex_ad_analysis/intermediate/
	zip ad_prices_price_level.zip ad_prices_price_level.csv
	$(PUT_TO_GIANTOAK_S3) ad_prices.zip s3://$(EXPORT_BUCKET)/sex_ad_analysis/intermediate/
	zip ad_zero_prices.zip ad_zero_prices.csv
	$(PUT_TO_GIANTOAK_S3) ad_zero_prices.zip s3://$(EXPORT_BUCKET)/sex_ad_analysis/intermediate/
	$(PUT_TO_GIANTOAK_S3) acs_2013_msa_gender_wage.csv s3://$(EXPORT_BUCKET)/sex_ad_analysis/ # The metro level wage distribution by gender
	$(PUT_TO_GIANTOAK_S3) counts.csv s3://$(EXPORT_BUCKET)/sex_ad_analysis/
	$(PUT_TO_GIANTOAK_S3) region_features.csv s3://$(EXPORT_BUCKET)/sex_ad_analysis/

	# Export final results
	zip ad_price_ad_level.zip ad_price_ad_level.csv
	$(PUT_TO_GIANTOAK_S3) ad_price_ad_level.zip s3://$(EXPORT_BUCKET)/sex_ad_analysis/output/
	#zip ad_price_ad_level_all.zip ad_price_ad_level_all.csv
	#$(PUT_TO_GIANTOAK_S3) ad_price_ad_level_all.zip s3://$(EXPORT_BUCKET)/sex_ad_analysis/output/
	zip ad_price_price_level.zip ad_prices_price_level.csv
	$(PUT_TO_GIANTOAK_S3) ad_price_ad_level.zip s3://$(EXPORT_BUCKET)/sex_ad_analysis/output/
	zip provider_panel.zip provider_panel.csv
	$(PUT_TO_GIANTOAK_S3) provider_panel.zip s3://$(EXPORT_BUCKET)/sex_ad_analysis/output/
	$(PUT_TO_GIANTOAK_S3) msa_month_characteristics.csv s3://$(EXPORT_BUCKET)/sex_ad_analysis/output/
	$(PUT_TO_GIANTOAK_S3) msa_characteristics.csv s3://$(EXPORT_BUCKET)/sex_ad_analysis/output/
	$(PUT_TO_GIANTOAK_S3) cpi_crosssection.csv s3://$(EXPORT_BUCKET)/sex_ad_analysis/output/

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
violence_nibrs.csv: female_violence_nibrs.csv
prostitution_nibrs.csv: female_violence_nibrs.csv
prostitution.csv: female_violence.csv
ad_zero_prices.csv: ad_prices_price_level.csv
ad_price_ad_level.csv: ad_prices_price_level.csv
ad_price_ad_level_all.csv: ad_prices_price_level.csv
lemas.csv: ucr.csv
data/forGiantOak6/incalloutcall-new.tsv: data/forGiantOak6/incall-new.tsv
data/forGiantOak6/outcall-new.tsv: data/forGiantOak6/incall-new.tsv
