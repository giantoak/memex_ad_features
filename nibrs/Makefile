### System Variables
HTTP_GET = wget
GET_FROM_DEEPDIVE_S3 = s3cmd -c ~/mdata.cfg get --skip-existing
PUT_TO_GIANTOAK_S3 = $(PUT_TO_GIANTOAK_S3)
### Download Related targets

### Data from Tempus export
acs_2015_03_18.csv:
	$(HTTP_GET) http://giantoakmemex.s3.amazonaws.com/sex_ad_analysis/input/acs_2015_03_18.csv
### Data from Greg
crosswalk_tract_msa.csv:
	$(HTTP_GET) http://giantoakmemex.s3.amazonaws.com/sex_ad_analysis/input/crosswalk_tract_msa.csv

ucr_lemas_msa.csv:
	# Get UCR data from Greg
	$(HTTP_GET) http://giantoakmemex.s3.amazonaws.com/sex_ad_analysis/input/ucr_lemas_msa.csv

### Data from Stanford
forGiantOak3/doc-provider-timestamp.tsv:
	# Get data from the Deep Dive data drop, and extract it
	$(GET_FROM_DEEPDIVE_S3) s3://memex-data/escort_beta/forGiantOak3.tgz .
	tar xvf forGiantOak3.tgz
	$(GET_FROM_DEEPDIVE_S3) s3://memex-data/escort_beta/rates2.tsv.gz forGiantOak3/
	gunzip -f forGiantOak3/rates2.tsv.gz
forGiantOak3/ismassageparlorad.tsv:
	# Get massage parlor extraction from the Deep Dive data drop, and extract it
	$(GET_FROM_DEEPDIVE_S3) s3://memex-data/escort_beta/ismassageparlorad.tsv.gz
	gunzip -f ismassageparlorad.tsv.gz
	mv ismassageparlorad.tsv forGiantOak3/
forGiantOak3/msa_locations.tsv: forGiantOak3/doc-provider-timestamp.tsv
	$(GET_FROM_DEEPDIVE_S3) s3://memex-data/escort_beta/msa_locations_round2.tsv.gz
	gunzip msa_locations_round2.tsv.gz
	mv msa_locations_round2.tsv forGiantOak3/msa_locations.tsv

forGiantOak3/isssexad.tsv:
	# Get the sex ad flag from deep dive
	$(HTTP_GET) http://giantoakmemex.s3.amazonaws.com/sex_ad_analysis/input/isssexad.tsv
	mv isssexad.tsv forGiantOak3/

## Publicly available data sources
cols_17.txt:
	# Get a hand-curated list of the columns of the usa_00017.dat file
	$(HTTP_GET) http://giantoakmemex.s3.amazonaws.com/sex_ad_analysis/input/cols_17.txt

usa_00017.dat:
	# IPUMS 2000 census and QCEW are used to create the wage instruments:
	# Get data from 2000 census, from IPUMS extract
	$(HTTP_GET) http://giantoakmemex.s3.amazonaws.com/sex_ad_analysis/input/usa_00017.dat.gz 
	gunzip -f usa_00017.dat.gz

usa_00018.dat: 
	# Get data from IPUMS extract of 2013 ACS
	$(HTTP_GET) http://giantoakmemex.s3.amazonaws.com/sex_ad_analysis/input/usa_00018.dat.gz 
	gunzip -f usa_00018.dat.gz

cols_18.txt:
	# Get a hand-curated list of the columns of the usa_00018.dat file
	$(HTTP_GET) http://giantoakmemex.s3.amazonaws.com/sex_ad_analysis/input/cols_18.txt

35036-0001-Data.txt:
	# Get data from ICPSR for the National Incident Based Reporting
	# system
	$(HTTP_GET) http://giantoakmemex.s3.amazonaws.com/sex_ad_analysis/input/35036-0001-Data.txt.gz
	gunzip -f 35036-0001-Data.txt.gz

ICPSR_34603/DS0001/34603-0001-Data.txt:
	# Download one of the raw ICPSR extract files for NIBRS
	# computations
	mkdir -p ICPSR_34603/
	mkdir -p ICPSR_34603/DS0001/
	$(HTTP_GET) http://giantoakmemex.s3.amazonaws.com/sex_ad_analysis/input/34603-0001-Data.txt.gz
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

month_msa_wage_instruments.csv: make_month_msa_wage_instruments.py census_2000_msa_industry_gender_wage.csv msa_crosswalk.csv
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
	python make_lemas.py

#all_merged.csv: geotag_export.py violence_nibrs.csv female_violence_nibrs.csv month_msa_wage_instruments.csv analytical_report_acs.json acs_2013_msa_gender_wage.csv
	#python geotag_export.py
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

ad_prices_price_level.csv: make_ad_prices.py forGiantOak3/msa_locations.tsv forGiantOak3/doc-provider-timestamp.tsv forGiantOak3/isssexad.tsv
	python make_ad_prices.py
############ End intermediate data targets

export: ad_prices_price_level.csv ad_zero_prices.csv census_2000_msa_industry_gender_wage.csv acs_2013_msa_gender_wage.csv ad_zero_prices.csv acs.csv month_msa_wage_instruments.csv acs.csv prostitution_nibrs.csv female_violence_nibrs.csv violence_nibrs.csv provider_panel.csv msa_month_characteristics.csv
	$(PUT_TO_GIANTOAK_S3) prostitution_nibrs.csv s3://giantoakmemex/sex_ad_analysis/intermediate/
	$(PUT_TO_GIANTOAK_S3) female_violence_nibrs.csv s3://giantoakmemex/sex_ad_analysis/intermediate/
	$(PUT_TO_GIANTOAK_S3) violence_nibrs.csv s3://giantoakmemex/sex_ad_analysis/intermediate/
	$(PUT_TO_GIANTOAK_S3) month_msa_wage_instruments.csv s3://giantoakmemex/sex_ad_analysis/intermediate/
	$(PUT_TO_GIANTOAK_S3) acs_2013_msa_gender_wage.csv s3://giantoakmemex/sex_ad_analysis/intermediate/
	$(PUT_TO_GIANTOAK_S3) census_2000_msa_industry_gender_wage.csv s3://giantoakmemex/sex_ad_analysis/intermediate/
	$(PUT_TO_GIANTOAK_S3) acs.csv s3://giantoakmemex/sex_ad_analysis/intermediate/
	zip ad_prices_price_level.zip ad_prices_price_level.csv
	$(PUT_TO_GIANTOAK_S3) ad_prices.zip s3://giantoakmemex/sex_ad_analysis/intermediate/
	zip ad_price_ad_level.zip ad_price_ad_level.csv
	$(PUT_TO_GIANTOAK_S3) ad_price_ad_level.zip s3://giantoakmemex/sex_ad_analysis/intermediate/
	zip ad_zero_prices.zip ad_zero_prices.csv
	$(PUT_TO_GIANTOAK_S3) ad_zero_prices.zip s3://giantoakmemex/sex_ad_analysis/intermediate/
	zip provider_panel.zip provider_panel.csv
	$(PUT_TO_GIANTOAK_S3) provider_panel.zip s3://giantoakmemex/sex_ad_analysis/intermediate/
	$(PUT_TO_GIANTOAK_S3) msa_month_characteristics.csv s3://giantoakmemex/sex_ad_analysis/intermediate/
	$(PUT_TO_GIANTOAK_S3) acs_2013_msa_gender_wage.csv s3://giantoakmemex/sex_ad_analysis/ # The metro level wage distribution by gender
	$(PUT_TO_GIANTOAK_S3) counts.csv s3://giantoakmemex/sex_ad_analysis/
	$(PUT_TO_GIANTOAK_S3) region_features.csv s3://giantoakmemex/sex_ad_analysis/

monthly_panel.csv: msa_panel.py all_merged.csv
	python msa_panel.py
panel_analysis: monthly_panel.csv monthly_panel.r
	R --vanilla < monthly_panel.r

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

### Below here are simultaneously created files. The approach to these is
# to make each of these dependent on the file that actually has a rule defined above
2012.q1-q4.singlefile.csv: 2011.q1-q4.singlefile.csv
2013.q1-q4.singlefile.csv: 2011.q1-q4.singlefile.csv
2014.q1-q3.singlefile.csv: 2011.q1-q4.singlefile.csv
forGiantOak3/email_addresses.tsv: forGiantOak3/doc-provider-timestamp.tsv    
forGiantOak3/ids_to_urls.tsv: forGiantOak3/doc-provider-timestamp.tsv   
forGiantOak3/links.tsv: forGiantOak3/doc-provider-timestamp.tsv  
forGiantOak3/phone_numbers.tsv: forGiantOak3/doc-provider-timestamp.tsv 
forGiantOak3/rates2.tsv: forGiantOak3/doc-provider-timestamp.tsv 
forGiantOak3/rates.tsv: forGiantOak3/doc-provider-timestamp.tsv 
violence_nibrs.csv: female_violence_nibrs.csv
prostitution_nibrs.csv: female_violence_nibrs.csv
prostitution.csv: female_violence.csv
ad_zero_prices.csv: ad_prices_price_level.csv
ad_price_ad_level.csv: ad_prices_price_level.csv
lemas.csv: ucr.csv
