### Download Related targets

### Data from Tempus export
acs_2015_03_18.csv:
	wget http://giantoakmemex.s3.amazonaws.com/sex_ad_analysis/input/acs_2015_03_18.csv
### Data from Greg
crosswalk_tract_msa.csv:
	wget http://giantoakmemex.s3.amazonaws.com/sex_ad_analysis/input/crosswalk_tract_msa.csv

ucr_lemas_msa.csv:
	# Get UCR data from Greg
	wget http://giantoakmemex.s3.amazonaws.com/sex_ad_analysis/input/ucr_lemas_msa.csv

### Data from Stanford
doc-provider-timestamp.tsv:
	# Get data from the Deep Dive data drop, and extract it
	s3cmd -c ~/mdata.cfg get --skip-existing s3://memex-data/escort_beta/forGiantOak3.tgz .
	tar xvf forGiantOak3.tgz
	s3cmd -c ~/mdata.cfg get --skip-existing s3://memex-data/escort_beta/rates2.tsv.gz forGiantOak3/
	gunzip -f forGiantOak3/rates2.tsv.gz
ismassageparlorad.tsv:
	# Get massage parlor extraction from the Deep Dive data drop, and extract it
	s3cmd -c ~/mdata.cfg get --skip-existing s3://memex-data/escort_beta/ismassageparlorad.tsv.gz
	gunzip -f ismassageparlorad.tsv.gz
	mv ismassageparlorad.tsv forGiantOak3/

isssexad.tsv:
	# Get the sex ad flag from deep dive
	wget http://giantoakmemex.s3.amazonaws.com/sex_ad_analysis/input/isssexad.tsv
	mv isssexad.tsv forGiantOak3/

## Publicly available data sources
cols_17.txt:
	# Get a hand-curated list of the columns of the usa_00017.dat file
	wget http://giantoakmemex.s3.amazonaws.com/sex_ad_analysis/input/cols_17.txt

usa_00017.dat:
	# IPUMS 2000 census and QCEW are used to create the wage instruments:
	# Get data from 2000 census, from IPUMS extract
	wget http://giantoakmemex.s3.amazonaws.com/sex_ad_analysis/input/usa_00017.dat.gz 
	gunzip -f usa_00017.dat.gz

usa_00018.dat: 
	# Get data from IPUMS extract of 2013 ACS
	wget http://giantoakmemex.s3.amazonaws.com/sex_ad_analysis/input/usa_00018.dat.gz 
	gunzip -f usa_00018.dat.gz

cols_18.txt:
	# Get a hand-curated list of the columns of the usa_00018.dat file
	wget http://giantoakmemex.s3.amazonaws.com/sex_ad_analysis/input/cols_18.txt

35036-0001-Data.txt:
	# Get data from ICPSR for the National Incident Based Reporting
	# system
	wget http://giantoakmemex.s3.amazonaws.com/sex_ad_analysis/input/35036-0001-Data.txt.gz
	gunzip -f 35036-0001-Data.txt.gz

ICPSR_34603/DS0001/34603-0001-Data.txt:
	# Download one of the raw ICPSR extract files for NIBRS
	# computations
	mkdir -p ICPSR_34603/
	mkdir -p ICPSR_34603/DS0001/
	wget http://giantoakmemex.s3.amazonaws.com/sex_ad_analysis/input/34603-0001-Data.txt.gz
	mv 34603-0001-Data.txt.gz ICPSR_34603/DS0001/
	gunzip -f ICPSR_34603/DS0001/34603-0001-Data.txt.gz 

2011.q1-q4.singlefile.csv: 
	## Work on QCEW downloads
	wget http://www.bls.gov/cew/data/files/2014/csv/2014_qtrly_singlefile.zip
	wget http://www.bls.gov/cew/data/files/2013/csv/2013_qtrly_singlefile.zip
	wget http://www.bls.gov/cew/data/files/2012/csv/2012_qtrly_singlefile.zip
	wget http://www.bls.gov/cew/data/files/2011/csv/2011_qtrly_singlefile.zip
	unzip 2014_qtrly_singlefile.zip
	unzip 2013_qtrly_singlefile.zip
	unzip 2012_qtrly_singlefile.zip
	unzip 2011_qtrly_singlefile.zip

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

ad_prices.csv: make_ad_prices.py msa_locations.tsv doc-provider-timestamp.tsv isssexad.tsv
	python make_ad_prices.py
#zero_price.csv: zero_prices.py all_merged.csv issexad.tsv rates2.tsv
	##These files are also created: ad_prices.csv ad_prices_msa_micro.csv zero_price_msa_aggregates.csv
	## Zero price analysis depends on rate information 
	#python zero_prices.py

#ad_prices_msa.csv ad_prices_msa_micro.csv: zero_prices.py all_merged.csv
	#python zero_prices.py
#zero_price_msa_aggregates.csv: zero_prices.py all_merged.csv
	## Zero price analysis depends on rate information 
	#python zero_prices.py
############ End intermediate data targets
export: all_merged.csv monthly_panel.csv ad_prices_msa.csv price_extraction_counts.csv ad_prices_msa_micro.csv ad_prices.csv ad_zero_prices.csv acs.csv
	# Note: this code needs Jeff's (or another Giant Oak person's) keys to upload to the giantoakmemex bucket
	zip all_merged.zip all_merged.csv
	s3cmd -c ~/jeffgo.cfg -P put all_merged.zip s3://giantoakmemex/sex_ad_analysis/
	zip ad_prices_msa_micro.zip ad_prices_msa_micro.csv

	s3cmd -c ~/jeffgo.csv -P put acs.csv s3://giantoakmemex/sex_ad_analysis/
	zip ad_prices.zip ad_prices.csv
	s3cmd -c ~/jeffgo.cfg -P put ad_prices.zip s3://giantoakmemex/sex_ad_analysis/
	zip ad_zero_prices.zip ad_zero_prices.csv
	s3cmd -c ~/jeffgo.cfg -P put ad_zero_prices.zip s3://giantoakmemex/sex_ad_analysis/

	s3cmd -c ~/jeffgo.cfg -P put ad_prices_msa_micro.zip s3://giantoakmemex/sex_ad_analysis/
	zip zero_price_msa_micro.zip zero_price_msa_micro.csv
	s3cmd -c ~/jeffgo.cfg -P put zero_price_msa_micro.zip s3://giantoakmemex/sex_ad_analysis/
	zip zero_price_msa_aggregates.zip zero_price_msa_aggregates.csv
	s3cmd -c ~/jeffgo.cfg -P put zero_price_msa_aggregates.zip s3://giantoakmemex/sex_ad_analysis/
	zip price_extraction_counts.zip price_extraction_counts.csv
	s3cmd -c ~/jeffgo.cfg -P put price_extraction_counts.zip s3://giantoakmemex/sex_ad_analysis/
	s3cmd -c ~/jeffgo.cfg -P put ad_prices_msa.csv s3://giantoakmemex/sex_ad_analysis/
	zip monthly_panel.zip monthly_panel.csv
	s3cmd -c ~/jeffgo.cfg -P put monthly_panel.zip s3://giantoakmemex/sex_ad_analysis/
	s3cmd -c ~/jeffgo.cfg -P put acs_2013_msa_gender_wage.csv s3://giantoakmemex/sex_ad_analysis/ # The metro level wage distribution by gender
	s3cmd -c ~/jeffgo.cfg -P put counts.csv s3://giantoakmemex/sex_ad_analysis/
	s3cmd -c ~/jeffgo.cfg -P put region_features.csv s3://giantoakmemex/sex_ad_analysis/

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
email_addresses.tsv: doc-provider-timestamp.tsv    
ids_to_urls.tsv: doc-provider-timestamp.tsv   
links.tsv: doc-provider-timestamp.tsv  
msa_locations.tsv: doc-provider-timestamp.tsv 
phone_numbers.tsv: doc-provider-timestamp.tsv 
rates2.tsv: doc-provider-timestamp.tsv 
rates.tsv: doc-provider-timestamp.tsv 
violence_nibrs.csv: female_violence_nibrs.csv
prostitution_nibrs.csv: female_violence_nibrs.csv
prostitution.csv: female_violence.csv
ad_zero_prices.csv: ad_prices.csv
