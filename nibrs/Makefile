download: ucr_lemas_msa.csv doc-provider-timestamp.tsv
	# This code downloads all data necessary
	


35036-0001-Data.txt:
	# Get data from ICPSR for the National Incident Based Reporting
	# system
	wget http://giantoakmemex.s3.amazonaws.com/sex_ad_analysis/input/35036-0001-Data.txt.gz
	gunzip 35036-0001-Data.txt.gz


ICPSR_34603/DS0001/34603-0001-Data.txt:
	# Download one of the raw ICPSR extract files for NIBRS
	# computations
	mkdir -p ICPSR_34603/
	mkdir -p ICPSR_34603/DS0001/
	wget http://giantoakmemex.s3.amazonaws.com/sex_ad_analysis/input/34603-0001-Data.txt.gz
	mv 34603-0001-Data.txt.gz ICPSR_34603/DS0001/
	gunzip ICPSR_34603/DS0001/34603-0001-Data.txt.gz 

crosswalk_tract_msa.csv:
	wget http://giantoakmemex.s3.amazonaws.com/sex_ad_analysis/input/crosswalk_tract_msa.csv

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


ucr_lemas_msa.csv:
	# Get UCR data from Greg
	wget http://giantoakmemex.s3.amazonaws.com/sex_ad_analysis/input/ucr_lemas_msa.csv

doc-provider-timestamp.tsv:
	# Get data from the Deep Dive data drop, and extract it
	s3cmd -c ~/mdata.cfg get s3://memex-data/escort_beta/forGiantOak3.tgz .
	tar xvf forGiantOak3.tgz
	s3cmd -c ~/mdata.cfg get s3://memex-data/escort_beta/rates2.tsv.gz forGiantOak3/
	gunzip forGiantOak3/rates2.tsv.gz

usa_00017.dat:
	# IPUMS 2000 census and QCEW are used to create the wage instruments:
	# Get data from 2000 census, from IPUMS extract
	wget http://giantoakmemex.s3.amazonaws.com/sex_ad_analysis/input/usa_00017.dat.gz 
	gunzip usa_00017.dat.gz

usa_00018.dat: 
	# Get data from IPUMS extract of 2013 ACS
	wget http://giantoakmemex.s3.amazonaws.com/sex_ad_analysis/input/usa_00018.dat.gz 
	gunzip usa_00018.dat.gz

female_violence_nibrs.csv: make_nibrs.py crosswalk_tract_msa.csv ICPSR_34603/DS0001/34603-0001-Data.txt 35036-0001-Data.txt
	# The female violence tabulations come from reading NIBRS data from
	# ICPSR as well as a crosswalk file from Greg DeAngelo
	python make_nibrs.py

female_opportunity_index.csv: qcew.py metro_level.csv msa_crosswalk.csv
	# This opportunity index is based on 2000 census data and computed
	# based on QCEW data
	python qcew.py
	#./qcew_download.sh

msa_crosswalk.csv: msa_crosswalk.py
	python msa_crosswalk.py
	# Hand recode counties bewteen Census 2000 in IPUMS and 2013 FIPS
	# used by QCEW
metro_level.csv: census_2000_msa.r
	R --vanilla < census_2000_msa.r
	# This command is untested

all_merged.csv: geotag_export.py violence_nibrs.csv female_violence_nibrs.csv female_opportunity_index.csv analytical_report_acs.json metro_level_wages.csv
	python geotag_export.py

zero_price.csv: zero_prices.py all_merged.csv
	#These files are also created: ad_prices.csv ad_prices_msa_micro.csv zero_price_msa_aggregates.csv
	# Zero price analysis depends on rate information 
	python zero_prices.py
ad_prices_msa.csv ad_prices_msa_micro.csv: zero_prices.py all_merged.csv
	python zero_prices.py
zero_price_msa_aggregates.csv: zero_prices.py all_merged.csv
	# Zero price analysis depends on rate information 
	python zero_prices.py
export: all_merged.csv monthly_panel.csv ad_prices_msa.csv price_extraction_counts.csv ad_prices_msa_micro.csv
	zip all_merged.zip all_merged.csv
	s3cmd -c ~/jeffgo.cfg -P put all_merged.zip s3://giantoakmemex/sex_ad_analysis/
	zip ad_prices_msa_micro.zip ad_prices_msa_micro.csv

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
	s3cmd -c ~/jeffgo.cfg -P put metro_level_wages.csv s3://giantoakmemex/sex_ad_analysis/ # The metro level wage distribution by gender
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

metro_level_wages.csv: acs_2013_ipums.r
	R --vanilla < acs_2013_wages.r

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
