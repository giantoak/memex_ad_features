download:
	# This code downloads all data necessary
	# Get data from the Deep Dive data drop
	s3cmd -c ~/mdata.cfg get s3://memex-data/escort_beta/forGiantOak3.tgz .
	tar xvf forGiantOak3.tgz
	# Get data from 2000 census, from IPUMS extract
	wget http://giantoak.memex.s3.amazonaws.com/sex_ad_analysis/input/usa_00017.dat.gz 
	gunzip usa_00017.dat.gz
	# Get data from IPUMS extract of 2013 ACS
	wget http://giantoak.memex.s3.amazonaws.com/sex_ad_analysis/input/usa_00018.dat.gz 
	gunzip usa_00018.dat.gz
	# Get data from ICPSR for the National Incident Based Reporting
	# system
	wget http://giantoak.memex.s3.amazonaws.com/sex_ad_analysis/input/35036-0001-Data.txt.gz
	gunzip 35036-0001-Data.txt.gz
	mkdir -p ICPSR_04634/
	mkdir -p ICPSR_04634/DS0001/
	wget http://giantoak.memex.s3.amazonaws.com/sex_ad_analysis/input/34603-0001-Data.txt.gz
	mv 34603-0001-Data.txt.gz ICPSR_04634/DS0001/
	gunzip ICPSR_04634/DS0001/34603-0001-Data.txt.gz 
	# Work on QCEW downloads
	wget http://www.bls.gov/cew/data/files/2014/csv/2014_qtrly_singlefile.zip
	wget http://www.bls.gov/cew/data/files/2013/csv/2013_qtrly_singlefile.zip
	wget http://www.bls.gov/cew/data/files/2012/csv/2012_qtrly_singlefile.zip
	wget http://www.bls.gov/cew/data/files/2011/csv/2011_qtrly_singlefile.zip
	unzip 2014_qtrly_singlefile.zip
	unzip 2013_qtrly_singlefile.zip
	unzip 2012_qtrly_singlefile.zip
	unzip 2011_qtrly_singlefile.zip
female_violence.csv violence.csv prostitution.csv: pd.py crosswalk_tract_msa.csv
	# The female violence tabulations come from reading UCR data from
	# ICPSR as well as a crosswalk file from Greg DeAngelo
	python pd.py

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

all_merged.csv: geotag_export.py violence.csv female_violence.csv female_opportunity_index.csv analytical_report_acs.json metro_level_wages.csv
	python geotag_export.py

zero_price.csv: zero_prices.py all_merged.csv
#zero_price.csv: forGiantOak/rates.tsv.gz forGiantOak/msa_locations.tsv.gz zero_prices.py all_merged.csv
	#These files are also created: ad_prices.csv ad_prices_msa_micro.csv zero_price_msa_aggregates.csv
	# Zero price analysis depends on rate information 
	python zero_prices.py
ad_prices_msa.csv: zero_prices.py all_merged.csv
	python zero_prices.py
zero_price_msa_aggregates.csv: zero_prices.py all_merged.csv
	# Zero price analysis depends on rate information 
	python zero_prices.py
export: all_merged.csv monthly_panel.csv ad_prices_msa.csv price_extraction_counts.csv ad_prices_msa_micro.csv
	zip all_merged.zip all_merged.csv
	s3cmd -c ~/jeffgo.cfg -P put all_merged.zip s3://giantoak.memex/sex_ad_analysis/
	zip ad_prices_msa_micro.zip ad_prices_msa_micro.csv
	s3cmd -c ~/jeffgo.cfg -P put ad_prices_msa_micro.zip s3://giantoak.memex/sex_ad_analysis/
	zip zero_price_msa_micro.zip zero_price_msa_micro.csv
	s3cmd -c ~/jeffgo.cfg -P put zero_price_msa_micro.zip s3://giantoak.memex/sex_ad_analysis/
	zip zero_price_msa_aggregates.zip zero_price_msa_aggregates.csv
	s3cmd -c ~/jeffgo.cfg -P put zero_price_msa_aggregates.zip s3://giantoak.memex/sex_ad_analysis/
	zip price_extraction_counts.zip price_extraction_counts.csv
	s3cmd -c ~/jeffgo.cfg -P put price_extraction_counts.zip s3://giantoak.memex/sex_ad_analysis/
	s3cmd -c ~/jeffgo.cfg -P put ad_prices_msa.csv s3://giantoak.memex/sex_ad_analysis/
	zip monthly_panel.zip monthly_panel.csv
	s3cmd -c ~/jeffgo.cfg -P put monthly_panel.zip s3://giantoak.memex/sex_ad_analysis/
	s3cmd -c ~/jeffgo.cfg -P put metro_level_wages.csv s3://giantoak.memex/sex_ad_analysis/ # The metro level wage distribution by gender

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
