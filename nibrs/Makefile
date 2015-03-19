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

all_merged.csv: geotag_export.py violence.csv female_violence.csv female_opportunity_index.csv analytical_report_acs.json
	python geotag_export.py

zero_price.csv: zero_prices.py all_merged.csv
#zero_price.csv: forGiantOak/rates.tsv.gz forGiantOak/msa_locations.tsv.gz zero_prices.py all_merged.csv
	#These files are also created: ad_prices.csv ad_prices_msa_micro.csv zero_price_msa_aggregates.csv
	# Zero price analysis depends on rate information 
	python zero_prices.py
export: all_merged.csv
	zip all_merged.zip all_merged.csv
	s3cmd -c ~/jeffgo.cfg -P put all_merged.zip s3://giantoak.memex/ 
	zip ad_prices_msa_micro.zip ad_prices_msa_micro.csv
	s3cmd -c ~/jeffgo.cfg -P put ad_prices_msa_micro.zip s3://giantoak.memex/ 
	zip zero_price_msa_micro.zip zero_price_msa_micro.csv
	s3cmd -c ~/jeffgo.cfg -P put zero_price_msa_micro.zip s3://giantoak.memex/ 
	zip zero_price_msa_aggregates.zip zero_price_msa_aggregates.csv
	s3cmd -c ~/jeffgo.cfg -P put zero_price_msa_aggregates.zip s3://giantoak.memex/ 
	zip price_extraction_counts.zip price_extraction_counts.csv
	s3cmd -c ~/jeffgo.cfg -P put price_extraction_counts.zip s3://giantoak.memex/ 
	s3cmd -c ~/jeffgo.cfg -P put ad_prices_msa.csv s3://giantoak.memex/ 
