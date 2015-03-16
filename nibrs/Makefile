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


#
#prostitution.csv
#crosswalk_tract_msa.csv: python pd.py
#violence.csv

#ACS:

#../acs/2011.q1-q4.singlefile.csv
#../acs/2012.q1-q4.singlefile.csv
#../acs/2013.q1-q4.singlefile.csv
#../acs/2014.q1-q2.singlefile.csv
#../acs/female_opportunity_index.csv
#../acs/industry_level.csv
#../acs/metro_level.csv
#../acs/msa_crosswalk.csv
#../acs/puma_level.csv
#../acs/state_level.csv
#../acs/temp.csv
