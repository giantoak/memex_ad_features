### Run process
make: get_data copy_data_to_s3

### Download all data
get_data:
	aws s3 sync s3://giantoak.memex/2016_summer_camp/flattened_201606_complete /home/ubuntu/flat_data
	aws s3 sync s3://giantoak.memex/2016_summer_camp/flattened_201607_complete /home/ubuntu/flat_data
	aws s3 sync s3://giantoak.memex/2016_summer_camp/flattened_before_201605 /home/ubuntu/flat_data

### Calculate features
calculate_features:
	cd /home/ubuntu/memex_ad_features
	python run.py

### Copy data back to s3
copy_data_to_s3:
	aws s3 cp --recursive /home/ubuntu/memex_ad_features/data/ s3://giantoak.memex/ad_features
