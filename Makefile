SHELL := /bin/bash
### Run process
make: calculate_features copy_data_to_s3

### Download all data
get_data:
	aws s3 sync s3://giantoak.memex/lattice_data_store/flat /home/ubuntu/flat_data

### Calculate features
calculate_features: install
	nohup python /home/ubuntu/memex_ad_features/run.py

### Copy data back to s3
copy_data_to_s3:
	aws s3 sync /home/ubuntu/memex_ad_features/data/ s3://giantoak.memex/giantoak_econ_results/

## Install dependencies
install:
	( \
	sudo pip install -r /home/ubuntu/memex_ad_features/requirements.txt; \
	)
