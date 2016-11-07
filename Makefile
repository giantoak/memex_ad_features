SHELL := /bin/bash
### Run process
make: get_data copy_data_to_s3

### Download all data
get_data:
	aws s3 sync s3://giantoak.memex/lattice_data_store/flat /home/ubuntu/flat_data --profile default

### Calculate features
calculate_features: install
	nohup python /home/ubuntu/memex_ad_features/run.py

### Copy data back to s3
copy_data_to_s3:
	aws configure set AWS_ACCESS_KEY_ID=AKIAJPTTGFMTV42WCXVA
	aws configure set AWS_SECRET_ACCESS_KEY=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
	aws configure set default.region us-east-1
	aws s3 sync /home/ubuntu/memex_ad_features/data/ s3://giantoak.memex/giantoak_econ_results/

## Install dependencies
install:
	( \
	sudo pip install -r /home/ubuntu/memex_ad_features/requirements.txt; \
	)
