### Download all data
get_data:
    cd
    aws s3 sync s3://giantoak.memex/2016_summer_camp/flattened_201606_complete .
    aws s3 sync s3://giantoak.memex/2016_summer_camp/flattened_201607_complete .
    aws s3 sync s3://giantoak.memex/2016_summer_camp/flattened_before_201605 .

### Calculate features
calculate_features:
    cd /home/ubuntu/memex_ad_features
    python run.py

### Copy data back to s3
copy_data_to_s3:
    aws cp --recursive /home/ubuntu/data/ s3://giantoak.memex/ad_features