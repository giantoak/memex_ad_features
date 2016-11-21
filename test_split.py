import gzip

with gzip.open('/home/gabriel/Documents/Memex/ad_features/ad_data/sample_data2.json.gz', 'rb') as in_file:
    with gzip.open('/home/gabriel/Documents/Memex/ad_features/ad_data/test.gz', 'wb') as out_file:
        out_file.write(in_file.read(100000000000))


for line in gzip.open('/home/ubuntu/flat_data/data_20160819-0000_1440_2016-08-27.json.gz'):
    print line

