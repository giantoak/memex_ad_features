#!/bin/bash
# This script prepares ACS from the GO website.
# 
# Remember that you need a bunch of disk space (~30GB?) and probably a lot
# of memory (again probably 30GB) to get this done
wget http://giantoak.1.s3.amazonaws.com/csv_pus.zip
unzip csv_pus.zip
cp ss13pusa.csv all.csv
cat ss13pusb.csv | sed -n '1!p' >> all.csv
cat ss13pusc.csv | sed -n '1!p' >> all.csv
cat ss13pusd.csv | sed -n '1!p' >> all.csv
