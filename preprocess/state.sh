#!/bin/bash

cat data/fips_states.csv | pit -d, -e 'print _[0] + "," + _[1].lower()' > data/fips_states_lower.csv
cat data/states.csv | pit -d , -e 'print _[0].lower() + "," + _[1]' > data/states_lower.csv

csvjoin -c state,State data/fips_states_lower.csv data/states_lower.csv | csvcut -c 1,2,4 > data/fips_states_all.csv

cat data/fips_county2.csv | pit '_.lower()' > data/fips_county3.csv
cat data/fips_place.csv | pit '_.lower()' > data/fips_place_lower.csv


