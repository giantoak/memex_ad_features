# coding: utf-8
import requests

for i in range(22):
	crimebase = 'http://www.ucrdatatool.gov/Search/Crime/Local/DownCrimeOneYearofData.cfm/LocalCrimeOneYearofData.csv'
	payload = {'CrimeCrossId': range(1000*i, 1000*(i+1)), 'YearStart': 2012, 'YearEnd': 2012, 'DataType': [1,2,3,4]}
	r = requests.post(crimebase, data=payload)
	with open('crime_scrape2013_{}.csv'.format(i), 'w') as f:
		f.write(r.text)
	print i
