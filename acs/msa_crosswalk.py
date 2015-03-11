import pandas
ipums = pandas.read_csv('ipums_msa.txt', sep='\t')
qcew = pandas.read_csv('qcew_msa.txt', sep='\t')
ipums['msa']=ipums['msa'].apply(lambda x: x.replace('/','-')) 
qcew['msa']=qcew['msa'].apply(lambda x: x.replace(' MSA','')) 
out=pandas.merge(ipums, qcew, how='outer')
out.loc[out.ipums_code==52,'qcew_code'] = 'C1207' # Atlanta, GA
out.loc[out.ipums_code==46,'qcew_code'] = 'C1154' # Appleton, WI
out.loc[out.ipums_code==50,'qcew_code'] = 'C1202' # Athens, GA
out.loc[out.ipums_code==45,'qcew_code'] = 'C1150' # Anniston, AL
out.loc[out.ipums_code==56,'qcew_code'] = 'C1210' # Atlantic City, NJ
out.loc[out.ipums_code==60,'qcew_code'] = 'C1226' # Augusta, GA
out.loc[out.ipums_code==64,'qcew_code'] = 'C1242' # Austin, TX
out.loc[out.ipums_code==72,'qcew_code'] = 'C1258' # Baltimore, MD

out[out.ipums_code.isnull()]
out[out.qcew_code.isnull()]
