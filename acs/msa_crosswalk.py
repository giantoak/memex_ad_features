import pandas
ipums = pandas.read_csv('ipums_msa.txt', sep=';')
ipums = ipums[ipums.exists=='X']
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
out.loc[out.ipums_code==74,'qcew_code'] = 'C1270' # Barnstable, MA
out.loc[out.ipums_code==112,'qcew_code'] = 'C1446' # Boston, MA
out.loc[out.ipums_code==100,'qcew_code'] = 'C1382' # Birmingham, AL
out.loc[out.ipums_code==126,'qcew_code'] = 'C1778' # College Station, TX
out.loc[out.ipums_code==128,'qcew_code'] = 'C1538' # Buffalo, NY
out.loc[out.ipums_code==132,'qcew_code'] = 'C1594' # Canton, OH
out.loc[out.ipums_code==140,'qcew_code'] = 'C1658' # Urbana, IL
out.loc[out.ipums_code==144,'qcew_code'] = 'C1670' # Charleston, SC
out.loc[out.ipums_code==152,'qcew_code'] = 'C1674' # Charlotte, NC
out.loc[out.ipums_code==160,'qcew_code'] = 'C1698' # Chicago, IL
out.loc[out.ipums_code==164,'qcew_code'] = 'C1714' # Cincinnati, OH
out.loc[out.ipums_code==168,'qcew_code'] = 'C1746' # Cleveland, OH
out.loc[out.ipums_code==192,'qcew_code'] = 'C1910' # Dallas, TX
out.loc[out.ipums_code==196,'qcew_code'] = 'C1934' # Davenport, IA
out.loc[out.ipums_code==208,'qcew_code'] = 'C1974' # Denver, CO
out.loc[out.ipums_code==212,'qcew_code'] = 'C1978' # Des Moines, IA
out.loc[out.ipums_code==216,'qcew_code'] = 'C1982' # Detroit, MI
out.loc[out.ipums_code==224,'qcew_code'] = 'C2026' # Duluth, MN
out.loc[out.ipums_code==252,'qcew_code'] = 'C2202' # Fargo, ND
out.loc[out.ipums_code==258,'qcew_code'] = 'C2222' # Fayetville, AR
out.loc[out.ipums_code==267,'qcew_code'] = 'C2266' # Fort Collins, CO
out.loc[out.ipums_code==268,'qcew_code'] = 'C3310' # Miami, FL
out.loc[out.ipums_code==312,'qcew_code'] = 'C2466' # Greensboro, NC
out.loc[out.ipums_code==324,'qcew_code'] = 'C2542' # Harrisburg, PA
out.loc[out.ipums_code==328,'qcew_code'] = 'C2554' # Hartford, CT
out.loc[out.ipums_code==336,'qcew_code'] = 'C2642' # Houston, TX
out.loc[out.ipums_code==348,'qcew_code'] = 'C2690' # Indianapolis, IN
out.loc[out.ipums_code==404,'qcew_code'] = 'C2962' # Lansing, MI
out.loc[out.ipums_code==412,'qcew_code'] = 'C2982' # Las Vegas, NV
out.loc[out.ipums_code==440,'qcew_code'] = 'C3078' # Little Rock, AK
out.loc[out.ipums_code==440,'qcew_code'] = 'C3078' # Little Rock, AK
out.loc[out.ipums_code==448,'qcew_code'] = 'C3108' # Los Angeles, CA (Note: there are 2 LA MSAs, one with Anaheim, one with Santa Ana
out.loc[out.ipums_code==452,'qcew_code'] = 'C3114' # Louisville, KY
out.loc[out.ipums_code==476,'qcew_code'] = 'C3170' # Manchester, NH
out.loc[out.ipums_code==490,'qcew_code'] = 'C3734' # Melbourne, FL
out.loc[out.ipums_code==492,'qcew_code'] = 'C3282' # Memphis, TN
out.loc[out.ipums_code==500,'qcew_code'] = 'C3310' # Miami, FL (Again)
out.loc[out.ipums_code==508,'qcew_code'] = 'C3334' # Milwaukee, WI
out.loc[out.ipums_code==512,'qcew_code'] = 'C3346' # Minneapolis, MN
out.loc[out.ipums_code==534,'qcew_code'] = 'C3498' # Nashville, TN
out.loc[out.ipums_code==556,'qcew_code'] = 'C3538' # New Orleans, LA
out.loc[out.ipums_code==560,'qcew_code'] = 'C3562' # New York, NY
out.loc[out.ipums_code==572,'qcew_code'] = 'C4726' # Norfolk, VA
out.loc[out.ipums_code==592,'qcew_code'] = 'C3654' # Omaha, NE
out.loc[out.ipums_code==596,'qcew_code'] = 'C3674' # Orlando, FL
out.loc[out.ipums_code==608,'qcew_code'] = 'C3786' # Pensacola, FL
out.loc[out.ipums_code==616,'qcew_code'] = 'C3798' # Philadelphia, PA
out.loc[out.ipums_code==620,'qcew_code'] = 'C3806' # Phoenix, AZ
out.loc[out.ipums_code==640,'qcew_code'] = 'C3886' # Portland, ME
out.loc[out.ipums_code==644,'qcew_code'] = 'C3890' # Portland, OR
out.loc[out.ipums_code==648,'qcew_code'] = 'C3930' # Providence, RI
out.loc[out.ipums_code==664,'qcew_code'] = 'C3958' # Raleigh, NC
out.loc[out.ipums_code==676,'qcew_code'] = 'C4006' # Richmond, VA
out.loc[out.ipums_code==678,'qcew_code'] = 'C4014' # Riverside, CA
out.loc[out.ipums_code==692,'qcew_code'] = 'C4090' # Sacramento, CA
out.loc[out.ipums_code==716,'qcew_code'] = 'C4162' # Salt Lake City, UT
out.loc[out.ipums_code==724,'qcew_code'] = 'C4170' # San Antonio, TX
out.loc[out.ipums_code==732,'qcew_code'] = 'C4174' # San Diego, TX
out.loc[out.ipums_code==736,'qcew_code'] = 'C4186' # San Francisco, CA
out.loc[out.ipums_code==740,'qcew_code'] = 'C4194' # San Jose, CA
out.loc[out.ipums_code==747,'qcew_code'] = 'C4206' # Santa Barbara, CA
out.loc[out.ipums_code==748,'qcew_code'] = 'C4210' # Santa Cruz, CA
out.loc[out.ipums_code==751,'qcew_code'] = 'C4226' # Santa Cruz, CA
out.loc[out.ipums_code==756,'qcew_code'] = 'C4254' # Scranton, PA
out.loc[out.ipums_code==760,'qcew_code'] = 'C4266' # Seattle, WA
out.loc[out.ipums_code==784,'qcew_code'] = 'C4406' # Spokane, WA
out.loc[out.ipums_code==812,'qcew_code'] = 'C4470' # Stockton, CA
out.loc[out.ipums_code==820,'qcew_code'] = 'C4266' # Seattle, WA (Again)
out.loc[out.ipums_code==884,'qcew_code'] = 'C4790' # Washington, DC
out.loc[out.ipums_code==896,'qcew_code'] = 'C3310' # West Palm Beach, FL (Again)
out.loc[out.ipums_code==916,'qcew_code'] = 'C3798' # Wilmington, DE (now Philadelphia)

out.loc[out.qcew_code.isnull(),'ipums_code']
out[out.ipums_code.isnull()]
out[out.qcew_code.isnull()]
