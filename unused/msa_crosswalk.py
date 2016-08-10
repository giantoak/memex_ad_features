import pandas as pd

ipums = pd.read_csv('ipums_msa.txt', sep=';')
ipums = ipums[ipums.exists == 'X']
qcew = pd.read_csv('qcew_msa.txt', sep='\t')
ipums['msa'] = ipums['msa'].apply(lambda x: x.replace('/', '-'))
qcew['msa'] = qcew['msa'].apply(lambda x: x.replace(' MSA', ''))
out = pd.merge(ipums, qcew, how='outer')  # Note: use this merge even though we fill in every row manually afterwards
out.loc[out.ipums_code == 4, 'qcew_code'] = 'C1018'  # Abilene, TX
out.loc[out.ipums_code == 6, 'qcew_code'] = 'C1038'  # Aguadilla, PR
out.loc[out.ipums_code == 8, 'qcew_code'] = 'C1042'  # Akron, OH
out.loc[out.ipums_code == 12, 'qcew_code'] = 'C1050'  # Albany, GA
out.loc[out.ipums_code == 16, 'qcew_code'] = 'C1058'  # Albany, NY
out.loc[out.ipums_code == 20, 'qcew_code'] = 'C1074'  # Albuquerque, NM
out.loc[out.ipums_code == 22, 'qcew_code'] = 'C1078'  # Alexandria, LA
out.loc[out.ipums_code == 24, 'qcew_code'] = 'C1090'  # Allentown, PA
out.loc[out.ipums_code == 28, 'qcew_code'] = 'C1102'  # Altoona, PA
out.loc[out.ipums_code == 32, 'qcew_code'] = 'C1110'  # Amarillo, TX
out.loc[out.ipums_code == 38, 'qcew_code'] = 'C1126'  # Anchorage, AK
out.loc[out.ipums_code == 40, 'qcew_code'] = 'C1130'  # Anderson, IN
out.loc[out.ipums_code == 44, 'qcew_code'] = 'C1146'  # Ann Arbor, MI
out.loc[out.ipums_code == 45, 'qcew_code'] = 'C1150'  # Anniston, AL
out.loc[out.ipums_code == 46, 'qcew_code'] = 'C1154'  # Appleton, WI
out.loc[out.ipums_code == 47, 'qcew_code'] = 'C1164'  # Arecibo, PR
out.loc[out.ipums_code == 48, 'qcew_code'] = 'C1170'  # Asheville, NC
out.loc[out.ipums_code == 50, 'qcew_code'] = 'C1202'  # Athens, GA
out.loc[out.ipums_code == 52, 'qcew_code'] = 'C1207'  # Atlanta, GA
out.loc[out.ipums_code == 56, 'qcew_code'] = 'C1210'  # Atlantic City, NJ
out.loc[out.ipums_code == 58, 'qcew_code'] = 'C1222'  # Auburn, AL
out.loc[out.ipums_code == 60, 'qcew_code'] = 'C1226'  # Augusta, GA
out.loc[out.ipums_code == 64, 'qcew_code'] = 'C1242'  # Austin, TX
out.loc[out.ipums_code == 68, 'qcew_code'] = 'C1254'  # Bakersfield, CA
out.loc[out.ipums_code == 72, 'qcew_code'] = 'C1258'  # Baltimore, MD
out.loc[out.ipums_code == 73, 'qcew_code'] = 'C1262'  # Bangor, ME
out.loc[out.ipums_code == 74, 'qcew_code'] = 'C1270'  # Barnstable, MA
out.loc[out.ipums_code == 76, 'qcew_code'] = 'C1294'  # Baton Rouge, LA
out.loc[out.ipums_code == 78, 'qcew_code'] = 'C1298'  # Battle Creek, MI
out.loc[out.ipums_code == 84, 'qcew_code'] = 'C1314'  # Beaumont, TX
out.loc[out.ipums_code == 86, 'qcew_code'] = 'C1338'  # Bellingham, WA
out.loc[out.ipums_code == 87, 'qcew_code'] = 'C3566'  # Niles-Benton Harbor, MI
out.loc[out.ipums_code == 88, 'qcew_code'] = 'C1374'  # Billings, MT
out.loc[out.ipums_code == 92, 'qcew_code'] = 'C2506'  # Biloxi-Gulfport, MS
out.loc[out.ipums_code == 96, 'qcew_code'] = 'C1378'  # Binghamton, NY
out.loc[out.ipums_code == 100, 'qcew_code'] = 'C1382'  # Birmingham, AL
out.loc[out.ipums_code == 102, 'qcew_code'] = 'C1402'  # Bloomington, IN
out.loc[out.ipums_code == 104, 'qcew_code'] = 'C1406'  # Bloomington-Normal, IL
out.loc[out.ipums_code == 108, 'qcew_code'] = 'C1426'  # Boise City, ID
out.loc[out.ipums_code == 112, 'qcew_code'] = 'C1446'  # Boston, MA
out.loc[out.ipums_code == 114, 'qcew_code'] = 'C3584'  # Bradenton, FL
out.loc[out.ipums_code == 115, 'qcew_code'] = 'C1474'  # Bremerton-Silverdale, WA
out.loc[out.ipums_code == 116, 'qcew_code'] = 'C1486'  # Bridgeport, CT
# out.loc[out.ipums_code == 120, 'qcew_code'] = 'XXXX'  # Brokton, MA, doesn't match
out.loc[out.ipums_code == 124, 'qcew_code'] = 'C1518'  # Brownsville-Harlingen-San Benito, TXX
out.loc[out.ipums_code == 126, 'qcew_code'] = 'C1778'  # Bryan-College Station, TXX
out.loc[out.ipums_code == 128, 'qcew_code'] = 'C1538'  # Buffalo-Niagara Falls, NYX
out.loc[out.ipums_code == 130, 'qcew_code'] = 'C1550'  # Burlington, NC
out.loc[out.ipums_code == 131, 'qcew_code'] = 'C1554'  # Burlington, VT
out.loc[out.ipums_code == 132, 'qcew_code'] = 'C1594'  # Canton, OHX
out.loc[out.ipums_code == 133, 'qcew_code'] = 'C4198'  # Caguas, PR
out.loc[out.ipums_code == 135, 'qcew_code'] = 'C1622'  # Casper, WY
out.loc[out.ipums_code == 136, 'qcew_code'] = 'C1630'  # Cedar Rapids, IAX
out.loc[out.ipums_code == 140, 'qcew_code'] = 'C1658'  # Champaign-Urbana-Rantoul, ILX
out.loc[out.ipums_code == 144, 'qcew_code'] = 'C1670'  # Charleston-N.Charleston,SCX
out.loc[out.ipums_code == 148, 'qcew_code'] = 'C1662'  # Charleston, WV
out.loc[out.ipums_code == 152, 'qcew_code'] = 'C1674'  # Charlotte-Gastonia-Rock Hill, NC-SCX
out.loc[out.ipums_code == 154, 'qcew_code'] = 'C1682'  # Charlottesville, VAX
out.loc[out.ipums_code == 156, 'qcew_code'] = 'C1686'  # Chattanooga, TN/GAX
out.loc[out.ipums_code == 158, 'qcew_code'] = 'C1694'  # Cheyenne, WY
out.loc[out.ipums_code == 160, 'qcew_code'] = 'C1698'  # Chicago, ILX
out.loc[out.ipums_code == 162, 'qcew_code'] = 'C1702'  # Chico, CAX
out.loc[out.ipums_code == 164, 'qcew_code'] = 'C1714'  # Cincinnati-Hamilton, OH/KY/INX
out.loc[out.ipums_code == 166, 'qcew_code'] = 'C1730'  # Clarksville- Hopkinsville, TN/KYX
out.loc[out.ipums_code == 168, 'qcew_code'] = 'C1746'  # Cleveland, OHX
out.loc[out.ipums_code == 172, 'qcew_code'] = 'C1782'  # Colorado Springs, COX
out.loc[out.ipums_code == 174, 'qcew_code'] = 'C1786'  # Columbia, MOX
out.loc[out.ipums_code == 176, 'qcew_code'] = 'C1790'  # Columbia, SCX
out.loc[out.ipums_code == 180, 'qcew_code'] = 'C1798'  # Columbus, GA/ALX
out.loc[out.ipums_code == 184, 'qcew_code'] = 'C1814'  # Columbus, OHX
out.loc[out.ipums_code == 188, 'qcew_code'] = 'C1858'  # Corpus Christi, TXX
out.loc[out.ipums_code == 190, 'qcew_code'] = 'C1906'  # Cumberland, MD/WV
out.loc[out.ipums_code == 192, 'qcew_code'] = 'C1910'  # Dallas-Fort Worth, TXX
# out.loc[out.ipums_code == 193, 'qcew_code'] = 'C'  # Danbury, CTX Didn't show up
# out.loc[out.ipums_code == 195, 'qcew_code'] = 'C'  # Danville, VAX Didn't show up
out.loc[out.ipums_code == 196, 'qcew_code'] = 'C1934'  # Davenport, IA-Rock Island -Moline, ILX
out.loc[out.ipums_code == 200, 'qcew_code'] = 'C1938'  # Dayton-Springfield, OHX
out.loc[out.ipums_code == 202, 'qcew_code'] = 'C1966'  # Daytona Beach, FLX
out.loc[out.ipums_code == 203, 'qcew_code'] = 'C1946'  # Decatur, ALX
out.loc[out.ipums_code == 204, 'qcew_code'] = 'C1950'  # Decatur, ILX
out.loc[out.ipums_code == 208, 'qcew_code'] = 'C1974'  # Denver-Boulder, COX
out.loc[out.ipums_code == 212, 'qcew_code'] = 'C1978'  # Des Moines, IAX
out.loc[out.ipums_code == 216, 'qcew_code'] = 'C1982'  # Detroit, MIX
out.loc[out.ipums_code == 218, 'qcew_code'] = 'C2002'  # Dothan, ALX
out.loc[out.ipums_code == 219, 'qcew_code'] = 'C2010'  # Dover, DEX
out.loc[out.ipums_code == 220, 'qcew_code'] = 'C2022'  # Dubuque, IA
out.loc[out.ipums_code == 224, 'qcew_code'] = 'C2026'  # Duluth-Superior, MN/WIX
# out.loc[out.ipums_code == 228, 'qcew_code'] = 'C'  # Dutchess Co., NYX Didn't show up
out.loc[out.ipums_code == 229, 'qcew_code'] = 'C2074'  # Eau Claire, WIX
out.loc[out.ipums_code == 231, 'qcew_code'] = 'C2134'  # El Paso, TXX
out.loc[out.ipums_code == 232, 'qcew_code'] = 'C2114'  # Elkhart-Goshen, INX
out.loc[out.ipums_code == 233, 'qcew_code'] = 'C2130'  # Elmira, NY
# out.loc[out.ipums_code == 234, 'qcew_code'] = 'C'  # Enid, OK Didn't show up
out.loc[out.ipums_code == 236, 'qcew_code'] = 'C2150'  # Erie, PAX
out.loc[out.ipums_code == 240, 'qcew_code'] = 'C2166'  # Eugene-Springfield, ORX
out.loc[out.ipums_code == 244, 'qcew_code'] = 'C2178'  # Evansville, IN/KYX
out.loc[out.ipums_code == 252, 'qcew_code'] = 'C2202'  # Fargo-Morehead, ND/MNX
out.loc[out.ipums_code == 256, 'qcew_code'] = 'C2218'  # Fayetteville, NCX
out.loc[out.ipums_code == 258, 'qcew_code'] = 'C2222'  # Fayetteville-Springdale, ARX
# out.loc[out.ipums_code == 260, 'qcew_code'] = 'C'  # Fitchburg-Leominster, MAX Didn't show up
out.loc[out.ipums_code == 262, 'qcew_code'] = 'C2238'  # Flagstaff, AZ-UTX
out.loc[out.ipums_code == 264, 'qcew_code'] = 'C2242'  # Flint, MIX
out.loc[out.ipums_code == 265, 'qcew_code'] = 'C2246'  # Florence, ALX
out.loc[out.ipums_code == 266, 'qcew_code'] = 'C2250'  # Florence, SC
out.loc[out.ipums_code == 267, 'qcew_code'] = 'C2266'  # Fort Collins-Loveland, COX
out.loc[out.ipums_code == 268, 'qcew_code'] = 'C3310'  # Fort Lauderdale-Hollywood-Pompano Beach, FLXi, matched to Miami, FL
out.loc[out.ipums_code == 270, 'qcew_code'] = 'C1598'  # Fort Myers-Cape Coral, FLX
# out.loc[out.ipums_code == 271, 'qcew_code'] = 'C'  # Fort Pierce, FLX Didn't show up
out.loc[out.ipums_code == 272, 'qcew_code'] = 'C2290'  # Fort Smith, AR/OKX
out.loc[out.ipums_code == 275, 'qcew_code'] = 'C2302'  # Fort Walton Beach, FLX
out.loc[out.ipums_code == 276, 'qcew_code'] = 'C2306'  # Fort Wayne, INX
out.loc[out.ipums_code == 284, 'qcew_code'] = 'C2342'  # Fresno, CAX
out.loc[out.ipums_code == 288, 'qcew_code'] = 'C2346'  # Gadsden, ALX
out.loc[out.ipums_code == 290, 'qcew_code'] = 'C2354'  # Gainesville, FLX
# out.loc[out.ipums_code == 292, 'qcew_code'] = 'C'  # Galveston-Texas City, TXX # Didn't show up
out.loc[out.ipums_code == 297, 'qcew_code'] = 'C2402'  # Glens Falls, NYX
out.loc[out.ipums_code == 298, 'qcew_code'] = 'C2414'  # Goldsboro, NCX
out.loc[out.ipums_code == 299, 'qcew_code'] = 'C2422'  # Grand Forks, ND
out.loc[out.ipums_code == 300, 'qcew_code'] = 'C2434'  # Grand Rapids, MIX
out.loc[out.ipums_code == 301, 'qcew_code'] = 'C2430'  # Grand Junction, COX
out.loc[out.ipums_code == 304, 'qcew_code'] = 'C2450'  # Great Falls, MT
out.loc[out.ipums_code == 306, 'qcew_code'] = 'C2454'  # Greeley, COX
out.loc[out.ipums_code == 308, 'qcew_code'] = 'C2458'  # Green Bay, WIX
out.loc[out.ipums_code == 312, 'qcew_code'] = 'C2466'  # Greensboro-Winston Salem-High Point, NCX
out.loc[out.ipums_code == 315, 'qcew_code'] = 'C2478'  # Greenville, NCX
out.loc[out.ipums_code == 316, 'qcew_code'] = 'C2486'  # Greenville-Spartanburg-Anderson SCX
out.loc[out.ipums_code == 318, 'qcew_code'] = 'C2518'  # Hagerstown, MDX
# out.loc[out.ipums_code == 320, 'qcew_code'] = 'C'  # Hamilton-Middleton, OHX # Does not exist
out.loc[out.ipums_code == 324, 'qcew_code'] = 'C2542'  # Harrisburg-Lebanon--Carlisle, PAX
out.loc[out.ipums_code == 328, 'qcew_code'] = 'C2554'  # Hartford-Bristol-Middleton- New Britain, CTX
out.loc[out.ipums_code == 329, 'qcew_code'] = 'C2586'  # Hickory-Morgantown, NCX
out.loc[out.ipums_code == 330, 'qcew_code'] = 'C2562'  # Hattiesburg, MSX
out.loc[out.ipums_code == 332, 'qcew_code'] = 'C2618'  # Honolulu, HIX
out.loc[out.ipums_code == 335, 'qcew_code'] = 'C2638'  # Houma-Thibodoux, LAX
out.loc[out.ipums_code == 336, 'qcew_code'] = 'C2642'  # Houston-Brazoria, TXX
out.loc[out.ipums_code == 340, 'qcew_code'] = 'C2658'  # Huntington-Ashland, WV/KY/OH
out.loc[out.ipums_code == 344, 'qcew_code'] = 'C2662'  # Huntsville, ALX
out.loc[out.ipums_code == 348, 'qcew_code'] = 'C2690'  # Indianapolis, INX
out.loc[out.ipums_code == 350, 'qcew_code'] = 'C2698'  # Iowa City, IAX
out.loc[out.ipums_code == 352, 'qcew_code'] = 'C2710'  # Jackson, MIX
out.loc[out.ipums_code == 356, 'qcew_code'] = 'C2714'  # Jackson, MSX
out.loc[out.ipums_code == 358, 'qcew_code'] = 'C2718'  # Jackson, TNX
out.loc[out.ipums_code == 359, 'qcew_code'] = 'C2726'  # Jacksonville, FLX
out.loc[out.ipums_code == 360, 'qcew_code'] = 'C2734'  # Jacksonville, NCX
# out.loc[out.ipums_code == 361, 'qcew_code'] = 'C'  # Jamestown-Dunkirk, NYX # does not exist
out.loc[out.ipums_code == 362, 'qcew_code'] = 'C2750'  # Janesville-Beloit, WIX
out.loc[out.ipums_code == 366, 'qcew_code'] = 'C2774'  # Johnson City-Kingsport--Bristol, TN/VAX
out.loc[out.ipums_code == 368, 'qcew_code'] = 'C2778'  # Johnstown, PAX
out.loc[out.ipums_code == 371, 'qcew_code'] = 'C2790'  # Joplin, MOX
out.loc[out.ipums_code == 372, 'qcew_code'] = 'C2802'  # Kalamazoo-Portage, MIX
out.loc[out.ipums_code == 374, 'qcew_code'] = 'C2810'  # Kankakee, ILX
out.loc[out.ipums_code == 376, 'qcew_code'] = 'C2814'  # Kansas City, MO-KSX
# out.loc[out.ipums_code == 380, 'qcew_code'] = 'C'  # Kenosha, WIX # Does not exist
out.loc[out.ipums_code == 381, 'qcew_code'] = 'C2866'  # Kileen-Temple, TXX
out.loc[out.ipums_code == 384, 'qcew_code'] = 'C2894'  # Knoxville, TNX
out.loc[out.ipums_code == 385, 'qcew_code'] = 'C2902'  # Kokomo, INX
out.loc[out.ipums_code == 387, 'qcew_code'] = 'C2910'  # LaCrosse, WIX
out.loc[out.ipums_code == 388, 'qcew_code'] = 'C2918'  # Lafayette, LAX
out.loc[out.ipums_code == 392, 'qcew_code'] = 'C2920'  # Lafayette-W. Lafayette, INX
out.loc[out.ipums_code == 396, 'qcew_code'] = 'C2934'  # Lake Charles, LAX
out.loc[out.ipums_code == 398, 'qcew_code'] = 'C2946'  # Lakeland-Winterhaven, FLX
out.loc[out.ipums_code == 400, 'qcew_code'] = 'C2954'  # Lancaster, PAX
out.loc[out.ipums_code == 404, 'qcew_code'] = 'C2962'  # Lansing-E. Lansing, MIX
out.loc[out.ipums_code == 408, 'qcew_code'] = 'C2970'  # Laredo, TXX
out.loc[out.ipums_code == 410, 'qcew_code'] = 'C2974'  # Las Cruces, NMX
out.loc[out.ipums_code == 412, 'qcew_code'] = 'C2982'  # Las Vegas, NVX
out.loc[out.ipums_code == 415, 'qcew_code'] = 'C2994'  # Lawrence, KS
out.loc[out.ipums_code == 420, 'qcew_code'] = 'C3002'  # Lawton, OK
out.loc[out.ipums_code == 424, 'qcew_code'] = 'C3034'  # Lewiston-Auburn, ME
out.loc[out.ipums_code == 428, 'qcew_code'] = 'C3046'  # Lexington-Fayette, KYX
out.loc[out.ipums_code == 432, 'qcew_code'] = 'C3062'  # Lima, OHX
out.loc[out.ipums_code == 436, 'qcew_code'] = 'C3070'  # Lincoln, NEX
out.loc[out.ipums_code == 440, 'qcew_code'] = 'C3078'  # Little Rock--North Little Rock, ARX
# out.loc[out.ipums_code == 441, 'qcew_code'] = 'C'  # Long Branch-Asbury Park,NJ # Does not exist
out.loc[out.ipums_code == 442, 'qcew_code'] = 'C3098'  # Longview-Marshall, TXX
out.loc[out.ipums_code == 444, 'qcew_code'] = 'C1746'  # Lorain-Elyria, OH# Note: grew into cleveland in 2013
out.loc[out.ipums_code == 448, 'qcew_code'] = 'C3110'  # Los Angeles-Long Beach, CAX (Note: there are 2 LA MSAs, one with Anaheim, one with Santa Ana
out.loc[out.ipums_code == 452, 'qcew_code'] = 'C3114'  # Louisville, KY/INX
out.loc[out.ipums_code == 460, 'qcew_code'] = 'C3118'  # Lubbock, TXX
out.loc[out.ipums_code == 464, 'qcew_code'] = 'C3134'  # Lynchburg, VAX
out.loc[out.ipums_code == 468, 'qcew_code'] = 'C3142'  # Macon-Warner Robins, GAX
out.loc[out.ipums_code == 472, 'qcew_code'] = 'C3154'  # Madison, WIX
out.loc[out.ipums_code == 476, 'qcew_code'] = 'C3170'  # Manchester, NHX
out.loc[out.ipums_code == 480, 'qcew_code'] = 'C3190'  # Mansfield, OHX
out.loc[out.ipums_code == 484, 'qcew_code'] = 'C3242'  # Mayaguez, PR
out.loc[out.ipums_code == 488, 'qcew_code'] = 'C3258'  # McAllen-Edinburg-Pharr-Mission, TXX
out.loc[out.ipums_code == 489, 'qcew_code'] = 'C3278'  # Medford, ORX
out.loc[out.ipums_code == 490, 'qcew_code'] = 'C3734'  # Melbourne-Titusville-Cocoa-Palm Bay, FLX
out.loc[out.ipums_code == 492, 'qcew_code'] = 'C3282'  # Memphis, TN/AR/MSX
out.loc[out.ipums_code == 494, 'qcew_code'] = 'C3290'  # Merced, CAX
out.loc[out.ipums_code == 500, 'qcew_code'] = 'C3310'  # Miami-Hialeah, FLX
out.loc[out.ipums_code == 504, 'qcew_code'] = 'C3326'  # Midland, TX
out.loc[out.ipums_code == 508, 'qcew_code'] = 'C3334'  # Milwaukee, WIX
out.loc[out.ipums_code == 512, 'qcew_code'] = 'C3346'  # Minneapolis-St. Paul, MNX
out.loc[out.ipums_code == 514, 'qcew_code'] = 'C3354'  # Missoula, MT
out.loc[out.ipums_code == 516, 'qcew_code'] = 'C3366'  # Mobile, ALX
out.loc[out.ipums_code == 517, 'qcew_code'] = 'C3370'  # Modesto, CAX
# out.loc[out.ipums_code == 519, 'qcew_code'] = 'C'  # Monmouth-Ocean, NJX # Does not exist
out.loc[out.ipums_code == 520, 'qcew_code'] = 'C3374'  # Monroe, LAX
out.loc[out.ipums_code == 524, 'qcew_code'] = 'C3386'  # Montgomery, ALX
out.loc[out.ipums_code == 528, 'qcew_code'] = 'C3462'  # Muncie, INX
out.loc[out.ipums_code == 532, 'qcew_code'] = 'C3474'  # Muskegon-Norton Shores-Muskegon Heights, MI
out.loc[out.ipums_code == 533, 'qcew_code'] = 'C3482'  # Myrtle Beach, SCX
out.loc[out.ipums_code == 534, 'qcew_code'] = 'C3494'  # Naples, FLX
out.loc[out.ipums_code == 535, 'qcew_code'] = 'C3170'  # Nashua, NHX
out.loc[out.ipums_code == 536, 'qcew_code'] = 'C3498'  # Nashville, TNX
# out.loc[out.ipums_code == 540, 'qcew_code'] = 'C'  # New Bedford, MAX # does not exist
# out.loc[out.ipums_code == 546, 'qcew_code'] = 'C'  # New Brunswick-Perth Amboy-Sayreville, NJ # Does not exist
out.loc[out.ipums_code == 548, 'qcew_code'] = 'C3530'  # New Haven-Meriden, CTX
out.loc[out.ipums_code == 552, 'qcew_code'] = 'C3598'  # New London-Norwich, CT/RI
out.loc[out.ipums_code == 556, 'qcew_code'] = 'C3538'  # New Orleans, LAX
out.loc[out.ipums_code == 560, 'qcew_code'] = 'C3562'  # New York-Northeastern NJX
# out.loc[out.ipums_code == 564, 'qcew_code'] = 'C'  # Newark, OH # does not exist
out.loc[out.ipums_code == 566, 'qcew_code'] = 'C3910'  # Newburgh-Middletown, NYX
out.loc[out.ipums_code == 572, 'qcew_code'] = 'C4726'  # Norfolk-VA Beach--Newport News, VAX
out.loc[out.ipums_code == 576, 'qcew_code'] = 'C1486'  # Norwalk, CT
out.loc[out.ipums_code == 579, 'qcew_code'] = 'C3610'  # Ocala, FLX
out.loc[out.ipums_code == 580, 'qcew_code'] = 'C3622'  # Odessa, TXX
out.loc[out.ipums_code == 588, 'qcew_code'] = 'C3642'  # Oklahoma City, OKX
out.loc[out.ipums_code == 591, 'qcew_code'] = 'C3650'  # Olympia, WAX
out.loc[out.ipums_code == 592, 'qcew_code'] = 'C3654'  # Omaha, NE/IAX
# out.loc[out.ipums_code == 595, 'qcew_code'] = 'C'  # Orange, NY
out.loc[out.ipums_code == 596, 'qcew_code'] = 'C3674'  # Orlando, FLX
out.loc[out.ipums_code == 599, 'qcew_code'] = 'C3698'  # Owensboro, KY
out.loc[out.ipums_code == 601, 'qcew_code'] = 'C3746'  # Panama City, FLX
out.loc[out.ipums_code == 602, 'qcew_code'] = 'C3762'  # Parkersburg-Marietta,WV/OH
out.loc[out.ipums_code == 603, 'qcew_code'] = 'C3770'  # Pascagoula-Moss Point, MS
out.loc[out.ipums_code == 608, 'qcew_code'] = 'C3786'  # Pensacola, FLX
out.loc[out.ipums_code == 612, 'qcew_code'] = 'C3790'  # Peoria, ILX
out.loc[out.ipums_code == 616, 'qcew_code'] = 'C3798'  # Philadelphia, PA/NJX
out.loc[out.ipums_code == 620, 'qcew_code'] = 'C3806'  # Phoenix, AZX
out.loc[out.ipums_code == 628, 'qcew_code'] = 'C3830'  # Pittsburgh, PAX
out.loc[out.ipums_code == 632, 'qcew_code'] = 'C3834'  # Pittsfield, MA
out.loc[out.ipums_code == 636, 'qcew_code'] = 'C3866'  # Ponce, PR
out.loc[out.ipums_code == 640, 'qcew_code'] = 'C3886'  # Portland, MEX
out.loc[out.ipums_code == 644, 'qcew_code'] = 'C3890'  # Portland, OR-WAX
# out.loc[out.ipums_code == 645, 'qcew_code'] = 'C'  # Portsmouth-Dover--Rochester, NH/ME # Doesn't exist
out.loc[out.ipums_code == 646, 'qcew_code'] = 'C3910'  # Poughkeepsie, NY
out.loc[out.ipums_code == 648, 'qcew_code'] = 'C3930'  # Providence-Fall River-Pawtucket, MA/RIX
out.loc[out.ipums_code == 652, 'qcew_code'] = 'C3934'  # Provo-Orem, UTX
out.loc[out.ipums_code == 656, 'qcew_code'] = 'C3938'  # Pueblo, COX
out.loc[out.ipums_code == 658, 'qcew_code'] = 'C3946'  # Punta Gorda, FLX
out.loc[out.ipums_code == 660, 'qcew_code'] = 'C3954'  # Racine, WIX
out.loc[out.ipums_code == 664, 'qcew_code'] = 'C3958'  # Raleigh-Durham, NCX
out.loc[out.ipums_code == 666, 'qcew_code'] = 'C3966'  # Rapid City, SD
out.loc[out.ipums_code == 668, 'qcew_code'] = 'C3974'  # Reading, PAX
out.loc[out.ipums_code == 669, 'qcew_code'] = 'C3982'  # Redding, CAX
out.loc[out.ipums_code == 672, 'qcew_code'] = 'C3990'  # Reno, NVX
out.loc[out.ipums_code == 674, 'qcew_code'] = 'C2842'  # Richland-Kennewick-Pasco, WAX
out.loc[out.ipums_code == 676, 'qcew_code'] = 'C4006'  # Richmond-Petersburg, VAX
out.loc[out.ipums_code == 678, 'qcew_code'] = 'C4014'  # Riverside-San Bernardino,CAX
out.loc[out.ipums_code == 680, 'qcew_code'] = 'C4022'  # Roanoke, VAX
out.loc[out.ipums_code == 682, 'qcew_code'] = 'C4034'  # Rochester, MNX
out.loc[out.ipums_code == 684, 'qcew_code'] = 'Ci4038'  # Rochester, NYX
out.loc[out.ipums_code == 688, 'qcew_code'] = 'C4042'  # Rockford, ILX
out.loc[out.ipums_code == 689, 'qcew_code'] = 'C4058'  # Rocky Mount, NCX
out.loc[out.ipums_code == 692, 'qcew_code'] = 'C4090'  # Sacramento, CAX
out.loc[out.ipums_code == 696, 'qcew_code'] = 'C4098'  # Saginaw-Bay City-Midland, MIX
out.loc[out.ipums_code == 698, 'qcew_code'] = 'C4106'  # St. Cloud, MNX
out.loc[out.ipums_code == 700, 'qcew_code'] = 'C4114'  # St. Joseph, MOX
out.loc[out.ipums_code == 704, 'qcew_code'] = 'C4118'  # St. Louis, MO-ILX
out.loc[out.ipums_code == 708, 'qcew_code'] = 'C4142'  # Salem, ORX
out.loc[out.ipums_code == 712, 'qcew_code'] = 'C4150'  # Salinas-Sea Side-Monterey, CAX
out.loc[out.ipums_code == 714, 'qcew_code'] = 'C1674'  # Salisbury-Concord, NC # Concord now merged into Charlotte
out.loc[out.ipums_code == 716, 'qcew_code'] = 'C4162'  # Salt Lake City-Ogden, UTX
out.loc[out.ipums_code == 720, 'qcew_code'] = 'C4166'  # San Angelo, TX
out.loc[out.ipums_code == 724, 'qcew_code'] = 'C4170'  # San Antonio, TXX
out.loc[out.ipums_code == 732, 'qcew_code'] = 'C4174'  # San Diego, CAX
out.loc[out.ipums_code == 736, 'qcew_code'] = 'C4186'  # San Francisco-Oakland-Vallejo, CAX
out.loc[out.ipums_code == 740, 'qcew_code'] = 'C4194'  # San Jose, CAX
out.loc[out.ipums_code == 744, 'qcew_code'] = 'C4198'  # San Juan-Bayamon, PR
out.loc[out.ipums_code == 746, 'qcew_code'] = 'C4202'  # San Luis Obispo-Atascad-P Robles, CAX
out.loc[out.ipums_code == 747, 'qcew_code'] = 'C4206'  # Santa Barbara-Santa Maria-Lompoc, CAX
out.loc[out.ipums_code == 748, 'qcew_code'] = 'C4210'  # Santa Cruz, CAX
out.loc[out.ipums_code == 749, 'qcew_code'] = 'C4214'  # Santa Fe, NMX
out.loc[out.ipums_code == 750, 'qcew_code'] = 'C4222'  # Santa Rosa-Petaluma, CAX
out.loc[out.ipums_code == 751, 'qcew_code'] = 'C4226'  # Sarasota, FLX
out.loc[out.ipums_code == 752, 'qcew_code'] = 'C4234'  # Savannah, GAX
out.loc[out.ipums_code == 756, 'qcew_code'] = 'C4254'  # Scranton-Wilkes-Barre, PAX
out.loc[out.ipums_code == 760, 'qcew_code'] = 'C4266'  # Seattle-Everett, WAX
# out.loc[out.ipums_code == 761, 'qcew_code'] = 'C'  # Sharon, PAX # Does not exist
out.loc[out.ipums_code == 762, 'qcew_code'] = 'C4310'  # Sheboygan, WIX
out.loc[out.ipums_code == 764, 'qcew_code'] = 'C4330'  # Sherman-Davidson, TX
out.loc[out.ipums_code == 768, 'qcew_code'] = 'C4334'  # Shreveport, LAX
out.loc[out.ipums_code == 772, 'qcew_code'] = 'C4358'  # Sioux City, IA/NEX
out.loc[out.ipums_code == 776, 'qcew_code'] = 'C4362'  # Sioux Falls, SDX
out.loc[out.ipums_code == 780, 'qcew_code'] = 'C4378'  # South Bend-Mishawaka, INX
out.loc[out.ipums_code == 784, 'qcew_code'] = 'C4406'  # Spokane, WAX
out.loc[out.ipums_code == 788, 'qcew_code'] = 'C4410'  # Springfield, ILX
out.loc[out.ipums_code == 792, 'qcew_code'] = 'C4418'  # Springfield, MOX
out.loc[out.ipums_code == 800, 'qcew_code'] = 'C4414'  # Springfield-Holyoke-Chicopee, MAX
out.loc[out.ipums_code == 804, 'qcew_code'] = 'C1486'  # Stamford, CTX
out.loc[out.ipums_code == 805, 'qcew_code'] = 'C4430'  # State College, PAX
out.loc[out.ipums_code == 808, 'qcew_code'] = 'C4826'  # Steubenville-Weirton,OH/WV
out.loc[out.ipums_code == 812, 'qcew_code'] = 'C4470'  # Stockton, CAX
out.loc[out.ipums_code == 814, 'qcew_code'] = 'C4494'  # Sumter, SCX
out.loc[out.ipums_code == 816, 'qcew_code'] = 'C4506'  # Syracuse, NYX
out.loc[out.ipums_code == 820, 'qcew_code'] = 'C4266'  # Tacoma, WAX
out.loc[out.ipums_code == 824, 'qcew_code'] = 'C4522'  # Tallahassee, FLX
out.loc[out.ipums_code == 828, 'qcew_code'] = 'C4530'  # Tampa-St. Petersburg-Clearwater, FLX
out.loc[out.ipums_code == 832, 'qcew_code'] = 'C4546'  # Terre Haute, INX
out.loc[out.ipums_code == 836, 'qcew_code'] = 'C4550'  # Texarkana, TX/AR
out.loc[out.ipums_code == 840, 'qcew_code'] = 'C4578'  # Toledo, OH/MIX
out.loc[out.ipums_code == 844, 'qcew_code'] = 'C4582'  # Topeka, KSX
out.loc[out.ipums_code == 848, 'qcew_code'] = 'C4594'  # Trenton, NJX
out.loc[out.ipums_code == 852, 'qcew_code'] = 'C4606'  # Tucson, AZX
out.loc[out.ipums_code == 856, 'qcew_code'] = 'C4614'  # Tulsa, OKX
out.loc[out.ipums_code == 860, 'qcew_code'] = 'C4622'  # Tuscaloosa, ALX
out.loc[out.ipums_code == 864, 'qcew_code'] = 'C4634'  # Tyler, TXX
out.loc[out.ipums_code == 868, 'qcew_code'] = 'C4654'  # Utica-Rome, NYX
out.loc[out.ipums_code == 873, 'qcew_code'] = 'C3710'  # Ventura-Oxnard-Simi Valley, CAX
out.loc[out.ipums_code == 875, 'qcew_code'] = 'C4702'  # Victoria, TX
out.loc[out.ipums_code == 876, 'qcew_code'] = 'C4722'  # Vineland-Milville-Bridgetown, NJX
out.loc[out.ipums_code == 878, 'qcew_code'] = 'C4730'  # Visalia-Tulare-Porterville, CAX
out.loc[out.ipums_code == 880, 'qcew_code'] = 'C4738'  # Waco, TXX
out.loc[out.ipums_code == 884, 'qcew_code'] = 'C4790'  # Washington, DC/MD/VAX
# out.loc[out.ipums_code == 888, 'qcew_code'] = 'C'  # Waterbury, CTX # Not mentioned
out.loc[out.ipums_code == 892, 'qcew_code'] = 'C4794'  # Waterloo-Cedar Falls, IAX
out.loc[out.ipums_code == 894, 'qcew_code'] = 'C4814'  # Wausau, WIX
out.loc[out.ipums_code == 896, 'qcew_code'] = 'C3310'  # West Palm Beach-Boca Raton-Delray Beach,FLX (Merged into Miami)
out.loc[out.ipums_code == 900, 'qcew_code'] = 'C4854'  # Wheeling, WV/OH
out.loc[out.ipums_code == 904, 'qcew_code'] = 'C4862'  # Wichita, KSX
out.loc[out.ipums_code == 908, 'qcew_code'] = 'C4866'  # Wichita Falls, TXX
out.loc[out.ipums_code == 914, 'qcew_code'] = 'C4870'  # Williamsport, PAX
out.loc[out.ipums_code == 916, 'qcew_code'] = 'C3798'  # Wilmington, DE/NJ/MDX
out.loc[out.ipums_code == 920, 'qcew_code'] = 'C4890'  # Wilmington, NCX
out.loc[out.ipums_code == 924, 'qcew_code'] = 'C4934'  # Worcester, MAX
out.loc[out.ipums_code == 926, 'qcew_code'] = 'C4942'  # Yakima, WAX
# out.loc[out.ipums_code == 927, 'qcew_code'] = 'C'  # Yolo, CAX # Does not exist
out.loc[out.ipums_code == 928, 'qcew_code'] = 'C4962'  # York, PAX
out.loc[out.ipums_code == 932, 'qcew_code'] = 'C4966'  # Youngstown-Warren, OH-PAX
out.loc[out.ipums_code == 934, 'qcew_code'] = 'C4970'  # Yuba City, CAX
out.loc[out.ipums_code == 936, 'qcew_code'] = 'C4974'  # Yuma, AZX

out[out.ipums_code.isnull()]
out[out.qcew_code.isnull()]
out = out[~out.qcew_code.isnull() & ~out.ipums_code.isnull()]  # Restrict to coded locations
out.to_csv('msa_crosswalk.csv', index=False)
