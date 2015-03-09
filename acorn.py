from sqlalchemy import create_engine
from sqlalchemy import *
import pandas
import requests
import phonenumbers
connection_string = open('connection_string.txt','r').read().split()[0]
tika_url = 'http://localhost:9998/tika'
print(connection_string)
engine = create_engine(connection_string, echo=True)
conn=engine.connect()
res= conn.execute('SHOW TABLES;')
print(res.fetchall())
print(conn.execute('DESCRIBE backpage_incoming;').fetchall())
#bp = Table('backpage_incoming', conn,
        #Column('id', Integer, primary_key=True),
        #Column('url', String(2048)),
        #Column('body', Text),
        #Column('timestamp', String),
        #)
def extract(body):
    r = requests.put(
            tika_url,
            data=body
            )
    r.raise_for_status()
    return r.content

out_list = []

######################################################################
# Pull out text in Tika and write to csv
#for id, url, body, timestamp in conn.execute('SELECT id, url, body, timestamp FROM backpage_incoming LIMIT 100000;'):
    ##print(id, url, body, timestamp)
    #try:
        #data = extract(body).strip()
    #except requests.ConnectionError:
        #continue
    ##print(id, data[0:100])
    #out_list.append( {
        #'id':id,
        #'url':url,
        #'body':data,
        #'timestamp':timestamp
        #} )
#out = pandas.DataFrame(out_list)
#out.to_csv('text_ads.csv', index=False)
######################################################################

#######################################################################
## merge bp_tx extracted phones with plain text bodies
#a = pandas.read_csv('bp_tx.csv', sep='\t')
#for index, row in a.iterrows():
    #for id, url, body, timestamp in conn.execute('SELECT id, url, body, timestamp FROM backpage_incoming WHERE url=\'%s\';' % row['url']):
        #try:
            #data = extract(body).strip()
        #except requests.ConnectionError:
            #continue
        #out_list.append( {
            #'id':id,
            #'url':url,
            #'body':data,
            #'phone':row['phone_number'],
            #'job_type':row['job_type'],
            #'timestamp':timestamp
            #} )
#out = pandas.DataFrame(out_list)
#out.to_csv('text_ads.csv', index=False)
#######################################################################

arizona = """phoenix
tempe
flagstaff
prescott
mohave
showlow
sierravista
yuma
california
tucson"""

states ="""alabama
alaska
arizona
arkansas
california
colorado
connecticut
delaware
districtofcolumbia
florida
georgia
hawaii
idaho
illinois
indiana
iowa
kansas
kentucky
louisiana
maine
montana
nebraska
nevada
newhampshire
newjersey
newmexico
newyork
northcarolina
northdakota
ohio
oklahoma
oregon
maryland
massachusetts
michigan
minnesota
mississippi
missouri
pennsylvania
rhodeisland
southcarolina
southdakota
tennessee
texas
utah
vermont
virginia
washington
westvirginia
wisconsin
wyoming"""
locs = arizona.split('\n')
locs = states.split('\n')
out_list = []
for s in locs:
    qurl = 'http://{}.backpage.com%'.format(s)
    print s, qurl
    r = conn.execute(text('SELECT id, url, body, timestamp FROM backpage_incoming WHERE url LIKE :qstr'), qstr=qurl)
    for i, (id, url, body, timestamp) in enumerate(r):
        try:
            data = extract(body).strip()
        except requests.ConnectionError:
            continue
        matches = phonenumbers.PhoneNumberMatcher(body, 'US')
        try:
            matches_list = [x for x in matches]

            for match in matches_list:
                # Format the phone numbers in E164 international format.
                num = phonenumbers.format_number(match.number, phonenumbers.PhoneNumberFormat.E164)
                if isinstance(num, basestring):
                    out_list.append({
                        'id':id,
                        'url':url,
                        'body':data,
                        'phone':num,
                        'job_type':url.split('/')[3].split('.')[0],
                        'location':url.split('/')[2].split('.')[0],
                        'timestamp':timestamp
                        })
        except UnicodeDecodeError:
            # It's a hackathon, right?
            print 'Error decoding', url
out = pandas.DataFrame(out_list)
out.to_csv('text_ads_with_phones.csv', index=False)
