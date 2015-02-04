from sqlalchemy import create_engine
from sqlalchemy import *
import pandas
import requests
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

a = pandas.read_csv('bp_tx.csv')
for index, row in a.iterrows():
    for id, url, body, timestamp conn.execute('SELECT id, url, body, timestamp FROM backpage_incoming WHERE url=\'%s\';' % row['url']):
        try:
            data = extract(body).strip()
        except requests.ConnectionError:
            continue
        out_list.append( {
            'id':id,
            'url':url,
            'body':data,
            'phone':row['phone_number'],
            'job_type':row['job_type'],
            'timestamp':timestamp
            } )

out = pandas.DataFrame(out_list)
out.to_csv('text_ads.csv', index=False)
