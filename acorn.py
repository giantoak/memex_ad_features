from sqlalchemy import create_engine
from sqlalchemy import *
import requests
connection_string = open('connection_string.txt','r').read().split()[0]
tika_url = 'http://localhost:9998/tika'
print(connection_string)
engine = create_engine(connection_string, echo=True)
conn=engine.connect()
res= conn.execute('SHOW TABLES;')
print(res.fetchall())
conn.execute('DESCRIBE backpage_incoming;').fetchall()
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

for id, url, body, timestamp in conn.execute('SELECT id, url, body, timestamp FROM backpage_incoming LIMIT 10;'):
    #print(id, url, body, timestamp)
    data = extract(body).strip()
    print(id, data[0:100])
