from sqlalchemy import create_engine
connection_string = open('connection_string.txt','r').read().split()[0]
print(connection_string)
engine = create_engine(connection_string, echo=True)
conn=engine.connect()
res= conn.execute('SHOW TABLES;')
print(res.fetchall())

