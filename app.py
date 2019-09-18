import ConnectionPool as cp
import pandas as pd
from DbQuery import Query

username='postgres'
password='name'
hostname='localhost'
port='5432'
database='postgres'

conn=cp.DbConnection.init(user=username,password=password,host=hostname,port=port,database=database)

data=pd.read_csv('game-clicks.csv').head(17)

Query().bulk_insert(data)