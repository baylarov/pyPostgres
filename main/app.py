import main.ConnectionPool as cp
from main.DbQuery import Query
import main.Sparking as sp

username='postgres'
password='name'
hostname='localhost'
port='5432'
database='postgres'

conn=cp.DbConnection.init(user=username,password=password,host=hostname,port=port,database=database)
title = ['datetime', 'clickid', 'userid', 'usersessionid', 'ishit', 'teamid', 'teamlevel']

# Bulk insert to PostgreSQL database
# data=pd.read_csv('dataSource\game-clicks.csv').head(100)
# Query().bulk_insert(data)

#initialise spark and sql query
spark=sp.Sparkling(app_name="justSparkling")
table_sql=Query().retrieve_data(20)

#getting line(RDD) and sql based(Spark dataframe) tables
table_data=spark.convert_to_spDF(table_sql)
line_data=spark.sc.textFile("C:\\Users\\mkay\\PycharmProjects\\DataLake\\dataSource\\u.data")