from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext
'''
Spark initilization and DataFrame converter
'''
class Sparkling:
    sqlContext = None
    title = ['datetime', 'clickid', 'userid', 'usersessionid', 'ishit', 'teamid', 'teamlevel']
    sc = None

    def __init__(self,app_name,master="local"):
        self.app_name=app_name
        self.master=master
        conf = SparkConf().setMaster(self.master).setAppName(self.app_name)
        self.sc = SparkContext(conf=conf)
        self.sqlContext = SQLContext(self.sc)

    def convert_to_spDF(self,df,columns=title):
        spDF=self.sqlContext.createDataFrame(df,schema=columns)
        return spDF