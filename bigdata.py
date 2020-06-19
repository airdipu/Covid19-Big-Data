from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os


#Filter data to find only country with Australia
def filterData(df):
      return df.filter(col("Country/Region") == "Australia")

#Relative path
dirname = os.path.dirname(__file__)
filepath = os.path.join(dirname, 'time_series_19-covid-Confirmed_archived_0325.csv')

sc = SparkContext("local", "covid_big_data")
sqlContext = SQLContext(sc)
df = sqlContext.read.option("inferSchema", "true").option("header", "true").csv(filepath)

#df.show(5)
#df.printSchema()

df = filterData(df)

counts = df.count()
print ("Number of rows in df -> %i" % (counts))

#df.show()