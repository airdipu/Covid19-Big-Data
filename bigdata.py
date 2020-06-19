from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import operator


def filterData(df):
      return df.filter(col("Country/Region") == "Australia")


 
filepath = "/Users/FAY/Bigdata/Covid19-Big-Data" 
sc = SparkContext("local", "covid_big_data")
sqlContext = SQLContext(sc)
df = sqlContext.read.option("inferSchema", "true").option("header", "true").csv(filepath)

#df.show(5)
#df.printSchema()

df = filterData(df)

counts = df.count()
print ("Number of rows in df -> %i" % (counts))

#df.show()