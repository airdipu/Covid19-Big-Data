from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
from datetime import datetime

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

#drop column  which are not between analyze time frame: 2/1/20 - 3/21/20
names = df.schema.names
start_date = datetime.strptime("1/31/20", '%m/%d/%y')
end_date = datetime.strptime("3/22/20", '%m/%d/%y')
#len(names)
for column in names:
    if column != "Province/State" and column != "Country/Region" and column != "Lat" and column != "Long":
        col_name = datetime.strptime(column, '%m/%d/%y')
        if not(col_name > start_date and col_name < end_date):
            df = df.drop(column)


df.show()