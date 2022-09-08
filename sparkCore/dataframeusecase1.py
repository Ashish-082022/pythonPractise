from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
#data="D:\\Study\\AVD class\\spark\\datasets\\donations.csv"
#df=spark.read.format("csv").option("header","true").load(data)
#if you mention header true, first line of data consider as columns
#if u have any mal records like first line second line wrong clean that data using rdd or udf.
#skip first line second line onwords original available.
data="D:\\Study\\AVD class\\spark\\datasets\\donations1.csv"
rdd=spark.sparkContext.textFile(data)
skip=rdd.first()
odata=rdd.filter(lambda x:x!=skip)
df=spark.read.csv(odata,header=True,inferSchema=True)
df.printSchema()
#printing columns and its datatype in nice tree formate.
df.show(5)
#display top 20 rows by default.If u want to display top 5 lines us show(5).
