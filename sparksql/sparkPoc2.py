from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc=spark.sparkContext
#convert these nums to rdd.. use parallalise(data)
#sparkContext used to create rdds
data=[1,2,3,4,5]
#2 ways to create rdds 1)sc.parallalize()...java/scala/python variables/objects convert to rdd
#2)external data(hdfs,lacal,s3..a physical data) convert to rdd, sc.textFile()
drdd=sc.parallelize(data)
#What is  RDD? collection of java objects called rdd, that
# rdd must follow 3 properties immutable,fault tolarence,laziness.
