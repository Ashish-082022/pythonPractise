import os
import sys
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[*]").appName("test").enableHiveSupport().getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
data=sys.argv[1]
tab=sys.argv[2]
df=spark.read.format('csv').option('header','true').option('inferSchema','true').load(data)
df.show()
df.write.format("hive").saveAsTable(tab)
#spark-submit --master local --deploy-mode client


