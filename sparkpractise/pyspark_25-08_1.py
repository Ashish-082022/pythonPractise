import os
import sys
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
win=Window.partitionBy('state').orderBy(col('zip').desc())
data="D:\\Study\\AVD class\\spark\\datasets\\us-500.csv"
#data=sys.argv[1]
#tab=sys.argv[2]

df=spark.read.format('csv').option('header','true')\
    .option('inferSchema','true').load(data)
ndf=df.withColumn('ntile_column',ntile(5).over(win))
ndf.show(10)
#ndf1=df.withColumn('ntile_col',ntile(10).over(win)).where(col('ntile_col')==1)
#ndf1.show(5)
#df.createOrReplaceTempView('tab')
#ndf2=spark.sql("select * from tab where zip between 85360 and 85381")
#ndf2.show()
#df.write.format("hive").saveAsTable(tab)