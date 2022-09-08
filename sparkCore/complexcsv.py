from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data="D:\\Study\\AVD class\\spark\\datasets\\10000Records.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").option("sep",",").load(data)
#inferSchema ...when ur readding data auto convert data to appropriate datatypes means value 4444 convert to int 4343.4 convert to double.
import re
#num=int(df.count())
cols =[re.sub('[^a-zA-Z0-9]',"",c.lower()) for c in df.columns]
#re ... replace ...except all Small letters , capital letters and number except those any other symbols if u have replace/remove
#cols =[re.sub(' ',"",c) for c in df.columns]#It will replace spaces in columns with no space
ndf =df.toDF(*cols)
#toDF used to rename all  columns and convert rdd to dataframe.
#print(num)
ndf.show(21,truncate=True)
#By default show method showing top 20 rows and if any field more than 20 chars its truncated and shows ...
ndf.printSchema()
#dataframe column names and its datatype display properly

#data processing programming friendly (dataframe api)
res=ndf.groupBy(col("gender")).agg(count(col("*")).alias("cnt"))
res.show()
