from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
host="jdbc:mysql://sparkpoc.c8v9olldtsvz.ap-south-1.rds.amazonaws.com:3306/mysqldb?useSSL=false"
data="D:\\Study\\AVD class\\spark\\datasets\\asl.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
#df.show()

#process data
#df.show()
#df.printSchema()
import re
cols =[re.sub('[^a-zA-Z0-9]',"_",c.lower()) for c in df.columns]
ndf=df.toDF(*cols)

ndf.write.mode("overwrite").format("jdbc").option("url",host).option("user","myuser").option("password","mypassword")\
    .option("dbtable","asl").option("driver","com.mysql.jdbc.Driver").save()
ndf.show(5)
#: java.lang.ClassNotFoundException: com.mysql.jdbc.Driver
#mysql dependency problem so pls add mysql jar and place in spark/jars folder.