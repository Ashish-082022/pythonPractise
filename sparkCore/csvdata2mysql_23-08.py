from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data="D:\\Study\\AVD class\\spark\\datasets\\10000Records.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").option("sep",",").load(data)
import re
cols =[re.sub('[^a-zA-Z0-9]',"",c.lower()) for c in df.columns]
ndf=df.toDF(*cols)
ndf.show()
host="jdbc:mysql://sparkpoc.c8v9olldtsvz.ap-south-1.rds.amazonaws.com:3306/mysqldb"
ndf.write.mode("overwrite").format("jdbc").option("url",host).option("user","myuser").option("password","mypassword")\
    .option("dbtable","10KRecordsClr").option("driver","com.mysql.jdbc.Driver").save()
print("Data Successfully loaded into RDS.")